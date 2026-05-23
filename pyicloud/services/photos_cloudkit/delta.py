"""Delta sync engine using CloudKit /changes/zone for incremental photo sync."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterator

from pyicloud.common.cloudkit import (
    CKRecord,
    CKTombstoneRecord,
    CKZoneChangesZoneReq,
    CKZoneID,
)

from .mappers import record_field_value, record_name
from .models import PhotosServiceException

_LOGGER = logging.getLogger(__name__)


@dataclass
class ChangeEvent:
    """A single classified change from the delta stream."""

    kind: str  # "created" | "soft_deleted" | "hard_deleted"
    master_record_name: str
    master_record: CKRecord | None = None
    asset_record: CKRecord | None = None


def classify_record(record: CKRecord) -> str:
    """Classify a CKRecord as created or soft-deleted based on its fields."""
    is_deleted = record_field_value(record, "isDeleted")
    if is_deleted and int(is_deleted) == 1:
        return "soft_deleted"
    return "created"


class DeltaRecordBuffer:
    """Buffer CPLMaster/CPLAsset records across /changes/zone pages for pairing.

    The /changes/zone API returns records in change-log order, not paired order.
    A CPLMaster may arrive on page 1 and its CPLAsset on page 3. This buffer
    holds unpaired records and emits ChangeEvents when pairs are found.
    """

    def __init__(self) -> None:
        """Initialize empty buffers for cross-page master/asset pairing."""
        # Unpaired CPLMaster records, keyed by their recordName
        self._pending_masters: dict[str, CKRecord] = {}
        # Unpaired CPLAsset records, keyed by the masterRef recordName they point to
        self._pending_assets: dict[str, CKRecord] = {}

    def process_records(self, records: list[Any]) -> list[ChangeEvent]:
        """Process a page of records from /changes/zone and emit paired events.

        Returns a list of ChangeEvents for any records that could be paired
        (or for tombstone deletions which need no pairing).
        """
        events: list[ChangeEvent] = []

        for record in records:
            if isinstance(record, CKTombstoneRecord):
                events.append(
                    ChangeEvent(
                        kind="hard_deleted",
                        master_record_name=record.recordName,
                    )
                )
                continue

            if not isinstance(record, CKRecord):
                # Skip CKErrorItem or unexpected types
                continue

            if record.recordType == "CPLMaster":
                master_name = record.recordName
                # Check if we already have a matching asset buffered
                if master_name in self._pending_assets:
                    asset_rec = self._pending_assets.pop(master_name)
                    kind = self._reconcile_kind(record, asset_rec)
                    events.append(
                        ChangeEvent(
                            kind=kind,
                            master_record_name=master_name,
                            master_record=record,
                            asset_record=asset_rec,
                        )
                    )
                else:
                    self._pending_masters[master_name] = record

            elif record.recordType == "CPLAsset":
                # Extract the masterRef to find the paired CPLMaster
                ref = record.fields.get_value("masterRef")
                master_ref_name = getattr(ref, "recordName", None)
                if not master_ref_name:
                    # Fallback: some edge cases
                    _LOGGER.debug(
                        "CPLAsset %s has no masterRef, skipping",
                        record.recordName,
                    )
                    continue

                if master_ref_name in self._pending_masters:
                    master_rec = self._pending_masters.pop(master_ref_name)
                    kind = self._reconcile_kind(master_rec, record)
                    events.append(
                        ChangeEvent(
                            kind=kind,
                            master_record_name=master_ref_name,
                            master_record=master_rec,
                            asset_record=record,
                        )
                    )
                else:
                    self._pending_assets[master_ref_name] = record
            # Skip other record types (CPLAlbum, CPLContainerRelation, etc.)

        return events

    def flush(self) -> list[ChangeEvent]:
        """Emit any remaining unpaired records after all pages are consumed.

        Unpaired masters are emitted as created events (asset record may follow
        in a future sync). Unpaired assets without a master are logged as warnings.
        """
        events: list[ChangeEvent] = []

        for master_name, master_rec in self._pending_masters.items():
            kind = classify_record(master_rec)
            events.append(
                ChangeEvent(
                    kind=kind,
                    master_record_name=master_name,
                    master_record=master_rec,
                    asset_record=None,
                )
            )
        self._pending_masters.clear()

        for master_ref_name, asset_rec in self._pending_assets.items():
            _LOGGER.debug(
                "Orphaned CPLAsset pointing to master %s (asset %s) — "
                "master may arrive in next sync",
                master_ref_name,
                record_name(asset_rec),
            )
        self._pending_assets.clear()

        return events

    @staticmethod
    def _reconcile_kind(master: CKRecord, asset: CKRecord) -> str:
        """When master and asset arrive from different pages, take the more severe."""
        master_kind = classify_record(master)
        asset_kind = classify_record(asset)
        # soft_deleted is more severe than created
        if master_kind == "soft_deleted" or asset_kind == "soft_deleted":
            return "soft_deleted"
        return "created"


def iter_delta_changes(
    library: Any,
    stored_cursor: str,
) -> Iterator[tuple[list[ChangeEvent], str]]:
    """Iterate change pages from /changes/zone, yielding paired events per page.

    Yields (events, page_sync_token) tuples. The caller should track the
    last successfully-processed page token for crash-safe resumption.

    Args:
        library: A BasePhotoLibrary instance (has ._client and .zone_id)
        stored_cursor: The sync token from the last successful sync

    Raises:
        DeltaSyncTokenInvalid: if Apple rejects the stored token (BAD_REQUEST)
        DeltaSyncZoneNotFound: if the zone no longer exists
    """
    client = _get_cloudkit_client(library)
    if client is None:
        raise DeltaSyncUnavailable(
            "Delta sync requires typed CloudKit client (not available for this library)"
        )

    zone_id = _get_zone_id(library)
    zone_req = CKZoneChangesZoneReq(
        zoneID=CKZoneID(**zone_id),
        syncToken=stored_cursor,
        reverse=False,
    )

    buffer = DeltaRecordBuffer()

    for zone_page in client.iter_changes(zone_req=zone_req):
        # Check for zone-level errors
        if hasattr(zone_page, "serverErrorCode") and zone_page.serverErrorCode:
            _handle_zone_error(zone_page.serverErrorCode, zone_page.reason)

        page_token = zone_page.syncToken
        events = buffer.process_records(zone_page.records)
        yield events, page_token

    # Flush any remaining buffered records
    final_events = buffer.flush()
    if final_events:
        yield final_events, page_token  # noqa: F821 — page_token from last iteration


def _get_cloudkit_client(library: Any):
    """Extract the typed CloudKit client from a library, if available."""
    client = getattr(library, "_client", None)
    return client


def _get_zone_id(library: Any) -> dict[str, Any]:
    """Extract zone_id dict from a library."""
    zone_id = getattr(library, "zone_id", None)
    if zone_id is None:
        raise DeltaSyncUnavailable("Library does not expose zone_id")
    return zone_id


def _handle_zone_error(error_code: str | None, reason: str | None) -> None:
    """Raise appropriate exceptions for /changes/zone errors."""
    if error_code == "BAD_REQUEST":
        raise DeltaSyncTokenInvalid(
            f"Sync token rejected by iCloud (BAD_REQUEST): {reason or 'token expired or invalid'}"
        )
    if error_code == "ZONE_NOT_FOUND":
        raise DeltaSyncZoneNotFound(
            f"Zone no longer exists: {reason or 'shared library may have been removed'}"
        )
    if error_code:
        raise DeltaSyncError(f"Unexpected /changes/zone error: {error_code} — {reason}")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class DeltaSyncError(PhotosServiceException):
    """Base exception for delta sync failures."""


class DeltaSyncTokenInvalid(DeltaSyncError):
    """The stored sync token was rejected by Apple (expired or invalid)."""


class DeltaSyncZoneNotFound(DeltaSyncError):
    """The target zone no longer exists (shared library removed)."""


class DeltaSyncUnavailable(DeltaSyncError):
    """Delta sync is not available for this library configuration."""
