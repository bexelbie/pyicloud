"""Tests for the delta sync engine (DeltaRecordBuffer, decision logic, etc.)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from pyicloud.common.cloudkit import CKRecord, CKTombstoneRecord
from pyicloud.services.photos_cloudkit.delta import (
    DeltaRecordBuffer,
    classify_record,
)
from pyicloud.services.photos_cloudkit.state import (
    MemoryPhotoSyncState,
    SyncedPhotoResource,
)
from pyicloud.services.photos_cloudkit.sync import (
    PhotoSyncOptions,
    _should_use_delta,
)
from pyicloud.services.photos import PhotosServiceException

TEST_BASE = Path(tempfile.gettempdir()) / "python-test-results" / "delta"
TEST_BASE.mkdir(parents=True, exist_ok=True)


def _make_ck_record(
    record_name: str, record_type: str, fields: dict | None = None
) -> CKRecord:
    """Create a minimal CKRecord for testing."""
    raw_fields = fields or {}
    return CKRecord.model_validate(
        {
            "recordName": record_name,
            "recordType": record_type,
            "fields": raw_fields,
        }
    )


def _make_master_record(record_name: str, *, is_deleted: bool = False) -> CKRecord:
    """Create a CPLMaster record."""
    fields = {}
    if is_deleted:
        fields["isDeleted"] = {"value": 1, "type": "INT64"}
    return _make_ck_record(record_name, "CPLMaster", fields)


def _make_asset_record(
    record_name: str, master_ref_name: str, *, is_deleted: bool = False
) -> CKRecord:
    """Create a CPLAsset record pointing to a master via masterRef."""
    fields: dict = {
        "masterRef": {
            "value": {
                "recordName": master_ref_name,
                "zoneID": {"zoneName": "PrimarySync"},
            },
            "type": "REFERENCE",
        }
    }
    if is_deleted:
        fields["isDeleted"] = {"value": 1, "type": "INT64"}
    return _make_ck_record(record_name, "CPLAsset", fields)


def _make_tombstone(record_name: str) -> CKTombstoneRecord:
    """Create a CKTombstoneRecord for a deleted record."""
    return CKTombstoneRecord.model_validate(
        {
            "recordName": record_name,
            "deleted": True,
        }
    )


# ---------------------------------------------------------------------------
# DeltaRecordBuffer tests
# ---------------------------------------------------------------------------


class TestDeltaRecordBuffer:
    """Tests for the cross-page CPLMaster/CPLAsset pairing buffer."""

    def test_paired_records_same_page(self):
        """Master + Asset on same page should emit a single created event."""
        buffer = DeltaRecordBuffer()
        master = _make_master_record("MASTER-001")
        asset = _make_asset_record("ASSET-001", "MASTER-001")

        events = buffer.process_records([master, asset])

        assert len(events) == 1
        assert events[0].kind == "created"
        assert events[0].master_record_name == "MASTER-001"
        assert events[0].master_record is master
        assert events[0].asset_record is asset

    def test_paired_records_asset_first(self):
        """Asset arriving before Master on same page should still pair."""
        buffer = DeltaRecordBuffer()
        master = _make_master_record("MASTER-002")
        asset = _make_asset_record("ASSET-002", "MASTER-002")

        events = buffer.process_records([asset, master])

        assert len(events) == 1
        assert events[0].kind == "created"
        assert events[0].master_record_name == "MASTER-002"

    def test_cross_page_pairing(self):
        """Master on page 1, Asset on page 2 should pair on page 2."""
        buffer = DeltaRecordBuffer()
        master = _make_master_record("MASTER-003")
        asset = _make_asset_record("ASSET-003", "MASTER-003")

        events_page1 = buffer.process_records([master])
        assert len(events_page1) == 0  # Master buffered, waiting for asset

        events_page2 = buffer.process_records([asset])
        assert len(events_page2) == 1
        assert events_page2[0].master_record_name == "MASTER-003"

    def test_tombstone_immediate_emit(self):
        """Tombstone records should emit hard_deleted immediately."""
        buffer = DeltaRecordBuffer()
        tombstone = _make_tombstone("MASTER-004")

        events = buffer.process_records([tombstone])

        assert len(events) == 1
        assert events[0].kind == "hard_deleted"
        assert events[0].master_record_name == "MASTER-004"
        assert events[0].master_record is None
        assert events[0].asset_record is None

    def test_soft_deleted_reconciliation(self):
        """If master has isDeleted=1, paired event should be soft_deleted."""
        buffer = DeltaRecordBuffer()
        master = _make_master_record("MASTER-005", is_deleted=True)
        asset = _make_asset_record("ASSET-005", "MASTER-005")

        events = buffer.process_records([master, asset])

        assert len(events) == 1
        assert events[0].kind == "soft_deleted"

    def test_soft_deleted_from_asset(self):
        """If asset has isDeleted=1, paired event should be soft_deleted."""
        buffer = DeltaRecordBuffer()
        master = _make_master_record("MASTER-006")
        asset = _make_asset_record("ASSET-006", "MASTER-006", is_deleted=True)

        events = buffer.process_records([master, asset])

        assert len(events) == 1
        assert events[0].kind == "soft_deleted"

    def test_flush_unpaired_master(self):
        """Flush should emit unpaired masters as created events."""
        buffer = DeltaRecordBuffer()
        master = _make_master_record("MASTER-007")

        buffer.process_records([master])
        events = buffer.flush()

        assert len(events) == 1
        assert events[0].kind == "created"
        assert events[0].master_record_name == "MASTER-007"
        assert events[0].master_record is master
        assert events[0].asset_record is None

    def test_flush_orphaned_asset_no_event(self):
        """Flush should NOT emit events for orphaned assets (just log)."""
        buffer = DeltaRecordBuffer()
        asset = _make_asset_record("ASSET-008", "MASTER-MISSING")

        buffer.process_records([asset])
        events = buffer.flush()

        # Orphaned assets are just logged, not emitted
        assert len(events) == 0

    def test_skips_other_record_types(self):
        """Non CPLMaster/CPLAsset/Tombstone records should be silently skipped."""
        buffer = DeltaRecordBuffer()
        album_record = _make_ck_record("ALBUM-001", "CPLAlbum")

        events = buffer.process_records([album_record])
        assert len(events) == 0

    def test_multiple_pairs_same_page(self):
        """Multiple pairs on one page should all emit."""
        buffer = DeltaRecordBuffer()
        records = [
            _make_master_record("M1"),
            _make_asset_record("A1", "M1"),
            _make_master_record("M2"),
            _make_asset_record("A2", "M2"),
            _make_tombstone("M3"),
        ]

        events = buffer.process_records(records)

        assert len(events) == 3
        kinds = [e.kind for e in events]
        assert kinds.count("created") == 2
        assert kinds.count("hard_deleted") == 1


# ---------------------------------------------------------------------------
# _should_use_delta tests
# ---------------------------------------------------------------------------


class TestShouldUseDelta:
    """Tests for the delta sync decision logic."""

    def test_full_scan_flag_forces_full(self):
        """--full-scan should always return False."""
        opts = PhotoSyncOptions(directory=TEST_BASE, full_scan=True)
        assert _should_use_delta(opts, "cursor-old", "cursor-new") is False

    def test_sync_mode_full_always_false(self):
        """sync_mode=full should always return False."""
        opts = PhotoSyncOptions(directory=TEST_BASE, sync_mode="full")
        assert _should_use_delta(opts, "cursor-old", "cursor-new") is False

    def test_sync_mode_incremental_no_cursor_errors(self):
        """sync_mode=incremental without stored cursor should raise."""
        opts = PhotoSyncOptions(directory=TEST_BASE, sync_mode="incremental")
        with pytest.raises(PhotosServiceException, match="stored sync cursor"):
            _should_use_delta(opts, None, "cursor-new")

    def test_sync_mode_incremental_with_cursor(self):
        """sync_mode=incremental with stored cursor should return True."""
        opts = PhotoSyncOptions(directory=TEST_BASE, sync_mode="incremental")
        assert _should_use_delta(opts, "cursor-old", "cursor-new") is True

    def test_auto_no_stored_cursor(self):
        """auto mode without stored cursor should return False (first run)."""
        opts = PhotoSyncOptions(directory=TEST_BASE, sync_mode="auto")
        assert _should_use_delta(opts, None, "cursor-new") is False

    def test_auto_cursors_match(self):
        """auto mode with matching cursors should return False."""
        opts = PhotoSyncOptions(directory=TEST_BASE, sync_mode="auto")
        assert _should_use_delta(opts, "same", "same") is False

    def test_auto_cursors_differ(self):
        """auto mode with differing cursors should return True."""
        opts = PhotoSyncOptions(directory=TEST_BASE, sync_mode="auto")
        assert _should_use_delta(opts, "old-cursor", "new-cursor") is True


# ---------------------------------------------------------------------------
# classify_record tests
# ---------------------------------------------------------------------------


class TestClassifyRecord:
    """Tests for record classification."""

    def test_normal_record_is_created(self):
        record = _make_master_record("M1")
        assert classify_record(record) == "created"

    def test_deleted_record_is_soft_deleted(self):
        record = _make_master_record("M2", is_deleted=True)
        assert classify_record(record) == "soft_deleted"


# ---------------------------------------------------------------------------
# State iter_resources_by_asset tests
# ---------------------------------------------------------------------------


class TestStateIterResourcesByAsset:
    """Tests for the new iter_resources_by_asset method."""

    def test_memory_state_by_asset(self):
        state = MemoryPhotoSyncState()
        with state as s:
            s.upsert_resource(
                SyncedPhotoResource(
                    asset_id="asset-1",
                    resource_key="original",
                    relative_path="photo1.jpg",
                    size=100,
                    checksum="abc",
                    downloaded_at="2024-01-01T00:00:00Z",
                )
            )
            s.upsert_resource(
                SyncedPhotoResource(
                    asset_id="asset-1",
                    resource_key="thumb",
                    relative_path="photo1_thumb.jpg",
                    size=50,
                    checksum="def",
                    downloaded_at="2024-01-01T00:00:00Z",
                )
            )
            s.upsert_resource(
                SyncedPhotoResource(
                    asset_id="asset-2",
                    resource_key="original",
                    relative_path="photo2.jpg",
                    size=200,
                    checksum="ghi",
                    downloaded_at="2024-01-01T00:00:00Z",
                )
            )

            results = list(s.iter_resources_by_asset("asset-1"))
            assert len(results) == 2
            assert all(r.asset_id == "asset-1" for r in results)

            results2 = list(s.iter_resources_by_asset("asset-2"))
            assert len(results2) == 1

            results3 = list(s.iter_resources_by_asset("nonexistent"))
            assert len(results3) == 0

    def test_sqlite_state_by_asset(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="delta-sync-state-", dir=TEST_BASE))
        try:
            db_path = temp_dir / "test.sqlite3"
            from pyicloud.services.photos_cloudkit.state import SQLitePhotoSyncState

            with SQLitePhotoSyncState(db_path) as s:
                s.upsert_resource(
                    SyncedPhotoResource(
                        asset_id="asset-A",
                        resource_key="original",
                        relative_path="a.jpg",
                        size=100,
                        checksum="x",
                        downloaded_at="2024-01-01T00:00:00Z",
                    )
                )
                s.upsert_resource(
                    SyncedPhotoResource(
                        asset_id="asset-A",
                        resource_key="medium",
                        relative_path="a_med.jpg",
                        size=50,
                        checksum="y",
                        downloaded_at="2024-01-01T00:00:00Z",
                    )
                )
                s.upsert_resource(
                    SyncedPhotoResource(
                        asset_id="asset-B",
                        resource_key="original",
                        relative_path="b.jpg",
                        size=200,
                        checksum="z",
                        downloaded_at="2024-01-01T00:00:00Z",
                    )
                )

                results = list(s.iter_resources_by_asset("asset-A"))
                assert len(results) == 2

                results2 = list(s.iter_resources_by_asset("asset-B"))
                assert len(results2) == 1
        finally:
            for path in sorted(temp_dir.rglob("*"), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()
            temp_dir.rmdir()
