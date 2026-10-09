from lakehouse.domain.manifest import Manifest, ManifestFile


def test_upsert_replaces_same_path():
    manifest = Manifest("bronze/orders", "1")
    manifest.upsert(ManifestFile(path="p", rows=1, size_bytes=10, schema_hash="h"))
    manifest.upsert(ManifestFile(path="p", rows=2, size_bytes=20, schema_hash="h"))
    assert len(manifest.files) == 1
    assert manifest.files[0].rows == 2
    assert manifest.total_rows() == 2


def test_bytes_roundtrip():
    manifest = Manifest("bronze/orders", "1")
    manifest.upsert(
        ManifestFile(
            path="p",
            rows=3,
            size_bytes=30,
            schema_hash="h",
            min_event_date="2026-10-09",
            max_event_date="2026-10-09",
        )
    )
    restored = Manifest.from_bytes(manifest.to_bytes(), "bronze/orders", "1")
    assert restored.files == manifest.files
    assert restored.dataset == "bronze/orders"


def test_empty_manifest_from_none():
    manifest = Manifest.from_bytes(None, "bronze/orders", "1")
    assert manifest.files == []
