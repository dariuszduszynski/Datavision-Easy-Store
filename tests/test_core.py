#!/usr/bin/env python3
"""
Simple end-to-end test for DES core functionality.

Usage: python test_core.py
"""

import os
import sys
import tempfile

from des.core import DesReader, DesWriter, InMemoryIndexCache


def test_basic_write_read():
    """Test basic write and read without external storage."""
    print("=" * 60)
    print("TEST: Basic Write/Read")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        des_path = os.path.join(tmpdir, "test.des")

        # Write files
        print("\n1. Writing files...")
        with DesWriter(des_path) as w:
            w.add_file("file1.txt", b"Hello World", meta={"type": "text"})
            w.add_file("file2.txt", b"Hello DES", meta={"type": "text"})
            w.add_file("file3.bin", b"\x00\x01\x02\x03" * 100, meta={"type": "binary"})

        print(f"✓ DES archive created: {des_path}")
        print(f"  Size: {os.path.getsize(des_path)} bytes")

        # Read files
        print("\n2. Reading files...")
        with DesReader(des_path) as r:
            print(f"  Files in archive: {r.list_files()}")

            # Test get_file
            data1 = r.get_file("file1.txt")
            assert data1 == b"Hello World", "File1 content mismatch"
            print(f"  ✓ file1.txt: {data1}")

            # Test get_meta
            meta1 = r.get_meta("file1.txt")
            assert meta1["type"] == "text", "Meta mismatch"
            print(f"  ✓ file1.txt meta: {meta1}")

            # Test __contains__
            assert "file1.txt" in r, "__contains__ failed"
            assert "nonexistent.txt" not in r, "__contains__ false positive"
            print("  ✓ __contains__ works")

            # Test get_files_batch
            batch = r.get_files_batch(["file1.txt", "file2.txt"])
            assert len(batch) == 2, "Batch read failed"
            print(f"  ✓ Batch read: {list(batch.keys())}")

            # Test get_stats
            stats = r.get_stats()
            print(f"  ✓ Stats: {stats}")
            assert stats.total_files == 3
            assert stats.internal_files == 3
            assert stats.external_files == 0

    print("\n✅ Basic test PASSED\n")


def test_with_cache():
    """Test with InMemoryIndexCache."""
    print("=" * 60)
    print("TEST: With Cache")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        des_path = os.path.join(tmpdir, "test.des")

        # Write
        with DesWriter(des_path) as w:
            for i in range(10):
                w.add_file(f"file_{i:03d}.txt", f"Data {i}".encode())

        print("✓ Created archive with 10 files")

        # Read with cache
        cache = InMemoryIndexCache(max_size=100, default_ttl=60)

        # First read (cold)
        with DesReader(des_path, cache=cache) as r:
            files = r.list_files()
            print(f"  Cold read: {len(files)} files")

        # Second read (warm)
        with DesReader(des_path, cache=cache) as r:
            files = r.list_files()
            print(f"  Warm read: {len(files)} files")

        # Check cache stats
        cache_stats = cache.get_stats()
        print(f"  Cache stats: {cache_stats}")
        assert cache_stats["entries"] > 0, "Cache not used"

    print("\n✅ Cache test PASSED\n")


def test_empty_archive():
    """Test empty DES archive."""
    print("=" * 60)
    print("TEST: Empty Archive")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        des_path = os.path.join(tmpdir, "empty.des")

        # Write empty
        with DesWriter(des_path):
            pass  # No files added

        print("✓ Created empty archive")

        # Read empty
        with DesReader(des_path) as r:
            files = r.list_files()
            assert len(files) == 0, "Empty archive should have 0 files"
            stats = r.get_stats()
            assert stats.total_files == 0
            print(f"  Files: {len(files)}")
            print(f"  Stats: {stats}")

    print("\n✅ Empty archive test PASSED\n")


def test_large_metadata():
    """Test with large metadata."""
    print("=" * 60)
    print("TEST: Large Metadata")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        des_path = os.path.join(tmpdir, "test.des")

        # Write with large meta
        large_meta = {
            "description": "x" * 10000,
            "tags": [f"tag_{i}" for i in range(1000)],
            "nested": {f"key_{i}": f"value_{i}" for i in range(100)},
        }

        with DesWriter(des_path) as w:
            w.add_file("file_with_large_meta.txt", b"Small data", meta=large_meta)

        print("✓ Created file with large metadata")

        # Read and verify
        with DesReader(des_path) as r:
            meta = r.get_meta("file_with_large_meta.txt")
            assert len(meta["description"]) == 10000
            assert len(meta["tags"]) == 1000
            print(f"  Meta description length: {len(meta['description'])}")
            print(f"  Meta tags count: {len(meta['tags'])}")

    print("\n✅ Large metadata test PASSED\n")


def test_batch_read_optimization():
    """Test batch read optimization with adjacent files."""
    print("=" * 60)
    print("TEST: Batch Read Optimization")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        des_path = os.path.join(tmpdir, "test.des")

        # Write files sequentially (will be adjacent)
        with DesWriter(des_path) as w:
            for i in range(20):
                w.add_file(f"file_{i:03d}.txt", b"x" * 1000)

        print("✓ Created 20 sequential files")

        # Batch read (should merge into few range reads)
        with DesReader(des_path) as r:
            # Read every other file
            names = [f"file_{i:03d}.txt" for i in range(0, 20, 2)]
            batch = r.get_files_batch(names, max_gap_size=2000)

            assert len(batch) == 10, "Batch read count mismatch"
            print(f"  Batch read: {len(batch)} files")

            # Verify content
            for name, data in batch.items():
                assert len(data) == 1000, f"Size mismatch for {name}"
            print("  ✓ All files verified")

    print("\n✅ Batch optimization test PASSED\n")


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("DES CORE - END-TO-END TESTS")
    print("=" * 60 + "\n")

    try:
        test_basic_write_read()
        test_with_cache()
        test_empty_archive()
        test_large_metadata()
        test_batch_read_optimization()

        print("=" * 60)
        print("✅ ALL TESTS PASSED")
        print("=" * 60 + "\n")
        return 0

    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}\n")
        return 1
    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
