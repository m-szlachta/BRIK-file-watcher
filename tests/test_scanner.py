import hashlib
import os
from pathlib import Path, PurePosixPath

from conftest import write

from brik2_dp_dispatcher.scanner import scan, sha256_file


def observe(root: Path, previous=None, **kwargs):
    result = scan(root, {o.path: o for o in (previous or [])}, **kwargs)
    return result, result.observations


def test_sha256_file_hashes_in_chunks(tmp_path):
    data = os.urandom(10_000)
    path = write(tmp_path, "a.bin", data)
    assert sha256_file(path, chunk_size=1024) == hashlib.sha256(data).hexdigest()


def test_file_becomes_stable_on_second_unchanged_scan(tmp_path):
    write(tmp_path, "a/x.laz", b"x")
    first, obs = observe(tmp_path)
    assert first.stable == [] and first.unstable == [PurePosixPath("a/x.laz")]
    second, _ = observe(tmp_path, obs)
    assert [f.path for f in second.stable] == [PurePosixPath("a/x.laz")]
    assert second.stable[0].content_hash == hashlib.sha256(b"x").hexdigest()


def test_stable_scans_one_uses_file_immediately(tmp_path):
    write(tmp_path, "x.laz", b"x")
    result, _ = observe(tmp_path, stable_scans=1)
    assert len(result.stable) == 1


def test_changing_file_is_not_stable(tmp_path):
    path = write(tmp_path, "x.laz", b"x")
    _, obs = observe(tmp_path)
    path.write_bytes(b"xy")
    second, obs = observe(tmp_path, obs)
    assert second.stable == []
    os.utime(path, ns=(1, 1))
    third, obs = observe(tmp_path, obs)
    assert third.stable == []
    fourth, _ = observe(tmp_path, obs)
    assert len(fourth.stable) == 1


def test_part_tmp_and_hidden_files_are_ignored(tmp_path):
    for rel in ["a.laz.part", "b.TMP", ".hidden", ".cache/inner.laz", "sub/c.tmp", "partial.tmp/inside.laz"]:
        write(tmp_path, rel, b"data")
    write(tmp_path, "ok.laz", b"ok")
    _, obs = observe(tmp_path, stable_scans=1)
    assert [o.path for o in obs] == ["ok.laz"]


def test_hidden_files_can_be_included(tmp_path):
    write(tmp_path, ".hidden", b"data")
    _, obs = observe(tmp_path, stable_scans=1, ignore_hidden=False)
    assert [o.path for o in obs] == [".hidden"]


def test_unchanged_file_is_not_rehashed(tmp_path):
    write(tmp_path, "x.laz", b"x")
    calls = []

    def hasher(path):
        calls.append(path)
        return sha256_file(path)

    _, obs = observe(tmp_path, hasher=hasher)
    _, obs = observe(tmp_path, obs, hasher=hasher)
    result, _ = observe(tmp_path, obs, hasher=hasher)
    assert len(calls) == 1
    assert len(result.stable) == 1 and result.hashed == 0


def test_file_modified_during_hashing_is_unstable(tmp_path):
    path = write(tmp_path, "x.laz", b"x")

    def hasher(p):
        digest = sha256_file(p)
        p.write_bytes(b"grown")
        return digest

    _, obs = observe(tmp_path)
    result, obs = observe(tmp_path, obs, hasher=hasher)
    assert result.stable == []
    assert obs[0].stable_count == 1 and obs[0].size == path.stat().st_size


def test_symlinks_are_not_followed(tmp_path):
    target = write(tmp_path, "real.laz", b"x")
    (tmp_path / "link.laz").symlink_to(target)
    _, obs = observe(tmp_path, stable_scans=1)
    assert [o.path for o in obs] == ["real.laz"]


def test_walk_order_is_lexicographic(tmp_path):
    for rel in ["b/x", "a/z/y", "a/x", "c", "a/b/w"]:
        write(tmp_path, rel, rel.encode())
    _, obs = observe(tmp_path, stable_scans=1)
    assert [o.path for o in obs] == ["c", "a/x", "a/b/w", "a/z/y", "b/x"]
