"""Object-store and local-path access through fsspec, so every tool takes a URI.

A corpus is written once and read by the producer, the scorer and whatever
inspects it afterwards, and those do not always run on the machine that wrote
it. Addressing a corpus by URI rather than by path is what lets the same
command line name a laptop directory and a bucket prefix, so a local run and a
cluster run differ in one argument rather than in a code path.
"""

from __future__ import annotations

import os
import re

import fsspec
from fsspec.implementations.local import LocalFileSystem

_REMOTE_SCHEMES = ("s3://", "gs://")
_LOCAL_SCHEME = "file://"
_ANY_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")


def is_remote(uri: str) -> bool:
    return uri.startswith(_REMOTE_SCHEMES)


def filesystem_for(uri: str) -> tuple[fsspec.AbstractFileSystem, str]:
    """The filesystem serving ``uri``, beside the path to hand it.

    S3-compatible stores other than AWS are reached by pointing
    ``AWS_ENDPOINT_URL`` at them, which is the same variable the AWS SDKs read,
    so a compose-hosted store needs no argument of its own.

    A scheme this module does not serve is refused rather than read as a
    relative path. Falling through would write a bucket's worth of corpus into
    a local directory named after the scheme, and nothing about that surfaces
    until a cluster cannot find the corpus it was pointed at.
    """
    if uri.startswith("s3://"):
        # No listings cache, because the scorer lists prefixes that are still
        # being written into: a producer shard's publish log appears in a prefix
        # an earlier poll already listed, and a cached listing would hide it —
        # leaving the offer looking as though it never ended.
        kwargs: dict[str, object] = {"use_listings_cache": False}
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        if endpoint:
            kwargs["client_kwargs"] = {"endpoint_url": endpoint}
        return fsspec.filesystem("s3", **kwargs), uri[len("s3://") :]
    if uri.startswith("gs://"):
        return fsspec.filesystem("gcs"), uri[len("gs://") :]
    path = uri[len(_LOCAL_SCHEME) :] if uri.startswith(_LOCAL_SCHEME) else uri
    if _ANY_SCHEME.match(path):
        raise ValueError(f"unsupported URI scheme in {uri!r}")
    return LocalFileSystem(auto_mkdir=True), path


def join(uri: str, *parts: str) -> str:
    return "/".join([uri.rstrip("/"), *(part.strip("/") for part in parts)])


def write_bytes(uri: str, data: bytes) -> None:
    fs, path = filesystem_for(uri)
    # An object store has no directories to create, and asking one to make them
    # costs a round trip that can also fail on a prefix a writer may not list.
    if not is_remote(uri):
        fs.makedirs(os.path.dirname(path), exist_ok=True)
    with fs.open(path, "wb") as handle:
        handle.write(data)


def read_bytes(uri: str) -> bytes:
    """The whole object at ``uri``, as it stands at this moment.

    One request rather than a buffered file, because objects here are rewritten
    while they are being read: a producer republishes its publish log every few
    seconds, and the scorer reads that log on every poll. A caching file object
    pins the ETag it opened with and fails a later range request with
    ``FileExpired`` once the object behind it has been replaced — so a read that
    happened to span a republish would end the run rather than return the newer
    bytes.
    """
    fs, path = filesystem_for(uri)
    return bytes(fs.cat_file(path))


def write_text(uri: str, text: str) -> None:
    write_bytes(uri, text.encode("utf-8"))


def read_text(uri: str) -> str:
    return read_bytes(uri).decode("utf-8")


def exists(uri: str) -> bool:
    fs, path = filesystem_for(uri)
    return bool(fs.exists(path))


def listdir(uri: str) -> list[str]:
    """The entry names directly under ``uri``, without their prefix."""
    fs, path = filesystem_for(uri)
    return sorted(str(entry).rsplit("/", 1)[-1] for entry in fs.ls(path, detail=False))
