"""Object-store and local-path access through fsspec, so every tool takes a URI.

A corpus is written once and read by the producer, the scorer and whatever
inspects it afterwards, and those do not always run on the machine that wrote
it. Addressing a corpus by URI rather than by path is what lets the same
command line name a laptop directory and a bucket prefix, so a local run and a
cluster run differ in one argument rather than in a code path.
"""

from __future__ import annotations

import os

import fsspec
from fsspec.implementations.local import LocalFileSystem

_SCHEMES = ("s3://", "gs://")


def is_remote(uri: str) -> bool:
    return uri.startswith(_SCHEMES)


def filesystem_for(uri: str) -> tuple[fsspec.AbstractFileSystem, str]:
    """The filesystem serving ``uri``, beside the path to hand it.

    S3-compatible stores other than AWS are reached by pointing
    ``AWS_ENDPOINT_URL`` at them, which is the same variable the AWS SDKs read,
    so a compose-hosted store needs no argument of its own.
    """
    if uri.startswith("s3://"):
        kwargs: dict[str, object] = {}
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        if endpoint:
            kwargs["client_kwargs"] = {"endpoint_url": endpoint}
        return fsspec.filesystem("s3", **kwargs), uri[len("s3://") :]
    if uri.startswith("gs://"):
        return fsspec.filesystem("gcs"), uri[len("gs://") :]
    return LocalFileSystem(auto_mkdir=True), uri


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
    fs, path = filesystem_for(uri)
    with fs.open(path, "rb") as handle:
        return bytes(handle.read())


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
