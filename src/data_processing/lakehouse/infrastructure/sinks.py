"""Object-store sink adapters (S3/MinIO via boto3, and in-memory).

Both implement the ``ObjectSink`` port from ``lakehouse.application.ports``.
"""

from __future__ import annotations

from lakehouse.domain.errors import TransientStoreError


class InMemorySink:
    """In-memory ObjectSink for tests and local dry-runs."""

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    def put(self, key: str, data: bytes) -> None:
        self._objects[key] = bytes(data)

    def get(self, key: str) -> bytes | None:
        return self._objects.get(key)

    def exists(self, key: str) -> bool:
        return key in self._objects

    def copy(self, source_key: str, destination_key: str) -> None:
        if source_key not in self._objects:
            raise TransientStoreError(f"missing source object: {source_key}")
        self._objects[destination_key] = self._objects[source_key]

    def delete(self, key: str) -> None:
        self._objects.pop(key, None)

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self._objects if key.startswith(prefix))

    @property
    def keys(self) -> list[str]:
        """All stored object keys, sorted (test/debug helper)."""
        return sorted(self._objects)


class Boto3Sink:
    """S3/MinIO ObjectSink backed by boto3 (SigV4, path-style access)."""

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str = "us-east-1",
    ) -> None:
        import boto3
        from botocore.config import Config as BotoConfig

        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def put(self, key: str, data: bytes) -> None:
        self._translate(
            lambda: self._client.put_object(Bucket=self._bucket, Key=key, Body=data)
        )

    def get(self, key: str) -> bytes | None:
        from botocore.exceptions import ClientError

        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if _is_missing(exc):
                return None
            raise TransientStoreError(str(exc)) from exc
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as exc:
            if _is_missing(exc):
                return False
            raise TransientStoreError(str(exc)) from exc

    def copy(self, source_key: str, destination_key: str) -> None:
        self._translate(
            lambda: self._client.copy_object(
                Bucket=self._bucket,
                CopySource={"Bucket": self._bucket, "Key": source_key},
                Key=destination_key,
            )
        )

    def delete(self, key: str) -> None:
        self._translate(
            lambda: self._client.delete_object(Bucket=self._bucket, Key=key)
        )

    def list_keys(self, prefix: str) -> list[str]:
        def _list() -> list[str]:
            paginator = self._client.get_paginator("list_objects_v2")
            keys = [
                obj["Key"]
                for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix)
                for obj in page.get("Contents", [])
            ]
            return sorted(keys)

        return self._translate(_list)

    def _translate(self, operation):
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            return operation()
        except (ClientError, BotoCoreError) as exc:
            raise TransientStoreError(str(exc)) from exc


def _is_missing(exc: Exception) -> bool:
    """Return True when a ClientError means the object does not exist."""
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    return code in {"NoSuchKey", "NoSuchBucket", "404", "NotFound"}
