#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Create and verify physical Parquet objects for an isolated Split benchmark."""

import argparse
import hashlib
import io
import json
import multiprocessing
import os
import re
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError


def client_for(endpoint, access_env, secret_env, connections):
    return boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                        aws_access_key_id=os.environ[access_env], aws_secret_access_key=os.environ[secret_env],
                        config=Config(max_pool_connections=connections, connect_timeout=5,
                                      read_timeout=30, retries={"max_attempts": 2},
                                      s3={"addressing_style": "path"}))


def init_upload_process(endpoint, bucket, access_env, secret_env, data, files, stopped):
    global process_client, process_bucket, process_data, process_files, process_stopped
    # Each spawned process owns its sockets; no boto client or connection pool is shared across processes.
    process_client = client_for(endpoint, access_env, secret_env, 1)
    process_bucket, process_data, process_files, process_stopped = bucket, data, files, stopped


def upload_in_process(partition):
    for number in range(process_files):
        if process_stopped.is_set():
            return False
        process_client.put_object(Bucket=process_bucket,
                                  Key="data/p=%d/file-%03d.parquet" % (partition, number), Body=process_data)
    return True


def verify(client, bucket, marker):
    counts = [0] * marker["partitions"]
    expression = re.compile(r"data/p=(\d+)/file-(\d+)\.parquet")
    objects = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="data/"):
        for item in page.get("Contents", []):
            match = expression.fullmatch(item["Key"])
            if match is None:
                raise ValueError("Unexpected fixture object: " + item["Key"])
            partition, number = map(int, match.groups())
            if partition >= marker["partitions"] or number >= marker["files_per_partition"]:
                raise ValueError("Object is outside the requested fixture shape: " + item["Key"])
            if item["Key"] != "data/p=%d/file-%03d.parquet" % (partition, number):
                raise ValueError("Noncanonical fixture object name: " + item["Key"])
            if item["Size"] != marker["file_bytes"] or item["ETag"].strip('"') != marker["file_md5"]:
                raise ValueError("Unexpected file size or checksum: " + item["Key"])
            counts[partition] += 1
            objects += 1
        if objects and objects % 100000 == 0:
            print("Verified %d physical objects" % objects, flush=True)
    if any(count != marker["files_per_partition"] for count in counts):
        raise ValueError("Physical object counts do not match the requested partition shape")
    return objects


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--access-key-env", default="SPLIT_S3_ACCESS_KEY")
    parser.add_argument("--secret-key-env", default="SPLIT_S3_SECRET_KEY")
    parser.add_argument("--partitions", type=int, default=10000)
    parser.add_argument("--files-per-partition", type=int, default=100)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--process-pool", action="store_true", help="Use independent upload processes")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.partitions, args.files_per_partition, args.workers) <= 0:
        parser.error("Shape and worker count must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "physical-manifest.json").unlink(missing_ok=True)
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": pa.array([1], type=pa.int64())}), buffer,
                   compression="NONE", version="1.0")
    data = buffer.getvalue()
    marker = {"format": "doris-split-physical-v1", "partitions": args.partitions,
              "files_per_partition": args.files_per_partition, "file_bytes": len(data),
              "file_md5": hashlib.md5(data).hexdigest(), "rows_per_file": 1, "prefix": "data/"}
    client = client_for(args.endpoint, args.access_key_env, args.secret_key_env, args.workers)
    try:
        client.head_bucket(Bucket=args.bucket)
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404 or args.verify_only:
            raise
        client.create_bucket(Bucket=args.bucket)
    marker_key = "_split_fixture.json"
    try:
        response = client.get_object(Bucket=args.bucket, Key=marker_key)
        with response["Body"] as body:
            existing = json.load(body)
        if existing != marker:
            raise ValueError("Existing bucket has a different fixture owner, shape or Parquet encoding")
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchKey" or args.verify_only:
            raise
        # A previous setup can stop after creating the bucket but before writing its marker.
        if client.list_objects_v2(Bucket=args.bucket, MaxKeys=1).get("Contents"):
            raise ValueError("Refusing to write into a nonempty bucket without the fixture ownership marker")
        client.put_object(Bucket=args.bucket, Key=marker_key, Body=json.dumps(marker).encode())
    checkpoint = args.output / "uploaded-partitions.json"
    completed = set(json.loads(checkpoint.read_text())) if checkpoint.exists() else set()
    if not completed.issubset(range(args.partitions)):
        raise ValueError("Upload checkpoint has a different partition shape")
    context = multiprocessing.get_context("spawn")
    stopped = context.Event() if args.process_pool else threading.Event()

    def upload_partition(partition):
        for number in range(args.files_per_partition):
            if stopped.is_set():
                return False
            key = "data/p=%d/file-%03d.parquet" % (partition, number)
            client.put_object(Bucket=args.bucket, Key=key, Body=data)
        return True

    started = time.monotonic()
    initial = len(completed)
    last_report = started
    if not args.verify_only:
        if args.process_pool:
            executor = ProcessPoolExecutor(max_workers=args.workers, mp_context=context,
                initializer=init_upload_process, initargs=(args.endpoint, args.bucket, args.access_key_env,
                    args.secret_key_env, data, args.files_per_partition, stopped))
            upload = upload_in_process
        else:
            executor = ThreadPoolExecutor(max_workers=args.workers)
            upload = upload_partition
        futures = {executor.submit(upload, p): p for p in range(args.partitions) if p not in completed}
        try:
            for future in as_completed(futures):
                if future.result():
                    completed.add(futures[future])
                now = time.monotonic()
                if len(completed) % 25 == 0 or now - last_report >= 5:
                    temporary = checkpoint.with_suffix(".tmp")
                    temporary.write_text(json.dumps(sorted(completed)))
                    temporary.replace(checkpoint)
                if now - last_report >= 5:
                    count = len(completed) * args.files_per_partition
                    rate = (len(completed) - initial) * args.files_per_partition / (now - started)
                    print("Uploaded %d/%d physical objects (%.0f objects/s)" % (
                        count, args.partitions * args.files_per_partition, rate), flush=True)
                    last_report = now
        except (Exception, KeyboardInterrupt):
            stopped.set()
            raise
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
            temporary = checkpoint.with_suffix(".tmp")
            temporary.write_text(json.dumps(sorted(completed)))
            temporary.replace(checkpoint)
    count = verify(client, args.bucket, marker)
    manifest = dict(marker, status="VERIFIED", mode="physical_files", bucket=args.bucket,
                    endpoint=args.endpoint.rstrip("/"), verified_objects=count,
                    verification="Full S3 listing checked every key, partition count, length and MD5 ETag")
    (args.output / "physical-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("Verified %d physical Parquet objects across %d partitions" % (count, args.partitions), flush=True)


if __name__ == "__main__":
    main()
