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

"""Build Blob v2 fixtures mirrored to s3://warehouse/lance/blob_v2/.

Run with lance_fixture_requirements.txt. Build refuses to replace existing data;
--check reads existing datasets without rewriting them. External URIs use the
published MinIO location, so copying the fixture never embeds a machine-local path.

The pinned writer's None round-trip produces an empty descriptor, so these files
use non-null Blob inputs. SQL NULL propagation is tested separately in Groovy;
native descriptor NULL shapes are covered by LanceBlobColumnReaderTest.
"""

import argparse
from pathlib import Path

import lance
import pyarrow as pa
from lance.blob import Blob, blob_array

REMOTE_ROOT = "s3://warehouse/lance/blob_v2"
DEDICATED_THRESHOLD = 128 * 1024
MANAGED = [b"", b"\x00\x01\xff", b"packed:" + b"p" * 65536,
           b"dedicated:" + b"d" * DEDICATED_THRESHOLD,
           b"second-fragment", b"repeat", b"\xff\x00\x7f", b"repeat"]
ALTERNATE = [b"alternate-" + str(i).encode() for i in range(len(MANAGED))]
OBJECTS = {"whole.bin": b"external\x00\xff", "slice.bin": b"prefix:SLICE:suffix"}


def check_writer():
    if not __debug__:
        raise RuntimeError("Fixture verification requires assertions; do not use python -O")
    if lance.__version__ != "7.0.0":
        raise RuntimeError("Use the pinned pylance 7.0.0 fixture writer")


def managed_table():
    payload = blob_array(MANAGED)
    # Keep dedicated objects small enough to commit while still verifying actual kinds.
    field = pa.field("payload", payload.type, metadata={
        b"lance-encoding:blob-dedicated-size-threshold": str(DEDICATED_THRESHOLD).encode()})
    schema = pa.schema([pa.field("id", pa.int32(), nullable=False),
                        pa.field("bucket", pa.int32(), nullable=False), field,
                        pa.field("alternate", payload.type),
                        pa.field("expected_payload", pa.binary()),
                        pa.field("expected_alternate", pa.binary())])
    return pa.Table.from_arrays([pa.array(range(len(MANAGED)), type=pa.int32()),
                                 pa.array([i % 2 for i in range(len(MANAGED))], type=pa.int32()),
                                 payload, blob_array(ALTERNATE), pa.array(MANAGED, type=pa.binary()),
                                 pa.array(ALTERNATE, type=pa.binary())], schema=schema)


def external_values():
    whole = f"{REMOTE_ROOT}/whole.bin"
    sliced = f"{REMOTE_ROOT}/slice.bin"
    return [Blob.from_uri(whole), Blob.from_uri(sliced, position=7, size=5),
            Blob.from_uri(whole), b"inline", b""]


def external_table(values, expected):
    return pa.table({"id": pa.array(range(len(values)), type=pa.int32()),
                     "payload": blob_array(values),
                     "expected_payload": pa.array(expected, type=pa.binary())})


def build(output):
    check_writer()
    if output.exists():
        raise FileExistsError(output)
    output.mkdir(parents=True)
    for name, data in OBJECTS.items():
        (output / name).write_bytes(data)
    rows = managed_table()
    lance.write_dataset(rows.slice(0, 4), str(output / "managed.lance"), data_storage_version="2.2")
    lance.write_dataset(rows.slice(4), str(output / "managed.lance"), mode="append",
                        data_storage_version="2.2")
    cases = {"external": (external_values(), [OBJECTS["whole.bin"], b"SLICE", OBJECTS["whole.bin"], b"inline", b""]),
             "missing_external": ([b"readable", Blob.from_uri(f"{REMOTE_ROOT}/missing.bin")], [b"readable", None]),
             "local_external": ([b"readable", Blob.from_uri("file:///lance-blob-not-shared.bin")], [b"readable", None])}
    for name, (values, expected) in cases.items():
        lance.write_dataset(external_table(values, expected), str(output / f"{name}.lance"),
                            data_storage_version="2.2", allow_external_blob_outside_bases=True)
    check(output)


def check(output):
    check_writer()
    dataset = lance.dataset(str(output / "managed.lance"))
    descriptions = dataset.to_table(blob_handling="blobs_descriptions")
    assert descriptions["id"].to_pylist() == list(range(len(MANAGED)))
    assert descriptions["bucket"].to_pylist() == [i % 2 for i in range(len(MANAGED))]
    assert descriptions["expected_payload"].to_pylist() == MANAGED
    assert descriptions["expected_alternate"].to_pylist() == ALTERNATE
    assert [row["kind"] for row in descriptions["payload"].to_pylist()] == [0, 0, 1, 2, 0, 0, 0, 0]
    assert descriptions["payload"].null_count == 0
    fragments = dataset.get_fragments()
    assert len(fragments) == 2
    assert [f.to_table(columns=["id"])["id"].to_pylist() for f in fragments] == [list(range(4)), list(range(4, 8))]
    assert descriptions["payload"].to_pylist()[0]["size"] == 0
    for column, expected in [("payload", MANAGED), ("alternate", ALTERNATE)]:
        # Reordering and repeats verify that row selection is independent of physical order.
        selection = [7, 3, 0, 2, 1, 7, 6, 5, 4]
        # pylance 7 omits empty objects from take_blobs results. Verify their descriptor
        # above; the Groovy test checks FL_GET_BINARY returns an empty, non-NULL value.
        selection = [i for i in selection if expected[i]]
        handles = dataset.take_blobs(column, indices=selection)
        assert [handle.read() for handle in handles] == [expected[i] for i in selection]
    external = lance.dataset(str(output / "external.lance"))
    values = external.to_table(blob_handling="blobs_descriptions")["payload"].to_pylist()
    assert [v["kind"] for v in values] == [3, 3, 3, 0, 0]
    assert values[0]["blob_uri"] == f"{REMOTE_ROOT}/whole.bin"
    assert values[1]["blob_uri"] == f"{REMOTE_ROOT}/slice.bin"
    assert (values[1]["position"], values[1]["size"]) == (7, 5)
    assert external.to_table(columns=["expected_payload"])["expected_payload"].to_pylist() == [
        OBJECTS["whole.bin"], b"SLICE", OBJECTS["whole.bin"], b"inline", b""]
    for name, expected in OBJECTS.items():
        assert (output / name).read_bytes() == expected
    for name, uri in [("missing_external", f"{REMOTE_ROOT}/missing.bin"),
                       ("local_external", "file:///lance-blob-not-shared.bin")]:
        error_table = lance.dataset(str(output / f"{name}.lance"))
        descriptors = error_table.to_table(blob_handling="blobs_descriptions")
        assert descriptors["id"].to_pylist() == [0, 1]
        assert descriptors["payload"].to_pylist()[1]["blob_uri"] == uri
        assert error_table.take_blobs("payload", indices=[0])[0].read() == b"readable"
    print(f"Verified managed kinds 0/1/2, external kind 3 and deferred-error fixtures in {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", type=Path, default=Path(__file__).parent / "preinstalled_data/lance/blob_v2")
    parser.add_argument("--check", action="store_true", help="Verify existing files without rewriting them")
    args = parser.parse_args()
    (check if args.check else build)(args.output)
