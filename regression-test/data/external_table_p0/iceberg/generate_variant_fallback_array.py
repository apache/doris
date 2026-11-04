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

"""Generate synthetic Variant fixtures; no external data is read.

Run from the repository root with:
  uv run --with pyarrow==24.0.0 --with thriftpy2==0.5.3 python \
    regression-test/data/external_table_p0/iceberg/generate_variant_fallback_array.py
"""
from pathlib import Path
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import thriftpy2
from thriftpy2.protocol import TCompactProtocolFactory
from thriftpy2.transport import TMemoryBuffer

ROOT = Path(__file__).resolve().parents[4]
PARQUET = thriftpy2.load(str(ROOT / "gensrc/thrift/parquet.thrift"),
                        module_name="fixture_parquet_thrift")
# A one-key metadata dictionary containing only the synthetic key "events".
METADATA = bytes([1, 1, 0, 6]) + b"events"


def generate(optional_wrappers):
    element = pa.struct([pa.field("value", pa.binary(), nullable=False)])
    values = pa.list_(pa.field("element", element, nullable=optional_wrappers))
    wrapper = pa.struct([pa.field("value", pa.binary()),
                         pa.field("typed_value", values)])
    typed = pa.struct([pa.field("events", wrapper, nullable=optional_wrappers)])
    variant = pa.struct([pa.field("metadata", pa.binary(), nullable=optional_wrappers),
                         pa.field("value", pa.binary()), pa.field("typed_value", typed)])
    schema = pa.schema([pa.field("id", pa.int32(), nullable=False),
                        pa.field("payload", variant)])

    def array(items):
        return {"metadata": METADATA, "value": None, "typed_value": {
            "events": {"value": None, "typed_value": [{"value": v} for v in items]}}}

    # 0x0c is the Variant int8 header. 0x00 is Variant null, not a Parquet null.
    payloads = [array([b"\x0c\x03", b"\x00", b"\x0c\x04"]), array([]), None,
                {"metadata": METADATA, "value": b"\x00", "typed_value": None},
                array([b"\x00"]),
                {"metadata": METADATA, "value": None, "typed_value": {
                    "events": None if optional_wrappers else {"value": None, "typed_value": None}}}]
    rows = [{"id": i + 1, "payload": value} for i, value in enumerate(payloads)]
    suffix = "optional" if optional_wrappers else "required"
    path = Path(__file__).with_name(f"variant_fallback_array_{suffix}.parquet")
    pq.write_table(pa.Table.from_pylist(rows, schema), path, compression="zstd",
                   row_group_size=2, store_schema=False)
    data = path.read_bytes()
    length = struct.unpack("<I", data[-8:-4])[0]
    footer_start = len(data) - 8 - length
    metadata = PARQUET.FileMetaData()
    metadata.read(TCompactProtocolFactory().get_protocol(TMemoryBuffer(data[footer_start:-8])))
    # Arrow writes the physical carrier; add the standard logical annotation to its footer.
    payload = next(field for field in metadata.schema if field.name == "payload")
    payload.logicalType = PARQUET.LogicalType(VARIANT=PARQUET.VariantType(specification_version=1))
    transport = TMemoryBuffer()
    metadata.write(TCompactProtocolFactory().get_protocol(transport))
    footer = transport.getvalue()
    path.write_bytes(data[:footer_start] + footer + struct.pack("<I", len(footer)) + b"PAR1")
    assert pq.read_table(path).num_rows == 6
    print(path.name, path.stat().st_size)


if __name__ == "__main__":
    generate(False)
    generate(True)
