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

"""Regenerate checked-in IPC fixtures with Python 3.12 and pyarrow==21.0.0.

The regression suite only reads the .arrow files and does not require Python.
Every stream includes an empty batch, followed by one-row batches, to exercise EOF,
nullable values and batch transitions. Failed loads can follow a complete valid stream.
"""

from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

ROOT = Path(__file__).resolve().parent


def stream(data_type, values):
    schema = pa.schema([pa.field("k", pa.int32(), False), pa.field("v", data_type)])
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, schema) as writer:
        writer.write_batch(
            pa.record_batch(
                [pa.array([], pa.int32()), pa.array([], data_type)], schema=schema
            )
        )
        for key, value in enumerate(values, 1):
            writer.write_batch(
                pa.record_batch(
                    [pa.array([key], pa.int32()), pa.array([value], data_type)], schema=schema
                )
            )
    return sink.getvalue().to_pybytes()


def write(name, data_type, values):
    (ROOT / (name + ".arrow")).write_bytes(stream(data_type, values))


TYPES = {
    "bool": (pa.bool_(), [True, False]),
    "decimal128": (pa.decimal128(38, 2), [Decimal("1.50"), Decimal("-2.25")]),
    "decimal256": (pa.decimal256(76, 2), [Decimal("1.50"), Decimal("-2.25")]),
    "array": (pa.list_(pa.bool_()), [[True, False], []]),
    "map": (pa.map_(pa.string(), pa.bool_()), [[("x", True)], []]),
    "struct": (pa.struct([("b", pa.bool_())]), [{"b": True}, {"b": False}]),
}
for name, (data_type, values) in TYPES.items():
    write(name + "_good", data_type, values)
    write(name + "_nullable", data_type, values + [None])
    write(name + "_null", data_type, [None])

# These are the original issue's wire types; V2 safely casts their valid values.
write("int32", pa.int32(), [1, 0])
write("int32_null", pa.int32(), [None])
write("decimal_string", pa.string(), ["1.50", "-2.25"])
write("bad_bool", pa.string(), ["not-a-boolean"])
write("bad_decimal", pa.string(), ["not-a-decimal"])
# Exceed the declared precision without overflowing the source Arrow decimal's bit width.
write("overflow_decimal128", pa.decimal256(76, 2), [Decimal("1" + "0" * 40 + ".00")])
write("overflow_decimal256", pa.string(), ["1" + "0" * 74 + ".00"])
write("scale_loss_decimal", pa.decimal128(10, 3), [Decimal("1.234")])
write("bad_array", pa.list_(pa.string()), [["not-a-boolean"]])
write("bad_map", pa.map_(pa.string(), pa.string()), [[("x", "not-a-boolean")]])
write("bad_struct", pa.struct([("b", pa.string())]), [{"b": "not-a-boolean"}])
write("short_struct", pa.struct([]), [{}])
write(
    "long_struct",
    pa.struct([("b", pa.bool_()), ("extra", pa.bool_())]),
    [{"b": True, "extra": False}],
)
write("array_nested_null", pa.list_(pa.bool_()), [[True, None]])
write("map_nested_null", pa.map_(pa.string(), pa.bool_()), [[("x", None)]])
write("struct_nested_null", pa.struct([("b", pa.bool_())]), [{"b": None}])
# The first stream produces rows before the second stream fails. Neither may commit.
(ROOT / "bool_late_failure.arrow").write_bytes(
    stream(pa.bool_(), [True]) + stream(pa.string(), ["not-a-boolean"])
)
(ROOT / "decimal_late_failure.arrow").write_bytes(
    stream(pa.decimal128(38, 2), [Decimal("1.50")])
    + stream(pa.string(), ["not-a-decimal"])
)
