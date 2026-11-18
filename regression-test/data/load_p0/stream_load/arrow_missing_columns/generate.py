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

"""Regenerate checked-in IPC fixtures with pyarrow.

The regression suite only reads the .arrow files and does not require Python.
kv.arrow carries columns k and v only. kv_then_kv_extra.arrow concatenates two IPC
streams: the first carries k and v, the second k, v and extra.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

ROOT = Path(__file__).resolve().parent


def stream(columns):
    schema = pa.schema([pa.field(name, data_type) for name, data_type, _ in columns])
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, schema) as writer:
        writer.write_batch(
            pa.record_batch(
                [pa.array(values, data_type) for _, data_type, values in columns],
                schema=schema,
            )
        )
    return sink.getvalue().to_pybytes()


def main():
    (ROOT / "kv.arrow").write_bytes(
        stream([("k", pa.int32(), [1, 2, 3]), ("v", pa.utf8(), ["a", None, "c"])])
    )
    (ROOT / "kv_then_kv_extra.arrow").write_bytes(
        stream([("k", pa.int32(), [1, 2]), ("v", pa.utf8(), ["a", None])])
        + stream(
            [
                ("k", pa.int32(), [3]),
                ("v", pa.utf8(), ["c"]),
                ("extra", pa.utf8(), ["carried"]),
            ]
        )
    )


if __name__ == "__main__":
    main()
