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

"""Regenerate native UUID fixtures with pyarrow 21.0.0 or later.

Run from any directory; outputs are written beside this script. Arrow 19 writes
UUID extension storage without a Parquet UUID annotation and is insufficient.
"""
from pathlib import Path
import uuid
import pyarrow as pa
import pyarrow.parquet as pq

values = [None, 0, 1, (1 << 127) - 1, 1 << 127, (1 << 128) - 1]
rows = 4097
uuids = [None if values[i % len(values)] is None else uuid.UUID(int=values[i % len(values)]).bytes
         for i in range(rows)]
array_storage = pa.array([None if i % 7 == 0 else [] if i % 7 == 1 else [uuids[i], None]
                          for i in range(rows)], type=pa.list_(pa.binary(16)))
array_values = pa.ExtensionArray.from_storage(pa.uuid(), array_storage.values)
array = pa.ListArray.from_arrays(array_storage.offsets, array_values, mask=array_storage.is_null())
struct_storage = pa.array([None if i % 5 == 0 else {"k": uuids[i]} for i in range(rows)],
                          type=pa.struct([pa.field("k", pa.binary(16))]))
struct_values = pa.ExtensionArray.from_storage(pa.uuid(), struct_storage.field("k"))
struct = pa.StructArray.from_arrays([struct_values], names=["k"], mask=struct_storage.is_null())
table = pa.table({"id": pa.array(range(rows), type=pa.int32()),
                  "u": pa.array(uuids, type=pa.uuid()), "a": array, "s": struct})
for dictionary in (False, True):
    path = Path(__file__).with_name("uuid_" + ("dictionary" if dictionary else "plain") + ".parquet")
    pq.write_table(table, path, use_dictionary=dictionary, row_group_size=1024,
                   data_page_size=1024, write_batch_size=64, compression="snappy")
    file = pq.ParquetFile(path)
    assert file.metadata.num_row_groups == 5
    for i in (1, 2, 3):
        column = file.schema.column(i)
        assert column.physical_type == "FIXED_LEN_BYTE_ARRAY"
        assert str(column.logical_type) == "UUID", "Regenerate with Arrow 21 or later"
    assert ("RLE_DICTIONARY" in file.metadata.row_group(0).column(1).encodings) == dictionary
    print(path.name, path.stat().st_size, file.metadata.num_row_groups)
