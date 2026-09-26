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

"""Generate selective native UUID row groups and page indexes using Arrow 21+."""
from pathlib import Path
import uuid
import pyarrow as pa
import pyarrow.parquet as pq

values = [None, 0, 1, uuid.UUID("00112233-4455-6677-8899-aabbccddeeff").int,
          (1 << 64) - 1, (1 << 127) - 1, 1 << 127, (1 << 128) - 1]
rows_per_group = 128
uuids = [None if v is None else uuid.UUID(int=v).bytes for v in values
         for _ in range(rows_per_group)]
table = pa.table({"id": pa.array(range(len(uuids)), type=pa.int32()),
                  "u": pa.array(uuids, type=pa.uuid())})
path = Path(__file__).with_name("uuid_predicate.parquet")
pq.write_table(table, path, use_dictionary=False, row_group_size=rows_per_group,
               data_page_size=64, write_batch_size=32, write_page_index=True,
               compression="snappy")
file = pq.ParquetFile(path)
assert str(file.schema.column(1).logical_type) == "UUID"
assert file.metadata.num_row_groups == len(values)
assert file.metadata.row_group(1).column(0).has_column_index
print(path.name, path.stat().st_size, file.metadata.num_row_groups)
