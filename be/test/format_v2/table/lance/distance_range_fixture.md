<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Distance range fixture

Synthetic single-vector data generated with `pylance==7.0.0`, using Lance data
format 2.2 and an IVF_FLAT V3 index. No external service is required to read it.
Rows 0–127 are indexed in fragment 0. Rows 128–130 were appended after indexing
in fragment 1. For query `[127, 0, 0]`, range `[1, 4)` returns rows 126 and 128,
which exercises both index coverage and the unindexed tail.

Regenerate from the repository root:

```python
import lance
import pyarrow as pa

path = "be/test/format_v2/table/lance/data/distance_range.lance"
schema = pa.schema([
    pa.field("row_id", pa.int64(), nullable=False),
    pa.field("embedding", pa.list_(pa.float32(), 3), nullable=False),
])

def batch(ids):
    return pa.Table.from_pydict({
        "row_id": list(ids),
        "embedding": [[float(i), 0.0, 0.0] for i in ids],
    }, schema=schema)

dataset = lance.write_dataset(batch(range(128)), path, mode="overwrite",
                              data_storage_version="2.2")
dataset.create_index("embedding", index_type="IVF_FLAT", num_partitions=1,
                     name="embedding_idx", index_file_version="V3")
lance.write_dataset(batch(range(128, 131)), path, mode="append",
                    data_storage_version="2.2")
```

Delete the previous generated dataset directory before regenerating to keep the
fixture free of obsolete files. Filenames and index UUIDs may change; tests discover
them from the manifest.
