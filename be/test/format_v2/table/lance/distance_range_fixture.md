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

## Range fallback fixtures

`distance_range_sq.lance` contains vectors `[i, 0, 0]` for integers 0 through 255,
followed by row 256 with `[0.49, 0, 0]`, and an IVF_SQ V3 index. With query
`[0.51, 0, 0]`, row 256 has squared L2 distance approximately `0.0004`, inside
`[0.0001, 0.001)`. Its quantized distance is zero, so applying the lower bound
before refinement incorrectly discards it.

`distance_range_dot.lance` contains row 0 with `[1, 0, 0]` and 127 rows with
`[0.5, 0, 0]`, and an IVF_FLAT V3 DOT index. Query `[-FLT_MAX, 0, 0]` with only
an inclusive lower bound of `FLT_MAX` must return row 0. Replacing the absent
upper bound with an exclusive `FLT_MAX` incorrectly discards it.

These fixtures exercise fragment splits without index UUIDs, as planned by the
FE for unsafe indexed range searches, with both `use_index=true` and `false`.
They use the same schema and writer versions as the fixture above:

```python
from pathlib import Path

root = Path("be/test/format_v2/table/lance/data")
for name, values, index_type, metric in [
    ("distance_range_sq", [[float(i), 0.0, 0.0] for i in range(256)]
        + [[0.49, 0.0, 0.0]], "IVF_SQ", "l2"),
    ("distance_range_dot", [[1.0, 0.0, 0.0]]
        + [[0.5, 0.0, 0.0] for _ in range(127)], "IVF_FLAT", "dot"),
]:
    data = pa.Table.from_pydict({
        "row_id": list(range(len(values))), "embedding": values,
    }, schema=schema)
    dataset = lance.write_dataset(data, str(root / (name + ".lance")),
                                  data_storage_version="2.2")
    dataset.create_index("embedding", index_type=index_type, metric=metric,
                         num_partitions=1, name="embedding_idx",
                         index_file_version="V3")
```
