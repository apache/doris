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

"""Build deterministic multi-vector fixtures with an independent distance oracle."""

import argparse
from pathlib import Path

import lance
import numpy as np
import pyarrow as pa


QUERY = [[1.0, 0.0], [0.0, 1.0]]
ROWS = [
    [[1.0, 0.0], [0.0, 1.0]],
    [[2.0, 0.0]],
    [[3.0, 0.0], [0.0, 3.0]],
    [],
    None,
    [[1.0, 1.0]],
]


def expected_table():
    fields = [pa.field("row_id", pa.int64(), nullable=False)]
    for name, dtype in [("vectors16", pa.float16()), ("vectors32", pa.float32()),
                        ("vectors64", pa.float64())]:
        vector = pa.list_(pa.field("item", dtype), 2)
        fields.append(pa.field(name, pa.list_(pa.field("item", vector, nullable=False))))
    for name in ["tiny_vectors", "batch_vectors"]:
        fields.append(pa.field(name, pa.list_(pa.field("item", pa.list_(pa.float32(), 2), nullable=False))))
    tiny = [[[0.00015, 0]], [[0.0001, 0]], [[0.0002, 0]], [], None, [[0.001, 0]]]
    batches = [[[1, 0]], [[0, 1]], [[1, 1]], [], None, [[2, 1]]]
    return pa.Table.from_pylist([
        dict(row_id=i + 1, vectors16=row, vectors32=row, vectors64=row,
             tiny_vectors=tiny[i], batch_vectors=batches[i])
        for i, row in enumerate(ROWS)
    ], schema=pa.schema(fields))


def distances(query=QUERY, metric="l2"):
    q = np.asarray(query, dtype=np.float64)
    result = []
    for row_id, row in enumerate(ROWS, 1):
        if not row:
            continue
        v = np.asarray(row, dtype=np.float64)
        if metric == "l2":
            pairs = ((q[:, None, :] - v[None, :, :]) ** 2).sum(axis=2)
        elif metric == "cosine":
            pairs = 1 - (q @ v.T) / (np.linalg.norm(q, axis=1)[:, None]
                                       * np.linalg.norm(v, axis=1)[None, :])
        elif metric == "dot":
            pairs = 1 - q @ v.T
        else:
            raise ValueError(metric)
        result.append((row_id, float(pairs.min(axis=1).sum())))
    return sorted(result, key=lambda pair: (pair[1], pair[0]))


def check(output):
    ds = lance.dataset(str(output))
    assert ds.to_table().equals(expected_table())
    assert len(ds.get_fragments()) == 2
    indices = ds.list_indices()
    assert {tuple(index["fields"]) for index in indices} == {
        ("vectors16",), ("vectors32",), ("vectors64",), ("batch_vectors",)}
    assert all(index["type"] == "IVF_FLAT" and index["fragment_ids"] == {0}
               for index in indices)
    assert distances() == [(1, 0.0), (6, 2.0), (2, 6.0), (3, 8.0)]
    invalid = lance.dataset(str(output.with_name("multivector_invalid.lance"))).to_table()
    assert invalid.num_rows == 1
    assert invalid["null_elements"][0].as_py() == [[None, 0.0]]
    assert np.isnan(invalid["nan_elements"][0].as_py()[0][0])
    assert np.isposinf(invalid["inf_elements"][0].as_py()[0][0])


def build(output):
    table = expected_table()
    lance.write_dataset(table.slice(0, 3), str(output), data_storage_version="2.2")
    # Keep null/empty rows outside the index; append also exercises mixed index coverage.
    ds = lance.dataset(str(output))
    for column in ["vectors16", "vectors32", "vectors64", "batch_vectors"]:
        ds.create_index(column, index_type="IVF_FLAT", metric="cosine", num_partitions=1)
    lance.write_dataset(table.slice(3), str(output), mode="append", data_storage_version="2.2")
    invalid_type = pa.list_(pa.field("item", pa.list_(pa.float32(), 2), nullable=False))
    invalid = pa.Table.from_arrays([
        pa.array([[[None, 0]]], type=invalid_type),
        pa.array([[[float("nan"), 0]]], type=invalid_type),
        pa.array([[[float("inf"), 0]]], type=invalid_type),
    ], names=["null_elements", "nan_elements", "inf_elements"])
    lance.write_dataset(invalid, str(output.with_name("multivector_invalid.lance")), data_storage_version="2.2")
    check(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    build(parser.parse_args().output)
