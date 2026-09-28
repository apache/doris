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


def zero_norm_table():
    vector_type = pa.list_(pa.field("item", pa.list_(pa.float32(), 2), nullable=False))
    return pa.Table.from_arrays([
        pa.array([1, 2, 3, 4], type=pa.int64()),
        pa.array([[[0, 0]], [[1, 0]], [[0, 0], [0, 1]], [[0, 0]]], type=vector_type),
    ], schema=pa.schema([pa.field("row_id", pa.int64(), nullable=False),
                         pa.field("vectors", vector_type)]))


def build_zero_norm(output):
    table = zero_norm_table()
    lance.write_dataset(table.slice(0, 3), str(output), data_storage_version="2.2")
    ds = lance.dataset(str(output))
    ds.create_index("vectors", index_type="IVF_FLAT", metric="cosine", num_partitions=1)
    # An all-zero appended fragment must not abort a query over valid indexed rows.
    lance.write_dataset(table.slice(3), str(output), mode="append", data_storage_version="2.2")


def check(output):
    check_coverage(output.parent)
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
    zero = lance.dataset(str(output.with_name("multivector_zero.lance")))
    assert zero.to_table().equals(zero_norm_table())
    assert len(zero.get_fragments()) == 2
    assert zero.list_indices()[0]["fragment_ids"] == {0}
    assert invalid.num_rows == 1
    assert invalid["null_elements"][0].as_py() == [[None, 0.0]]
    assert np.isnan(invalid["nan_elements"][0].as_py()[0][0])
    assert np.isposinf(invalid["inf_elements"][0].as_py()[0][0])


# Binary fractions keep the three element types comparable without hiding dimension bugs.
DIMENSIONS = (1, 3, 8, 128)
REPRESENTATIVE_ROWS = 768
REPRESENTATIVE_DIMENSION = 128


def dimension_vectors(row_id, dimension):
    if row_id == 5:
        return []
    if row_id == 6:
        return None
    return [[((row_id * 3 + subvector * 5 + j * 7) % 23 - 11) / 16.0
             for j in range(dimension)] for subvector in range(1 + row_id % 4)]


def dimension_table():
    arrays = [pa.array(range(1, 7), type=pa.int64())]
    fields = [pa.field("row_id", pa.int64(), nullable=False)]
    for bits, dtype in [(16, pa.float16()), (32, pa.float32()), (64, pa.float64())]:
        for dim in DIMENSIONS:
            datatype = pa.list_(pa.field("item", pa.list_(dtype, dim), nullable=False))
            fields.append(pa.field(f"v{bits}_d{dim}", datatype))
            arrays.append(pa.array([dimension_vectors(i, dim) for i in range(1, 7)], type=datatype))
    # These schemas are deliberately unsupported, even when every stored value is valid.
    for name, dtype, nullable in [("integer_vectors", pa.int8(), False),
                                   ("nullable_vectors", pa.float32(), True)]:
        datatype = pa.list_(pa.field("item", pa.list_(dtype, 3), nullable=nullable))
        fields.append(pa.field(name, datatype))
        arrays.append(pa.array([[[1, 2, 3]]] * 6, type=datatype))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def representative_vector(row_id, subvector):
    return [((row_id * 17 + subvector * 29 + j * 13 + j * j * 7 + row_id * j * 3)
             % 1009 - 504) / 512.0 for j in range(REPRESENTATIVE_DIMENSION)]


def representative_table(fragment):
    # Interleave IDs across physical fragments so a global result cannot concatenate local TopK.
    ids = list(range(fragment + 1, REPRESENTATIVE_ROWS + 1, 3))
    datatype = pa.list_(pa.field("item", pa.list_(pa.float32(), REPRESENTATIVE_DIMENSION), nullable=False))
    return pa.Table.from_arrays([
        pa.array(ids, type=pa.int64()),
        pa.array([f"document-{i}" for i in ids]),
        pa.array([None if i % 11 == 0 else f"note-{i}" for i in ids]),
        pa.array([[representative_vector(i, sub) for sub in range(1 + i % 4)] for i in ids], type=datatype),
    ], schema=pa.schema([pa.field("row_id", pa.int64(), nullable=False),
                         pa.field("label", pa.string()), pa.field("note", pa.string()),
                         pa.field("vectors", datatype)]))


def invalid_types_table():
    fields, arrays = [], []
    for bits, dtype in [(16, pa.float16()), (32, pa.float32()), (64, pa.float64())]:
        datatype = pa.list_(pa.field("item", pa.list_(dtype, 3), nullable=False))
        for name, value in [("null", None), ("nan", float("nan")), ("inf", float("inf"))]:
            fields.append(pa.field(f"{name}{bits}", datatype))
            arrays.append(pa.array([[[value, 0, 1]]], type=datatype))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def build_coverage(root):
    lance.write_dataset(invalid_types_table(), str(root / "multivector_invalid_types.lance"), data_storage_version="2.2")
    lance.write_dataset(dimension_table(), str(root / "multivector_dimensions.lance"), data_storage_version="2.2")
    for kind in ("IVF_FLAT", "IVF_PQ"):
        output = str(root / f"multivector_{kind.lower()}.lance")
        lance.write_dataset(representative_table(0), output, data_storage_version="2.2")
        options = dict(num_sub_vectors=8, num_bits=4) if kind == "IVF_PQ" else {}
        ds = lance.dataset(output)
        ds.create_index("vectors", index_type=kind, metric="cosine", num_partitions=4, **options)
        lance.write_dataset(representative_table(1), output, mode="append", data_storage_version="2.2")
        lance.dataset(output).optimize.optimize_indices(num_indices_to_merge=0)
        lance.write_dataset(representative_table(2), output, mode="append", data_storage_version="2.2")
    check_coverage(root)


def check_coverage(root):
    invalid = lance.dataset(str(root / "multivector_invalid_types.lance")).to_table()
    assert invalid.schema == invalid_types_table().schema
    for bits in (16, 32, 64):
        assert invalid[f"null{bits}"][0].as_py() == [[None, 0.0, 1.0]]
        assert np.isnan(invalid[f"nan{bits}"][0].as_py()[0][0])
        assert np.isposinf(invalid[f"inf{bits}"][0].as_py()[0][0])
    assert lance.dataset(str(root / "multivector_dimensions.lance")).to_table().equals(dimension_table())
    expected = pa.concat_tables([representative_table(i) for i in range(3)])
    for kind in ("IVF_FLAT", "IVF_PQ"):
        ds = lance.dataset(str(root / f"multivector_{kind.lower()}.lance"))
        assert ds.to_table().equals(expected)
        assert len(ds.get_fragments()) == 3
        indices = ds.list_indices()
        assert len(indices) == 2, indices
        assert all(index["type"] == kind for index in indices), indices
        assert {frozenset(index["fragment_ids"]) for index in indices} == {frozenset({0}), frozenset({1})}
        stats = ds.index_statistics(indices[0]["name"])
        assert stats["num_indexed_rows"] == 512 and stats["num_unindexed_rows"] == 256, stats
        assert all(index["num_partitions"] == 4 for index in stats["indices"]), stats


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
    build_zero_norm(output.with_name("multivector_zero.lance"))
    build_coverage(output.parent)
    check(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    build(parser.parse_args().output)
