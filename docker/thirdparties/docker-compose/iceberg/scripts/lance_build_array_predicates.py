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

"""Build isolated array pushdown fixtures with lance_fixture_requirements.txt.

The standard MinIO mirror publishes these below warehouse/lance/predicate_arrays.
Each dataset uses relative data/index paths and two eight-row fragments. The
partial dataset appends one unindexed fragment after creating its LabelList index.
"""

import argparse
from pathlib import Path

import lance
import pyarrow as pa


LABELS = [["red"], ["blue"], ["red", "blue"], [], None,
          ["red", "red"], [None, "blue"], ["blue", "red"]] * 2


def build(output):
    if not __debug__:
        raise RuntimeError("Fixture verification requires assertions; do not use python -O")
    if lance.__version__ != "7.0.0":
        raise RuntimeError("Use the pinned pylance 7.0.0 fixture writer")
    output.mkdir(parents=True, exist_ok=True)
    table = pa.table({"id": pa.array(range(16), type=pa.int64()),
                      "labels": pa.array(LABELS, type=pa.list_(pa.string())),
                      "category": pa.array([i % 3 for i in range(16)], type=pa.int32())})
    for name in ["indexed", "partial", "unindexed"]:
        path = output / (name + ".lance")
        if path.exists():
            raise FileExistsError(path)
        first = table.slice(0, 8) if name == "partial" else table
        dataset = lance.write_dataset(first, str(path), max_rows_per_file=8, max_rows_per_group=8)
        if name != "unindexed":
            dataset.create_scalar_index("labels", "LABEL_LIST", name="labels_idx")
            dataset.create_scalar_index("category", "BTREE", name="category_idx")
        if name == "partial":
            lance.write_dataset(table.slice(8), str(path), mode="append", max_rows_per_file=8)
        dataset = lance.dataset(str(path))
        assert dataset.to_table().to_pydict() == table.to_pydict()
        assert len(dataset.get_fragments()) == 2
        for predicate, expected in [
                ("array_contains(labels, 'red')", [0, 2, 5, 7, 8, 10, 13, 15]),
                ("array_contains(labels, 'red') AND array_contains(labels, 'blue')", [2, 7, 10, 15]),
                ("array_contains(labels, 'red') OR array_contains(labels, 'blue')",
                 [0, 1, 2, 5, 6, 7, 8, 9, 10, 13, 14, 15])]:
            for use_index in [False, True]:
                ids = dataset.to_table(columns=["id"], filter=predicate,
                                       use_scalar_index=use_index)["id"].to_pylist()
                assert sorted(ids) == expected, (name, predicate, ids)
        assert len(dataset.list_indices()) == (0 if name == "unindexed" else 2)
    print("Verified array predicate fixtures (indexed, partial, unindexed)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(__file__).parent /
                        "preinstalled_data/lance/predicate_arrays")
    build(parser.parse_args().output)
