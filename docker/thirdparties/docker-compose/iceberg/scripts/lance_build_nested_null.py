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

"""Generate the nested Null regression fixture with pylance 7.0.0 and PyArrow 21.0.0."""

import argparse
from pathlib import Path

import lance
import pyarrow as pa


def build(output: Path) -> None:
    null_struct = pa.struct([("empty", pa.null()), ("value", pa.int32())])
    schema = pa.schema([
        ("id", pa.int32()),
        ("null_list", pa.list_(pa.null())),
        ("null_large_list", pa.large_list(pa.null())),
        ("null_fixed_list", pa.list_(pa.null(), 2)),
        ("null_struct", null_struct),
        ("nested_list", pa.list_(null_struct)),
        ("null_map", pa.map_(pa.string(), pa.null())),
    ])
    rows = [
        {"id": 1, "null_list": [None, None], "null_large_list": [None],
         "null_fixed_list": [None, None], "null_struct": {"empty": None, "value": 10},
         "nested_list": [{"empty": None, "value": 11}, {"empty": None, "value": 12}],
         "null_map": [("a", None), ("b", None)]},
        {"id": 2, "null_list": None, "null_large_list": None, "null_fixed_list": None,
         "null_struct": {"empty": None, "value": 20}, "nested_list": None, "null_map": None},
        {"id": 3, "null_list": [], "null_large_list": [],
         "null_fixed_list": [None, None], "null_struct": {"empty": None, "value": 30},
         "nested_list": [], "null_map": []},
        {"id": 4, "null_list": [None], "null_large_list": [None, None, None],
         "null_fixed_list": [None, None], "null_struct": {"empty": None, "value": 40},
         "nested_list": [{"empty": None, "value": 41}], "null_map": [("c", None)]},
    ]
    # pylance 7 cannot encode a null parent struct with a Null child; BE tests cover that shape.
    table = pa.Table.from_pylist(rows, schema=schema)
    dataset = lance.write_dataset(table, str(output), data_storage_version="2.2")
    assert dataset.to_table().equals(table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    build(parser.parse_args().output)
