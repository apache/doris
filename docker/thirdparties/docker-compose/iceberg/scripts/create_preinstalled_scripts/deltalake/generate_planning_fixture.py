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

"""Generate one million synthetic AddFile actions for planning acceptance, without data objects.

Requires PyArrow and the pinned only-checkpoint-files golden table. Use only metadata
COUNT or the opt-in FE planning test: querying data from this fixture must fail.
"""

import argparse
import copy
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def generate(golden_table, output):
    template = pq.read_table(golden_table / "_delta_log/00000000000000000002.checkpoint.parquet")
    rows = template.to_pylist()
    metadata = next(row for row in rows if row["metaData"])
    protocol = next(row for row in rows if row["protocol"])
    metadata["metaData"]["id"] = "doris-delta-million-addfiles-planning-only"
    metadata["metaData"]["configuration"] = []
    add = next(row for row in rows if row["add"])["add"]
    add.update(size=527, modificationTime=0,
               stats='{"numRecords":1,"minValues":{"id":1},"maxValues":{"id":1},"nullCount":{"id":0}}')
    log = output / "_delta_log"
    log.mkdir(parents=True, exist_ok=True)
    checkpoint = log / "00000000000000000010.checkpoint.parquet"
    with pq.ParquetWriter(checkpoint, template.schema, compression="snappy") as writer:
        writer.write_table(pa.Table.from_pylist([metadata, protocol], schema=template.schema))
        for start in range(0, 1_000_000, 10000):
            batch = [{"add": dict(add, path=f"planning-only-{i:08}.parquet")}
                     for i in range(start, start + 10000)]
            writer.write_table(pa.Table.from_pylist(batch, schema=template.schema))
    (log / "_last_checkpoint").write_text(json.dumps({
        "version": 10, "size": 1_000_002, "sizeInBytes": checkpoint.stat().st_size,
        "numOfAddFiles": 1_000_000,
    }))
    # Keep the checkpoint's own commit for timestamp resolution; older JSON commits are retired.
    meta = copy.deepcopy(metadata["metaData"])
    meta["configuration"] = {}
    meta["format"]["options"] = {}
    actions = [
        {"commitInfo": {"timestamp": 1700000000000}},
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": meta},
    ]
    (log / "00000000000000000010.json").write_text(
        "".join(json.dumps(action) + "\n" for action in actions))
    print(f"Created planning-only fixture at {output}: 1,000,000 AddFiles, "
          f"{checkpoint.stat().st_size} checkpoint bytes; data objects deliberately absent")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--golden-table", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    generate(args.golden_table, args.output)
