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

Paimon vector-reader fixture

Generated with Apache Paimon Java 2.1.0. GenerateVectorFixture.java records the
SDK-planned compacted DataFileMeta and serializes BucketVectorSearchSplit; no hand-built
wire bytes or expected result files are used.

Each table is a first-row primary-key table with four compacted Parquet files
containing IDs 0..2047 (file row counts 1000,24,1000,24). Both commits force real
SDK compaction; uncompacted APPEND files are not PK-vector index sources. The vector is [id, 0]. The main table declares embedding NOT NULL;
nullable/ permits NULL and stores a NULL vector at ID 5. SQL distance tests use
the main table; only the TVF tests intentionally skip the nullable row.

schema.json selects L2; schema-dot.json selects inner product on the same files.
Both schemas use FULL search mode and the payloads contain no ANN index sources.
This exercises the real Rust C API search/materialization chain through exact
fallback, without Spark, an object store or a private index factory at test time.
It does not replace coverage for an actual ANN index.

full.bin: all physical rows in all four files.
ranges.bin: physical positions 4..6 in the first file and 1..2 in the others,
            excluding the nearest ID 0. Main table -> IDs 4,5,6,1001,1002,
            1025,1026,2025,2026; nullable table omits the NULL vector at ID 5.
empty.bin: no physical rows allowed in any file.

The pinned snapshot, manifest and schema directories are also SDK-generated.
The native search resolves the pinned snapshot even with preplanned bucket work.

The Java writeUTF bucket path is __BUCKET__. The test replaces it, including its
encoded length, with the fixture directory's absolute path. No generator-machine
path is retained in the serialized table options.

To regenerate, supply Paimon 2.1.0 core/format and their runtime dependencies in
PAIMON_FIXTURE_CP. Use a fresh output directory (the generator does not overwrite
existing files):

    javac -cp "$PAIMON_FIXTURE_CP" -d "$PAIMON_FIXTURE_CLASSES" GenerateVectorFixture.java
    java -cp "$PAIMON_FIXTURE_CLASSES:$PAIMON_FIXTURE_CP" GenerateVectorFixture "$PAIMON_FIXTURE_OUTPUT"

Copy the generated tables to this directory. Paimon chooses new data-file names;
replace old generated Parquet files together with the matching schema and splits.
