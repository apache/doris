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

# Paimon batch-size fixture

Generated with Apache Paimon 1.3.1 as an append-only Parquet table with
`bucket=-1` and `write-only=true`. It contains 65 rows: `k` ranges from 0 to 64,
and every `v` is 128 KiB of the ASCII character `x`.

`schema/schema-0` is the table schema. `split.bin` is a Java `DataSplit`
serialization whose bucket path is the neutral `__BUCKET__` placeholder.
The reader test replaces that Java `writeUTF` field with the fixture's bucket
path at runtime. No catalog, object store, or Java process is needed to read it.

The fixture exercises the actual Rust Arrow reader with small batch sizes and
checks all values across batch boundaries and repeated split preparation.
