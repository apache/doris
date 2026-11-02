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

# Doris FE parser benchmarks

The optional benchmark Maven profile builds JMH benchmarks against the lexer and
syntax parser in fe-core. JMH is excluded from the default FE build and its runtime
artifacts. This branch does not have a standalone fe-sql-parser module.

From the fe/ directory:

```bash
mvn -Pbenchmark -pl fe-sql-parser-benchmark -am package -DskipTests
java -jar fe-sql-parser-benchmark/target/doris-fe-sql-parser-benchmarks.jar \
  '.*StringLiteralBenchmark.*' -prof gc -rf json -rff /tmp/string-literal-benchmark.json
```

Use the same JDK, corpus, JMH arguments, and machine state for baseline and candidate
runs. Run the baseline twice first, and retain raw JSON results and artifact hashes.
StringLiteralTokenCorpus provides deterministic token and error snapshots for a
baseline/candidate comparison.
