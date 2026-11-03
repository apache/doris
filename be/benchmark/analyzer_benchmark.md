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

# Analyzer benchmark

`benchmark_analyzer.hpp` measures the text analyzers on the path the SNII writer takes for one row,
and writes a digest of every row's tokens, so a change to an analyzer can be checked for speed and
for identical output against a baseline binary.

## Build

Build a release benchmark binary of only this header:

```shell
BUILD_TYPE=RELEASE DORIS_BENCHMARK_HEADER=benchmark_analyzer.hpp ./build.sh --benchmark
```

The binary is `be/output/lib/benchmark_test`; copy it away before building the other side. The
baseline is the commit that added this benchmark, on the branch `snii-analysis-bench-baseline`:
build it the same way from that commit.

## Corpora

The corpora are `<name>.rows` files, each row a little-endian u32 length and that many bytes.
They are not in the repository; `analyzer_corpus.py` builds them and lists the sources and
licenses in its header. The comparisons use at least the mixed corpora `http_request`,
`otel_body`, `agent_input` and `prose_en`, and the real Chinese and Japanese text `zh_wiki` and
`ja_wiki`:

```shell
be/benchmark/analyzer_corpus.py wikipedia zh corpus/zh_wiki.rows 8
be/benchmark/analyzer_corpus.py wikipedia ja corpus/ja_wiki.rows 8
be/benchmark/analyzer_corpus.py ndjson documents-171998.json request corpus/http_request.rows
```

## Environment

```shell
export DORIS_HOME=<source tree>
export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}
export ANALYZER_BENCH_CORPUS_DIR=<directory of .rows files>
export ANALYZER_BENCH_CORPUS_MB=8           # bytes used from each corpus file
export ANALYZER_BENCH_DICT_DIR=<output/be/dict of a BE build>
```

The dictionary directory has the layout of a BE package: the jieba files at the top, and `icu`,
`ik`, `pinyin` and `kuromoji` below. Without `ANALYZER_BENCH_CORPUS_DIR` the header registers
nothing.

The benchmarks are named `row/<analyzer>/<corpus>`, a new stream per row as the SNII writer does,
and `reuse/<analyzer>/<corpus>`, one stream across the rows.

## Throughput

Host load moves single measurements by 10 to 20 percent, so the two binaries are compared in
paired rounds and only ratios are reported:

```shell
be/benchmark/analyzer_ab.py --baseline baseline/benchmark_test --candidate new/benchmark_test \
    --filter '^row/' --out ab --rounds 5
```

Each round runs both binaries, alternating which goes first; the report gives, for every analyzer,
the median over the rounds of the candidate/baseline ratio per corpus and their geometric mean.
Run the baseline against itself the same way first (A/A): a ratio further than 5% from 1 means the
host is too noisy for the comparison.

## Identical output

With `ANALYZER_BENCH_DIGEST` set, the binary registers only the benchmark `digest`, which writes one
line per analyzer, corpus and row: the number of terms and a digest of their bytes and positions,
or `threw`. Run it with both binaries over the same corpora and compare the files:

```shell
ANALYZER_BENCH_DIGEST=baseline.digest baseline/benchmark_test --benchmark_filter=digest
ANALYZER_BENCH_DIGEST=new.digest new/benchmark_test --benchmark_filter=digest
diff baseline.digest new.digest
```

Every line of the diff is a row whose tokens changed. Unlike the golden tests in
`be/test/storage/index/inverted/analysis/golden/`, this covers real text and the real kuromoji
dictionary.
