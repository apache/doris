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

# Hive Split early-stop acceptance tools

These tools make `test_hive_split_early_stop` reproducible on an isolated FE/BE
cluster and an existing HMS. Run commands from the Doris repository root.
Use Python 3.10+ and JDK 17. Build Doris through `build.sh`; this directory's
build script compiles only a test observer.

The default fixture serves 100 distinct, valid one-row Parquet paths per partition.
`prepare.py` creates 10,000 real HMS partitions: this tests one million **logical
file paths**, without creating one million physical objects. It supplies accurate
HMS row-count metadata so statistics preparation does not enumerate files before
Split initialization. Report this distinction when sharing results.

1. Install dependencies and compile the observer into an output directory:

   ```bash
   split_test_output="$PWD/output/hive-split-early-stop"
   python3 -m venv "$split_test_output/venv"
   "$split_test_output/venv/bin/pip" install -r regression-test/script/hive_split_early_stop/requirements.txt
   JAVA_HOME=/path/to/jdk17 bash regression-test/script/hive_split_early_stop/build_probe.sh \
       "$split_test_output"
   ```

2. Add these options to the isolated FE's existing `JAVA_OPTS_FOR_JDK_17` in its
   runtime `output/fe/conf/fe.conf`, substituting absolute paths, then start FE/BE
   with non-conflicting ports. Wait at least 30 seconds and check BE health.

   ```text
   --add-exports=java.base/jdk.internal.org.objectweb.asm=ALL-UNNAMED
   -javaagent:/absolute/output/hive-split-early-stop/split-probe.jar=/absolute/output/hive-split-early-stop/split-probe.csv
   ```

   The observer retains assignments until FE exit, allowing explicit queue,
   listener, resource and producer cleanup to be checked independently of GC.
   Use it only for this isolated test. For this FE with BE 4.1.3, set the block
   execution version to 10 and disable the newer local-shuffle planner in the
   isolated cluster. Otherwise a multi-BE aggregation can fail before acceptance
   completes because BE 4.1.3 cannot deserialize newer block versions:

   ```sql
   ADMIN SET FRONTEND CONFIG ("be_exec_version" = "10");
   SET GLOBAL enable_local_shuffle_planner = false;
   ```

   These compatibility settings belong to the test environment. Remove the
   observer options after testing.

3. Start the fixture in another terminal; choose a free port reachable from both
   FE and every BE. A remote BE needs a routable `--host` and endpoint address.

   ```bash
   "$split_test_output/venv/bin/python" regression-test/script/hive_split_early_stop/fixture.py \
       --host 127.0.0.1 --port 39367 --output "$split_test_output"
   ```

4. Prepare a dedicated catalog and HMS database. The script preserves existing
   tables and refuses to reuse an unrelated table or existing catalog. To rerun,
   use a new catalog name; the matching fixture HMS table can be reused. An FE
   password can be supplied through `DORIS_TEST_PASSWORD`. Generated connection
   configuration has mode `0600` and stays in `output/`.

   ```bash
   "$split_test_output/venv/bin/python" regression-test/script/hive_split_early_stop/prepare.py \
       --hms-host 127.0.0.1 --hms-port 28650 \
       --fe-host 127.0.0.1 --fe-port 40167 --fe-http-port 39167 \
       --fixture-url http://127.0.0.1:39367 --catalog hive_split_early_stop \
       --output "$split_test_output"
   ```

   Keep the default 100 files per partition for the committed regression results.
   `--file-cache` enables the shared Hive file-list cache for additional checks.
   No global Doris settings are changed by `prepare.py`.

5. Run the regression and validate its resource observations:

   ```bash
   ./run-regression-test.sh --run --conf "$split_test_output/regression-conf.groovy" \
       -d external_table_p0/hive -s test_hive_split_early_stop -parallel 1 \
       > "$split_test_output/regression.log" 2>&1
   python3 regression-test/script/hive_split_early_stop/report.py --output "$split_test_output"
   ```

   The suite checks LIMIT, cancellation and query timeout during both execution
   and first-Split waiting, plus first-Split deadlines of 1000 and 2500 ms. It
   waits for a real BE GET before execution-stage cancellation. After the
   10-second exit deadline it observes another second for new LIST requests.
   `report.py` checks the complete scenario sequence, stable producer termination,
   empty buffers/listeners/resources, fresh samples and no late LIST requests.
   A successful SQL return alone is insufficient. Results are in `results.json`.
   Use the shell entry point's `--conf`; its internal Java runner's `-cf` option
   does not override the configuration selected by the shell script.

6. After the queries finish, snapshot the callback registry:

   ```bash
   "$JAVA_HOME/bin/java" --add-modules jdk.attach -jar "$split_test_output/split-probe.jar" \
       "$(cat output/fe/bin/fe.pid)" "$split_test_output/split-probe.jar" "$split_test_output/registry.csv"
   ```

   With no other queries running, `registry.csv` should contain only its header.
   Stop the fixture with Ctrl-C, stop the isolated cluster, and remove the FE agent
   options. HMS metadata is retained for debugging; the tool does not drop tables.

For an existing physical dataset, start `fixture.py --upstream http://S3_HOST:PORT`
to observe real requests, then configure a dedicated catalog for that proxy and
the existing HMS table. Preserve the original S3 credentials: the proxy preserves
the request Host and path used for signing. `prepare.py` intentionally refuses
this mode because it only creates logical-fixture metadata. Empty/delayed listings
are test controls for first-Split waiting; document those controls separately from
normal reads of physical data. Verify the actual partition and object counts
before describing a dataset as a physical million-file benchmark.

To check cross-query cache isolation, prepare another catalog with the same HMS
database/table and `--file-cache`, writing its config into a child directory:

```bash
"$split_test_output/venv/bin/python" regression-test/script/hive_split_early_stop/prepare.py \
    --hms-host 127.0.0.1 --hms-port 28650 \
    --fe-host 127.0.0.1 --fe-port 40167 --fe-http-port 39167 \
    --fixture-url http://127.0.0.1:39367 --catalog hive_split_early_stop_cache \
    --file-cache --output "$split_test_output/cache"
./run-regression-test.sh --run --conf "$split_test_output/cache/regression-conf.groovy" \
    -d external_table_p0/hive -s test_hive_split_cancel_cache_isolation -parallel 1 \
    > "$split_test_output/cache/regression.log" 2>&1
python3 regression-test/script/hive_split_early_stop/report.py --output "$split_test_output" \
    --concurrency-log "$split_test_output/cache/regression.log" --registry "$split_test_output/registry.csv"
```

Run the registry snapshot after both suites. The cache test holds both SDK
directory-list calls at a barrier, cancels one query, then immediately releases
the calls. The other query must return the complete result, both assignments must
dispose their resources, and a follow-up query must reuse the completed listing.
This barrier synchronizes the cache race; normal request cessation is checked by
the seven-scenario suite. Run these suites sequentially because they control the
same fixture. The committed cache result assumes 100 files per partition.

Run the analyzer's negative tests with:

```bash
"$split_test_output/venv/bin/python" -m unittest discover \
    -s regression-test/script/hive_split_early_stop -p 'test_*.py' -v
```
