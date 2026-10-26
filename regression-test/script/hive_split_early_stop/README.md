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

For a physical million-file benchmark, use a dedicated, empty S3-compatible
bucket. Store its credentials in `SPLIT_S3_ACCESS_KEY` and `SPLIT_S3_SECRET_KEY`
without printing them or putting them on command lines. Create the actual files:

```bash
"$split_test_output/venv/bin/python" regression-test/script/hive_split_early_stop/materialize.py \
    --endpoint http://S3_HOST:PORT --bucket split-physical --process-pool \
    --output "$split_test_output"
```

The defaults create 10,000 partition prefixes with 100 unique Parquet objects
each, containing one `id=1` row per object. Allow for object-store metadata and
filesystem overhead in addition to the 481 MB of Parquet payload (the exact
file size depends on PyArrow). Uploads resume from a partition checkpoint in
the output directory. The tool refuses a nonempty bucket without its ownership
marker or with a different shape or encoding. Use a distinct output directory
for each bucket. `--verify-only` skips uploads and verifies existing objects.

After uploading, the tool lists the complete dataset and checks every key,
per-partition count, size and MD5 ETag. Only successful verification produces
`physical-manifest.json`. Use an S3 implementation whose single-PUT ETags are
MD5 hashes; encrypted or multipart ETags require a different verifier.

Start the fixture with `--bucket split-physical --upstream http://S3_HOST:PORT`
and prepare a new database and catalog using the verified manifest:

```bash
"$split_test_output/venv/bin/python" regression-test/script/hive_split_early_stop/prepare.py \
    --hms-host 127.0.0.1 --hms-port 28650 \
    --fe-host 127.0.0.1 --fe-port 40167 --fe-http-port 39167 \
    --fixture-url http://127.0.0.1:39367 --bucket split-physical \
    --database doris_split_physical --catalog hive_split_physical \
    --physical-manifest "$split_test_output/physical-manifest.json" --output "$split_test_output"
```

The HMS service must also have filesystem access to this bucket, using its own
S3 endpoint and credentials configuration. HMS checks database/table locations
when creating metadata; configuring only the Doris catalog does not give HMS
access to a separate object store.

The proxy preserves the signed Host and path; the catalog uses the same S3
credentials. `prepare.py` requires a verified manifest matching the upstream,
bucket and shape. It supplies accurate row-count metadata for these one-row
objects. The generated regression config selects physical controls: first-Split
waiting delays real upstream LIST responses instead of synthesizing empty
directories. Execution-stage cancellation still waits for a real BE file read.
Run the same seven-scenario suite and report command above. The report includes
the verified physical manifest and identifies the response-delay control.
Keep the dataset unchanged between verification and the queries. Report a
self-created dataset separately from any original production benchmark table.

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

For physical cache isolation, also pass the physical benchmark's `--database`,
`--bucket` and `--physical-manifest` to the cache preparation command. Keep the
same running proxy and credentials. The two readers must access the same real
partition directory.

Run the analyzer's negative tests with:

```bash
"$split_test_output/venv/bin/python" -m unittest discover \
    -s regression-test/script/hive_split_early_stop -p 'test_*.py' -v
```

For performance comparisons, build complete FE packages for the parent of the
early-stop change and the changed revision using `build.sh`. Keep their connector
and filesystem plugins with the corresponding FE; their public API versions
differ. Run the packages sequentially against the same FE metadata and two local
BE processes, with equal JVM settings and no Java agents. Keep BE versions,
catalog properties, object-store resources and data fixed. Do not compile while
sampling queries. The process sampler requires Linux `/proc` and local BE PIDs.

Start the physical proxy in a separate performance output directory. Extend the
generated regression config with these entries, giving every pass a unique run
name, for example `after-a`, `before`, then `after-b`:

```groovy
hiveSplitPerfControlUrl = "http://127.0.0.1:39367"
hiveSplitPerfRun = "after-a"
hiveSplitPerfRepetitions = "3"
hiveSplitPerfCacheTable = "hive_split_physical_cache.doris_split_physical.files_10000_partitions_100_files"
```

Copy the prepared physical benchmark's `hms-fixture.json` and verified
`physical-manifest.json` into the performance output directory. The report binds
these to the running proxy's `fixture.json` before publishing a comparison.

Prepare the cache catalog with the same verified physical table and `--file-cache`.
Verify the object-store endpoint after container restarts; dynamically published
Docker ports can change. Check that a one-partition query returns count=100,sum=100
before sampling. The benchmark resets proxy delays to zero, disables SQL result
and BE file caches, and restores session settings afterward. It distinguishes
refreshed catalog metadata from warm metadata. This does not flush the OS cache,
BE footer caches or JIT compilation state.

```bash
"$split_test_output/venv/bin/python" regression-test/script/hive_split_early_stop/performance.py run \
    --output "$split_test_output/performance" --label after-a \
    --config "$split_test_output/performance/after-a/regression-conf.groovy" --generate
```

`--generate` uses the standard runner's `-forceGenOut` to regenerate this owned
benchmark's expected results. Verify the generated counts, then run subsequent
passes without it to compare results. After stopping one FE and starting the
other, wait at least 30 seconds and check both BEs. Pass its package directory
through `--fe-dir`; `--be-dir` and `--be2-dir` select the two local BE directories.

The seven scenarios cover cold/warm LIMIT 10, SUM cancellation after a real BE
read, a three-second SUM timeout, and complete SUM over 100 partitions / 10000
files using batch, synchronous and completed-list-cache paths. Every early-stop
case observes ten seconds after SQL returns, then waits for file requests to
become idle before the next phase. Existing idle queues are left untouched.
The timeout may fire during planning; cancellation explicitly waits for BE reads.

```bash
python3 regression-test/script/hive_split_early_stop/performance.py report \
    --output "$split_test_output/performance" --before before --after after-b
```

The report checks complete scenarios, real listings, cache hits, BE activity and
process-sample coverage. It records SQL elapsed time, requests after return,
process CPU time and RSS, and reports medians and ranges. RSS is not retained
Java heap. Counts until the next idle phase are also available for early-stop
cases; complete-scan phase totals exclude the separate cache warmup. Use the
initial changed-version pass to assess drift, and compare baseline with the
changed-version repeat after both have exercised the same BE caches. Shared-host
load and framework/proxy overhead limit conclusions about absolute throughput
or small latency differences. No full million-file SUM completion is implied.

To isolate cancellation before BE reads, prepare two more pass configs with
unique run names and `hiveSplitPerfPlanningCancelOnly = "true"`. This mode
refreshes catalog metadata before each repetition and cancels after at least
eight real LIST requests, requiring that no BE GET has started. Run it once per
FE version without regenerating the shared expected results. Include these
passes using `--planning-before before-planning --planning-after after-planning`
in the report command. Compare `return_after_signal_ms` for cancellation response
latency; total SQL elapsed time also includes work before cancellation was sent.

For a separate consecutive LIMIT check, set `hiveSplitPerfSteadyLimitOnly = "true"`
in two additional pass configs. Both execute three untimed all-table LIMIT queries
before three measured queries, without catalog refresh between them. Include
these passes with `--steady-before before-steady --steady-after after-steady`.
Keep this supplementary comparison separate from the main warm-metadata row;
do not replace inconvenient samples or interpret a few overlapping ranges as
proof of a throughput gain or regression.
