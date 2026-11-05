<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Ranger policy memory and authorization benchmark

This standalone tool compares the original Ranger plugin with the Doris plugin's
resource compaction and parent-scope pruning. It is independent of the default
Maven build and adds no production dependency. It measures in-process policy
installation and authorization, **not SQL execution, FE throughput or OIDC login**.
It does not connect to Ranger or start background download threads.

Recorded local measurements and their limits are in [RESULTS.txt](RESULTS.txt).
They include the full authorization sequence and a separate column-only check.

## Runtime policy storage

Ranger keeps an installed policy engine in FE memory. A SQL authorization question
uses that local engine; it does not download the current user's policies or build
a per-user result cache. The engine holds policy evaluators and resource indexes
for the downloaded service snapshot. The admin refresh path can deliver a full
snapshot or policy deltas, and the old engine remains reachable while active
readers finish. Full refresh therefore needs room for old and new state plus
temporary construction objects.

Ranger 2.8/2.9 libraries default `ranger.plugin.<type>.supports.policy.deltas`
to false. An unchanged policy version avoids rebuilding; without compatible
client/admin delta support, a changed version normally delivers a full snapshot.
The benchmark below installs full snapshots directly and does not measure that
transport negotiation.

Resource compaction runs after Ranger's string deduplication and before the
evaluators retain resource maps. It shares equal single-value resource
descriptors within one downloaded payload. Resource maps remain ordinary
HashMaps: alternate map implementations increased allocation in a column-only
diagnostic. HashMaps freshly normalized by string deduplication are reused by
replacing equal descriptor references; other input maps are copied into HashMaps
sized for their resource count. Flags and the values in each OR block stay
intact. The construction pool has a bound of 16384
descriptors per resource level and is discarded after that payload. It never
retains previous engines or creates a global interning cache.

Sharing covers an entire full snapshot, but a delta payload only shares among
the policies in that payload. A sequence of separate user updates does not
re-intern all descriptors against the retained engine. Consequently a memory
reduction measured after a full load or full replacement must not be promised
for an arbitrary long sequence of small deltas. Delta correctness is covered by
the regression tests; the benchmark below measures full replacement costs.

The read-scope summary retains only bounded user/scope contributions, engine
identity and version. It neither caches individual access verdicts nor grants
permissions. It is invalidated before replacement and is published only after
the new engine is installed. Services with unsupported policy features use the
original engine path for the entire snapshot. In particular, enabled group/role
read grants, deny/exception rules, conditions or validity schedules, tag policies,
security zones, custom matchers and resource-name transformations disable scope
pruning. This includes an otherwise simple service that has a default group
policy. Resource compaction remains independent of scope-pruning eligibility.

In the checked Ranger 2.9 runtime, enabled in-place policy deltas can change
permissions without advancing the engine's exposed service policy version. The
version guard then disables the summary, and native Ranger continues deciding
requests until a verifiable full replacement permits rebuilding it. The in-place
regression compares stock and optimized grants, revocation and result policy
versions; it does not manufacture an engine version change. The full-refresh
benchmark uses the default configuration with in-place updates disabled.

## Run

Use JDK 17 and Python 3. First package the actual plugin artifacts, with the build
cache disabled so tests execute:

```bash
mvn -f fe/pom.xml \
  -pl :fe-authorization-plugin-ranger-doris -am package \
  -Dmaven.build.cache.enabled=false
```

Create a UTF-8 file containing the runtime dependency classpath, separated by the
platform classpath separator (`:` on Linux). It must contain one consistent Ranger
version plus its dependencies. For example, this builds a classpath from an
operator-provided FE library directory:

```bash
python3 - /path/to/fe/lib /path/to/ranger-classpath.txt <<'PY'
import os
from pathlib import Path
import sys
Path(sys.argv[2]).write_text(os.pathsep.join(str(p.resolve()) for p in sorted(
    Path(sys.argv[1]).glob('*.jar'))))
PY
```

Use the **Doris service definition exported from the same Ranger version**. The
resource hierarchy, matchers and options affect the result. Example invocation
from the repository root:

```bash
python3 fe/fe-authorization/benchmarks/run_benchmark.py \
  --stock-classpath-file /path/to/ranger-classpath.txt \
  --service-def /path/to/doris-servicedef.json \
  --label ranger-2.9.0 \
  --destination /path/to/new-result-directory \
  --users 260 500 3000 --tables 5000 --tables-per-user 800 \
  --samples 120 --warmups 20 --heap 12g
```

Start with `--users 2 --tables 10 --tables-per-user 4 --samples 8 --warmups 2`
to check the environment. A second run with `--mode column` isolates a single
column question. `--optimized-classpath-file` optionally supplies a different
dependency classpath for the optimized JVM. A new destination is required for
each run; the tool preserves the packaged jars, source hashes and benchmark
sources in `runtime/` and refuses to use stale packaged classes.

The runner starts one stock JVM and one optimized JVM per user count, serially.
Each gets `-Xms256m`, the selected maximum heap, and G1. Compare on a quiet host
and record CPU, JDK and other workloads separately. Identical dependency versions
and JVM flags are necessary for a useful before/after comparison.

## Workload and interpretation

There are 5000 distinct table names by default. Each user gets one SELECT policy
with 800 separate resource blocks selected from those tables. Each block has
catalog, database, table, and three actual columns plus a fourth column named
after the user. That fourth column retains its ordinary Ranger authorization
meaning. Blocks are never joined into a Cartesian product.

The tool uses the real `RangerBasePlugin(RangerPluginConfig)` constructor and
explicit `null_safe.supplier=v2`. It models Doris's read requirement as
**ADMIN OR SELECT**, in that order. `cascade` asks global, catalog, database,
table, then each requested column, stopping on success or the first rejected
column. Half the sampled table requests are allowed and half are denied.
The stock and optimized runs must agree on their verdict/policy/audit fingerprint
and number of logical authorization questions. `nativeEngineCalls` counts the
calls that actually reach Ranger, separately from the logical questions.
Ranger user-store group lookup and context enrichers are explicitly disabled in
this isolated workload, and no audit sink/result processor is registered. These
settings are part of the benchmark conditions and exclude costs and fallback
paths that can exist in an actual FE service.

The optimized adapter mirrors the controller's private Boolean parent checks.
It never grants from the summary, and all column questions reach the original
engine. It assumes no registered result processor, matching this Doris
controller. Production pruning is disabled when a result processor is installed.
Do not generalize synthetic parent result metadata to public Ranger results or
audit callback behavior; those paths have separate regression tests.

JSON results include GC-retained heap above an initialized-plugin baseline,
installation time, sampled heap peaks, main-thread CPU and allocation, latency
P50/P95/P99, and native call counts. Heap peaks are polled every 5 ms; they are
observations, not a JVM capacity upper bound. A full replacement policy snapshot
is then generated while the old engine is still live. The tool checks revocation
of `ad_0:t0`, the new grant on `ad_0:t800` (with default dimensions), and retained
heap after refresh. Installation time is recorded separately from the wider
measurement window. `initialPreparationAndInstallSeconds` also includes the
post-install retained-heap measurement. `fullRefreshPreparationAndInstallSeconds`
includes the revocation/new-grant checks and retained-heap measurement. The
sampled refresh peak covers that entire window, including those checks and
explicit GC; it is not a construction-only peak.

The heap includes the locally generated policy objects retained by the engine.
It excludes the rest of an FE, Ranger user stores, real audit sinks, HTTP/JSON
download buffers and on-disk policy cache I/O. Refresh is an in-memory full
replacement, not an admin-server poll. No concurrent SQL clients are simulated.
Use the output for relative engine cost and memory sizing; validate the actual
FE package and workload before claiming a production SLA. Tail percentiles from
a small sample are diagnostic and require larger repeated runs for capacity
planning.

The summary is invalidated during policy installation and rebuilding, so queries
in that window temporarily use the native path. The steady-request percentiles
above do not measure concurrent SQL tail latency during refresh.
