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

# Read resource index in the Doris Ranger plugin

The Doris controller can answer eligible column ADMIN/SELECT checks by looking
up the table and user, then invoking the existing Ranger resource matcher for
the relevant blocks. This avoids repeated policy candidate selection and scans
of unrelated blocks in large per-user policies. It extends the previous resource
descriptor sharing and negative parent-scope summary.

Ranger sources, artifacts and dependency versions stay unchanged. The plugin
uses public getters on the loaded `RangerPolicyEngineImpl` to obtain its native
resource evaluators and matchers. This adapter is isolated in
`RangerReadResourceIndex`; a different engine implementation falls back to the
ordinary API. These getters are version-coupled library APIs, so dependency
upgrades still require the native differential tests.

## Query path

```text
RangerDorisAccessController.checkColumns
  -> existing global/catalog/database/table checks
  -> checkPrivilegeByPlugin for each required column action
     -> mayGrantReadPrivilege: omit provably impossible parent checks
     -> indexedReadPrivilege: eligible column checks only
        -> table key
        -> user ID and binary search
        -> native matcher.getMatchType on that user's blocks for this table
     -> original isAccessAllowed when an optimization is unavailable
```

The index returns a Boolean only at the controller's existing Boolean boundary.
It does not override `RangerBasePlugin.isAccessAllowed`, manufacture production
`RangerAccessResult` objects, or replace native policy/audit result metadata.

The original upper-scope grants and ADMIN-or-SELECT requirement remain intact.
SHOW visibility, data masking and row filtering continue through their existing
paths. User-named fake columns retain their ordinary Ranger meaning because
the original column matcher is used.

## Memory layout

```text
ReadScopes for one installed engine/version/config
  + existing negative scope summary
  + Map<user name, integer ID>
  + Map<(catalog, database, table), TableAccess>
      TableAccess
        int[] user IDs, sorted
        byte[] ADMIN/SELECT flags
        RangerPolicyResourceMatcher[] references to existing native matchers
  + TableAccess for global/parent/pattern resources, also sorted by user ID
  + Set<user name> requiring native evaluation because of non-ASCII policy resources
```

The new index does not copy column names, column sets, resource matchers or
policies. Multiple resource blocks and policies can contribute entries for the
same user/table; a column action succeeds if a relevant native matcher accepts
the full column resource and the direct user item grants the action.

With compressed object references, the three arrays hold approximately nine
payload bytes per user/resource binding, plus array headers, table keys and the
user dictionary. The build refuses more than four million bindings and falls
back to the previous implementation. There is no unbounded query-result cache
or expiry-based authorization decision.

## Eligibility and invalidation

The complete snapshot must first pass `RangerReadScopeSummary` eligibility:
static direct-user access policies with no deny/exception, group/role grants,
conditions, validity schedules, tags, zones, custom matchers, transformations or
other unsupported service overrides. A policy can disable eligibility for the
whole snapshot.

Single, exact ASCII catalog/database/table values use the table buckets.
Global, parent, wildcard and multi-value ASCII resource prefixes use a separate
per-user matcher bucket. A global administrator policy therefore does not disable
employee indexing. Non-ASCII or pattern-bearing request prefixes and non-ASCII
request columns fall back to the ordinary API. ASCII column patterns are matched
by Ranger.

Native resource-trie lookup and final resource matching can disagree on Unicode
case folding. For example, a policy database named `\u017fales` and a request for
`sales` are rejected by the native trie even though the final case-insensitive
matcher accepts them. Reusing that matcher without native candidate lookup must
not turn a native refusal into a grant. Therefore any enabled read policy block
containing a non-ASCII resource value marks its direct read users for native
evaluation. Other users retain the index; full and copy-on-update delta refreshes
recompute this set. A non-ASCII user name alone is supported, but using that name
as a fake column makes it a non-ASCII resource value and triggers this fallback.

In-place policy updates disable this
index because they can mutate the matchers referenced by a previously published
snapshot. Normal full replacement and verified copy-on-update deltas rebuild
the index from the installed engine.

`setPolicies` clears the old `ReadScopes` before installation. The new summary
and index are published together only after installation and version checks.
Queries during rebuilding fall back; engine, version and effective-config
identity must still agree at lookup. Cleanup clears the snapshot reference.

Registered result processors and runtime super-user/group overrides also force
the native path. In particular, installing an audit result processor preserves
the original calls and their callbacks instead of granting through the index.

The optional property `ranger.plugin.doris.read.resource.index.enabled` defaults
to `true`. Set it to `false` before policy installation to retain descriptor
sharing and the original scope-only optimization. Values other than the exact
strings `true` and `false` are rejected.

Successful construction logs the policy version, table count, dictionary user
count, binding count and `nativeUsers` count. Existing native failure logs remain
intact; DEBUG logs distinguish
indexed controller decisions from raw Ranger requests. No production metric or
per-query atomic counter was introduced.

## Correctness checks

`RangerReadResourceIndexTest` compares indexed answers with the unmodified native
engine, including multiple users/items/blocks, disabled policies/accesses,
column patterns, fake columns, resource access restrictions, case options,
catalog/database isolation and coexisting global/parent/pattern policies.
It also covers controller allow/deny, audit fallback,
full/delta revocation, runtime super users, concurrent refresh, failed index
configuration and cleanup. The existing authorization behavior baseline is not
regenerated.

`RangerReadResourceIndexCorrectnessTest` adds controlled full/delta publication
gaps and close/refresh interleavings using latches, independently installed native
controller comparisons across randomized full and mixed delta snapshots, audit
result-sequence equivalence, actual in-place revocation, binding-limit fallback,
failed-install rejection, native delegate-admin pruning and Unicode candidate
selection parity. See `READ_RESOURCE_INDEX_CORRECTNESS.txt` for the reproduced
defect, regression evidence and current validation limits.

## Reproduce the comparison

First package the current plugins with the Maven build cache disabled, as in
the parent benchmark README. Supply a frozen pre-index `runtime` directory
containing the five Doris jars produced by the portable benchmark runner.

```bash
python3 fe/fe-authorization/benchmarks/run_read_index_benchmark.py \
  --classpath-file /path/to/native-ranger-classpath.txt \
  --service-def /path/to/doris-servicedef.json \
  --baseline-runtime /path/to/pre-index-benchmark/runtime \
  --destination /path/to/new-result-directory \
  --users 260 500 3000 --warmups 20 --samples 120 --heap 12g \
  --variants scope sr indexed
```

The variants run serially in separate JVMs with identical dependencies, input
policies, query seeds and heap settings:

The comparison runner includes one bootstrap global ADMIN policy in addition
to the employee policies; its administrator is not one of the sampled employees.
The legacy runner leaves this extra policy disabled, retaining its original
raw metadata comparison assumptions.

| Variant | Authorization sequence |
|---|---|
| `stock` | Original native Ranger, Doris ADMIN-or-SELECT scope cascade |
| `scope` | Frozen descriptor-sharing and scope-summary implementation |
| `sr` | Native per-column SELECT calls modeled after the inspected SR code |
| `indexed` | New Boolean controller index with native matcher references |

`sr` isolates the call sequence using the same Doris service definition and
policies; it is not a StarRocks FE, audit, planner or SQL benchmark.

The new runner compares the final request verdict fingerprint across variants.
The indexed adapter uses synthetic Boolean results only to fit the measurement
interface; result metadata is deliberately excluded and labeled
`controller-boolean`. The original `run_benchmark.py` continues comparing raw
result metadata with the resource index disabled in its scope adapter.

Retained heap includes the original native engine and the new index. Request
measurements exclude login, SQL planning/execution, Admin HTTP downloads and an
audit sink. Full refresh checks old-grant revocation and new-grant acquisition;
queries during index rebuilding can temporarily pay the native matching cost.
See `READ_RESOURCE_INDEX_RESULTS.txt` for measured numbers and their limits.
