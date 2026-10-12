#!/usr/bin/env python3
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

"""Layering guard for BE headers.

A handful of headers -- runtime/exec_env.h above all -- are included, directly or
not, by most of the backend. Anything they pull in becomes a dependency of nearly
every translation unit, so one stray include can multiply rebuild cost by an order
of magnitude and does so silently: the code still compiles, only the build slows
down. This script turns that into a build error instead.

Four kinds of guard, all on the same text include graph (be/src + be/test, all
ifdef branches -- not comparable to ninja-measured radii, each baseline compares
only with itself):

  RULES                    a hub header must not reach a subsystem. Keeping a
                           hub out of a subsystem is what lets that subsystem's
                           headers be edited cheaply.
  ANGLE_BANS               a header must not include a named third-party header
                           whose template machinery was deliberately moved out
                           of line.
  FORWARD/REVERSE budgets  the two-axis safety net for edges no rule names:
                           light hubs must stay light (closure budget, zero
                           slack), heavy payloads must not spread (reach
                           baseline +10%).
  PCH whitelist            pch/pch.h's quoted project includes are pinned
                           exactly; the PCH is the single most leveraged
                           regression surface in the repo.

Usage:
    build-support/check-header-deps.py                  # enforce everything
    build-support/check-header-deps.py --report         # rank headers by rebuild cost
    build-support/check-header-deps.py --budget         # budgets only (rebaselining)
    build-support/check-header-deps.py --closure HEADER # list a hub's closure, with chains
    build-support/check-header-deps.py --reach HEADER   # rank an includer's edge weights
"""

import argparse
import collections
import os
import re
import sys

INCLUDE = re.compile(r'^\s*#\s*include\s+"([^"]+)"')
# Generated headers are conventionally angle-included; they never include project
# headers back, so capturing the edge (without resolving it) is enough to let a
# rule name a gen_cpp header as a forbidden target.
GEN_INCLUDE = re.compile(r"^\s*#\s*include\s+<(gen_cpp/[^>]+)>")
SOURCE_ROOTS = ("be/src", "be/test")
INCLUDE_ROOT = "be/src"

# (hub header, forbidden prefix, allowed exceptions, why).
# A hub must not reach the forbidden subtree through ANY chain of includes, except
# through the listed headers. An exception is only appropriate for a header that
# carries declarations or plain data types and pulls in nothing of its own -- adding
# one should be a deliberate decision, which is exactly why they are listed here
# instead of being inferred.
RULES = [
    (
        "runtime/exec_env.h",
        "storage/index/",
        {
            # A leaf statistics struct with no project includes of its own, which
            # storage/olap_common.h carries as a plain data type.
            "storage/index/inverted/inverted_index_stats.h",
            "storage/index/snii/snii_query_stats.h",
        },
        "ExecEnv only ever names index types as pointers and already forward-declares "
        "them; reaching the index implementation headers from here puts the whole "
        "index writer stack (and CLucene) in front of most of the backend",
    ),
    (
        "runtime/exec_env.h",
        "information_schema/",
        set(),
        "ExecEnv only names RoutineLoadTaskExecutor (forward-declared); the schema "
        "scanner headers carry gen_cpp/FrontendService_types.h, which must not ride "
        "into the ~1000 TUs that include ExecEnv transitively",
    ),
    (
        "runtime/exec_env.h",
        "io/cache/",
        set(),
        "ExecEnv holds the file-cache machinery as pointers and forward-declares "
        "io::FDCache and io::FileCacheFactory; fs_file_cache_storage.h used to drag "
        "gen_cpp/internal_service.pb.h, descriptors.pb.h and the io/fs family into "
        "the ~1060 TUs that include ExecEnv",
    ),
    (
        "runtime/exec_env.h",
        "load/memtable/",
        set(),
        "ExecEnv only names MemTableMemoryLimiter through a unique_ptr member and "
        "accessors (forward-declared, setter defined out of line); the memtable "
        "stack must not ride the ExecEnv superhighway",
    ),
    (
        "runtime/exec_env.h",
        "runtime/frontend_info.h",
        set(),
        "FrontendInfo embeds TFrontendInfo by value, so frontend_info.h carries "
        "gen_cpp/HeartbeatService_types.h and AgentService_types.h; ExecEnv keeps "
        "the frontends map behind a unique_ptr and forward-declares the types",
    ),
    (
        "runtime/exec_env.h",
        "runtime/cluster_info.h",
        set(),
        "ExecEnv only holds ClusterInfo* (forward-declared); cluster_info.h carries "
        "gen_cpp/Types_types.h, which must not enter every TU through this header",
    ),
    (
        "runtime/exec_env.h",
        "util/threadpool.h",
        set(),
        "ExecEnv holds every pool as unique_ptr<ThreadPool> with .get() accessors "
        "and forward-declares the type (assigning setters defined out of line); "
        "threadpool.h carries thread.h, metrics.h and the blocking-queue family, "
        "which must not ride into the ~1060 TUs that include ExecEnv",
    ),
    (
        "runtime/exec_env.h",
        "storage/options.h",
        set(),
        "ExecEnv stores StorePath/CachePath only inside std::vector members and "
        "reference-returning accessors, which work with forward declarations; "
        "options.h carries gen_cpp/Types_types.h and io/cache/file_cache_common.h "
        "into every TU that includes ExecEnv",
    ),
    (
        "common/status.h",
        "gen_cpp/",
        set(),
        "Status is included by essentially every TU; its error-code constants "
        "carry literal values pinned to Status.thrift by static_asserts in "
        "status.cpp, and the TStatus/PStatus converters are declared on "
        "forward declarations with bodies in status.cpp, precisely so no "
        "generated thrift/protobuf header rides this superhighway",
    ),
    (
        "common/exception.h",
        "gen_cpp/",
        set(),
        "Exception only uses ErrorCode constants and Status; it reaches "
        "nearly every TU through status/exception macros and must stay free "
        "of generated headers for the same reason as common/status.h",
    ),
    (
        "util/hash_util.hpp",
        "gen_cpp/",
        set(),
        "hash_util is carried by string_ref.h, column_string.h, "
        "vdatetime_value.h and storage/olap_common.h into most of the "
        "backend; the std::hash specializations for TUniqueId and "
        "TNetworkAddress live with their carriers (util/uid_util.h, "
        "util/network_util.h), so no thrift header is needed here",
    ),
    (
        "runtime/exec_env.h",
        "gen_cpp/",
        set(),
        "ExecEnv reaches ~1060 TUs, so any generated protobuf/thrift header it "
        "pulls in is reparsed by most of the backend; every thrift struct it "
        "stores is behind a pointer or forward declaration (the old "
        "Status_types/types.pb ride-along died with the common/status.h cut)",
    ),
    (
        "runtime/thread_context.h",
        "runtime/workload_group/",
        set(),
        "ThreadContext is included by nearly every TU and only holds WorkloadGroup "
        "through weak_ptr; workload_group.h carries gen_cpp/BackendService_types.h "
        "(the whole thrift type universe), so it must stay out of this superhighway",
    ),
    (
        "runtime/runtime_state.h",
        "runtime/workload_group/",
        set(),
        "RuntimeState only returns WorkloadGroupPtr by declaration; keeping "
        "workload_group.h (and its thrift payload) out of it keeps the exec layer "
        "from re-spreading BackendService_types.h",
    ),
    (
        "runtime/runtime_state.h",
        "io/fs/s3_file_system.h",
        set(),
        "RuntimeState holds the error-log S3 filesystem only behind a shared_ptr "
        "(forward-declared, dereferenced in runtime_state.cpp); s3_file_system.h "
        "carries util/s3_util.h, the AWS SDK surface and gen_cpp/cloud.pb.h, "
        "which must not ride into the ~1060 TUs that include RuntimeState",
    ),
    (
        "runtime/thread_context.h",
        "runtime/workload_management/",
        set(),
        "ThreadContext stores ResourceContext behind a shared_ptr (forward-declared; "
        "attach_task and the orphan fallback are defined out of line) and the "
        "SCOPED/LIMIT macros only expand at call sites; resource_context.h used to "
        "carry the whole workload_management family plus task_controller's "
        "PaloInternalService_types.h into nearly every TU",
    ),
    (
        "runtime/thread_context.h",
        "gen_cpp/",
        {
            # PUniqueId is embedded by value in util/uid_util.h (rides in via
            # mem_tracker_limiter.h); TUniqueId lives in Types_types.h; the
            # profile family rides in through runtime_profile.h held by the
            # memory-tracker chain.
            "gen_cpp/types.pb.h",
            "gen_cpp/Types_types.h",
            "gen_cpp/Metrics_types.h",
            "gen_cpp/RuntimeProfile_types.h",
            "gen_cpp/runtime_profile.pb.h",
        },
        "ThreadContext reaches ~1000 TUs; any generated protobuf/thrift header "
        "beyond the types/profile carriers listed here is reparsed by "
        "most of the backend",
    ),
    (
        "runtime/workload_management/resource_context.h",
        "gen_cpp/data.pb.h",
        set(),
        "resource_context.h references no data.pb symbol (TQueryStatistics is "
        "thrift and forward-declared); this was a dead include spreading PBlock "
        "and segment_v2.pb.h to ~595 TUs through thread_context.h",
    ),
    (
        "core/pod_array.h",
        "runtime/thread_context.h",
        set(),
        "dead include left over from the PODArray memory-tracking experiment "
        "(#50549); the tracking logic since moved into Allocator and pod_array.h "
        "references no thread_context symbol, yet the edge dragged thread_context, "
        "exec_env.h and mem_tracker_limiter.h into 203 TUs of core/",
    ),
    (
        "core/column/column.h",
        "exec/sort/",
        set(),
        "core must not depend on the exec sort machinery: column.h only names "
        "HybridSorter in virtual signatures (forward-declared, bodies in "
        "column.cpp); the old hybrid_sorter.h include was a layering violation "
        "that pushed pdqsort/timsort into 808 TUs "
        "(exec/common/endian.h still rides in via storage/olap_common.h -> "
        "util/hash_util.hpp, a separate pre-existing wart)",
    ),
    (
        "format/parquet/decoder.h",
        "util/rle_encoding.h",
        set(),
        "BaseDictDecoder holds RleBatchDecoder<uint32_t> behind a unique_ptr "
        "(forward-declared; the dtor and every member that dereferences it are "
        "defined in decoder.cpp); the old include made every one of ~530 TUs "
        "that transitively see a parquet decoder instantiate the whole "
        "RLE/BitPacking decode chain at ~0.4 CPU s each",
    ),
    (
        "format_v2/parquet/reader/native/decoder.h",
        "util/rle_encoding.h",
        set(),
        "same contract as format/parquet/decoder.h: the dictionary index "
        "decoder is forward-declared and only decoder.cpp needs the complete "
        "RleBatchDecoder type; keeping rle_encoding.h (and the unrolled "
        "bit_packing.inline.h it carries) out of this header keeps the RLE "
        "instantiation chain out of the native-reader include tree",
    ),
    (
        "exec/pipeline/dependency.h",
        "exec/common/hash_table/",
        {
            # Declarations-only phmap forward header (the sanctioned way through
            # the barrier; its name predates the *_fwd.h convention).
            "exec/common/hash_table/phmap_fwd_decl.h",
        },
        "the SharedState classes hold every DataVariants behind unique_ptr/"
        "shared_ptr with ctors/dtors/close bodies defined in dependency.cpp; "
        "any path back into the hash-table machinery re-instantiates the "
        "Agg/Join/Set variant surface (~0.85 CPU s) in each of the ~128 TUs "
        "that include dependency.h transitively",
    ),
    (
        "exec/pipeline/dependency.h",
        "exec/operator/join/process_hash_table_probe.h",
        set(),
        "dead include: dependency.h references no ProcessHashTableProbe "
        "symbol; the probe machinery belongs to the hash-join TUs that "
        "include process_hash_table_probe_impl.h",
    ),
    (
        "exec/pipeline/dependency.h",
        "util/brpc_closure.h",
        set(),
        "dead include: dependency.h references no brpc symbol, yet this edge "
        "carried runtime/query_context.h, runtime/thread_context.h and "
        "service/brpc.h (1.36 MB of preprocessed payload) into ~100 TUs "
        "whose only other route to them was this header",
    ),
    (
        "exec/pipeline/rec_cte_shared_state.h",
        "exec/common/hash_table/",
        {
            "exec/common/hash_table/phmap_fwd_decl.h",
        },
        "DistinctDataVariants is forward-declared and only touched in "
        "rec_cte_shared_state.cpp (emplace_block's std::visit); the distinct "
        "hash-table family must not ride the rec_cte operator headers into "
        "the pipeline registry TUs",
    ),
    (
        "exec/pipeline/rec_cte_shared_state.h",
        "util/brpc_client_cache.h",
        set(),
        "send_data_to_targets/build_basic_param bodies live in "
        "rec_cte_shared_state.cpp; the brpc client stack must not ride a "
        "SharedState header",
    ),
    (
        "exprs/function/function.h",
        "storage/",
        {
            # Plain result struct (<cstdint> only); the sanctioned carrier left
            # behind when the zonemap machinery edge was cut.
            "storage/index/zone_map/zonemap_filter_result.h",
            # Plain id struct, methods defined in rowset_id.cpp; rides in via
            # core/column/column.h.
            "storage/rowset_id.h",
        },
        "function.h is the base header of every scalar function; cutting its "
        "three storage edges (expr_zonemap_filter, inverted_index_iterator, "
        "inverted_index_parser) removed 122,885 preprocessed lines from the "
        "exprs TUs, and one edge flowing back re-couples the whole expression "
        "layer to the storage stack",
    ),
    (
        "core/field.h",
        "util/json/path_in_data.h",
        set(),
        "a single using-alias here used to drag path_in_data.h and with it "
        "gen_cpp/segment_v2.pb.h (11.6k lines) into every TU that sees a "
        "Field; the alias users include path_in_data.h themselves now",
    ),
    (
        "core/data_type/primitive_type.h",
        "util/json/path_in_data.h",
        set(),
        "primitive_type.h is included by ~44 project headers transitively; "
        "the path_in_data edge would put gen_cpp/segment_v2.pb.h behind the "
        "most basic type-enum header in the backend",
    ),
    (
        "core/value/variant/variant_field.h",
        "util/json/path_in_data.h",
        set(),
        "the field.h cut moved the variant alias down here, so this header "
        "is where the path_in_data edge would most naturally regrow; variant "
        "consumers that need PathInData include it directly",
    ),
    (
        "core/types.h",
        "storage/olap_common.h",
        set(),
        "the int128/uint128 typedefs moved to core/extended_types.h precisely "
        "so core/types.h stops paying for the storage domain; the binary_cast "
        "include chain used to put all of olap_common behind two lines of "
        "typedef",
    ),
    (
        "pch/pch.h",
        "storage/olap_common.h",
        set(),
        "every header on the PCH is on the everything-rebuilds line: touch it "
        "and the whole backend plus the precompiled header itself rebuild; "
        "olap_common.h alone used to carry 22 project headers onto that line",
    ),
]

# Third-party bans, (header, banned include, why). The include graph above only
# follows quoted project includes and <gen_cpp/...>, so forbidden *third-party*
# edges get their own single-file check: the named header must not include the
# banned spelling (a trailing '/' bans the whole directory, otherwise the match
# is exact; angle and quoted forms are both caught). These are headers whose
# formatting/queueing bodies were deliberately moved out of line -- the ban keeps
# the heavy third-party template machinery from re-entering every includer.
ANGLE_BANS = [
    (
        "core/uint24.h",
        "fmt/",
        "to_string/to_buffer bodies live in uint24.cpp precisely so the "
        "FMT_COMPILE formatter templates (53.5 CPU s over ~1150 TUs for the "
        "date format alone) are instantiated once instead of in every includer",
    ),
    (
        "core/value/large_int_value.h",
        "fmt/",
        "the fmt formatting bodies moved to large_int_value.cpp under the same "
        "contract as core/uint24.h: the formatter templates must be "
        "instantiated once, not in every includer",
    ),
    (
        "exec/pipeline/dependency.h",
        "concurrentqueue.h",
        "was a dead 152 KB third-party include here; the moodycamel users "
        "(local_exchanger.h, scanner_context.h, async_result_writer.h) "
        "include it themselves",
    ),
    (
        "util/pretty_printer.h",
        "boost/",
        "one boost::algorithm::join dragged ~60k preprocessed lines into every "
        "TU that sees runtime_profile.h; the join is a plain loop in "
        "pretty_printer.cpp now",
    ),
    # <ranges> is both a heavy header (object_pool.h alone reaches most of the
    # backend) and a build breaker: libc++'s <ranges> rejects the
    # -fno-access-control flag that doris_be_test builds with, so a <ranges>
    # include in a src header can take the whole UT build down (#66615).
    (
        "common/object_pool.h",
        "ranges",
        "widely-included header; <ranges> is heavy for every includer and "
        "breaks the -fno-access-control UT build on libc++",
    ),
    (
        "exprs/lambda_function/lambda_execution_context.h",
        "ranges",
        "rides into every lambda-capable expression TU; <ranges> is heavy and "
        "breaks the -fno-access-control UT build on libc++",
    ),
    (
        "format/table/iceberg_reader_mixin.h",
        "ranges",
        "rides the table-reader stack; <ranges> is heavy and breaks the "
        "-fno-access-control UT build on libc++",
    ),
    (
        "storage/index/inverted/query_v2/collect/top_k_collector.h",
        "ranges",
        "rides the inverted-index query stack; <ranges> is heavy and breaks "
        "the -fno-access-control UT build on libc++",
    ),
    (
        "storage/index/inverted/query_v2/composite_reader.h",
        "ranges",
        "rides the inverted-index query stack; <ranges> is heavy and breaks "
        "the -fno-access-control UT build on libc++",
    ),
    (
        "storage/index/inverted/query_v2/wand/block_wand.h",
        "ranges",
        "rides the inverted-index query stack; <ranges> is heavy and breaks "
        "the -fno-access-control UT build on libc++",
    ),
]

# Budgets: the edge rules and bans above pin the edges someone has already
# thought about; the two budget axes below catch the ones nobody thought about.
# Both read the same text include graph (be/src + be/test, all ifdef branches);
# neither is comparable to ninja-measured rebuild radii -- each compares only
# against its own baseline.
#
# Forward axis, "this header must stay light": the number of project headers
# transitively reachable from the hub (the hub itself excluded). Slack is ZERO
# by design -- growing a hub's closure must be a visible, deliberate act, so the
# legal way over the budget is to bump the number here in the same PR and say
# why in the commit message.
# Baselines: master 9a48f8120c0, 2026-08-18.
FORWARD_CLOSURE_BUDGETS = {
    "common/logging.h": 0,  # a leaf on purpose: logging must not drag project headers
    "util/uid_util.h": 1,
    "common/status.h": 6,
    "runtime/exec_env.h": 10,
    "core/pod_array.h": 18,
    "util/pretty_printer.h": 31,
    "storage/olap_common.h": 32,
    "core/types.h": 35,
    "runtime/runtime_state.h": 43,
    "core/data_type/primitive_type.h": 44,
    "runtime/thread_context.h": 55,
    "core/field.h": 60,
    "core/column/column.h": 66,
    "exprs/function/function.h": 106,
    # +1 (was 357): storage/index/snii/format/core_metadata.h now carries an
    # optional gram::GramScheme member (SNII core metadata gram_scheme field,
    # regex sparse-gram-index P0 Task 12), reachable via
    # inverted_index_reader.h -> inverted_index_cache.h ->
    # snii/reader/logical_index_reader.h -> snii/format/core_metadata.h ->
    # inverted/gram/gram_scheme.h. gram_scheme.h itself is a leaf (cstdint,
    # map, string, common/status.h -- the last already on this hub), so this
    # is exactly +1 project header, not a chain.
    "exec/pipeline/dependency.h": 358,
    "pch/pch.h": 8,
}

# Reverse axis, "this heavy payload must not spread": how many TUs (src + test)
# transitively include the header. New TUs legitimately reference these, so the
# limit is baseline +10% (rounded up); rebaseline when the audit cadence
# re-measures, or in the same PR with justification when a real growth burst is
# intended.
# Baselines: master 9a48f8120c0, 2026-08-18.
REVERSE_REACH_BASELINES = {
    # Locked down by the common/status.h decouple (P35): Status_types must not
    # creep back toward its old everything-line reach of 2453 TUs.
    "gen_cpp/Status_types.h": 37,
    # Still carried legitimately by util/uid_util.h (PUniqueId by value) and
    # the profile family; the status.h cut brought these down from 2458/2091.
    "gen_cpp/types.pb.h": 1791,
    "gen_cpp/Types_types.h": 1993,
    "gen_cpp/segment_v2.pb.h": 1234,
    "gen_cpp/PaloInternalService_types.h": 1133,
    "util/threadpool.h": 988,
    # parsed_page.h holds RleDecoder<bool> by value and constructs it in the
    # header, so the whole storage read stack sees the RLE machinery; a live
    # dependency, budgeted as-is (trimming it is a refactor, not a gate).
    "util/rle_encoding.h": 823,
    "gen_cpp/internal_service.pb.h": 741,
    "gen_cpp/FrontendService_types.h": 576,
    "storage/options.h": 464,
    "gen_cpp/cloud.pb.h": 342,
    "gen_cpp/BackendService_types.h": 222,
    "gen_cpp/data.pb.h": 220,
    "io/fs/s3_file_system.h": 109,  # the AWS SDK surface
    "util/brpc_closure.h": 61,
    "runtime/workload_group/workload_group.h": 42,  # thrift type universe carrier
}
REVERSE_SLACK = 0.10

# The PCH is the single most leveraged file in the repo: every header on it is
# on the everything-rebuilds line (touch one and the whole backend plus the
# precompiled header itself rebuild). Its quoted project includes are therefore
# pinned exactly; the transitive closure is capped by the pch/pch.h entry in
# FORWARD_CLOSURE_BUDGETS. Generated <gen_cpp/...> includes are the PCH's whole
# point and stay out of this lock.
PCH_HEADER = "pch/pch.h"
PCH_QUOTED_WHITELIST = {
    "common/config.h",
    "common/status.h",
    "common/version_internal.h",
}

# Forward-declaration headers are the sanctioned way through a barrier: they carry
# declarations only, so they cost nothing to include.
FWD_SUFFIX = "_fwd.h"


def load_includes():
    """Maps each repo file to the list of project headers it includes."""
    includes = {}
    for root in SOURCE_ROOTS:
        for directory, _, names in os.walk(root):
            for name in names:
                if not name.endswith((".h", ".hpp", ".cpp", ".cc")):
                    continue
                path = os.path.join(directory, name)
                with open(path, encoding="utf-8", errors="ignore") as handle:
                    includes[path] = [
                        match.group(1)
                        for match in (
                            INCLUDE.match(line) or GEN_INCLUDE.match(line)
                            for line in handle
                        )
                        if match
                    ]
    return includes


def resolve(header, includes):
    """Maps an include spelling to a repo path, or None when it is external."""
    path = os.path.join(INCLUDE_ROOT, header)
    return path if path in includes else None


def reachable(start, includes):
    """Every header reachable from `start`, with the chain that got there."""
    chains = {start: [start]}
    frontier = [start]
    while frontier:
        current = frontier.pop()
        path = resolve(current, includes)
        if path is None:
            continue
        for nxt in includes[path]:
            if nxt in chains:
                continue
            chains[nxt] = chains[current] + [nxt]
            frontier.append(nxt)
    return chains


def translation_units_affected(includes):
    """How many translation units each header can force a rebuild of."""
    users = collections.defaultdict(set)
    for path, headers in includes.items():
        for header in headers:
            users[header].add(path)
    counts = {}
    for header in users:
        seen, frontier = set(), [header]
        while frontier:
            current = frontier.pop()
            for user in users.get(current, ()):
                if user in seen:
                    continue
                seen.add(user)
                if user.startswith(INCLUDE_ROOT + "/"):
                    frontier.append(user[len(INCLUDE_ROOT) + 1:])
        counts[header] = sum(1 for f in seen if f.endswith((".cpp", ".cc")))
    return counts


def enforce(includes):
    failures = 0
    for hub, forbidden, allowed, why in RULES:
        if resolve(hub, includes) is None:
            print(f"error: rule names a missing header: {hub}", file=sys.stderr)
            failures += 1
            continue
        chains = reachable(hub, includes)
        for header, chain in sorted(chains.items()):
            if not header.startswith(forbidden) or header.endswith(FWD_SUFFIX):
                continue
            if header in allowed:
                continue
            failures += 1
            print(f"error: {hub} must not reach {forbidden}*", file=sys.stderr)
            print(f"  reason: {why}", file=sys.stderr)
            print("  chain:  " + "\n       -> ".join(chain), file=sys.stderr)
            print(
                "  fix:    forward-declare the type in the header and include the "
                "real header in the .cpp, or route it through a *_fwd.h",
                file=sys.stderr,
            )
            break
    return failures


def forward_closure(hub, includes):
    """Project headers transitively reachable from `hub`, hub excluded."""
    chains = reachable(hub, includes)
    return sorted(
        h for h in chains if h != hub and resolve(h, includes) is not None
    )


ANY_INCLUDE = re.compile(r'^\s*#\s*include\s+[<"]([^>"]+)[>"]')


def enforce_angle_bans():
    failures = 0
    for header, banned, why in ANGLE_BANS:
        path = os.path.join(INCLUDE_ROOT, header)
        if not os.path.exists(path):
            print(f"error: angle ban names a missing header: {header}", file=sys.stderr)
            failures += 1
            continue
        with open(path, encoding="utf-8", errors="ignore") as handle:
            spellings = [m.group(1) for m in map(ANY_INCLUDE.match, handle) if m]
        hits = [
            s
            for s in spellings
            if s == banned or (banned.endswith("/") and s.startswith(banned))
        ]
        for hit in hits:
            failures += 1
            print(f"error: {header} must not include <{hit}>", file=sys.stderr)
            print(f"  reason: {why}", file=sys.stderr)
            print(
                "  fix:    move the code that needs it into the matching .cpp "
                "(that is where the previous cut put it), or take this ban out "
                "of ANGLE_BANS in the same PR and justify it in the commit "
                "message",
                file=sys.stderr,
            )
    return failures


def enforce_budgets(includes):
    failures = 0
    for hub, budget in FORWARD_CLOSURE_BUDGETS.items():
        if resolve(hub, includes) is None:
            print(
                f"error: forward budget names a missing header: {hub} "
                "(renamed or moved? update FORWARD_CLOSURE_BUDGETS)",
                file=sys.stderr,
            )
            failures += 1
            continue
        actual = len(forward_closure(hub, includes))
        if actual > budget:
            failures += 1
            print(
                f"error: {hub} include closure grew to {actual} project "
                f"headers (budget {budget})",
                file=sys.stderr,
            )
            print(
                "  reason: everything this hub includes is reparsed by every "
                "TU that includes the hub, so closure growth is a rebuild-cost "
                "multiplier nobody sees in a diff; the budget makes it visible",
                file=sys.stderr,
            )
            print(
                f"  list:   build-support/check-header-deps.py --closure {hub}",
                file=sys.stderr,
            )
            print(
                "  fix:    cut the new edge (forward-declare the type, or "
                "route it through a *_fwd.h), or bump the budget in "
                "FORWARD_CLOSURE_BUDGETS in the same PR and justify it in the "
                "commit message",
                file=sys.stderr,
            )
    counts = translation_units_affected(includes)
    for header, baseline in REVERSE_REACH_BASELINES.items():
        limit = -(-baseline * (100 + int(REVERSE_SLACK * 100)) // 100)
        actual = counts.get(header, 0)
        is_project = resolve(header, includes) is not None
        if not is_project and not header.startswith("gen_cpp/"):
            print(
                f"error: reverse budget names a missing header: {header} "
                "(renamed or moved? update REVERSE_REACH_BASELINES)",
                file=sys.stderr,
            )
            failures += 1
            continue
        if actual == 0:
            # A watched header nobody includes any more is either renamed
            # (the budget is watching nothing) or genuinely dead -- both mean
            # the table must be updated, loudly.
            print(
                f"error: reverse budget sentinel is no longer referenced: "
                f"{header} (renamed, or truly unused? update "
                "REVERSE_REACH_BASELINES)",
                file=sys.stderr,
            )
            failures += 1
            continue
        if actual > limit:
            failures += 1
            print(
                f"error: {header} now reaches {actual} TUs "
                f"(baseline {baseline} +{int(REVERSE_SLACK * 100)}% = {limit})",
                file=sys.stderr,
            )
            print(
                "  reason: this is a heavy payload (generated code / SDK "
                "surface / template machinery); some header on a popular path "
                "gained an include of it, taxing every TU downstream",
                file=sys.stderr,
            )
            print(
                f"  list:   build-support/check-header-deps.py --reach {header}",
                file=sys.stderr,
            )
            print(
                "  fix:    cut the spreading edge (forward-declare, or move "
                "the include into the .cpp), or rebaseline in "
                "REVERSE_REACH_BASELINES in the same PR and justify it in the "
                "commit message",
                file=sys.stderr,
            )
    return failures


def enforce_pch(includes):
    path = resolve(PCH_HEADER, includes)
    if path is None:
        print(f"error: {PCH_HEADER} not found", file=sys.stderr)
        return 1
    with open(path, encoding="utf-8", errors="ignore") as handle:
        quoted = {m.group(1) for m in map(INCLUDE.match, handle) if m}
    extra = sorted(quoted - PCH_QUOTED_WHITELIST)
    missing = sorted(PCH_QUOTED_WHITELIST - quoted)
    if not extra and not missing:
        return 0
    print("error: pch/pch.h quoted includes diverged from the whitelist", file=sys.stderr)
    for header in extra:
        print(f"  added:   \"{header}\"", file=sys.stderr)
    for header in missing:
        print(f"  removed: \"{header}\"", file=sys.stderr)
    print(
        "  reason: every header on the PCH is on the everything-rebuilds "
        "line -- touch it and the whole backend plus the precompiled header "
        "itself rebuild; adding one is the single most leveraged regression "
        "in the repo",
        file=sys.stderr,
    )
    print(
        "  fix:    include the header in the TUs that need it instead, or "
        "change PCH_QUOTED_WHITELIST in the same PR and justify it in the "
        "commit message",
        file=sys.stderr,
    )
    return 1


def closure_listing(hub, includes):
    if resolve(hub, includes) is None:
        print(f"error: no such project header: {hub}", file=sys.stderr)
        return 1
    chains = reachable(hub, includes)
    members = forward_closure(hub, includes)
    print(f"{hub}: {len(members)} project header(s) in the include closure")
    for member in members:
        print("  " + " -> ".join(chains[member]))
    return 0


def reach_listing(header, includes):
    """Direct includers of `header` ranked by how many TUs each edge carries."""
    users = collections.defaultdict(set)
    for path, headers in includes.items():
        for h in headers:
            users[h].add(path)
    if not users.get(header):
        print(f"error: nothing includes {header}", file=sys.stderr)
        return 1

    def tus(banned_edge=None):
        seen, frontier = set(), [header]
        while frontier:
            current = frontier.pop()
            for user in users.get(current, ()):
                if banned_edge and (user, current) == banned_edge:
                    continue
                if user in seen:
                    continue
                seen.add(user)
                if user.startswith(INCLUDE_ROOT + "/"):
                    frontier.append(user[len(INCLUDE_ROOT) + 1:])
        return sum(1 for f in seen if f.endswith((".cpp", ".cc")))

    total = tus()
    print(f"{header}: reaches {total} TU(s); direct includers by edge weight")
    ranked = sorted(
        ((total - tus((user, header)), user) for user in users[header]),
        reverse=True,
    )
    for marginal, user in ranked:
        print(f"  {marginal:>5} via this edge alone  {user}")
    return 0


def report(includes):
    counts = translation_units_affected(includes)
    ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:30]
    print(f"{'TUs rebuilt':>11}  header")
    for header, count in ranked:
        print(f"{count:>11}  {header}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        action="store_true",
        help="rank headers by how many translation units they force a rebuild of",
    )
    parser.add_argument(
        "--budget",
        action="store_true",
        help="run only the closure/reach budget checks (used when rebaselining)",
    )
    parser.add_argument(
        "--closure",
        metavar="HEADER",
        help="list the project headers in HEADER's include closure, with chains",
    )
    parser.add_argument(
        "--reach",
        metavar="HEADER",
        help="list HEADER's direct includers ranked by how many TUs each edge carries",
    )
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    includes = load_includes()

    if args.report:
        report(includes)
        return 0
    if args.closure:
        return closure_listing(args.closure, includes)
    if args.reach:
        return reach_listing(args.reach, includes)

    if args.budget:
        failures = enforce_budgets(includes)
    else:
        failures = (
            enforce(includes)
            + enforce_angle_bans()
            + enforce_budgets(includes)
            + enforce_pch(includes)
        )
    if failures:
        print(f"\n{failures} header hygiene violation(s)", file=sys.stderr)
        return 1
    if args.budget:
        print(
            f"header budgets: {len(FORWARD_CLOSURE_BUDGETS)} forward + "
            f"{len(REVERSE_REACH_BASELINES)} reverse within budget"
        )
    else:
        print(
            f"header hygiene: {len(RULES)} layering rule(s), "
            f"{len(ANGLE_BANS)} third-party ban(s), "
            f"{len(FORWARD_CLOSURE_BUDGETS)}+{len(REVERSE_REACH_BASELINES)} "
            "budget(s), pch whitelist: all satisfied"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
