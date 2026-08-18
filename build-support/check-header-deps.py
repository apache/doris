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

Each rule names a hub header and a directory prefix it must not reach. Keeping a
hub out of a subsystem is what lets that subsystem's headers be edited cheaply.

Usage:
    build-support/check-header-deps.py            # enforce the rules
    build-support/check-header-deps.py --report   # rank headers by rebuild cost
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
        "runtime/exec_env.h",
        "gen_cpp/",
        {
            # Status embeds TStatus/PStatus; these two ride in through
            # common/status.h and are the only generated headers ExecEnv may keep.
            "gen_cpp/Status_types.h",
            "gen_cpp/types.pb.h",
        },
        "ExecEnv reaches ~1060 TUs, so any generated protobuf/thrift header it "
        "pulls in is reparsed by most of the backend; every thrift struct it "
        "stores is behind a pointer or forward declaration",
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
            # Status embeds TStatus/PStatus (ride in through common/status.h);
            # TUniqueId lives in Types_types.h; the profile family rides in
            # through runtime_profile.h held by the memory-tracker chain.
            "gen_cpp/Status_types.h",
            "gen_cpp/types.pb.h",
            "gen_cpp/Types_types.h",
            "gen_cpp/Metrics_types.h",
            "gen_cpp/RuntimeProfile_types.h",
            "gen_cpp/runtime_profile.pb.h",
        },
        "ThreadContext reaches ~1000 TUs; any generated protobuf/thrift header "
        "beyond the status/types/profile carriers listed here is reparsed by "
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
]

# Not expressible as RULES entries (the scanner only follows quoted project
# includes and <gen_cpp/...>): core/uint24.h and core/value/large_int_value.h
# must not regain <fmt/compile.h> / <fmt/format.h>. Their to_string/to_buffer
# bodies live in the matching .cpp files precisely so the FMT_COMPILE formatter
# templates (53.5 CPU s over ~1150 TUs for the uint24 date format alone) are
# instantiated once instead of in every includer.
#
# Likewise <concurrentqueue.h> must not return to exec/pipeline/dependency.h:
# it was a dead 152 KB third-party include there; the moodycamel users
# (local_exchanger.h, scanner_context.h, async_result_writer.h) include it
# themselves.

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
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    includes = load_includes()

    if args.report:
        report(includes)
        return 0

    failures = enforce(includes)
    if failures:
        print(f"\n{failures} header layering violation(s)", file=sys.stderr)
        return 1
    print(f"header layering: {len(RULES)} rule(s) satisfied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
