// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "lance/lance_test_util.h"

// LanceIndexWorkerSemanticsTest: the three S6 new-write blocks of the worker
// library - DROP physical-list unique resolution, REPLACE
// missing-name-new-create, and the pre-invocation IF no-ops - plus the
// contract revalidation branch, driven through the execute_worker_mutation
// seam against real local lance datasets (writable copies of the shared
// golden fixtures; the raw listing API behavior is pinned separately by
// LanceIndexPhysicalListProbeTest). The full run_index_worker path reaches
// this seam only behind a successful non-local pinned open, which no
// hermetic environment provides; the seam receives exactly the state the
// worker would have: a pinned-version dataset handle and the validated
// dispatch.
//
// The admitted contract JSON below is the recomputed contract of the s14.v1
// vector column (golden fixture fsl_f32_s14_v); every passing build case
// proves the equality branch accepts it byte-for-byte.

namespace doris::lance {
namespace {

constexpr int32_t STALE_ADMISSION = -1;
constexpr int32_t UNSUPPORTED = -2;
constexpr int32_t INDEX_UNRESOLVED = -5;

const char* VALID_CONTRACT_JSON =
        R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"fixed_size_list","nul":false,"fsd":4,"vet":"float32","ven":true}]})";

VectorIndexArguments ivf_pq_arguments() {
    VectorIndexArguments arguments;
    arguments.num_partitions = 2;
    arguments.num_sub_vectors = 2;
    arguments.metric = LANCE_METRIC_L2;
    return arguments;
}

// One mutation dispatch aimed at a local dataset copy. The URI passes the
// D11 shape only because the seam sits behind that gate in production.
TLanceIndexWorkerDispatch make_dispatch(TLanceIndexWorkerMutationType::type type,
                                        const std::string& index_name) {
    TLanceIndexWorkerDispatch dispatch;
    dispatch.invocation_id = "0123456789abcdef0123456789abcdef";
    dispatch.be_process_epoch = 909090;
    dispatch.deadline_ms = 1700000000000LL;
    dispatch.mutation_type = type;
    dispatch.index_name = index_name;
    dispatch.column_name = "v";
    dispatch.index_type = "IVF_PQ";
    dispatch.__set_properties_json(
            R"({"index_type":"IVF_PQ","metric":"l2","num_bits":"8","num_partitions":"2","num_sub_vectors":"2"})");
    dispatch.admitted_dataset_version = 1;
    dispatch.__set_max_num_partitions(1024);
    dispatch.__set_max_num_sub_vectors(64);
    if (type != TLanceIndexWorkerMutationType::DROP) {
        dispatch.__set_schema_contract_json(VALID_CONTRACT_JSON);
    }
    return dispatch;
}

// Creates one IVF_PQ index on the dataset (the external-writer role: what a
// concurrent mutation or the fixture history would have committed).
bool create_index(LanceDataset* dataset, const char* name) {
    LanceVectorIndexParams params = {};
    params.index_type = LANCE_INDEX_IVF_PQ;
    params.metric = LANCE_METRIC_L2;
    params.num_partitions = 2;
    params.num_sub_vectors = 2;
    params.num_bits = 8;
    return lance_dataset_create_vector_index(dataset, "v", name, &params,
                                             /*replace=*/false) == 0;
}

// The stored spellings of the dataset's latest committed snapshot, in list
// order.
std::vector<std::string> latest_index_names(const std::string& uri) {
    LanceDatasetPtr dataset = open_dataset_path(uri, 0 /* latest */);
    if (dataset == nullptr) {
        return {};
    }
    const char* raw = lance_dataset_index_list_json(dataset.get());
    if (raw == nullptr) {
        return {};
    }
    std::string json(raw);
    lance_free_string(raw);
    rapidjson::Document doc;
    doc.Parse<rapidjson::kParseIterativeFlag>(json.data(), json.size());
    std::vector<std::string> names;
    if (doc.HasParseError() || !doc.IsArray()) {
        return {};
    }
    for (rapidjson::SizeType i = 0; i < doc.Size(); ++i) {
        if (doc[i].IsObject() && doc[i].HasMember("name") && doc[i]["name"].IsString()) {
            names.emplace_back(doc[i]["name"].GetString(), doc[i]["name"].GetStringLength());
        }
    }
    return names;
}

void assert_rejected(const WorkerMutationOutcome& outcome, int32_t expected_code) {
    ASSERT_TRUE(outcome.trusted);
    EXPECT_EQ(outcome.phase, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED);
    EXPECT_EQ(outcome.lance_error_code, expected_code);
    EXPECT_FALSE(outcome.no_op);
}

void assert_native(const WorkerMutationOutcome& outcome, int32_t expected_code) {
    ASSERT_TRUE(outcome.trusted);
    EXPECT_EQ(outcome.phase, TLanceIndexWorkerResultPhase::NATIVE_RETURN);
    EXPECT_EQ(outcome.lance_error_code, expected_code);
}

// ── resolve_physical_index: the pure resolution seam ──

TEST(LanceIndexWorkerSemanticsTest, ResolvePhysicalIndexMatrix) {
    std::string stored;
    // The empirically locked provider shape (LanceIndexPhysicalListProbeTest).
    const std::string real_shape =
            R"([{"name":"idx_v","uuid":"76def275-181d-498a-8e32-49babf17a7bd","columns":["v"],"type":"IVF_PQ","dataset_version":1}])";
    EXPECT_EQ(resolve_physical_index(real_shape, "idx_v", &stored),
              PhysicalIndexResolution::UNIQUE);
    EXPECT_EQ(stored, "idx_v");
    // Case-insensitive under the ASCII fold; the stored spelling wins.
    EXPECT_EQ(resolve_physical_index(real_shape, "IDX_V", &stored),
              PhysicalIndexResolution::UNIQUE);
    EXPECT_EQ(stored, "idx_v");
    stored.clear();
    EXPECT_EQ(resolve_physical_index(real_shape, "other", &stored),
              PhysicalIndexResolution::ABSENT);
    EXPECT_EQ(resolve_physical_index("[]", "idx_v", &stored), PhysicalIndexResolution::ABSENT);
    // Exact-duplicate spellings collapse to one candidate (physical entries
    // can repeat one logical name).
    EXPECT_EQ(resolve_physical_index(R"([{"name":"a"},{"name":"a"}])", "a", &stored),
              PhysicalIndexResolution::UNIQUE);
    EXPECT_EQ(stored, "a");
    // Two case-only variants of one logical name: ambiguous, no guess.
    EXPECT_EQ(resolve_physical_index(R"([{"name":"Idx_A"},{"name":"idx_a"}])", "idx_a", &stored),
              PhysicalIndexResolution::AMBIGUOUS);
    // System entries are skipped entirely.
    EXPECT_EQ(resolve_physical_index(
                      R"([{"name":"__lance_frag_reuse"},{"name":"__lance_mem_wal"}])", "x",
                      &stored),
              PhysicalIndexResolution::ABSENT);
    // Malformed payloads (iterative parse; never a crash).
    EXPECT_EQ(resolve_physical_index("", "a", &stored), PhysicalIndexResolution::MALFORMED);
    EXPECT_EQ(resolve_physical_index("{not json", "a", &stored),
              PhysicalIndexResolution::MALFORMED);
    EXPECT_EQ(resolve_physical_index(R"({"name":"a"})", "a", &stored),
              PhysicalIndexResolution::MALFORMED); // not an array
    EXPECT_EQ(resolve_physical_index(R"(["a"])", "a", &stored),
              PhysicalIndexResolution::MALFORMED); // entry not an object
    EXPECT_EQ(resolve_physical_index(R"([{"uuid":"x"}])", "a", &stored),
              PhysicalIndexResolution::MALFORMED); // entry without a name
    EXPECT_EQ(resolve_physical_index(R"([{"name":42}])", "a", &stored),
              PhysicalIndexResolution::MALFORMED); // name not a string
    // Non-ASCII never folds: exact byte equality only.
    EXPECT_EQ(resolve_physical_index(R"([{"name":"向量"}])",
                                     "\xe5\x90\x91\xe9\x87\x8f", &stored),
              PhysicalIndexResolution::UNIQUE);
    EXPECT_EQ(resolve_physical_index(R"([{"name":"向量"}])", "a", &stored),
              PhysicalIndexResolution::ABSENT);
    EXPECT_TRUE(is_system_index_name("__lance_frag_reuse"));
    EXPECT_TRUE(is_system_index_name("__lance_mem_wal"));
    EXPECT_FALSE(is_system_index_name("idx_v"));
}

TEST(LanceIndexWorkerSemanticsTest, ResolvePhysicalIndexBounds) {
    std::string stored;
    // Entry count above MAX_PHYSICAL_INDEX_ENTRIES: unbounded, fail closed.
    {
        std::string json = "[";
        for (size_t i = 0; i <= MAX_PHYSICAL_INDEX_ENTRIES; ++i) {
            json += R"({"name":"i)" + std::to_string(i) + R"("},)";
        }
        json += R"({"name":"last"}])";
        EXPECT_EQ(resolve_physical_index(json, "i0", &stored),
                  PhysicalIndexResolution::UNBOUNDED);
    }
    // A stored name above MAX_INDEX_NAME_BYTES: unbounded.
    {
        const std::string long_name(MAX_INDEX_NAME_BYTES + 1, 'x');
        EXPECT_EQ(resolve_physical_index(R"([{"name":")" + long_name + R"("}])", "a", &stored),
                  PhysicalIndexResolution::UNBOUNDED);
        // At the bound: admitted.
        const std::string bound_name(MAX_INDEX_NAME_BYTES, 'y');
        EXPECT_EQ(resolve_physical_index(R"([{"name":")" + bound_name + R"("}])", "a", &stored),
                  PhysicalIndexResolution::ABSENT);
    }
}

// ── execute_worker_mutation: DROP semantics ──

// DROP resolves the normalized name against the pinned snapshot's physical
// list and issues exactly one drop by the STORED spelling.
TEST(LanceIndexWorkerSemanticsTest, DropUniqueUsesStoredSpellingOnce) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    {
        LanceDatasetPtr writer = open_dataset_path(uri, 0 /* latest */);
        ASSERT_NE(writer, nullptr);
        ASSERT_TRUE(create_index(writer.get(), "Idx_V")); // display-case spelling
    }
    // Admit the snapshot that carries the index; a pinned v1 handle would
    // legitimately see nothing (the probe pins this snapshot behavior).
    LanceDatasetPtr pinned = open_dataset_path(uri, 2);
    ASSERT_NE(pinned, nullptr);

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::DROP, "idx_v");
    dispatch.admitted_dataset_version = 2;
    // A contract carried by a DROP is simply not consumed.
    dispatch.__set_schema_contract_json(R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"int64","nul":false}]})");
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 2, dispatch, ivf_pq_arguments());
    assert_native(outcome, static_cast<int32_t>(LANCE_OK));
    EXPECT_FALSE(outcome.no_op);

    // Exactly one drop happened: the latest snapshot no longer carries any
    // entry of the logical name, in any spelling.
    EXPECT_TRUE(latest_index_names(uri).empty());
}

// DROP of an absent name without IF EXISTS: the -5 pre-invocation rejection
// (a post-invocation native NOT_FOUND could never prove a clean no-commit).
TEST(LanceIndexWorkerSemanticsTest, DropAbsentWithoutIfExistsIsIndexUnresolved) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    LanceDatasetPtr pinned = open_dataset_path(uri, 1);
    ASSERT_NE(pinned, nullptr);

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::DROP, "idx_v");
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
    assert_rejected(outcome, INDEX_UNRESOLVED);
}

// DROP of an absent name with IF EXISTS: the authoritative pre-invocation
// no-op - phase NATIVE_RETURN, code 0, and provably no version commit.
TEST(LanceIndexWorkerSemanticsTest, DropAbsentWithIfExistsIsAuthoritativeNoOp) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    LanceDatasetPtr pinned = open_dataset_path(uri, 1);
    ASSERT_NE(pinned, nullptr);
    const uint64_t latest_before = lance_dataset_latest_version(pinned.get());

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::DROP, "idx_v");
    dispatch.__set_if_exists(true);
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
    assert_native(outcome, static_cast<int32_t>(LANCE_OK));
    EXPECT_TRUE(outcome.no_op);

    // The no-op is authoritative: nothing was committed.
    EXPECT_EQ(lance_dataset_latest_version(pinned.get()), latest_before);
    EXPECT_TRUE(latest_index_names(uri).empty());
}

// An ambiguous snapshot (two case-only spellings of the logical name) never
// yields a single-drop guess - with or without IF EXISTS (the index exists;
// absence semantics do not apply).
TEST(LanceIndexWorkerSemanticsTest, DropAmbiguousIsIndexUnresolvedEvenWithIfExists) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    {
        LanceDatasetPtr writer = open_dataset_path(uri, 0 /* latest */);
        ASSERT_NE(writer, nullptr);
        ASSERT_TRUE(create_index(writer.get(), "idx_alpha")); // v2
        ASSERT_TRUE(create_index(writer.get(), "IDX_ALPHA")); // v3
    }
    LanceDatasetPtr pinned = open_dataset_path(uri, 3);
    ASSERT_NE(pinned, nullptr);

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::DROP, "idx_alpha");
    dispatch.admitted_dataset_version = 3;
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 3, dispatch, ivf_pq_arguments());
    assert_rejected(outcome, INDEX_UNRESOLVED);

    TLanceIndexWorkerDispatch if_exists = dispatch;
    if_exists.__set_if_exists(true);
    outcome = execute_worker_mutation(pinned.get(), 3, if_exists, ivf_pq_arguments());
    assert_rejected(outcome, INDEX_UNRESOLVED);

    // Nothing was dropped: both spellings still exist.
    EXPECT_EQ(latest_index_names(uri).size(), 2U);
}

// ── execute_worker_mutation: CREATE semantics ──

// CREATE with IF NOT EXISTS against an occupied logical name: the
// authoritative pre-invocation no-op, provably without a commit.
TEST(LanceIndexWorkerSemanticsTest, CreateIfNotExistsHitsExistingIsNoOp) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    {
        LanceDatasetPtr writer = open_dataset_path(uri, 0 /* latest */);
        ASSERT_NE(writer, nullptr);
        ASSERT_TRUE(create_index(writer.get(), "Idx_V")); // v2
    }
    LanceDatasetPtr pinned = open_dataset_path(uri, 2);
    ASSERT_NE(pinned, nullptr);
    const uint64_t latest_before = lance_dataset_latest_version(pinned.get());

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::CREATE, "idx_v");
    dispatch.admitted_dataset_version = 2;
    dispatch.__set_index_display_name("Idx_V_New");
    dispatch.__set_if_not_exists(true);
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 2, dispatch, ivf_pq_arguments());
    assert_native(outcome, static_cast<int32_t>(LANCE_OK));
    EXPECT_TRUE(outcome.no_op);

    EXPECT_EQ(lance_dataset_latest_version(pinned.get()), latest_before);
    const std::vector<std::string> names = latest_index_names(uri);
    ASSERT_EQ(names.size(), 1U);
    EXPECT_EQ(names[0], "Idx_V") << "the no-op must not have created the display spelling";
}

// An ambiguous occupancy also proves the name taken: CREATE IF NOT EXISTS
// no-ops rather than adding a third case-variant entry.
TEST(LanceIndexWorkerSemanticsTest, CreateIfNotExistsAmbiguousOccupancyIsNoOp) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    {
        LanceDatasetPtr writer = open_dataset_path(uri, 0 /* latest */);
        ASSERT_NE(writer, nullptr);
        ASSERT_TRUE(create_index(writer.get(), "idx_alpha")); // v2
        ASSERT_TRUE(create_index(writer.get(), "IDX_ALPHA")); // v3
    }
    LanceDatasetPtr pinned = open_dataset_path(uri, 3);
    ASSERT_NE(pinned, nullptr);
    const uint64_t latest_before = lance_dataset_latest_version(pinned.get());

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::CREATE, "idx_alpha");
    dispatch.admitted_dataset_version = 3;
    dispatch.__set_if_not_exists(true);
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 3, dispatch, ivf_pq_arguments());
    assert_native(outcome, static_cast<int32_t>(LANCE_OK));
    EXPECT_TRUE(outcome.no_op);
    EXPECT_EQ(lance_dataset_latest_version(pinned.get()), latest_before);
    EXPECT_EQ(latest_index_names(uri).size(), 2U);
}

// CREATE passes through to the native call with the display spelling (the
// statement's case is preserved in storage); without a display name the
// normalized name is the spelling.
TEST(LanceIndexWorkerSemanticsTest, CreateUsesDisplaySpelling) {
    {
        LanceTempDir tmp;
        const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
        LanceDatasetPtr pinned = open_dataset_path(uri, 1);
        ASSERT_NE(pinned, nullptr);
        TLanceIndexWorkerDispatch dispatch =
                make_dispatch(TLanceIndexWorkerMutationType::CREATE, "idx_v");
        dispatch.__set_index_display_name("MyIndex");
        WorkerMutationOutcome outcome =
                execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
        assert_native(outcome, static_cast<int32_t>(LANCE_OK));
        EXPECT_FALSE(outcome.no_op);
        const std::vector<std::string> names = latest_index_names(uri);
        ASSERT_EQ(names.size(), 1U);
        EXPECT_EQ(names[0], "MyIndex");
    }
    {
        LanceTempDir tmp;
        const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
        LanceDatasetPtr pinned = open_dataset_path(uri, 1);
        ASSERT_NE(pinned, nullptr);
        TLanceIndexWorkerDispatch dispatch =
                make_dispatch(TLanceIndexWorkerMutationType::CREATE, "idx_plain");
        WorkerMutationOutcome outcome =
                execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
        assert_native(outcome, static_cast<int32_t>(LANCE_OK));
        const std::vector<std::string> names = latest_index_names(uri);
        ASSERT_EQ(names.size(), 1U);
        EXPECT_EQ(names[0], "idx_plain") << "an older request without field 15 falls back to "
                                            "the normalized name";
    }
}

// CREATE IF NOT EXISTS over an absent name proceeds to the real create.
TEST(LanceIndexWorkerSemanticsTest, CreateIfNotExistsAbsentProceeds) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    LanceDatasetPtr pinned = open_dataset_path(uri, 1);
    ASSERT_NE(pinned, nullptr);
    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::CREATE, "idx_v");
    dispatch.__set_if_not_exists(true);
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
    assert_native(outcome, static_cast<int32_t>(LANCE_OK));
    EXPECT_FALSE(outcome.no_op);
    EXPECT_EQ(latest_index_names(uri).size(), 1U);
}

// ── execute_worker_mutation: REPLACE semantics ──

// REPLACE of a uniquely resolved name replaces by the STORED spelling; the
// stored spelling survives the replacement.
TEST(LanceIndexWorkerSemanticsTest, ReplaceUniqueReplacesByStoredSpelling) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    {
        LanceDatasetPtr writer = open_dataset_path(uri, 0 /* latest */);
        ASSERT_NE(writer, nullptr);
        ASSERT_TRUE(create_index(writer.get(), "Idx_V")); // v2
    }
    LanceDatasetPtr pinned = open_dataset_path(uri, 2);
    ASSERT_NE(pinned, nullptr);

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::REPLACE, "idx_v");
    dispatch.admitted_dataset_version = 2;
    // The display spelling is NOT what a unique-resolved replace uses.
    dispatch.__set_index_display_name("TotallyDifferent");
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 2, dispatch, ivf_pq_arguments());
    assert_native(outcome, static_cast<int32_t>(LANCE_OK));
    EXPECT_FALSE(outcome.no_op);

    const std::vector<std::string> names = latest_index_names(uri);
    ASSERT_EQ(names.size(), 1U);
    EXPECT_EQ(names[0], "Idx_V") << "replace keeps the stored spelling, not the display one";
}

// REPLACE of an absent name becomes a new create by the display spelling
// (field 15), falling back to the normalized name on older requests.
TEST(LanceIndexWorkerSemanticsTest, ReplaceAbsentBecomesNewCreate) {
    {
        LanceTempDir tmp;
        const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
        LanceDatasetPtr pinned = open_dataset_path(uri, 1);
        ASSERT_NE(pinned, nullptr);
        TLanceIndexWorkerDispatch dispatch =
                make_dispatch(TLanceIndexWorkerMutationType::REPLACE, "idx_v");
        dispatch.__set_index_display_name("FreshIdx");
        WorkerMutationOutcome outcome =
                execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
        assert_native(outcome, static_cast<int32_t>(LANCE_OK));
        EXPECT_FALSE(outcome.no_op);
        const std::vector<std::string> names = latest_index_names(uri);
        ASSERT_EQ(names.size(), 1U);
        EXPECT_EQ(names[0], "FreshIdx");
    }
    {
        // Without field 15: the normalized name is the create spelling.
        LanceTempDir tmp;
        const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
        LanceDatasetPtr pinned = open_dataset_path(uri, 1);
        ASSERT_NE(pinned, nullptr);
        TLanceIndexWorkerDispatch dispatch =
                make_dispatch(TLanceIndexWorkerMutationType::REPLACE, "idx_v");
        WorkerMutationOutcome outcome =
                execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
        assert_native(outcome, static_cast<int32_t>(LANCE_OK));
        const std::vector<std::string> names = latest_index_names(uri);
        ASSERT_EQ(names.size(), 1U);
        EXPECT_EQ(names[0], "idx_v");
    }
}

// An ambiguous resolution with no display-name fallback is -5 for REPLACE;
// with the fallback set the missing-name-new-create path applies (the IDL's
// -5 band: ambiguous spellings reject a REPLACE "when no display-name
// fallback field is set").
TEST(LanceIndexWorkerSemanticsTest, ReplaceAmbiguousFollowsTheIdlBand) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    {
        LanceDatasetPtr writer = open_dataset_path(uri, 0 /* latest */);
        ASSERT_NE(writer, nullptr);
        ASSERT_TRUE(create_index(writer.get(), "idx_alpha")); // v2
        ASSERT_TRUE(create_index(writer.get(), "IDX_ALPHA")); // v3
    }
    LanceDatasetPtr pinned = open_dataset_path(uri, 3);
    ASSERT_NE(pinned, nullptr);

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::REPLACE, "idx_alpha");
    dispatch.admitted_dataset_version = 3;
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 3, dispatch, ivf_pq_arguments());
    assert_rejected(outcome, INDEX_UNRESOLVED);
    EXPECT_EQ(latest_index_names(uri).size(), 2U) << "nothing was mutated by the rejection";

    TLanceIndexWorkerDispatch fallback = dispatch;
    fallback.__set_index_display_name("NewThird");
    outcome = execute_worker_mutation(pinned.get(), 3, fallback, ivf_pq_arguments());
    assert_native(outcome, static_cast<int32_t>(LANCE_OK));
    const std::vector<std::string> names = latest_index_names(uri);
    ASSERT_EQ(names.size(), 3U);
    EXPECT_EQ(names.back(), "NewThird");
}

// ── execute_worker_mutation: contract branch ──

// A mismatching admitted contract is the stale-admission rejection, with the
// read-only latest observation as diagnostic evidence when latest advanced.
TEST(LanceIndexWorkerSemanticsTest, ContractMismatchIsStaleAdmissionWithLatestDiagnostic) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    {
        // An external writer advances latest past the admitted version.
        LanceDatasetPtr writer = open_dataset_path(uri, 0 /* latest */);
        ASSERT_NE(writer, nullptr);
        ASSERT_TRUE(create_index(writer.get(), "idx_external")); // v2
    }
    LanceDatasetPtr pinned = open_dataset_path(uri, 1);
    ASSERT_NE(pinned, nullptr);

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::CREATE, "idx_v");
    dispatch.__set_schema_contract_json(
            R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"fixed_size_list","nul":false,"fsd":8,"vet":"float32","ven":true}]})");
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
    assert_rejected(outcome, STALE_ADMISSION);
    EXPECT_EQ(outcome.observed_latest_version, 2)
            << "the read-only latest observation rides the pre-invocation rejection";
    EXPECT_EQ(latest_index_names(uri).size(), 1U)
            << "the rejection proved no change: only the external index exists";
}

// A missing, empty, or unparsable contract on a build is the safe
// unsupported-contract rejection, never a skip.
TEST(LanceIndexWorkerSemanticsTest, BuildContractRequired) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    LanceDatasetPtr pinned = open_dataset_path(uri, 1);
    ASSERT_NE(pinned, nullptr);

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::CREATE, "idx_v");
    dispatch.__isset.schema_contract_json = false;
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
    assert_rejected(outcome, UNSUPPORTED);

    dispatch.__set_schema_contract_json("");
    outcome = execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
    assert_rejected(outcome, UNSUPPORTED);

    dispatch.__set_schema_contract_json("{not json");
    outcome = execute_worker_mutation(pinned.get(), 1, dispatch, ivf_pq_arguments());
    assert_rejected(outcome, UNSUPPORTED);
    EXPECT_TRUE(latest_index_names(uri).empty());
}

// The no-op carries no latest diagnostic even when latest advanced: the
// observed-latest field is reserved for PRE_INVOCATION_REJECTED results.
TEST(LanceIndexWorkerSemanticsTest, NoOpCarriesNoLatestDiagnostic) {
    LanceTempDir tmp;
    const std::string uri = copy_fixture_dataset("s14.lance", tmp.path);
    {
        LanceDatasetPtr writer = open_dataset_path(uri, 0 /* latest */);
        ASSERT_NE(writer, nullptr);
        ASSERT_TRUE(create_index(writer.get(), "Idx_V"));   // v2: the admitted snapshot
        ASSERT_TRUE(create_index(writer.get(), "idx_other")); // v3: latest advances further
    }
    // Admit v2 (the snapshot that carries the logical name); latest is v3.
    LanceDatasetPtr pinned = open_dataset_path(uri, 2);
    ASSERT_NE(pinned, nullptr);

    TLanceIndexWorkerDispatch dispatch =
            make_dispatch(TLanceIndexWorkerMutationType::CREATE, "idx_v");
    dispatch.admitted_dataset_version = 2;
    dispatch.__set_if_not_exists(true);
    WorkerMutationOutcome outcome =
            execute_worker_mutation(pinned.get(), 2, dispatch, ivf_pq_arguments());
    assert_native(outcome, static_cast<int32_t>(LANCE_OK));
    EXPECT_TRUE(outcome.no_op);
    EXPECT_EQ(outcome.observed_latest_version, 0)
            << "the latest diagnostic is reserved for pre-invocation rejections";
    EXPECT_EQ(latest_index_names(uri).size(), 2U) << "the no-op committed nothing";
}

} // namespace
} // namespace doris::lance
