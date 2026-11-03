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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <thread>

#include <poll.h>

#include "lance/lance_test_util.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

// LanceIndexWorkerParamsTest: drives run_index_worker in-process over pipe
// trios and pins the full-path envelope contract - dispatch frame bounds, the
// handshake-before-every-result ordering, the internal-frame revalidation
// rails (storage-option bounds, scalar sanity, the bounds snapshot replay of
// frame fields 16/17, the properties_json five-key whitelist), the D11
// local-dataset rejection, the typed open-failure classification, and the
// worker-fault debug points.
//
// Hermetic reach note: every local dataset is rejected at the D11 file gate
// and lance-c 0.1.9 has no hermetic non-local object store (memory:// state
// does not survive across FFI calls; http:// has no provider; cloud schemes
// need the network). The steps behind a successful open (contract
// revalidation, physical-list resolution, the native invocation and its code
// passthrough) are covered by LanceIndexWorkerSemanticsTest through the
// execute_worker_mutation seam against real local datasets.

namespace doris::lance {
namespace {

constexpr int32_t RESOURCE_REJECTED = -4;
constexpr int32_t UNSUPPORTED = -2;

class LanceIndexWorkerParamsTest : public ::testing::Test {
protected:
    sighandler_t old_sigpipe_ = SIG_DFL;

    void SetUp() override {
        // A writer thread can outlive the worker's dispatch drain (an early
        // protocol exit); never let SIGPIPE kill the test process.
        old_sigpipe_ = signal(SIGPIPE, SIG_IGN);
    }

    void TearDown() override { signal(SIGPIPE, old_sigpipe_); }

    // A complete, valid CREATE dispatch pointing at a real local dataset
    // through a file:// URI. Validation order makes this the distinguishing
    // probe:
    //   * a rail that fires in dispatch revalidation yields its specific
    //     envelope (UNSUPPORTED or RESOURCE_REJECTED) after the handshake;
    //   * passing everything yields the step-5 D11 RESOURCE_REJECTED envelope.
    static TLanceIndexWorkerDispatch make_dispatch() {
        TLanceIndexWorkerDispatch dispatch;
        dispatch.invocation_id = "0123456789abcdef0123456789abcdef";
        dispatch.be_process_epoch = 909090;
        dispatch.deadline_ms = 1700000000000LL;
        dispatch.mutation_type = TLanceIndexWorkerMutationType::CREATE;
        dispatch.index_name = "idx_v";
        dispatch.column_name = "v";
        dispatch.index_type = "IVF_PQ";
        dispatch.__set_properties_json(
                R"({"index_type":"IVF_PQ","metric":"l2","num_bits":"8","num_partitions":"2","num_sub_vectors":"2"})");
        dispatch.dataset_uri = "file://" + lance_test_dataset_path("s14.lance");
        dispatch.admitted_dataset_version = 1;
        dispatch.__set_schema_contract_json(
                R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"fixed_size_list","nul":false,"fsd":4,"vet":"float32","ven":true}]})");
        // The admitted bounds snapshot (fields 16/17): the worker subprocess
        // reads no BE config, so the bounds ride the internal frame.
        dispatch.__set_max_num_partitions(1024);
        dispatch.__set_max_num_sub_vectors(64);
        // The per-launch debug-point snapshot (field 18) is names-only and
        // never affects the revalidation rails; its absence is the default.
        return dispatch;
    }

    // Runs the worker on one dispatch and asserts the envelope shape shared by
    // every trusted answer: exit 0, exactly two frames (handshake first, then
    // the result), no trailing garbage, and the expected typed code under the
    // expected phase.
    static WorkerFrames run_and_assert_envelope(const TLanceIndexWorkerDispatch& dispatch,
                                                TLanceIndexWorkerResultPhase::type expected_phase,
                                                int32_t expected_code, WorkerRunResult* run_out) {
        WorkerRunResult run = lance_run_worker(lance_worker_dispatch_frame(dispatch));
        EXPECT_EQ(run.exit_code, 0);
        WorkerFrames frames = lance_decode_worker_frames(run);
        EXPECT_FALSE(frames.trailing_garbage);
        EXPECT_EQ(frames.frame_count, 2U)
                << "every trusted result is preceded by the handshake frame";
        assert_handshake_sane(frames);
        EXPECT_TRUE(frames.result.has_value())
                << "the second stdout frame must decode as the worker result";
        if (frames.result.has_value()) {
            EXPECT_EQ(frames.result->phase, expected_phase);
            EXPECT_EQ(frames.result->lance_error_code, expected_code);
            EXPECT_FALSE(frames.result->__isset.observed_latest_version);
        }
        if (run_out != nullptr) {
            *run_out = run;
        }
        return frames;
    }

    static void assert_handshake_sane(const WorkerFrames& frames) {
        ASSERT_TRUE(frames.handshake.has_value())
                << "the first stdout frame must decode as the worker handshake";
        EXPECT_EQ(frames.handshake->protocol_magic, HANDSHAKE_PROTOCOL_MAGIC);
        EXPECT_EQ(frames.handshake->protocol_version, HANDSHAKE_PROTOCOL_VERSION);
        // Self-reported confinement facts are present; the supervisor distrusts
        // and re-checks them, here they only need to be well-formed.
        EXPECT_FALSE(frames.handshake->cgroup_path.empty());
        EXPECT_TRUE(frames.handshake->memory_max_bytes >= -1);
        EXPECT_TRUE(frames.handshake->pids_max >= -1);
        EXPECT_TRUE(frames.handshake->rlimit_as_bytes >= -1);
        EXPECT_TRUE(frames.handshake->rlimit_cpu_seconds >= -1);
        EXPECT_GT(frames.handshake->rlimit_nofile, 0);
        EXPECT_TRUE(frames.handshake->rlimit_core >= 0);
    }

    // Runs the worker on raw dispatch bytes that must be rejected as a
    // malformed or undecodable frame: nonzero exit, NO frame of any kind on
    // the result pipe, and exactly the static diagnostic line on the diag
    // pipe.
    static void assert_frame_rejected(const std::vector<uint8_t>& bytes,
                                      const std::string& expected_diag) {
        WorkerRunResult run = lance_run_worker(bytes);
        EXPECT_EQ(run.exit_code, 1);
        EXPECT_TRUE(run.result_stream.empty())
                << "a malformed dispatch must not produce any stdout frame";
        EXPECT_EQ(run.diag_text(), expected_diag);
    }
};

// The D11 envelope: a local dataset (file:// or scheme-less) is rejected
// before any native call with a complete PRE_INVOCATION_REJECTED
// RESOURCE_REJECTED frame, no observed-latest diagnostic, and a static-only
// message - after a sane handshake.
TEST_F(LanceIndexWorkerParamsTest, LocalDatasetYieldsResourceRejectedEnvelope) {
    WorkerRunResult run;
    WorkerFrames frames = run_and_assert_envelope(
            make_dispatch(), TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED,
            RESOURCE_REJECTED, &run);
    EXPECT_EQ(frames.result->message, "resource rejected");
    EXPECT_TRUE(run.diag_stream.empty()) << "a trusted rejection needs no diagnostic line";

    // A scheme-less absolute local path is local by definition (the FE
    // normalizer admits nothing else without a scheme).
    TLanceIndexWorkerDispatch bare = make_dispatch();
    bare.dataset_uri = lance_test_dataset_path("s14.lance");
    run_and_assert_envelope(bare, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED,
                            RESOURCE_REJECTED, nullptr);
}

// Dispatch frame bounds and undecodable payloads: bare nonzero exit, no frame
// on the result pipe, only a static diag line.
TEST_F(LanceIndexWorkerParamsTest, MalformedAndUndecodableFrames) {
    // Length prefix above the (default 512KiB) cap.
    {
        std::vector<uint8_t> bytes = {0x00, 0x08, 0x00, 0x01}; // 512KiB + 1
        bytes.insert(bytes.end(), {'x', 'x'});
        assert_frame_rejected(bytes, "lance worker: malformed dispatch frame\n");
    }
    // Zero length prefix.
    assert_frame_rejected({0, 0, 0, 0}, "lance worker: malformed dispatch frame\n");
    // Length prefix above a tiny injected cap.
    {
        WorkerRunResult run = lance_run_worker({0x00, 0x00, 0x01, 0x00}, 255, 8 * 1024);
        EXPECT_EQ(run.exit_code, 1);
        EXPECT_TRUE(run.result_stream.empty());
        EXPECT_EQ(run.diag_text(), "lance worker: malformed dispatch frame\n");
    }
    // Truncated frame: the header promises more than the pipe delivers.
    {
        std::vector<uint8_t> bytes = {0x00, 0x00, 0x01, 0x00, 'a', 'b', 'c'};
        assert_frame_rejected(bytes, "lance worker: malformed dispatch frame\n");
    }
    // Well-formed length but garbage payload.
    {
        std::vector<uint8_t> payload(64, 0xAB);
        std::vector<uint8_t> bytes = {0, 0, 0, 64};
        bytes.insert(bytes.end(), payload.begin(), payload.end());
        assert_frame_rejected(bytes, "lance worker: undecodable dispatch frame\n");
    }
    // A valid dispatch frame with trailing garbage inside the frame violates
    // one-struct-per-frame (bytes appended beyond the frame stay unread in
    // the pipe, so the padding must ride inside the length prefix).
    {
        std::vector<uint8_t> bytes = lance_worker_dispatch_frame(make_dispatch());
        uint32_t padded = static_cast<uint32_t>(bytes.size()) - 4 + 3;
        bytes[0] = static_cast<uint8_t>(padded >> 24);
        bytes[1] = static_cast<uint8_t>(padded >> 16);
        bytes[2] = static_cast<uint8_t>(padded >> 8);
        bytes[3] = static_cast<uint8_t>(padded);
        bytes.insert(bytes.end(), {1, 2, 3});
        assert_frame_rejected(bytes, "lance worker: undecodable dispatch frame\n");
    }
    // A storage_options map beyond the thrift container limit fails the decode
    // before any validation runs.
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        std::map<std::string, std::string> options;
        for (int i = 0; i < 2000; ++i) {
            options["k" + std::to_string(i)] = "v";
        }
        dispatch.__set_storage_options(options);
        assert_frame_rejected(lance_worker_dispatch_frame(dispatch),
                              "lance worker: undecodable dispatch frame\n");
    }
    // An incomplete frame header (EOF mid-prefix).
    assert_frame_rejected({0x00, 0x01}, "lance worker: malformed dispatch frame\n");
}

// Storage-option bounds: the worker is the last line of defense before the
// values cross a C-string boundary. The pass/reject oracle pairs the options
// with a deliberately broken properties_json: RESOURCE_REJECTED means the
// storage rail fired first; UNSUPPORTED means the options passed and the
// properties rail fired.
TEST_F(LanceIndexWorkerParamsTest, StorageOptionsBounds) {
    const std::string broken_properties = R"({"index_type":"ivf_flat"})";

    auto with_options = [](std::map<std::string, std::string> options,
                           const std::string& properties) {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.__set_properties_json(properties);
        dispatch.__set_storage_options(std::move(options));
        return dispatch;
    };
    auto expect = [](const TLanceIndexWorkerDispatch& dispatch,
                     TLanceIndexWorkerResultPhase::type phase, int32_t code) {
        return run_and_assert_envelope(dispatch, phase, code, nullptr);
    };

    // 64 entries: at the bound, admitted (the properties rail fires next).
    {
        std::map<std::string, std::string> options;
        for (int i = 0; i < 64; ++i) {
            options["key_" + std::to_string(i)] = "value_" + std::to_string(i);
        }
        expect(with_options(options, broken_properties),
               TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, UNSUPPORTED);
    }
    // 65 entries: over the bound, rejected before the properties rail.
    {
        std::map<std::string, std::string> options;
        for (int i = 0; i < 65; ++i) {
            options["key_" + std::to_string(i)] = "value_" + std::to_string(i);
        }
        expect(with_options(options, broken_properties),
               TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, RESOURCE_REJECTED);
    }
    // 256-byte key: at the bound, admitted. 257-byte key: rejected.
    {
        expect(with_options({{std::string(256, 'k'), "v"}}, broken_properties),
               TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, UNSUPPORTED);
        expect(with_options({{std::string(257, 'k'), "v"}}, broken_properties),
               TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, RESOURCE_REJECTED);
    }
    // 4096-byte value: at the bound, admitted. 4097-byte value: rejected.
    {
        expect(with_options({{"k", std::string(4096, 'v')}}, broken_properties),
               TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, UNSUPPORTED);
        expect(with_options({{"k", std::string(4097, 'v')}}, broken_properties),
               TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, RESOURCE_REJECTED);
    }
    // NUL bytes can never cross the C-string boundary (broken_properties
    // oracle: a pass would surface the properties rail as UNSUPPORTED
    // instead).
    {
        expect(with_options({{std::string("ak\0ima", 6), "v"}}, broken_properties),
               TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, RESOURCE_REJECTED);
        expect(with_options({{"k", std::string("v\0x", 3)}}, broken_properties),
               TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, RESOURCE_REJECTED);
    }
}

// Sanity: the pass oracle itself - valid storage options with valid
// properties reach the D11 file gate, and with broken properties the
// properties rail fires.
TEST_F(LanceIndexWorkerParamsTest, StorageOptionsPassOracleIsSound) {
    TLanceIndexWorkerDispatch dispatch = make_dispatch();
    dispatch.__set_storage_options(
            std::map<std::string, std::string>{{"endpoint", "https://oss-cn-beijing.aliyuncs.com"},
                                               {"region", "cn-beijing"}});
    run_and_assert_envelope(dispatch, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED,
                            RESOURCE_REJECTED, nullptr);
    dispatch.__set_properties_json(R"({"index_type":"ivf_flat"})");
    run_and_assert_envelope(dispatch, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED,
                            UNSUPPORTED, nullptr);
}

// The no-leak discipline: a storage-option rejection envelope carries at most
// the static category - never a key or value - and neither does the diag
// pipe.
TEST_F(LanceIndexWorkerParamsTest, StorageOptionSecretsNeverLeak) {
    const std::string secret_key = "aws_secret_access_key_id";
    const std::string secret_value = "AKIAIOSFODNN7EXAMPLE";
    TLanceIndexWorkerDispatch dispatch = make_dispatch();
    std::map<std::string, std::string> options;
    // Force the count rail with the secret-looking pair included.
    for (int i = 0; i < 64; ++i) {
        options["key_" + std::to_string(i)] = "value_" + std::to_string(i);
    }
    options[secret_key] = secret_value;
    dispatch.__set_storage_options(options);

    WorkerRunResult run;
    WorkerFrames frames = run_and_assert_envelope(
            dispatch, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED, RESOURCE_REJECTED,
            &run);
    EXPECT_EQ(frames.result->message, "resource rejected");
    std::string whole_stream(run.result_stream.begin(), run.result_stream.end());
    EXPECT_EQ(whole_stream.find(secret_key), std::string::npos);
    EXPECT_EQ(whole_stream.find(secret_value), std::string::npos);
    EXPECT_EQ(run.diag_text().find(secret_key), std::string::npos);
    EXPECT_EQ(run.diag_text().find(secret_value), std::string::npos);
    EXPECT_TRUE(run.diag_stream.empty());
}

// The properties_json five-key whitelist and the admitted-bounds snapshot
// replay (frame fields 16/17). Oracle: pass = the step-5 D11
// RESOURCE_REJECTED envelope; fail = the step-4 UNSUPPORTED envelope.
TEST_F(LanceIndexWorkerParamsTest, PropertiesMappingMatrix) {
    auto with_properties = [](const std::string& properties) {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.__set_properties_json(properties);
        return dispatch;
    };
    constexpr auto REJECTED = TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED;
    auto expect_pass = [&](const TLanceIndexWorkerDispatch& dispatch) {
        run_and_assert_envelope(dispatch, REJECTED, RESOURCE_REJECTED, nullptr);
    };
    auto expect_fail = [&](const TLanceIndexWorkerDispatch& dispatch) {
        WorkerRunResult run;
        WorkerFrames frames =
                run_and_assert_envelope(dispatch, REJECTED, UNSUPPORTED, &run);
        EXPECT_EQ(frames.result->message, "unsupported schema contract");
    };

    // The baseline passes; key case-insensitivity is honored.
    expect_pass(make_dispatch());
    expect_pass(with_properties(
            R"({"INDEX_TYPE":"IVF_PQ","Metric":"l2","NUM_PARTITIONS":"2","num_sub_vectors":"2"})"));
    // Unknown key, duplicate keys under the fold.
    expect_fail(with_properties(
            R"({"index_type":"IVF_PQ","num_partitions":"2","num_sub_vectors":"2","num_clusters":"4"})"));
    expect_fail(with_properties(
            R"({"index_type":"IVF_PQ","metric":"l2","METRIC":"l2","num_partitions":"2","num_sub_vectors":"2"})"));
    // index_type must be ivf_pq (fold-insensitive).
    expect_pass(
            with_properties(R"({"index_type":"ivf_pq","num_partitions":"2","num_sub_vectors":"2"})"));
    expect_fail(
            with_properties(R"({"index_type":"IVF_FLAT","num_partitions":"2","num_sub_vectors":"2"})"));
    expect_fail(with_properties(R"({"num_partitions":"2","num_sub_vectors":"2"})"));
    // metric: the request-omitted default passes (lance L2); the vocabulary
    // is l2/cosine/dot, fold-insensitive; hamming is refused.
    expect_pass(
            with_properties(R"({"index_type":"IVF_PQ","num_partitions":"2","num_sub_vectors":"2"})"));
    expect_pass(with_properties(
            R"({"index_type":"IVF_PQ","metric":"COSINE","num_partitions":"2","num_sub_vectors":"2"})"));
    expect_pass(with_properties(
            R"({"index_type":"IVF_PQ","metric":"dot","num_partitions":"2","num_sub_vectors":"2"})"));
    expect_fail(with_properties(
            R"({"index_type":"IVF_PQ","metric":"hamming","num_partitions":"2","num_sub_vectors":"2"})"));
    // num_bits is pinned to 8 (absent means the FE pinned 8 already).
    expect_pass(with_properties(
            R"({"index_type":"IVF_PQ","num_bits":"8","num_partitions":"2","num_sub_vectors":"2"})"));
    expect_fail(with_properties(
            R"({"index_type":"IVF_PQ","num_bits":"4","num_partitions":"2","num_sub_vectors":"2"})"));
    expect_fail(with_properties(
            R"({"index_type":"IVF_PQ","num_bits":"abc","num_partitions":"2","num_sub_vectors":"2"})"));
    // Positive integers only, digits only.
    expect_fail(
            with_properties(R"({"index_type":"IVF_PQ","num_partitions":"0","num_sub_vectors":"2"})"));
    expect_fail(
            with_properties(R"({"index_type":"IVF_PQ","num_partitions":"-1","num_sub_vectors":"2"})"));
    expect_fail(
            with_properties(R"({"index_type":"IVF_PQ","num_partitions":" 2","num_sub_vectors":"2"})"));
    expect_fail(with_properties(
            R"({"index_type":"IVF_PQ","num_partitions":"4294967296","num_sub_vectors":"2"})"));
    expect_fail(with_properties(R"({"index_type":"IVF_PQ","num_partitions":"2"})"));
    expect_fail(with_properties(R"({"index_type":"IVF_PQ","num_sub_vectors":"2"})"));
    expect_fail(
            with_properties(R"({"index_type":"IVF_PQ","num_partitions":"2","num_sub_vectors":"0"})"));
    // Non-string values and non-object payloads.
    expect_fail(
            with_properties(R"({"index_type":"IVF_PQ","num_partitions":2,"num_sub_vectors":"2"})"));
    expect_fail(with_properties(R"(["IVF_PQ"])"));
    expect_fail(with_properties("{not json"));
    // The admitted bounds snapshot replays the admission-time config ceiling:
    // within-snapshot passes, exceeded fails.
    expect_pass(with_properties(
            R"({"index_type":"IVF_PQ","num_partitions":"1024","num_sub_vectors":"64"})"));
    expect_fail(with_properties(
            R"({"index_type":"IVF_PQ","num_partitions":"1025","num_sub_vectors":"64"})"));
    expect_fail(with_properties(
            R"({"index_type":"IVF_PQ","num_partitions":"1024","num_sub_vectors":"65"})"));

    // The dispatch-level index_type fold is revalidated independently.
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.index_type = "ivf_pq";
        expect_pass(dispatch);
        dispatch.index_type = "IVF_FLAT";
        expect_fail(dispatch);
    }
    // Missing/empty properties_json on a build is a safe rejection.
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.__isset.properties_json = false;
        expect_fail(dispatch);
        dispatch.__set_properties_json("");
        expect_fail(dispatch);
    }
    // A missing column name on a build is a safe rejection.
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.column_name.clear();
        expect_fail(dispatch);
    }
}

// Deep nesting inside the frame cap: ~400KB of nested arrays as
// properties_json must be a safe UNSUPPORTED rejection through the iterative
// parser - rapidjson's default recursive descent would blow the worker's
// stack on this FE-controlled payload, and the in-process driver turns a
// crash into a test-binary SEGV, so this test fails loudly on a regression.
TEST_F(LanceIndexWorkerParamsTest, DeeplyNestedPropertiesJsonRejectedNotCrash) {
    TLanceIndexWorkerDispatch dispatch = make_dispatch();
    const size_t depth = 200 * 1024;
    std::string nested(depth, '[');
    nested.append(depth, ']');
    dispatch.__set_properties_json(nested);
    run_and_assert_envelope(dispatch, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED,
                            UNSUPPORTED, nullptr);
}

// The admitted bounds snapshot (frame fields 16/17): absent or non-positive
// is a safe rejection, never judged against a hard-coded bound - the worker
// subprocess reads no BE config.
TEST_F(LanceIndexWorkerParamsTest, AdmittedBoundsSnapshotRequired) {
    constexpr auto REJECTED = TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED;
    auto expect_unsupported = [&](TLanceIndexWorkerDispatch dispatch) {
        run_and_assert_envelope(dispatch, REJECTED, UNSUPPORTED, nullptr);
    };
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.__isset.max_num_partitions = false;
        dispatch.__isset.max_num_sub_vectors = false;
        expect_unsupported(dispatch);
    }
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.__isset.max_num_sub_vectors = false;
        expect_unsupported(dispatch);
    }
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.__set_max_num_partitions(0);
        expect_unsupported(dispatch);
        dispatch.__set_max_num_partitions(-5);
        expect_unsupported(dispatch);
    }
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.__set_max_num_sub_vectors(0);
        expect_unsupported(dispatch);
    }
}

// Scalar-sanity rails: identity fields that cross C boundaries or drive the
// pinned open are revalidated before anything native happens.
TEST_F(LanceIndexWorkerParamsTest, ScalarSanityRails) {
    constexpr auto REJECTED = TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED;
    auto expect_resource_rejected = [&](TLanceIndexWorkerDispatch dispatch) {
        run_and_assert_envelope(dispatch, REJECTED, RESOURCE_REJECTED, nullptr);
    };
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.dataset_uri.clear();
        expect_resource_rejected(dispatch);
    }
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.index_name.clear();
        expect_resource_rejected(dispatch);
    }
    {
        // Version 0 is the latest sentinel; the pinned open never uses it.
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.admitted_dataset_version = 0;
        expect_resource_rejected(dispatch);
        dispatch.admitted_dataset_version = -1;
        expect_resource_rejected(dispatch);
    }
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.dataset_uri = std::string("s3://bucket/\0evil", 17);
        expect_resource_rejected(dispatch);
    }
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.index_name = std::string("idx\0v", 6);
        expect_resource_rejected(dispatch);
    }
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.column_name = std::string("v\0x", 3);
        expect_resource_rejected(dispatch);
    }
    {
        // A mutation type outside CREATE/REPLACE/DROP.
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.mutation_type = static_cast<TLanceIndexWorkerMutationType::type>(0);
        expect_resource_rejected(dispatch);
    }
}

// A DROP dispatch skips the build-only properties validation: no
// properties_json and no contract at all still passes the parameter stage
// (the D11 file gate is what fires here).
TEST_F(LanceIndexWorkerParamsTest, DropSkipsBuildOnlyValidation) {
    TLanceIndexWorkerDispatch dispatch = make_dispatch();
    dispatch.mutation_type = TLanceIndexWorkerMutationType::DROP;
    dispatch.__isset.properties_json = false;
    dispatch.__isset.schema_contract_json = false;
    run_and_assert_envelope(dispatch, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED,
                            RESOURCE_REJECTED, nullptr);
}

// Typed open failures on non-local URIs classify as RESOURCE_REJECTED - the
// CREDENTIAL_EXPIRED band value (-3) is reserved for typed evidence
// lance-c 0.1.9 cannot produce - and the envelope message stays a static
// category with no provider text.
TEST_F(LanceIndexWorkerParamsTest, OpenFailuresNeverClassifyAsCredentialExpired) {
    constexpr auto REJECTED = TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED;
    // NOT_FOUND with a failed observation open: memory:// passes the D11 gate
    // but holds no cross-call state, so both the pinned open and the
    // read-only observation fail typed.
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.dataset_uri = "memory://probe/nonexistent-" + std::to_string(::getpid());
        WorkerRunResult run;
        WorkerFrames frames =
                run_and_assert_envelope(dispatch, REJECTED, RESOURCE_REJECTED, &run);
        EXPECT_NE(frames.result->lance_error_code, -3);
        EXPECT_EQ(frames.result->message, "resource rejected");
    }
    // A typed INVALID_ARGUMENT open failure (http:// has no object-store
    // provider): still RESOURCE_REJECTED, still no provider text.
    {
        TLanceIndexWorkerDispatch dispatch = make_dispatch();
        dispatch.dataset_uri = "http://127.0.0.1:1/ds";
        std::map<std::string, std::string> options = {
                {"aws_access_key_id", "AKIAIOSFODNN7EXAMPLE"},
                {"aws_secret_access_key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
        };
        dispatch.__set_storage_options(options);
        WorkerRunResult run;
        WorkerFrames frames =
                run_and_assert_envelope(dispatch, REJECTED, RESOURCE_REJECTED, &run);
        EXPECT_NE(frames.result->lance_error_code, -3);
        EXPECT_EQ(frames.result->message, "resource rejected");
        std::string whole_stream(run.result_stream.begin(), run.result_stream.end());
        EXPECT_EQ(whole_stream.find("AKIAIOSFODNN7EXAMPLE"), std::string::npos);
        EXPECT_EQ(run.diag_text().find("AKIAIOSFODNN7EXAMPLE"), std::string::npos);
        // No provider message text ever crosses the envelope either.
        EXPECT_EQ(frames.result->message.find("http"), std::string::npos);
    }
}

// A result frame that cannot fit the injected bound is a failed write:
// nonzero exit, no result frame, only the handshake on the result pipe,
// static diag.
TEST_F(LanceIndexWorkerParamsTest, ResultFrameBound) {
    WorkerRunResult run = lance_run_worker(lance_worker_dispatch_frame(make_dispatch()),
                                           512 * 1024, 16);
    EXPECT_EQ(run.exit_code, 1);
    WorkerFrames frames = lance_decode_worker_frames(run);
    EXPECT_EQ(frames.frame_count, 1U) << "only the handshake may precede the failed result";
    EXPECT_TRUE(frames.handshake.has_value());
    EXPECT_EQ(run.diag_text(), "lance worker: result write failed\n");
}

// ---------------------------------------------------------------------------
// Fault-injection debug points. WORKER_DEBUG_POINT_HANG sits after the
// dispatch is fully read and validated, before the first lance FFI call;
// these cases observe the block in-process (handshake out, then silence) and
// release it by removing the point (the DBUG_BLOCK poll). The post-FFI
// WORKER_DEBUG_POINT_SKIP_RESULT point cannot be reached hermetically
// in-process (no non-local object store survives across FFI calls - see the
// reach note at the top of this file); its chain is covered supervisor-side
// in a later commit with the fake worker honoring the handoff env.
// ---------------------------------------------------------------------------

int64_t steady_now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
}

bool wait_pred(const std::function<bool()>& pred, int64_t deadline_ms) {
    const int64_t deadline = steady_now_ms() + deadline_ms;
    while (steady_now_ms() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

// Runs run_index_worker on a dedicated thread so a test can observe the hang
// point and release it. The thread state is heap-shared so the (move-only)
// handle can be returned by value safely.
struct AsyncWorkerRun {
    int result_read_fd = -1;
    int diag_read_fd = -1;
    std::thread worker_thread;
    std::thread writer_thread;
    std::shared_ptr<std::atomic<bool>> exited = std::make_shared<std::atomic<bool>>(false);
    std::shared_ptr<std::atomic<int>> exit_code = std::make_shared<std::atomic<int>>(-1);
};

AsyncWorkerRun start_async_worker(const TLanceIndexWorkerDispatch& dispatch) {
    int dispatch_pipe[2];
    int result_pipe[2];
    int diag_pipe[2];
    if (::pipe(dispatch_pipe) != 0 || ::pipe(result_pipe) != 0 || ::pipe(diag_pipe) != 0) {
        throw std::runtime_error("pipe() failed");
    }
    AsyncWorkerRun run;
    run.result_read_fd = result_pipe[0];
    run.diag_read_fd = diag_pipe[0];
    const int result_write_fd = result_pipe[1];
    const int diag_write_fd = diag_pipe[1];
    const std::vector<uint8_t> bytes = lance_worker_dispatch_frame(dispatch);
    run.writer_thread = std::thread([bytes, fd = dispatch_pipe[1]]() {
        const uint8_t* next = bytes.data();
        size_t left = bytes.size();
        while (left > 0) {
            ssize_t count = ::write(fd, next, left);
            if (count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                break; // EPIPE: the worker exited early; nothing more to feed
            }
            next += count;
            left -= static_cast<size_t>(count);
        }
        ::close(fd);
    });
    run.worker_thread = std::thread([dispatch_fd = dispatch_pipe[0], result_write_fd,
                                     diag_write_fd, exited = run.exited,
                                     exit_code = run.exit_code]() {
        IndexWorkerParams params;
        params.dispatch_fd = dispatch_fd;
        params.result_fd = result_write_fd;
        params.diag_fd = diag_write_fd;
        const int code = run_index_worker(params);
        ::close(dispatch_fd);
        ::close(result_write_fd);
        ::close(diag_write_fd);
        exit_code->store(code);
        exited->store(true);
    });
    return run;
}

// Joins the threads and drains the (already EOF-terminated) pipes. Any frame
// bytes the test already consumed from result_read_fd are excluded by design.
WorkerRunResult finish_async_worker(AsyncWorkerRun& run) {
    run.worker_thread.join();
    run.writer_thread.join();
    WorkerRunResult result;
    result.exit_code = run.exit_code->load();
    result.result_stream = lance_read_all_bytes(run.result_read_fd);
    result.diag_stream = lance_read_all_bytes(run.diag_read_fd);
    ::close(run.result_read_fd);
    ::close(run.diag_read_fd);
    return result;
}

// Scope-exit backstop pairing with start_async_worker: a failed ASSERT between
// start and finish_async_worker returns from the test body with both threads
// still joinable, and destroying a joinable std::thread calls std::terminate -
// one placement regression would abort the whole shard instead of failing a
// single test. Teardown first releases the hang point (a worker blocked in
// DBUG_BLOCK would otherwise keep the join from ever returning; the removal
// is idempotent and mirrors the release-then-finish order the tests use on
// their success paths), then joins whatever start_async_worker left joinable.
// After a normal finish_async_worker both threads are already joined and only
// the point removal runs. Declared after each test's own Defer so it
// destructs first, before that Defer touches the point or the debug-point
// gate.
struct AsyncWorkerJoinGuard {
    explicit AsyncWorkerJoinGuard(AsyncWorkerRun& run) : _run(run) {}
    ~AsyncWorkerJoinGuard() {
        DebugPoints::instance()->remove(WORKER_DEBUG_POINT_HANG);
        if (_run.worker_thread.joinable()) {
            _run.worker_thread.join();
        }
        if (_run.writer_thread.joinable()) {
            _run.writer_thread.join();
        }
    }

private:
    AsyncWorkerRun& _run;
};

// Reads exactly one length-prefixed frame within the timeout (nullopt on
// timeout or EOF). Used to observe the handshake arriving while the worker is
// blocked on the hang point.
std::optional<std::vector<uint8_t>> read_frame_within(int fd, int64_t timeout_ms) {
    const int64_t deadline = steady_now_ms() + timeout_ms;
    auto read_exactly = [fd, deadline](uint8_t* out, size_t size) -> bool {
        size_t got = 0;
        while (got < size) {
            const int64_t left = deadline - steady_now_ms();
            if (left <= 0) {
                return false;
            }
            struct pollfd pfd {fd, POLLIN, 0};
            const int ready = ::poll(&pfd, 1, static_cast<int>(std::min<int64_t>(left, 200)));
            if (ready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return false;
            }
            if (ready == 0) {
                continue;
            }
            const ssize_t count = ::read(fd, out + got, size - got);
            if (count == 0) {
                return false; // EOF
            }
            if (count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return false;
            }
            got += static_cast<size_t>(count);
        }
        return true;
    };
    uint8_t header[4];
    if (!read_exactly(header, sizeof(header))) {
        return std::nullopt;
    }
    const uint32_t length = lance_worker_frame_be32(header);
    if (length == 0 || length > 8 * 1024) {
        return std::nullopt;
    }
    std::vector<uint8_t> payload(length);
    if (!read_exactly(payload.data(), payload.size())) {
        return std::nullopt;
    }
    return payload;
}

// True when the pipe signals readability or hangup within the timeout (used
// to assert that NOTHING arrives while the worker is blocked).
bool pipe_signals_within(int fd, int64_t timeout_ms) {
    struct pollfd pfd {fd, static_cast<short>(POLLIN | POLLHUP | POLLERR), 0};
    int ready;
    do {
        ready = ::poll(&pfd, 1, static_cast<int>(timeout_ms));
    } while (ready < 0 && errno == EINTR);
    return ready > 0;
}

TEST_F(LanceIndexWorkerParamsTest, DebugPointHangBlocksBeforeFfiUntilRemoved) {
    const bool saved_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add(WORKER_DEBUG_POINT_HANG);
    Defer defer([saved_debug_points] {
        DebugPoints::instance()->remove(WORKER_DEBUG_POINT_HANG);
        config::enable_debug_points = saved_debug_points;
    });

    // A dispatch that passes every pre-FFI rail and fails the pinned open
    // with a typed NOT_FOUND (memory:// holds no cross-call state).
    TLanceIndexWorkerDispatch dispatch = make_dispatch();
    dispatch.dataset_uri = "memory://probe/hang-" + std::to_string(::getpid());
    AsyncWorkerRun run = start_async_worker(dispatch);
    // Declared before the first ASSERT below: any early return still joins
    // the two threads instead of std::terminate-ing the shard.
    AsyncWorkerJoinGuard join_guard(run);

    // The block sits after the handshake: the first frame arrives, then
    // silence - the FFI has not run yet, so no result frame can exist.
    const std::optional<std::vector<uint8_t>> handshake =
            read_frame_within(run.result_read_fd, 10000);
    ASSERT_TRUE(handshake.has_value()) << "the handshake must precede the hang point";
    TLanceIndexWorkerHandshake decoded_handshake;
    ASSERT_TRUE(lance_worker_decode_compact(*handshake, &decoded_handshake));
    EXPECT_EQ(decoded_handshake.protocol_magic, HANDSHAKE_PROTOCOL_MAGIC);
    EXPECT_FALSE(pipe_signals_within(run.result_read_fd, 500))
            << "a worker hung before the FFI must not emit a result frame";
    EXPECT_FALSE(run.exited->load());

    // Releasing the point unblocks the worker (the DBUG_BLOCK poll); the
    // typed open failure then completes with the trusted rejection envelope.
    DebugPoints::instance()->remove(WORKER_DEBUG_POINT_HANG);
    ASSERT_TRUE(wait_pred([&] { return run.exited->load(); }, 20000));
    WorkerRunResult result = finish_async_worker(run);
    EXPECT_EQ(result.exit_code, 0);
    const std::vector<std::vector<uint8_t>> frames = result.frames();
    ASSERT_EQ(frames.size(), 1U) << "only the post-FFI rejection frame remains";
    TLanceIndexWorkerResult worker_result;
    ASSERT_TRUE(lance_worker_decode_compact(frames.front(), &worker_result));
    EXPECT_EQ(worker_result.phase, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED);
    EXPECT_EQ(worker_result.lance_error_code, RESOURCE_REJECTED);
    EXPECT_EQ(worker_result.message, "resource rejected");
    EXPECT_TRUE(result.diag_stream.empty());

    // The point sits after validation: a dispatch whose properties exceed the
    // admitted bounds is rejected with the UNSUPPORTED envelope without ever
    // blocking. Async so a placement regression releases cleanly instead of
    // hanging the test binary.
    DebugPoints::instance()->add(WORKER_DEBUG_POINT_HANG);
    TLanceIndexWorkerDispatch invalid = make_dispatch();
    invalid.__set_properties_json(
            R"({"index_type":"IVF_PQ","num_partitions":"99999","num_sub_vectors":"2"})");
    AsyncWorkerRun invalid_run = start_async_worker(invalid);
    if (!wait_pred([&] { return invalid_run.exited->load(); }, 10000)) {
        DebugPoints::instance()->remove(WORKER_DEBUG_POINT_HANG); // release for cleanup
        finish_async_worker(invalid_run);
        FAIL() << "the hang point fired before dispatch validation completed";
    }
    WorkerRunResult invalid_result = finish_async_worker(invalid_run);
    EXPECT_EQ(invalid_result.exit_code, 0);
    WorkerFrames invalid_frames = lance_decode_worker_frames(invalid_result);
    EXPECT_EQ(invalid_frames.frame_count, 2U);
    ASSERT_TRUE(invalid_frames.result.has_value());
    EXPECT_EQ(invalid_frames.result->lance_error_code, UNSUPPORTED);
}

TEST_F(LanceIndexWorkerParamsTest, DebugPointHangReachesWorkerThroughControlledEnv) {
    // The exec'd worker starts with a default-off gate and an empty registry;
    // the supervisor's controlled environment is the handoff channel (the
    // per-launch snapshot). Drive the worker entry exactly as the exec'd
    // process would: gate off, point present only in the env.
    const bool saved_debug_points = config::enable_debug_points;
    config::enable_debug_points = false;
    ASSERT_EQ(::setenv(WORKER_DEBUG_POINTS_ENV, WORKER_DEBUG_POINT_HANG, 1), 0);
    Defer defer([saved_debug_points] {
        ::unsetenv(WORKER_DEBUG_POINTS_ENV);
        DebugPoints::instance()->remove(WORKER_DEBUG_POINT_HANG);
        config::enable_debug_points = saved_debug_points;
    });

    TLanceIndexWorkerDispatch dispatch = make_dispatch();
    dispatch.dataset_uri = "memory://probe/hang-env-" + std::to_string(::getpid());
    AsyncWorkerRun run = start_async_worker(dispatch);
    // Declared before the first ASSERT below: any early return still joins
    // the two threads instead of std::terminate-ing the shard.
    AsyncWorkerJoinGuard join_guard(run);

    const std::optional<std::vector<uint8_t>> handshake =
            read_frame_within(run.result_read_fd, 10000);
    ASSERT_TRUE(handshake.has_value());
    EXPECT_FALSE(pipe_signals_within(run.result_read_fd, 500));
    EXPECT_FALSE(run.exited->load()) << "the env-handed point must block the worker";

    DebugPoints::instance()->remove(WORKER_DEBUG_POINT_HANG);
    ASSERT_TRUE(wait_pred([&] { return run.exited->load(); }, 20000));
    WorkerRunResult result = finish_async_worker(run);
    EXPECT_EQ(result.exit_code, 0);
    const std::vector<std::vector<uint8_t>> frames = result.frames();
    ASSERT_EQ(frames.size(), 1U);
    TLanceIndexWorkerResult worker_result;
    ASSERT_TRUE(lance_worker_decode_compact(frames.front(), &worker_result));
    EXPECT_EQ(worker_result.lance_error_code, RESOURCE_REJECTED);
    // The worker entry re-enabled the gate itself (join() synchronizes).
    EXPECT_TRUE(config::enable_debug_points);
}

TEST_F(LanceIndexWorkerParamsTest, DebugPointEnvHandoffIgnoresUnknownNames) {
    // Names outside the fixed allowlist never register and never arm the
    // gate: the worker proceeds to the typed open failure unblocked.
    const bool saved_debug_points = config::enable_debug_points;
    config::enable_debug_points = false;
    ASSERT_EQ(::setenv(WORKER_DEBUG_POINTS_ENV, "LanceIndexWorker.skip_report,Other.Point", 1),
              0);
    Defer defer([saved_debug_points] {
        ::unsetenv(WORKER_DEBUG_POINTS_ENV);
        config::enable_debug_points = saved_debug_points;
    });

    TLanceIndexWorkerDispatch dispatch = make_dispatch();
    dispatch.dataset_uri = "memory://probe/unknown-point-" + std::to_string(::getpid());
    WorkerRunResult run = lance_run_worker(lance_worker_dispatch_frame(dispatch));
    EXPECT_EQ(run.exit_code, 0);
    WorkerFrames frames = lance_decode_worker_frames(run);
    EXPECT_EQ(frames.frame_count, 2U);
    ASSERT_TRUE(frames.result.has_value());
    EXPECT_EQ(frames.result->lance_error_code, RESOURCE_REJECTED);
    EXPECT_FALSE(config::enable_debug_points)
            << "an allowlist-foreign name must not arm the worker gate";
}

} // namespace
} // namespace doris::lance
