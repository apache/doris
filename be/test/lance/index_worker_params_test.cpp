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
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include "lance/lance_test_util.h"

// LanceIndexWorkerParamsTest: drives run_index_worker in-process over pipe
// trios and pins the full-path envelope contract — dispatch frame bounds, the
// handshake-before-every-result ordering, the D11 local-dataset rejection, the
// storage-option bounds and their no-leak discipline, the properties_json
// five-key whitelist mapping, the admitted-bounds snapshot replay, and the
// typed open-failure classification.
//
// Hermetic reach note: every local dataset is rejected at the D11 file gate and
// lance-c 0.1.9 has no hermetic non-local object store (memory:// state does
// not survive across FFI calls; http:// has no provider; cloud schemes need the
// network). The full path is therefore covered through the pinned-open failure
// classification; the steps behind a successful open (contract revalidation,
// the native invocation and its code mapping) are covered at the contract and
// mapping seams in LanceIndexWorkerContractTest and NativeErrorCodeMappingTable.

namespace doris::lance {
namespace {

constexpr TLanceIndexJobResultCode::type RESOURCE_REJECTED =
        TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED;
constexpr TLanceIndexJobResultCode::type UNSUPPORTED =
        TLanceIndexJobResultCode::PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT;
constexpr TLanceIndexJobResultCode::type CREDENTIAL_EXPIRED =
        TLanceIndexJobResultCode::PRE_INVOCATION_CREDENTIAL_EXPIRED;

class LanceIndexWorkerParamsTest : public ::testing::Test {
protected:
    sighandler_t old_sigpipe_ = SIG_DFL;

    void SetUp() override {
        // A writer thread can outlive the worker's dispatch drain (an early
        // protocol exit); never let SIGPIPE kill the test process.
        old_sigpipe_ = signal(SIGPIPE, SIG_IGN);
    }

    void TearDown() override { signal(SIGPIPE, old_sigpipe_); }

    // A complete, valid CREATE dispatch pointing at a real local dataset through
    // a file:// URI. Validation order makes this the distinguishing probe:
    //   * a rail that fires in dispatch revalidation yields its specific
    //     envelope (UNSUPPORTED or RESOURCE_REJECTED) after the handshake;
    //   * passing everything yields the step-5 D11 RESOURCE_REJECTED envelope.
    static TLanceIndexJobDispatch make_dispatch() {
        TLanceIndexJobDispatch dispatch;
        dispatch.job_id = 424242;
        dispatch.dispatch_revision = 7;
        dispatch.invocation_id = "0123456789abcdef0123456789abcdef";
        dispatch.be_process_epoch = 909090;
        dispatch.deadline_ms = 1700000000000LL;
        dispatch.mutation_type = TLanceIndexMutationType::CREATE;
        dispatch.index_name = "idx_v";
        dispatch.column_name = "v";
        dispatch.index_type = "IVF_PQ";
        dispatch.__set_properties_json(
                R"({"index_type":"IVF_PQ","metric":"l2","num_bits":"8","num_partitions":"2","num_sub_vectors":"2"})");
        dispatch.dataset_uri = "file://" + lance_test_dataset_path("s14.lance");
        dispatch.admitted_dataset_version = 1;
        dispatch.schema_contract_json =
                R"({"scv":1,"flds":[{"fid":0,"nn":"v","nt":"fixed_size_list","nul":false,"fsd":4,"vet":"float32","ven":true}]})";
        dispatch.__set_max_num_partitions(1024);
        dispatch.__set_max_num_sub_vectors(64);
        return dispatch;
    }

    // Runs the worker on one dispatch and asserts the envelope shape shared by
    // every trusted rejection: exit 0, exactly two frames (handshake first, then
    // the result), the identity quad echoed, no trailing garbage.
    static WorkerFrames run_and_assert_envelope(const TLanceIndexJobDispatch& dispatch,
                                                TLanceIndexJobResultCode::type expected_code,
                                                WorkerRunResult* run_out) {
        WorkerRunResult run = run_worker(dispatch_frame_bytes(dispatch));
        EXPECT_EQ(run.exit_code, 0);
        WorkerFrames frames = decode_worker_frames(run);
        EXPECT_FALSE(frames.trailing_garbage);
        EXPECT_EQ(frames.frame_count, 2U)
                << "every trusted result is preceded by the handshake frame";
        assert_handshake_sane(frames);
        assert_report_identity(frames, dispatch);
        if (frames.report.has_value()) {
            EXPECT_EQ(frames.report->result_code, expected_code);
            EXPECT_TRUE(frames.report->__isset.external_metadata_advanced);
            EXPECT_FALSE(frames.report->external_metadata_advanced);
            EXPECT_FALSE(frames.report->__isset.completion_reason);
            EXPECT_FALSE(frames.report->__isset.termination_proof);
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

    static void assert_report_identity(const WorkerFrames& frames,
                                       const TLanceIndexJobDispatch& dispatch) {
        ASSERT_TRUE(frames.report.has_value())
                << "the second stdout frame must decode as the job report";
        EXPECT_EQ(frames.report->job_id, dispatch.job_id);
        EXPECT_EQ(frames.report->dispatch_revision, dispatch.dispatch_revision);
        EXPECT_EQ(frames.report->invocation_id, dispatch.invocation_id);
        EXPECT_EQ(frames.report->be_process_epoch, dispatch.be_process_epoch);
    }

    // Runs the worker on raw dispatch bytes that must be rejected as a malformed
    // or undecodable frame: nonzero exit, NO frame of any kind on the result
    // pipe, and exactly the static diagnostic line on the diag pipe.
    static void assert_frame_rejected(const std::vector<uint8_t>& bytes,
                                      const std::string& expected_diag) {
        WorkerRunResult run = run_worker(bytes);
        EXPECT_EQ(run.exit_code, 1);
        EXPECT_TRUE(run.result_stream.empty())
                << "a malformed dispatch must not produce any stdout frame";
        EXPECT_EQ(run.diag_text(), expected_diag);
    }
};

// The D11 envelope: a local dataset (file:// or scheme-less) is rejected before
// any native call with a complete, identity-matched RESOURCE_REJECTED frame,
// EMA false, and a static-only message — after a sane handshake.
TEST_F(LanceIndexWorkerParamsTest, LocalDatasetYieldsResourceRejectedEnvelope) {
    WorkerRunResult run;
    WorkerFrames frames = run_and_assert_envelope(make_dispatch(), RESOURCE_REJECTED, &run);
    ASSERT_TRUE(frames.report.has_value());
    EXPECT_EQ(frames.report->sanitized_message, "resource rejected");
    EXPECT_TRUE(run.diag_stream.empty())
            << "a trusted rejection needs no diagnostic line";

    // A scheme-less absolute local path is local by definition (FE normalizer
    // admits nothing else without a scheme).
    TLanceIndexJobDispatch bare = make_dispatch();
    bare.dataset_uri = lance_test_dataset_path("s14.lance");
    run_and_assert_envelope(bare, RESOURCE_REJECTED, nullptr);
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
        WorkerRunResult run = run_worker({0x00, 0x00, 0x01, 0x00}, 255, 8 * 1024);
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
    // one-struct-per-frame (bytes appended beyond the frame stay unread in the
    // pipe, so the padding must ride inside the length prefix).
    {
        std::vector<uint8_t> bytes = dispatch_frame_bytes(make_dispatch());
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
        TLanceIndexJobDispatch dispatch = make_dispatch();
        std::map<std::string, std::string> options;
        for (int i = 0; i < 2000; ++i) {
            options["k" + std::to_string(i)] = "v";
        }
        dispatch.__set_storage_options(options);
        assert_frame_rejected(dispatch_frame_bytes(dispatch),
                              "lance worker: undecodable dispatch frame\n");
    }
    // An incomplete frame header (EOF mid-prefix).
    assert_frame_rejected({0x00, 0x01}, "lance worker: malformed dispatch frame\n");
}

// Storage-option bounds: the BE is the last line of defense before the values
// cross a C-string boundary. The pass/reject oracle pairs the options with a
// deliberately broken properties_json: RESOURCE_REJECTED means the storage rail
// fired first; UNSUPPORTED means the options passed and the properties rail
// fired.
TEST_F(LanceIndexWorkerParamsTest, StorageOptionsBounds) {
    const std::string broken_properties = R"({"index_type":"ivf_flat"})";

    auto with_options = [](std::map<std::string, std::string> options,
                               const std::string& properties) {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.__set_properties_json(properties);
        dispatch.__set_storage_options(std::move(options));
        return dispatch;
    };

    // 64 entries: at the bound, admitted (the properties rail fires next).
    {
        std::map<std::string, std::string> options;
        for (int i = 0; i < 64; ++i) {
            options["key_" + std::to_string(i)] = "value_" + std::to_string(i);
        }
        run_and_assert_envelope(with_options(options, broken_properties), UNSUPPORTED, nullptr);
    }
    // 65 entries: over the bound, rejected before the properties rail.
    {
        std::map<std::string, std::string> options;
        for (int i = 0; i < 65; ++i) {
            options["key_" + std::to_string(i)] = "value_" + std::to_string(i);
        }
        run_and_assert_envelope(with_options(options, broken_properties), RESOURCE_REJECTED,
                                nullptr);
    }
    // 256-byte key: at the bound, admitted. 257-byte key: rejected.
    {
        run_and_assert_envelope(
                with_options({{std::string(256, 'k'), "v"}}, broken_properties), UNSUPPORTED,
                nullptr);
        run_and_assert_envelope(
                with_options({{std::string(257, 'k'), "v"}}, broken_properties),
                RESOURCE_REJECTED, nullptr);
    }
    // 4096-byte value: at the bound, admitted. 4097-byte value: rejected.
    {
        run_and_assert_envelope(
                with_options({{"k", std::string(4096, 'v')}}, broken_properties), UNSUPPORTED,
                nullptr);
        run_and_assert_envelope(
                with_options({{"k", std::string(4097, 'v')}}, broken_properties),
                RESOURCE_REJECTED, nullptr);
    }
    // NUL bytes can never cross the C-string boundary (broken_properties oracle:
    // a pass would surface the properties rail as UNSUPPORTED instead).
    {
        run_and_assert_envelope(
                with_options({{std::string("ak\0ima", 6), "v"}}, broken_properties),
                RESOURCE_REJECTED, nullptr);
        run_and_assert_envelope(with_options({{"k", std::string("v\0x", 3)}}, broken_properties),
                                RESOURCE_REJECTED, nullptr);
    }
}

// Sanity: the pass oracle itself — valid storage options with valid properties
// reach the D11 file gate, and with broken properties the properties rail fires.
TEST_F(LanceIndexWorkerParamsTest, StorageOptionsPassOracleIsSound) {
    TLanceIndexJobDispatch dispatch = make_dispatch();
    dispatch.__set_storage_options(
            std::map<std::string, std::string>{{"endpoint", "https://oss-cn-beijing.aliyuncs.com"},
                                               {"region", "cn-beijing"}});
    run_and_assert_envelope(dispatch, RESOURCE_REJECTED, nullptr);
    dispatch.__set_properties_json(R"({"index_type":"ivf_flat"})");
    run_and_assert_envelope(dispatch, UNSUPPORTED, nullptr);
}

// The no-leak discipline: a storage-option rejection envelope carries at most
// the static category — never a key or value — and neither does the diag pipe.
TEST_F(LanceIndexWorkerParamsTest, StorageOptionSecretsNeverLeak) {
    const std::string secret_key = "aws_secret_access_key_id";
    const std::string secret_value = "AKIAIOSFODNN7EXAMPLE";
    TLanceIndexJobDispatch dispatch = make_dispatch();
    std::map<std::string, std::string> options;
    // Force the count rail with the secret-looking pair included.
    for (int i = 0; i < 64; ++i) {
        options["key_" + std::to_string(i)] = "value_" + std::to_string(i);
    }
    options[secret_key] = secret_value;
    dispatch.__set_storage_options(options);

    WorkerRunResult run;
    WorkerFrames frames = run_and_assert_envelope(dispatch, RESOURCE_REJECTED, &run);
    ASSERT_TRUE(frames.report.has_value());
    EXPECT_EQ(frames.report->sanitized_message, "resource rejected");
    std::string whole_stream(run.result_stream.begin(), run.result_stream.end());
    EXPECT_EQ(whole_stream.find(secret_key), std::string::npos);
    EXPECT_EQ(whole_stream.find(secret_value), std::string::npos);
    EXPECT_EQ(run.diag_text().find(secret_key), std::string::npos);
    EXPECT_EQ(run.diag_text().find(secret_value), std::string::npos);
    EXPECT_TRUE(run.diag_stream.empty());
}

// The properties_json five-key whitelist and the admitted-bounds snapshot
// replay. Oracle: pass = the step-5 D11 RESOURCE_REJECTED envelope;
// fail = the step-4 UNSUPPORTED envelope.
TEST_F(LanceIndexWorkerParamsTest, PropertiesMappingMatrix) {
    auto with_properties = [](const std::string& properties) {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.__set_properties_json(properties);
        return dispatch;
    };
    auto expect_pass = [](const TLanceIndexJobDispatch& dispatch) {
        run_and_assert_envelope(dispatch, RESOURCE_REJECTED, nullptr);
    };
    auto expect_fail = [](const TLanceIndexJobDispatch& dispatch) {
        WorkerRunResult run;
        WorkerFrames frames = run_and_assert_envelope(dispatch, UNSUPPORTED, &run);
        ASSERT_TRUE(frames.report.has_value());
        EXPECT_EQ(frames.report->sanitized_message, "unsupported schema contract");
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
    expect_pass(with_properties(R"({"index_type":"ivf_pq","num_partitions":"2","num_sub_vectors":"2"})"));
    expect_fail(with_properties(R"({"index_type":"IVF_FLAT","num_partitions":"2","num_sub_vectors":"2"})"));
    expect_fail(with_properties(R"({"num_partitions":"2","num_sub_vectors":"2"})"));
    // metric: the request-omitted default passes (lance L2); the vocabulary is
    // l2/cosine/dot, fold-insensitive; hamming is refused.
    expect_pass(with_properties(R"({"index_type":"IVF_PQ","num_partitions":"2","num_sub_vectors":"2"})"));
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
    expect_fail(with_properties(R"({"index_type":"IVF_PQ","num_partitions":"0","num_sub_vectors":"2"})"));
    expect_fail(with_properties(R"({"index_type":"IVF_PQ","num_partitions":"-1","num_sub_vectors":"2"})"));
    expect_fail(with_properties(R"({"index_type":"IVF_PQ","num_partitions":" 2","num_sub_vectors":"2"})"));
    expect_fail(with_properties(R"({"index_type":"IVF_PQ","num_partitions":"4294967296","num_sub_vectors":"2"})"));
    expect_fail(with_properties(R"({"index_type":"IVF_PQ","num_partitions":"2"})"));
    expect_fail(with_properties(R"({"index_type":"IVF_PQ","num_sub_vectors":"2"})"));
    expect_fail(with_properties(R"({"index_type":"IVF_PQ","num_partitions":"2","num_sub_vectors":"0"})"));
    // Non-string values and non-object payloads.
    expect_fail(with_properties(R"({"index_type":"IVF_PQ","num_partitions":2,"num_sub_vectors":"2"})"));
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
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.index_type = "ivf_pq";
        expect_pass(dispatch);
        dispatch.index_type = "IVF_FLAT";
        expect_fail(dispatch);
    }
    // Missing/empty properties_json on a build is a safe rejection.
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.__isset.properties_json = false;
        expect_fail(dispatch);
        dispatch.__set_properties_json("");
        expect_fail(dispatch);
    }
    // A missing column name on a build is a safe rejection.
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.column_name.clear();
        expect_fail(dispatch);
    }
}

// Deep nesting inside the frame cap: ~400KB of nested arrays as
// properties_json must be a safe UNSUPPORTED rejection through the iterative
// parser — rapidjson's default recursive descent would blow the worker's stack
// on this FE-controlled payload (review M7), and the in-process driver turns a
// crash into a test-binary SEGV, so this test fails loudly on a regression.
TEST_F(LanceIndexWorkerParamsTest, DeeplyNestedPropertiesJsonRejectedNotCrash) {
    TLanceIndexJobDispatch dispatch = make_dispatch();
    const size_t depth = 200 * 1024;
    std::string nested(depth, '[');
    nested.append(depth, ']');
    dispatch.__set_properties_json(nested);
    run_and_assert_envelope(dispatch, UNSUPPORTED, nullptr);
}

// The admitted bounds snapshot (fields 17/18): absent or non-positive is a safe
// rejection of a pre-snapshot record, never judged against a hard-coded bound.
TEST_F(LanceIndexWorkerParamsTest, AdmittedBoundsSnapshotRequired) {
    auto expect_unsupported = [](TLanceIndexJobDispatch dispatch) {
        run_and_assert_envelope(dispatch, UNSUPPORTED, nullptr);
    };
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.__isset.max_num_partitions = false;
        dispatch.__isset.max_num_sub_vectors = false;
        expect_unsupported(dispatch);
    }
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.__isset.max_num_sub_vectors = false;
        expect_unsupported(dispatch);
    }
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.__set_max_num_partitions(0);
        expect_unsupported(dispatch);
        dispatch.__set_max_num_partitions(-5);
        expect_unsupported(dispatch);
    }
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.__set_max_num_sub_vectors(0);
        expect_unsupported(dispatch);
    }
}

// Scalar-sanity rails: identity fields that cross C boundaries or drive the
// pinned open are revalidated before anything native happens.
TEST_F(LanceIndexWorkerParamsTest, ScalarSanityRails) {
    auto expect_resource_rejected = [](TLanceIndexJobDispatch dispatch) {
        run_and_assert_envelope(dispatch, RESOURCE_REJECTED, nullptr);
    };
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.dataset_uri.clear();
        expect_resource_rejected(dispatch);
    }
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.index_name.clear();
        expect_resource_rejected(dispatch);
    }
    {
        // Version 0 is the latest sentinel; the pinned open never uses it.
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.admitted_dataset_version = 0;
        expect_resource_rejected(dispatch);
        dispatch.admitted_dataset_version = -1;
        expect_resource_rejected(dispatch);
    }
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.dataset_uri = std::string("s3://bucket/\0evil", 17);
        expect_resource_rejected(dispatch);
    }
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.index_name = std::string("idx\0v", 6);
        expect_resource_rejected(dispatch);
    }
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.column_name = std::string("v\0x", 3);
        expect_resource_rejected(dispatch);
    }
    {
        // A mutation type outside CREATE/REPLACE/DROP.
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.mutation_type = static_cast<TLanceIndexMutationType::type>(0);
        expect_resource_rejected(dispatch);
    }
}

// A DROP dispatch skips the build-only properties validation: no
// properties_json at all still passes the parameter stage (the D11 file gate is
// what fires here).
TEST_F(LanceIndexWorkerParamsTest, DropSkipsBuildOnlyValidation) {
    TLanceIndexJobDispatch dispatch = make_dispatch();
    dispatch.mutation_type = TLanceIndexMutationType::DROP;
    dispatch.__isset.properties_json = false;
    dispatch.schema_contract_json.clear();
    run_and_assert_envelope(dispatch, RESOURCE_REJECTED, nullptr);
}

// Typed open failures on non-local URIs classify as RESOURCE_REJECTED — never
// CREDENTIAL_EXPIRED (lance-c 0.1.9 carries no typed credential-expiry
// evidence, so AccessDenied/InvalidAccessKeyId-shaped failures keep the
// resource classification), and the envelope message stays a static category
// with no provider text.
TEST_F(LanceIndexWorkerParamsTest, OpenFailuresNeverClassifyAsCredentialExpired) {
    // NOT_FOUND with a failed observation open: memory:// passes the D11 gate
    // but holds no cross-call state, so both the pinned open and the read-only
    // observation fail typed.
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.dataset_uri = "memory://probe/nonexistent-" + std::to_string(::getpid());
        WorkerRunResult run;
        WorkerFrames frames = run_and_assert_envelope(dispatch, RESOURCE_REJECTED, &run);
        ASSERT_TRUE(frames.report.has_value());
        EXPECT_NE(frames.report->result_code, CREDENTIAL_EXPIRED);
        EXPECT_EQ(frames.report->sanitized_message, "resource rejected");
        EXPECT_FALSE(frames.report->external_metadata_advanced);
    }
    // A typed INVALID_ARGUMENT open failure (http:// has no object-store
    // provider): still RESOURCE_REJECTED, still no provider text.
    {
        TLanceIndexJobDispatch dispatch = make_dispatch();
        dispatch.dataset_uri = "http://127.0.0.1:1/ds";
        std::map<std::string, std::string> options = {
                {"aws_access_key_id", "AKIAIOSFODNN7EXAMPLE"},
                {"aws_secret_access_key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
        };
        dispatch.__set_storage_options(options);
        WorkerRunResult run;
        WorkerFrames frames = run_and_assert_envelope(dispatch, RESOURCE_REJECTED, &run);
        ASSERT_TRUE(frames.report.has_value());
        EXPECT_NE(frames.report->result_code, CREDENTIAL_EXPIRED);
        EXPECT_EQ(frames.report->sanitized_message, "resource rejected");
        std::string whole_stream(run.result_stream.begin(), run.result_stream.end());
        EXPECT_EQ(whole_stream.find("AKIAIOSFODNN7EXAMPLE"), std::string::npos);
        EXPECT_EQ(run.diag_text().find("AKIAIOSFODNN7EXAMPLE"), std::string::npos);
        // No provider message text ever crosses the envelope either.
        EXPECT_EQ(frames.report->sanitized_message.find("http"), std::string::npos);
    }
}

// A result frame that cannot fit the injected bound is a failed write: nonzero
// exit, no result frame, only the handshake on the result pipe, static diag.
TEST_F(LanceIndexWorkerParamsTest, ResultFrameBound) {
    WorkerRunResult run = run_worker(dispatch_frame_bytes(make_dispatch()), 512 * 1024, 16);
    EXPECT_EQ(run.exit_code, 1);
    WorkerFrames frames = decode_worker_frames(run);
    EXPECT_EQ(frames.frame_count, 1U) << "only the handshake may precede the failed result";
    EXPECT_TRUE(frames.handshake.has_value());
    EXPECT_EQ(run.diag_text(), "lance worker: result write failed\n");
}

// The native error code mapping table (R4 §1.7) at its seam: every code with a
// wire counterpart maps; DATASET_ALREADY_EXISTS, PANIC-shaped codes, and any
// unknown code map to "no result frame" (nullopt). The behavioral half of the
// unmapped-code path — run_index_worker exiting nonzero without a result frame —
// sits behind a successful pinned open, which no hermetic environment provides
// (see the reach note at the top of this file); it is covered by the
// supervisor-side fake-worker tests and code inspection.
TEST_F(LanceIndexWorkerParamsTest, NativeErrorCodeMappingTable) {
    auto map = [](LanceErrorCode code) { return wire_result_code_for_native_error(code); };
    EXPECT_EQ(map(LANCE_OK), TLanceIndexJobResultCode::NATIVE_OK);
    EXPECT_EQ(map(LANCE_ERR_COMMIT_CONFLICT), TLanceIndexJobResultCode::NATIVE_COMMIT_CONFLICT);
    EXPECT_EQ(map(LANCE_ERR_NOT_FOUND), TLanceIndexJobResultCode::NATIVE_NOT_FOUND);
    EXPECT_EQ(map(LANCE_ERR_INVALID_ARGUMENT),
              TLanceIndexJobResultCode::NATIVE_INVALID_ARGUMENT);
    EXPECT_EQ(map(LANCE_ERR_NOT_SUPPORTED), TLanceIndexJobResultCode::NATIVE_NOT_SUPPORTED);
    EXPECT_EQ(map(LANCE_ERR_INDEX), TLanceIndexJobResultCode::NATIVE_INDEX);
    EXPECT_EQ(map(LANCE_ERR_IO), TLanceIndexJobResultCode::NATIVE_IO);
    EXPECT_EQ(map(LANCE_ERR_INTERNAL), TLanceIndexJobResultCode::NATIVE_INTERNAL);
    // No wire counterpart: DATASET_ALREADY_EXISTS, an out-of-range code such as
    // 9, and negative provider garbage all refuse to frame a result.
    EXPECT_EQ(map(LANCE_ERR_DATASET_ALREADY_EXISTS), std::nullopt);
    EXPECT_EQ(map(static_cast<LanceErrorCode>(9)), std::nullopt);
    EXPECT_EQ(map(static_cast<LanceErrorCode>(100)), std::nullopt);
    EXPECT_EQ(map(static_cast<LanceErrorCode>(-1)), std::nullopt);
}

} // namespace
} // namespace doris::lance
