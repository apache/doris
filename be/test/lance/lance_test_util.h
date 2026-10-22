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

#pragma once

// Shared helpers for the Lance index worker unit tests: golden-fixture access
// (the same JSON assets the FE golden test reads - amendment G11: a missing
// fixture is a hard FAIL, never a skip), pinned-open dataset RAII, writable
// fixture copies (index mutation commits a new dataset version), and the
// in-process pipe driver for the worker library. The thrift-compact frame
// codecs live in the production header
// (lance/index/lance_index_worker_frame.h) and are consumed from there.

#include <gen_cpp/LanceIndexWorker_types.h>
#include <lance/lance.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "lance/index/lance_index_contract.h"
#include "lance/index/lance_index_worker.h"
#include "lance/index/lance_index_worker_frame.h"

namespace doris::lance {

// ── Asset resolution (G11: missing fixture = hard FAIL, never skip) ──

// Locates <repo>/fe/fe-core/src/test/resources/lance, the single source of the
// cross-language golden assets. run-be-ut.sh runs the test binary from the repo
// root; for other working directories the helper walks upwards looking for the
// marker subtree. LANCE_TEST_DATA_DIR overrides everything.
inline std::string lance_test_assets_dir() {
    if (const char* env = std::getenv("LANCE_TEST_DATA_DIR");
        env != nullptr && env[0] != '\0') {
        if (std::filesystem::is_directory(std::filesystem::path(env) / "datasets")) {
            return env;
        }
        throw std::runtime_error(std::string(
                "LANCE_TEST_DATA_DIR does not contain a datasets/ subtree: ") + env);
    }
    namespace fs = std::filesystem;
    fs::path dir = fs::current_path();
    for (int depth = 0; depth < 8; ++depth) {
        fs::path candidate = dir / "fe/fe-core/src/test/resources/lance";
        if (fs::is_directory(candidate / "datasets")) {
            return candidate.string();
        }
        if (!dir.has_parent_path() || dir == dir.parent_path()) {
            break;
        }
        dir = dir.parent_path();
    }
    throw std::runtime_error(
            "cannot locate fe/fe-core/src/test/resources/lance from the current working "
            "directory (amendment G11: missing fixtures fail, never skip)");
}

inline std::string lance_test_dataset_path(const std::string& dataset_dir) {
    std::string path = lance_test_assets_dir() + "/datasets/" + dataset_dir;
    if (!std::filesystem::is_directory(path)) {
        throw std::runtime_error("missing lance test dataset: " + path);
    }
    return path;
}

inline std::string lance_test_golden_text(const std::string& fixture_name) {
    std::string path = lance_test_assets_dir() + "/schema_contract_golden/" + fixture_name + ".json";
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("missing golden fixture: " + path);
    }
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

// ── Golden fixture model ──

struct GoldenFixture {
    std::string name;
    std::string dataset_dir;
    int64_t version = 0;
    std::string column;
    std::string verdict; // "compare" | "unsupported"
    SchemaContract expected;
};

// Loads one golden JSON and parses its expected_contract through the production
// contract parser (the same parser the worker applies to the FE wire payload).
// Throws on any missing or malformed asset.
inline GoldenFixture load_golden_fixture(const std::string& fixture_name) {
    GoldenFixture fixture;
    fixture.name = fixture_name;
    std::string text = lance_test_golden_text(fixture_name);
    rapidjson::Document doc;
    doc.Parse(text.data(), text.size());
    if (doc.HasParseError() || !doc.IsObject()) {
        throw std::runtime_error("golden fixture is not a JSON object: " + fixture_name);
    }
    auto require_string = [&doc, &fixture_name](const char* key) -> std::string {
        auto it = doc.FindMember(key);
        if (it == doc.MemberEnd() || !it->value.IsString()) {
            throw std::runtime_error(std::string("golden fixture lacks string slot '") + key +
                                     "': " + fixture_name);
        }
        return std::string(it->value.GetString(), it->value.GetStringLength());
    };
    fixture.dataset_dir = require_string("dataset_dir");
    fixture.column = require_string("column");
    fixture.verdict = require_string("verdict");
    auto version = doc.FindMember("version");
    if (version == doc.MemberEnd() || !version->value.IsInt64() || version->value.GetInt64() <= 0) {
        throw std::runtime_error("golden fixture lacks a positive integer slot 'version': " +
                                 fixture_name);
    }
    fixture.version = version->value.GetInt64();
    auto contract = doc.FindMember("expected_contract");
    if (contract == doc.MemberEnd() || !contract->value.IsObject()) {
        throw std::runtime_error("golden fixture lacks object slot 'expected_contract': " +
                                 fixture_name);
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    contract->value.Accept(writer);
    ContractStatus status =
            parse_schema_contract(std::string(buffer.GetString(), buffer.GetSize()),
                                  &fixture.expected);
    if (status != ContractStatus::OK) {
        throw std::runtime_error("golden fixture's expected_contract does not parse: " +
                                 fixture_name);
    }
    return fixture;
}

// ── Lance dataset RAII ──

struct LanceDatasetCloser {
    void operator()(LanceDataset* dataset) const {
        if (dataset != nullptr) {
            lance_dataset_close(dataset);
        }
    }
};
using LanceDatasetPtr = std::unique_ptr<LanceDataset, LanceDatasetCloser>;

// Pinned open of a local test dataset by plain path; nullptr on provider failure.
inline LanceDatasetPtr open_pinned_dataset(const std::string& dataset_dir, uint64_t version) {
    std::string path = lance_test_dataset_path(dataset_dir);
    return LanceDatasetPtr(lance_dataset_open(path.c_str(), nullptr, version));
}

// Opens a dataset by absolute path (open_pinned_dataset resolves fixture-dir
// names under the shared assets root; writable copies live elsewhere).
// Version 0 is lance's latest sentinel.
inline LanceDatasetPtr open_dataset_path(const std::string& path, uint64_t version) {
    return LanceDatasetPtr(lance_dataset_open(path.c_str(), nullptr, version));
}

// ── Writable fixture copies ──

// A per-test temporary directory, removed on destruction.
struct LanceTempDir {
    std::filesystem::path path;

    LanceTempDir() {
        std::filesystem::path tmpl =
                std::filesystem::temp_directory_path() / "lance-ut-XXXXXX";
        std::string tmpl_text = tmpl.string();
        std::vector<char> raw(tmpl_text.begin(), tmpl_text.end());
        raw.push_back('\0');
        char* made = ::mkdtemp(raw.data());
        if (made == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path = made;
    }
    ~LanceTempDir() {
        if (!path.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    }
    LanceTempDir(const LanceTempDir&) = delete;
    LanceTempDir& operator=(const LanceTempDir&) = delete;
};

// A throwaway writable copy of one fixture dataset: index mutation commits a
// new dataset version, and the shared golden fixtures stay pristine.
inline std::string copy_fixture_dataset(const std::string& dataset_dir,
                                        const std::filesystem::path& into) {
    std::filesystem::path source = lance_test_dataset_path(dataset_dir);
    std::filesystem::path target = into / dataset_dir;
    std::error_code ec;
    std::filesystem::copy(source, target, std::filesystem::copy_options::recursive, ec);
    if (ec) {
        throw std::runtime_error("copy_fixture_dataset failed: " + ec.message());
    }
    return target.string();
}

// ── In-process pipe driver for run_index_worker ──

// One complete length-prefixed dispatch frame for the internal pipe protocol.
inline std::vector<uint8_t> lance_worker_dispatch_frame(const TLanceIndexWorkerDispatch& value) {
    std::vector<uint8_t> payload;
    if (!lance_worker_encode_compact(value, &payload)) {
        throw std::runtime_error("dispatch frame encode failed");
    }
    std::vector<uint8_t> frame;
    lance_worker_frame_wrap(payload, &frame);
    return frame;
}

struct WorkerRunResult {
    int exit_code = -1;
    std::vector<uint8_t> result_stream; // everything the worker wrote to result_fd
    std::vector<uint8_t> diag_stream;   // everything the worker wrote to diag_fd

    // Splits result_stream into length-prefixed frames. Trailing partial bytes
    // (a torn frame) leave trailing_garbage true.
    std::vector<std::vector<uint8_t>> frames(bool* trailing_garbage = nullptr) const {
        std::vector<std::vector<uint8_t>> out;
        size_t pos = 0;
        bool garbage = false;
        while (pos < result_stream.size()) {
            if (result_stream.size() - pos < 4) {
                garbage = true;
                break;
            }
            uint32_t length = lance_worker_frame_be32(result_stream.data() + pos);
            pos += 4;
            if (result_stream.size() - pos < length) {
                garbage = true;
                break;
            }
            out.emplace_back(result_stream.begin() + static_cast<long>(pos),
                             result_stream.begin() + static_cast<long>(pos + length));
            pos += length;
        }
        if (trailing_garbage != nullptr) {
            *trailing_garbage = garbage;
        }
        return out;
    }

    std::string diag_text() const { return std::string(diag_stream.begin(), diag_stream.end()); }
};

inline std::vector<uint8_t> lance_read_all_bytes(int fd) {
    std::vector<uint8_t> out;
    uint8_t chunk[4096];
    for (;;) {
        ssize_t count = ::read(fd, chunk, sizeof(chunk));
        if (count == 0) {
            break;
        }
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        out.insert(out.end(), chunk, chunk + count);
    }
    return out;
}

// Drives one run_index_worker invocation over pipe trios: a writer thread
// feeds the dispatch bytes (a >64KiB dispatch would otherwise deadlock
// against the pipe buffer), then the result and diag streams are drained to
// EOF.
inline WorkerRunResult lance_run_worker(const std::vector<uint8_t>& dispatch_bytes,
                                        uint32_t max_dispatch_bytes = 512 * 1024,
                                        uint32_t max_result_bytes = 8 * 1024) {
    int dispatch_pipe[2];
    int result_pipe[2];
    int diag_pipe[2];
    if (::pipe(dispatch_pipe) != 0 || ::pipe(result_pipe) != 0 || ::pipe(diag_pipe) != 0) {
        throw std::runtime_error("pipe() failed");
    }
    std::thread writer([&]() {
        const uint8_t* next = dispatch_bytes.data();
        size_t left = dispatch_bytes.size();
        while (left > 0) {
            ssize_t count = ::write(dispatch_pipe[1], next, left);
            if (count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                break; // EPIPE: the worker exited early; nothing more to feed
            }
            next += count;
            left -= static_cast<size_t>(count);
        }
        ::close(dispatch_pipe[1]);
    });
    IndexWorkerParams params;
    params.dispatch_fd = dispatch_pipe[0];
    params.result_fd = result_pipe[1];
    params.diag_fd = diag_pipe[1];
    params.max_dispatch_bytes = max_dispatch_bytes;
    params.max_result_bytes = max_result_bytes;
    int exit_code = run_index_worker(params);
    writer.join();
    ::close(dispatch_pipe[0]);
    ::close(result_pipe[1]);
    ::close(diag_pipe[1]);
    WorkerRunResult result;
    result.exit_code = exit_code;
    result.result_stream = lance_read_all_bytes(result_pipe[0]);
    result.diag_stream = lance_read_all_bytes(diag_pipe[0]);
    ::close(result_pipe[0]);
    ::close(diag_pipe[0]);
    return result;
}

// Decoded views of the worker's output frames.
struct WorkerFrames {
    std::optional<TLanceIndexWorkerHandshake> handshake;
    std::optional<TLanceIndexWorkerResult> result;
    size_t frame_count = 0;
    bool trailing_garbage = false;
};

// Interprets the result stream per the pipe protocol: the first frame is the
// handshake, the (at most one) following frame is the result.
inline WorkerFrames lance_decode_worker_frames(const WorkerRunResult& run) {
    WorkerFrames decoded;
    std::vector<std::vector<uint8_t>> raw = run.frames(&decoded.trailing_garbage);
    decoded.frame_count = raw.size();
    if (!raw.empty()) {
        TLanceIndexWorkerHandshake handshake;
        if (lance_worker_decode_compact(raw[0], &handshake)) {
            decoded.handshake = std::move(handshake);
        }
    }
    if (raw.size() > 1) {
        TLanceIndexWorkerResult result;
        if (lance_worker_decode_compact(raw[1], &result)) {
            decoded.result = std::move(result);
        }
    }
    return decoded;
}

} // namespace doris::lance
