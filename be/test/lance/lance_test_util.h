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

#pragma once

// Shared helpers for the Lance index worker unit tests: golden-fixture access
// (the same JSON assets the FE golden test reads — amendment G11: a missing
// fixture is a hard FAIL, never a skip), thrift-compact frame codecs, and an
// in-process pipe driver for run_index_worker.
//
// These tests never fork: run_index_worker is a pure library function, so the
// SIGCHLD-to-SIG_DFL fixture of python_env_test.cpp:53-67 is not needed here.
// The driver only spawns a writer thread for the dispatch pipe.

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <lance/lance.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include "lance/index_worker.h"
#include "lance/index_worker_contract.h"

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

// ── Thrift-compact codecs ──

template <typename T>
inline std::vector<uint8_t> thrift_compact_bytes(const T& value) {
    auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    apache::thrift::protocol::TCompactProtocol protocol(transport);
    value.write(&protocol);
    uint8_t* buffer = nullptr;
    uint32_t size = 0;
    transport->getBuffer(&buffer, &size);
    return std::vector<uint8_t>(buffer, buffer + size);
}

template <typename T>
inline bool thrift_compact_decode(const std::vector<uint8_t>& bytes, T* out) {
    try {
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
                const_cast<uint8_t*>(bytes.data()), static_cast<uint32_t>(bytes.size()));
        apache::thrift::protocol::TCompactProtocol protocol(transport);
        out->read(&protocol);
    } catch (...) {
        return false;
    }
    return true;
}

// One length-prefixed frame (uint32 big-endian length + payload), the worker's
// dispatch wire format.
template <typename T>
inline std::vector<uint8_t> dispatch_frame_bytes(const T& value) {
    std::vector<uint8_t> payload = thrift_compact_bytes(value);
    uint32_t length = static_cast<uint32_t>(payload.size());
    std::vector<uint8_t> frame = {static_cast<uint8_t>(length >> 24),
                                  static_cast<uint8_t>(length >> 16),
                                  static_cast<uint8_t>(length >> 8),
                                  static_cast<uint8_t>(length)};
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

// ── In-process pipe driver for run_index_worker ──

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
            uint32_t length = (static_cast<uint32_t>(result_stream[pos]) << 24) |
                              (static_cast<uint32_t>(result_stream[pos + 1]) << 16) |
                              (static_cast<uint32_t>(result_stream[pos + 2]) << 8) |
                              static_cast<uint32_t>(result_stream[pos + 3]);
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

inline std::vector<uint8_t> read_all_bytes(int fd) {
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

// Drives one run_index_worker invocation over pipe trios: a writer thread feeds
// the dispatch bytes (a >64KiB dispatch would otherwise deadlock against the
// pipe buffer), then the result and diag streams are drained to EOF.
inline WorkerRunResult run_worker(const std::vector<uint8_t>& dispatch_bytes,
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
    result.result_stream = read_all_bytes(result_pipe[0]);
    result.diag_stream = read_all_bytes(diag_pipe[0]);
    ::close(result_pipe[0]);
    ::close(diag_pipe[0]);
    return result;
}

// Decoded views of the two worker output frames.
struct WorkerFrames {
    std::optional<TLanceIndexWorkerHandshake> handshake;
    std::optional<TLanceIndexJobReport> report;
    size_t frame_count = 0;
    bool trailing_garbage = false;
};

// Interprets the result stream per the pipe protocol: the first frame is the
// handshake, the (at most one) following frame is the result report.
inline WorkerFrames decode_worker_frames(const WorkerRunResult& run) {
    WorkerFrames decoded;
    std::vector<std::vector<uint8_t>> raw = run.frames(&decoded.trailing_garbage);
    decoded.frame_count = raw.size();
    if (!raw.empty()) {
        TLanceIndexWorkerHandshake handshake;
        if (thrift_compact_decode(raw[0], &handshake)) {
            decoded.handshake = std::move(handshake);
        }
    }
    if (raw.size() > 1) {
        TLanceIndexJobReport report;
        if (thrift_compact_decode(raw[1], &report)) {
            decoded.report = std::move(report);
        }
    }
    return decoded;
}

} // namespace doris::lance
