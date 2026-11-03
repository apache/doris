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
// fixture is a hard FAIL, never a skip) and pinned-open dataset RAII. The
// thrift-compact frame codecs live in the production header
// (lance/index/lance_index_worker_frame.h) and are consumed from there; the
// in-process pipe driver for the worker library joins this header with the
// worker library itself.

#include <lance/lance.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>

#include "lance/index/lance_index_contract.h"

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

} // namespace doris::lance
