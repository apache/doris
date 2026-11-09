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

#include <lance/lance.h>
#include <rapidjson/document.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include <unistd.h>

#include "lance/lance_test_util.h"

// LanceIndexPhysicalListProbeTest: empirical evidence lock for the lance-c
// index-listing API the worker's physical-snapshot semantics consume
// (lance_dataset_index_list_json / lance_dataset_drop_index /
// lance_dataset_create_vector_index / lance_free_string), against real local
// fixture datasets. The worker library's DROP unique-resolution,
// REPLACE-missing-name-new-create, and IF pre-invocation no-op blocks are all
// specified on top of the behaviors pinned here; a lance-c upgrade that
// changes any of them fails this probe before the worker semantics drift.
//
// The committed fixtures are shared read-only golden assets (the FE golden
// test reads the same directories), and index creation commits a new dataset
// version: every mutating case therefore works on a throwaway copy of a
// fixture dataset under a per-test temporary directory.

namespace doris::lance {
namespace {

// s14.lance v1: column "v" is a non-nullable fixed_size_list<float32> of
// dimension 4 (golden fixture fsl_f32_s14_v) - the canonical vector column.
constexpr const char* VECTOR_FIXTURE = "s14.lance";
constexpr const char* VECTOR_COLUMN = "v";

struct TempDir {
    std::filesystem::path path;

    TempDir() {
        std::filesystem::path tmpl =
                std::filesystem::temp_directory_path() / "lance-probe-XXXXXX";
        std::string tmpl_text = tmpl.string();
        std::vector<char> raw(tmpl_text.begin(), tmpl_text.end());
        raw.push_back('\0');
        char* made = ::mkdtemp(raw.data());
        if (made == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path = made;
    }
    ~TempDir() {
        if (!path.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    }
};

// A throwaway writable copy of one fixture dataset (index mutation commits a
// new version; the golden fixtures stay pristine).
std::string copy_fixture(const std::string& dataset_dir, const std::filesystem::path& into) {
    std::filesystem::path source = lance_test_dataset_path(dataset_dir);
    std::filesystem::path target = into / dataset_dir;
    std::error_code ec;
    std::filesystem::copy(source, target, std::filesystem::copy_options::recursive, ec);
    if (ec) {
        throw std::runtime_error("copy_fixture failed: " + ec.message());
    }
    return target.string();
}

// Opens a dataset by absolute path (lance_test_util's open_pinned_dataset
// resolves fixture-dir names under the shared assets root; the throwaway
// copies live elsewhere). Version 0 is lance's latest sentinel, used here
// deliberately: the tests control the fixture copy's whole history.
LanceDatasetPtr open_dataset_path(const std::string& path, uint64_t version) {
    return LanceDatasetPtr(lance_dataset_open(path.c_str(), nullptr, version));
}

LanceVectorIndexParams ivf_pq_params(uint32_t num_partitions, uint32_t num_sub_vectors) {
    LanceVectorIndexParams params = {};
    params.index_type = LANCE_INDEX_IVF_PQ;
    params.metric = LANCE_METRIC_L2;
    params.num_partitions = num_partitions;
    params.num_sub_vectors = num_sub_vectors;
    params.num_bits = 8;
    return params;
}

// Reads the index list JSON, freeing the provider string immediately.
std::string index_list_json(LanceDataset* dataset) {
    const char* raw = lance_dataset_index_list_json(dataset);
    if (raw == nullptr) {
        return std::string();
    }
    std::string json(raw);
    lance_free_string(raw);
    return json;
}

TEST(LanceIndexPhysicalListProbeTest, EmptyIndexSetIsAnEmptyJsonArray) {
    LanceDatasetPtr dataset = open_pinned_dataset(VECTOR_FIXTURE, 1);
    ASSERT_NE(dataset, nullptr);
    EXPECT_EQ(lance_dataset_version(dataset.get()), 1U);

    // Bounded read of the provider string with the mandatory free pairing.
    const std::string json = index_list_json(dataset.get());
    EXPECT_EQ(json, "[]") << "an empty index set is an empty JSON array, never null";
    rapidjson::Document doc;
    doc.Parse(json.data(), json.size());
    ASSERT_FALSE(doc.HasParseError());
    ASSERT_TRUE(doc.IsArray());
    EXPECT_EQ(doc.Size(), 0U) << "a pristine fixture carries no user index";
}

TEST(LanceIndexPhysicalListProbeTest, CreatedIndexEntryShapeAndVersionBump) {
    TempDir tmp;
    const std::string uri = copy_fixture(VECTOR_FIXTURE, tmp.path);
    {
        LanceDatasetPtr dataset = open_dataset_path(uri, 0 /* latest */);
        ASSERT_NE(dataset, nullptr);
        const uint64_t before = lance_dataset_latest_version(dataset.get());

        LanceVectorIndexParams params = ivf_pq_params(2, 2);
        ASSERT_EQ(lance_dataset_create_vector_index(dataset.get(), VECTOR_COLUMN, "idx_v",
                                                    &params, /*replace=*/false),
                  0);
        EXPECT_GT(lance_dataset_latest_version(dataset.get()), before)
                << "index creation commits a new dataset version";

        // Entry shape (empirically locked, lance-c 0.1.9): every entry is an
        // object with exactly the slots below; "name" is the stored spelling
        // the worker's unique resolution drops by, "dataset_version" the
        // version the index was built against (not its commit version).
        const std::string json = index_list_json(dataset.get());
        rapidjson::Document doc;
        doc.Parse(json.data(), json.size());
        ASSERT_FALSE(doc.HasParseError());
        ASSERT_TRUE(doc.IsArray());
        ASSERT_EQ(doc.Size(), 1U);
        ASSERT_TRUE(doc[0].IsObject());
        EXPECT_TRUE(doc[0].HasMember("name"));
        EXPECT_TRUE(doc[0]["name"].IsString());
        EXPECT_EQ(std::string(doc[0]["name"].GetString(),
                              doc[0]["name"].GetStringLength()),
                  "idx_v");
        EXPECT_TRUE(doc[0].HasMember("uuid"));
        EXPECT_TRUE(doc[0]["uuid"].IsString());
        EXPECT_TRUE(doc[0].HasMember("columns"));
        EXPECT_TRUE(doc[0]["columns"].IsArray());
        EXPECT_TRUE(doc[0].HasMember("type"));
        EXPECT_TRUE(doc[0]["type"].IsString());
        EXPECT_TRUE(doc[0].HasMember("dataset_version"));
        EXPECT_TRUE(doc[0]["dataset_version"].IsInt64());
        EXPECT_EQ(lance_dataset_index_count(dataset.get()), 1U);
    }
    // A handle pinned at the pre-create version must not see the index: the
    // worker's physical-snapshot semantics operate on the admitted version.
    {
        LanceDatasetPtr pinned = open_dataset_path(uri, 1);
        ASSERT_NE(pinned, nullptr);
        const std::string json = index_list_json(pinned.get());
        EXPECT_EQ(json, "[]");
        rapidjson::Document doc;
        doc.Parse(json.data(), json.size());
        ASSERT_FALSE(doc.HasParseError());
        ASSERT_TRUE(doc.IsArray());
        EXPECT_EQ(doc.Size(), 0U) << "the pinned snapshot predates the index commit";
    }
}

TEST(LanceIndexPhysicalListProbeTest, MultipleEntriesAndCaseVariants) {
    TempDir tmp;
    const std::string uri = copy_fixture(VECTOR_FIXTURE, tmp.path);
    LanceDatasetPtr dataset = open_dataset_path(uri, 0 /* latest */);
    ASSERT_NE(dataset, nullptr);

    LanceVectorIndexParams params = ivf_pq_params(2, 2);
    ASSERT_EQ(lance_dataset_create_vector_index(dataset.get(), VECTOR_COLUMN, "idx_alpha",
                                                &params, false),
              0);
    // A differently-named second index: two physical entries, both visible.
    ASSERT_EQ(lance_dataset_create_vector_index(dataset.get(), VECTOR_COLUMN, "idx_beta",
                                                &params, false),
              0);
    {
        rapidjson::Document doc;
        const std::string json = index_list_json(dataset.get());
        doc.Parse(json.data(), json.size());
        ASSERT_FALSE(doc.HasParseError());
        ASSERT_EQ(doc.Size(), 2U);
    }

    // A case-only variant of an existing spelling IS accepted by lance-c: two
    // physical entries whose names differ only by case can coexist, which is
    // exactly the ambiguous-snapshot scenario the worker's unique resolution
    // (and the FE's case-only duplicate rejection) exist for. Constructible
    // through the public create path - no synthetic JSON needed.
    ASSERT_EQ(lance_dataset_create_vector_index(dataset.get(), VECTOR_COLUMN, "IDX_ALPHA",
                                                &params, false),
              0);
    EXPECT_EQ(lance_dataset_index_count(dataset.get()), 3U);

    // Drop resolution is exact-spelling: a wrong-case drop is NOT_FOUND, never
    // a case-insensitive hit - case-insensitive matching is the worker's job,
    // performed against the stored spellings of the pinned snapshot.
    EXPECT_EQ(lance_dataset_drop_index(dataset.get(), "IDX_BETA"), -1);
    EXPECT_EQ(lance_last_error_code(), LANCE_ERR_NOT_FOUND);
}

} // namespace
} // namespace doris::lance
