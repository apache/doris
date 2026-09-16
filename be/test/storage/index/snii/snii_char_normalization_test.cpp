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

#include <array>
#include <memory>
#include <string>
#include <vector>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/index/snii_query_test_util.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

using namespace doris::snii::snii_test;

constexpr char kTestDir[] = "./ut_dir/snii_char_normalization_test";
constexpr char kPrefix[] = "./ut_dir/snii_char_normalization_test/segment";
constexpr int64_t kIndexId = 61;
constexpr uint32_t kDocCount = 6;

struct QueryEnv {
    QueryEnv() {
        TQueryOptions options;
        options.query_type = TQueryType::SELECT;
        options.enable_inverted_index_query_cache = true;
        options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(options);
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
    }
    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    RuntimeState runtime_state;
    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
};

class SniiCharNormalizationTest : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
        TabletIndexPB pb;
        pb.set_index_type(IndexType::INVERTED);
        pb.set_index_id(kIndexId);
        pb.set_index_name("char_idx");
        pb.add_col_unique_id(0);
        pb.mutable_properties()->insert({"parser", "none"});
        _meta.init_from_pb(pb);
        _previous_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous_query_cache);
        _query_cache.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    Status write_char_index(bool is_array) {
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file;
        RETURN_IF_ERROR(
                fs->create_file(InvertedIndexDescriptor::get_index_file_path_v2(kPrefix), &file));
        IndexFileWriter index_writer(fs, kPrefix, "char_rowset", 0,
                                     InvertedIndexStorageFormatPB::SNII, std::move(file), true,
                                     900);
        SniiIndexColumnWriter writer(&index_writer, &_meta, FieldType::OLAP_FIELD_TYPE_CHAR);
        RETURN_IF_ERROR(writer.init());
        const std::array<std::string, 5> values = {
                std::string("a\0b\0", 4), std::string("a\0\0\0", 4), std::string("\0x\0\0", 4),
                std::string(4, '\0'), std::string("a\0c\0", 4)};
        std::vector<Slice> slices;
        for (const auto& value : values) {
            slices.emplace_back(value);
        }
        if (is_array) {
            // Five singleton arrays followed by a NULL array, as in ArrayColumnWriter.
            const std::array<uint64_t, 7> offsets = {0, 1, 2, 3, 4, 5, 5};
            const std::array<uint8_t, kDocCount> nulls = {0, 0, 0, 0, 0, 1};
            RETURN_IF_ERROR(writer.add_array_values(
                    sizeof(Slice), slices.data(), nullptr,
                    reinterpret_cast<const uint8_t*>(offsets.data()), kDocCount));
            RETURN_IF_ERROR(writer.add_array_nulls(nulls.data(), nulls.size()));
        } else {
            RETURN_IF_ERROR(writer.add_values("c", slices.data(), slices.size()));
            RETURN_IF_ERROR(writer.add_nulls(1));
        }
        RETURN_IF_ERROR(writer.finish());
        RETURN_IF_ERROR(index_writer.begin_close());
        return index_writer.finish_close();
    }

    Status write_legacy_index() {
        // The old CHAR writer indexed a\0b as "a". Build its on-disk representation
        // directly; the production writer must never have a legacy-normalization switch.
        doris::snii::writer::SniiIndexInput input;
        input.index_id = kIndexId;
        input.config = doris::snii::format::IndexConfig::kDocsPositions;
        input.doc_count = kDocCount;
        input.terms = {make_term("a", {{.docid = 0, .positions = {0}}})};
        MemoryFile memory;
        doris::snii::writer::SniiCompoundWriter compound(&memory);
        RETURN_IF_ERROR(compound.add_logical_index(input));
        RETURN_IF_ERROR(compound.finish());
        doris::snii::io::LocalFileWriter file;
        RETURN_IF_ERROR(file.open(InvertedIndexDescriptor::get_index_file_path_v2(kPrefix)));
        RETURN_IF_ERROR(file.append(doris::snii::Slice(memory.data())));
        return file.finalize();
    }

    std::shared_ptr<IndexFileReader> open_file() {
        auto file = std::make_shared<IndexFileReader>(io::global_local_filesystem(), kPrefix,
                                                      InvertedIndexStorageFormatPB::SNII);
        assert_ok(file->init());
        return file;
    }

    TabletIndex _meta;
    InvertedIndexQueryCache* _previous_query_cache = nullptr;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
};

class SniiCharWriterTest : public SniiCharNormalizationTest,
                           public testing::WithParamInterface<bool> {};

TEST_P(SniiCharWriterTest, PreservesEmbeddedNulsAndOnlyStripsTailPadding) {
    ASSERT_TRUE(write_char_index(GetParam()).ok());
    auto file = open_file();
    const auto logical = file->open_snii_index(&_meta);
    ASSERT_TRUE(logical.has_value()) << logical.error();
    const std::array<std::string, 5> queries = {std::string("a\0b", 3), "a", std::string("\0x", 2),
                                                "", std::string("a\0c", 3)};
    for (uint32_t id = 0; id < queries.size(); ++id) {
        SCOPED_TRACE(id);
        std::vector<uint32_t> docids;
        assert_ok(doris::snii::query::term_query(*logical.value(), queries[id], &docids));
        EXPECT_EQ(docids, (std::vector<uint32_t> {id}));
    }

    // One embedded-NUL query covers cold/warm cache and NULL bitmap handling for each shape.
    DataTypePtr type = std::make_shared<DataTypeString>(4, TYPE_CHAR);
    if (GetParam()) {
        type = std::make_shared<DataTypeArray>(make_nullable(type));
    }
    auto reader = SniiIndexReader::create_shared(&_meta, file, InvertedIndexReaderType::STRING_TYPE,
                                                 kDocCount, type);
    for (int attempt = 0; attempt < 2; ++attempt) {
        QueryEnv env;
        std::shared_ptr<roaring::Roaring> bitmap;
        InvertedIndexQueryCacheHandle null_handle;
        assert_ok(reader->query_with_null_bitmap(
                env.context, "c", Field::create_field<TYPE_STRING>(queries.front()),
                InvertedIndexQueryType::EQUAL_QUERY, bitmap, &null_handle));
        ASSERT_NE(bitmap, nullptr);
        EXPECT_EQ((std::vector<uint32_t>(bitmap->begin(), bitmap->end())),
                  (std::vector<uint32_t> {0}));
        ASSERT_NE(null_handle.get_bitmap(), nullptr);
        EXPECT_EQ((std::vector<uint32_t>(null_handle.get_bitmap()->begin(),
                                         null_handle.get_bitmap()->end())),
                  (std::vector<uint32_t> {5}));
        EXPECT_EQ(env.stats.inverted_index_query_cache_hit, attempt);
    }
}

INSTANTIATE_TEST_SUITE_P(CharShape, SniiCharWriterTest, testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) {
                             return info.param ? "Array" : "Scalar";
                         });

TEST_F(SniiCharNormalizationTest, LegacyCharSkipsBeforeQueryCacheAndCountFastPath) {
    ASSERT_TRUE(write_legacy_index().ok());
    auto file = open_file();
    auto string_reader =
            SniiIndexReader::create_shared(&_meta, file, InvertedIndexReaderType::STRING_TYPE,
                                           kDocCount, std::make_shared<DataTypeString>());
    const DataTypePtr char_type = std::make_shared<DataTypeString>(4, TYPE_CHAR);
    const std::array<DataTypePtr, 3> char_shapes = {
            char_type, make_nullable(char_type),
            std::make_shared<DataTypeArray>(make_nullable(char_type))};
    const std::array<std::string, 2> queries = {"a", std::string("a\0b", 3)};
    for (const auto& query : queries) {
        const Field value = Field::create_field<TYPE_STRING>(query);
        // Non-CHAR indexes without the new marker stay usable. Warm both a hit
        // and a miss to prove that neither cached bitmap can bypass the CHAR guard.
        QueryEnv seed;
        std::shared_ptr<roaring::Roaring> bitmap;
        ASSERT_TRUE(string_reader
                            ->query(seed.context, "c", value, InvertedIndexQueryType::EQUAL_QUERY,
                                    bitmap)
                            .ok());
        ASSERT_NE(bitmap, nullptr);
        EXPECT_EQ(bitmap->cardinality(), query == "a" ? 1 : 0);
        EXPECT_EQ(seed.stats.inverted_index_query_cache_insert, 1);
        for (const auto& type : char_shapes) {
            auto reader = SniiIndexReader::create_shared(
                    &_meta, file, InvertedIndexReaderType::STRING_TYPE, kDocCount, type);
            QueryEnv env;
            env.context->count_on_index_fastpath = true;
            std::shared_ptr<roaring::Roaring> char_bitmap;
            const auto status = reader->query(env.context, "c", value,
                                              InvertedIndexQueryType::EQUAL_QUERY, char_bitmap);
            EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED) << status;
            EXPECT_EQ(char_bitmap, nullptr);
            EXPECT_EQ(env.stats.inverted_index_query_cache_lookup, 0);
            EXPECT_FALSE(env.context->count_on_index_fastpath_hit);
        }
    }
}

TEST_F(SniiCharNormalizationTest, LegacyCharScoringRequiresRebuild) {
    ASSERT_TRUE(write_legacy_index().ok());
    TabletIndexPB pb;
    _meta.to_schema_pb(&pb);
    (*pb.mutable_properties())["parser"] = "english";
    (*pb.mutable_properties())["support_phrase"] = "true";
    _meta.init_from_pb(pb);
    auto reader = SniiIndexReader::create_shared(&_meta, open_file(),
                                                 InvertedIndexReaderType::FULLTEXT, kDocCount,
                                                 std::make_shared<DataTypeString>(4, TYPE_CHAR));
    QueryEnv env;
    env.context->collection_similarity = std::make_shared<CollectionSimilarity>();
    std::shared_ptr<roaring::Roaring> bitmap;
    const auto status = reader->query(env.context, "c", Field::create_field<TYPE_STRING>("a"),
                                      InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap);
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED) << status;
    EXPECT_NE(status.to_string().find("rebuild"), std::string::npos);
    EXPECT_EQ(bitmap, nullptr);
}

} // namespace
} // namespace doris::segment_v2
