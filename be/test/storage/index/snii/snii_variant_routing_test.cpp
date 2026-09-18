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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "exec/common/variant_util.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/snii_bkd_index_writer.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {
constexpr int64_t kParentIndexId = 90;
constexpr const char* kTestDir = "./ut_dir/snii_variant_routing_test";
constexpr const char* kTmpRoot = "./ut_dir/snii_variant_routing_test_tmp";
} // namespace

// An extracted sub-column of a VARIANT parent must reach the SNII writer factory carrying its
// own type and its own index suffix. The suffix is what keeps two sub-columns of one parent
// apart inside the SNII container, whose logical index key is (index_id, index_suffix).
class SniiVariantRoutingTest : public testing::Test {
protected:
    // A couple of these SNII writer tests drive the real IndexColumnWriter::create factory
    // end-to-end, which needs ExecEnv's tmp-file directories configured the same way the other
    // SNII writer test suites (e.g. SniiNoBigramWriter, InvertedIndexWriterTest) set them up.
    static void SetUpTestSuite() {
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(kTmpRoot).ok());
        ASSERT_TRUE(fs->create_directory(kTmpRoot).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(kTmpRoot, 1024 * 1024);
        auto tmp_file_dirs = std::make_unique<TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        // ExecEnv retains this owner process-wide, so its root outlives the suite.
    }

    void SetUp() override {
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(kTestDir).ok());
        ASSERT_TRUE(fs->create_directory(kTestDir).ok());
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    static TabletColumn make_extracted_column(int32_t parent_unique_id, const std::string& path,
                                              FieldType type) {
        TabletColumn column;
        column.set_unique_id(-1);
        column.set_name(path);
        column.set_type(type);
        column.set_parent_unique_id(parent_unique_id);
        column.set_path_info(PathInData(path));
        column.set_is_nullable(true);
        return column;
    }

    // The parent VARIANT column's own inverted index. "parser": "none" keeps the writer on the
    // untokenized keyword path, so this test does not also need an Analyzer set up.
    static TabletIndex make_parent_index() {
        TabletIndexPB pb;
        pb.set_index_type(IndexType::INVERTED);
        pb.set_index_id(kParentIndexId);
        pb.set_index_name("idx_variant_parent");
        pb.add_col_unique_id(7);
        pb.mutable_properties()->insert({"parser", "none"});
        TabletIndex index;
        index.init_from_pb(pb);
        return index;
    }
};

TEST_F(SniiVariantRoutingTest, ExtractedColumnKeepsParentAndPath) {
    const auto text_column = make_extracted_column(7, "v.title", FieldType::OLAP_FIELD_TYPE_STRING);
    const auto numeric_column = make_extracted_column(7, "v.score", FieldType::OLAP_FIELD_TYPE_INT);

    EXPECT_TRUE(text_column.is_extracted_column());
    EXPECT_TRUE(numeric_column.is_extracted_column());
    EXPECT_EQ(text_column.parent_unique_id(), 7);
    EXPECT_EQ(numeric_column.parent_unique_id(), 7);
    EXPECT_NE(text_column.suffix_path(), numeric_column.suffix_path());
}

TEST_F(SniiVariantRoutingTest, SubColumnTypesAreRoutable) {
    // The SNII branch of IndexColumnWriter::create routes on these two predicates and refuses
    // everything else, so a sub-column type that satisfies neither would produce an index no
    // query path can serve.
    EXPECT_TRUE(is_string_type(FieldType::OLAP_FIELD_TYPE_STRING));
    EXPECT_TRUE(field_is_numeric_type(FieldType::OLAP_FIELD_TYPE_INT));
    EXPECT_FALSE(is_string_type(FieldType::OLAP_FIELD_TYPE_JSONB));
    EXPECT_FALSE(field_is_numeric_type(FieldType::OLAP_FIELD_TYPE_JSONB));
}

// Design promise (docs/design/2026-08-10-variant-support-design.md §3.5): a string sub-column
// routes through IndexColumnWriter::create to the SNII SPIMI writer, a numeric sub-column to the
// native BKD writer, and -- because variant_util::inherit_index copies the SAME parent
// TabletIndex (same index_id) for every sub-column, only varying its index_suffix -- the two
// land in ONE container under distinct (index_id, suffix) keys instead of colliding.
//
// This drives the real factory (IndexColumnWriter::create), a real IndexFileWriter/IndexFileReader
// round trip through one shared SNII container, and asserts the concrete writer type each
// sub-column actually gets -- unlike the two tests above, whose predicates were true by
// construction of their own fixture arguments and could never fail.
TEST_F(SniiVariantRoutingTest, StringAndNumericSubColumnsRouteAndCoexistInOneContainer) {
    const TabletIndex parent_index = make_parent_index();
    const auto text_column = make_extracted_column(7, "v.title", FieldType::OLAP_FIELD_TYPE_STRING);
    const auto numeric_column = make_extracted_column(7, "v.score", FieldType::OLAP_FIELD_TYPE_INT);

    std::vector<const TabletIndex*> parent_indexes {&parent_index};
    TabletIndexes text_indexes;
    TabletIndexes numeric_indexes;
    ASSERT_TRUE(variant_util::inherit_index(parent_indexes, text_indexes, text_column));
    ASSERT_TRUE(variant_util::inherit_index(parent_indexes, numeric_indexes, numeric_column));
    ASSERT_EQ(1u, text_indexes.size());
    ASSERT_EQ(1u, numeric_indexes.size());
    const TabletIndex* text_index = text_indexes[0].get();
    const TabletIndex* numeric_index = numeric_indexes[0].get();

    // Same logical index inherited from the same parent, but distinct container keys -- the
    // real mechanism the two tests above only paraphrase with hand-picked suffix_path values.
    EXPECT_EQ(text_index->index_id(), numeric_index->index_id());
    EXPECT_NE(text_index->get_index_suffix(), numeric_index->get_index_suffix());

    const std::string prefix = std::string(kTestDir) + "/routing";
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                      &file_writer)
                        .ok());
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "routing_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer));

    std::unique_ptr<IndexColumnWriter> text_writer;
    ASSERT_TRUE(
            IndexColumnWriter::create(&text_column, &text_writer, &index_file_writer, text_index)
                    .ok());
    // The factory routing under test: a string sub-column must get the SPIMI writer, never BKD.
    EXPECT_NE(nullptr, dynamic_cast<SniiIndexColumnWriter*>(text_writer.get()));
    EXPECT_EQ(nullptr, dynamic_cast<SniiBkdIndexColumnWriter*>(text_writer.get()));
    std::vector<Slice> text_values {Slice("alpha"), Slice("beta")};
    ASSERT_TRUE(text_writer->add_values("v.title", text_values.data(), text_values.size()).ok());
    ASSERT_TRUE(text_writer->finish().ok());

    std::unique_ptr<IndexColumnWriter> numeric_writer;
    ASSERT_TRUE(IndexColumnWriter::create(&numeric_column, &numeric_writer, &index_file_writer,
                                          numeric_index)
                        .ok());
    // ... and a numeric sub-column must get the native BKD writer, never SPIMI.
    EXPECT_NE(nullptr, dynamic_cast<SniiBkdIndexColumnWriter*>(numeric_writer.get()));
    EXPECT_EQ(nullptr, dynamic_cast<SniiIndexColumnWriter*>(numeric_writer.get()));
    std::vector<int32_t> numeric_values {7, 42};
    ASSERT_TRUE(numeric_writer->add_values("v.score", numeric_values.data(), numeric_values.size())
                        .ok());
    ASSERT_TRUE(numeric_writer->finish().ok());

    ASSERT_TRUE(index_file_writer.begin_close().ok());
    ASSERT_TRUE(index_file_writer.finish_close().ok());

    IndexFileReader index_file_reader(io::global_local_filesystem(), prefix,
                                      InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(index_file_reader.init().ok());

    // The container keeps both entries apart: each is reachable only through its own
    // (index_id, suffix) key.
    auto text_reader = index_file_reader.open_snii_index(text_index);
    ASSERT_TRUE(text_reader.has_value()) << text_reader.error();
    bool found = false;
    doris::snii::format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    ASSERT_TRUE((*text_reader)->lookup("alpha", &found, &entry, &frq_base, &prx_base).ok());
    EXPECT_TRUE(found);

    auto numeric_reader = index_file_reader.open_snii_bkd_index(numeric_index, nullptr);
    ASSERT_TRUE(numeric_reader.has_value()) << numeric_reader.error();

    // Cross-lookups under the WRONG kind must not accidentally succeed -- that would be exactly
    // the collision the suffix keying exists to prevent.
    auto wrong_kind_blob = index_file_reader.open_snii_bkd_index(text_index, nullptr);
    EXPECT_FALSE(wrong_kind_blob.has_value());
    auto wrong_kind_text = index_file_reader.open_snii_index(numeric_index);
    EXPECT_FALSE(wrong_kind_text.has_value());
}

} // namespace doris::segment_v2
