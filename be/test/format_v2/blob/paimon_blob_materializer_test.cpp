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

#include "format_v2/blob/paimon_blob_materializer.h"

#include <gtest/gtest.h>

#include <limits>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "format_v2/blob/paimon_blob_test_util.h"

namespace doris::format::paimon {
namespace {

class PaimonBlobMaterializerTest : public blob_test::PaimonBlobTest {};

TEST_F(PaimonBlobMaterializerTest, ProfilesMaterializationAcrossSplits) {
    RuntimeProfile profile("PaimonBlobTest");
    PaimonBlobMaterializer materializer;
    ASSERT_TRUE(materializer.init({}, &profile).ok());
    const std::vector<ColumnDefinition> columns {blob_column("image", 7)};
    auto context = read_context();
    context.profile = &profile;
    auto* timer = profile.get_counter("PaimonBlobMaterializeTime");
    ASSERT_NE(timer, nullptr);
    EXPECT_EQ(timer->value(), 0);
    for (int split = 0; split < 2; ++split) {
        auto file = write_blob("split-" + std::to_string(split) + ".blob", 7, 0, {"hello"});
        ASSERT_TRUE(materializer.prepare_split(blob_split(0, 1, {file}), columns).ok());
        auto block = output_block(columns, 1);
        const auto before = timer->value();
        ASSERT_TRUE(materializer.materialize(*row_positions({0}), &block, context).ok());
        EXPECT_GT(timer->value(), before);
        expect_inline(*block.get_by_position(0).column, 0, "hello");
    }
    materializer.reset();
    EXPECT_GT(timer->value(), 0);
}

TEST_F(PaimonBlobMaterializerTest, MatchesFieldIdsAfterRenameAndProjectionReordering) {
    auto image_first = write_blob("image_first.blob", 7, 1000, {"image-0", std::nullopt});
    auto image_second = write_blob("image_second.blob", 7, 1002, {"", "image-3"});
    auto thumbnail =
            write_blob("thumbnail.blob", 9, 1000, {"thumb-0", "thumb-1", std::nullopt, "thumb-3"});
    auto split = blob_split(1000, 4, {thumbnail, image_second, image_first});
    ColumnDefinition id_column;
    id_column.name = "id";
    id_column.type = std::make_shared<DataTypeInt32>();
    std::vector<ColumnDefinition> columns {blob_column("renamed_thumbnail", 9), id_column,
                                           blob_column("renamed_image", 7),
                                           blob_column("added_image", 11)};
    PaimonBlobMaterializer materializer;
    ASSERT_TRUE(materializer.init({}).ok());
    ASSERT_TRUE(materializer.prepare_split(split, columns).ok());
    const std::set<std::string> expected_names {"renamed_thumbnail", "renamed_image",
                                                "added_image"};
    EXPECT_EQ(materializer.blob_column_names(), expected_names);

    // Filtering/byte splitting supplies original anchor positions, not dense output ordinals.
    auto positions = row_positions({3, 0, 2});
    auto block = output_block(columns, 3);
    auto ids = ColumnInt32::create();
    for (const int32_t id : {103, 100, 102}) {
        ids->insert_value(id);
    }
    block.replace_by_position(1, std::move(ids));
    auto context = read_context();
    ASSERT_TRUE(materializer.materialize(*positions, &block, context).ok());
    ASSERT_EQ(block.rows(), 3);
    const auto& thumbnails = *block.get_by_position(0).column;
    expect_inline(thumbnails, 0, "thumb-3");
    expect_inline(thumbnails, 1, "thumb-0");
    expect_inline(thumbnails, 2, std::nullopt);
    const auto& images = *block.get_by_position(2).column;
    expect_inline(images, 0, "image-3");
    expect_inline(images, 1, "image-0");
    expect_inline(images, 2, "");
    for (size_t row = 0; row < 3; ++row) {
        expect_inline(*block.get_by_position(3).column, row, std::nullopt);
    }
    EXPECT_EQ((*block.get_by_position(1).column)[0].get<TYPE_INT>(), 103);
    EXPECT_EQ((*block.get_by_position(1).column)[1].get<TYPE_INT>(), 100);
    EXPECT_EQ((*block.get_by_position(1).column)[2].get<TYPE_INT>(), 102);
}

TEST_F(PaimonBlobMaterializerTest, SplitChangesReleaseFilesButRetainDescriptorMode) {
    auto first = write_blob("first.blob", 7, 1000, {"first"});
    const std::vector<ColumnDefinition> columns {blob_column("image", 7)};
    PaimonBlobMaterializer materializer;
    ASSERT_TRUE(materializer.init({{"blob-as-descriptor", "true"}}).ok());
    ASSERT_TRUE(materializer.prepare_split(blob_split(1000, 1, {first}), columns).ok());
    auto block = output_block(columns, 1);
    auto context = read_context(1);
    ASSERT_TRUE(materializer.materialize(*row_positions({0}), &block, context).ok());
    expect_descriptor(*block.get_by_position(0).column, 0, first.path, 4, 5);

    auto second = write_blob("second.blob", 7, 2000, {"second"});
    ASSERT_TRUE(materializer.prepare_split(blob_split(2000, 1, {second}), columns).ok());
    block = output_block(columns, 1);
    ASSERT_TRUE(materializer.materialize(*row_positions({0}), &block, context).ok());
    expect_descriptor(*block.get_by_position(0).column, 0, second.path, 4, 6);

    // A present empty list means an added nullable field, not an ordinary non-BLOB split.
    ASSERT_TRUE(materializer.prepare_split(blob_split(3000, 1, {}), columns).ok());
    EXPECT_TRUE(materializer.has_blob_columns());
    block = output_block(columns, 1);
    ASSERT_TRUE(materializer.materialize(*row_positions({0}), &block, context).ok());
    expect_inline(*block.get_by_position(0).column, 0, std::nullopt);

    // An absent list disables BLOB materialization and must not retain the previous split.
    ASSERT_TRUE(materializer.prepare_split(TPaimonFileDesc {}, columns).ok());
    EXPECT_FALSE(materializer.has_blob_columns());
    EXPECT_TRUE(materializer.blob_column_names().empty());
}

TEST_F(PaimonBlobMaterializerTest, ValidatesUnprojectedMetadataWithoutOpeningUnprojectedFiles) {
    auto image = write_blob("image.blob", 7, 1000, {"a", "b", "c"});
    auto unprojected = image;
    unprojected.__set_field_id(9);
    unprojected.__set_path((_temp_directory.path() / "absent.blob").string());
    const std::vector<ColumnDefinition> columns {blob_column("image", 7)};
    PaimonBlobMaterializer materializer;
    ASSERT_TRUE(
            materializer.prepare_split(blob_split(1000, 3, {image, unprojected}), columns).ok());
    auto block = output_block(columns, 1);
    auto context = read_context(1);
    ASSERT_TRUE(materializer.materialize(*row_positions({2}), &block, context).ok());
    expect_inline(*block.get_by_position(0).column, 0, "c");

    // Even an unprojected field must not hide a hole in its manifest row coverage.
    unprojected.__set_row_count(2);
    EXPECT_TRUE(materializer.prepare_split(blob_split(1000, 3, {image, unprojected}), columns)
                        .is<ErrorCode::CORRUPTION>());
}

TEST_F(PaimonBlobMaterializerTest, RejectsMissingNegativeAndOverflowingAnchorRanges) {
    const auto valid = blob_split(1000, 1, {});
    std::vector<TPaimonFileDesc> invalid(7, valid);
    invalid[0].__isset.anchor_first_row_id = false;
    invalid[1].__isset.anchor_row_count = false;
    invalid[2].__set_anchor_first_row_id(-1);
    invalid[3].__set_anchor_row_count(0);
    invalid[4].__set_anchor_row_count(-1);
    invalid[5].__set_anchor_first_row_id(std::numeric_limits<int64_t>::max());
    invalid[6].__set_anchor_row_count(std::numeric_limits<int64_t>::max());
    PaimonBlobMaterializer materializer;
    const std::vector<ColumnDefinition> columns {blob_column("image", 7)};
    for (const auto& split : invalid) {
        EXPECT_TRUE(materializer.prepare_split(split, columns).is<ErrorCode::INVALID_ARGUMENT>());
    }
    EXPECT_TRUE(materializer
                        .prepare_split(blob_split(std::numeric_limits<int64_t>::max() - 1, 1, {}),
                                       columns)
                        .ok());
}

TEST_F(PaimonBlobMaterializerTest, RejectsMissingOrInvalidFieldIds) {
    auto file = write_blob("image.blob", 7, 0, {"a"});
    PaimonBlobMaterializer materializer;
    const std::vector<ColumnDefinition> columns {blob_column("image", 7)};
    file.__isset.field_id = false;
    EXPECT_TRUE(materializer.prepare_split(blob_split(0, 1, {file}), columns)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    file.__set_field_id(-1);
    EXPECT_TRUE(materializer.prepare_split(blob_split(0, 1, {file}), columns)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    auto no_field_id = blob_column("image", 7);
    no_field_id.identifier = Field();
    EXPECT_TRUE(materializer.prepare_split(blob_split(0, 1, {}), {no_field_id})
                        .is<ErrorCode::INVALID_ARGUMENT>());
}

TEST_F(PaimonBlobMaterializerTest, RejectsMisalignedAndOutOfRangePositionsBeforeReadingFiles) {
    const std::vector<ColumnDefinition> columns {blob_column("image", 7)};
    auto absent = write_blob("image.blob", 7, 1000, {"a", "b"});
    absent.__set_path((_temp_directory.path() / "absent.blob").string());
    PaimonBlobMaterializer materializer;
    ASSERT_TRUE(materializer.prepare_split(blob_split(1000, 2, {absent}), columns).ok());
    auto context = read_context();
    auto block = output_block(columns, 2);
    EXPECT_TRUE(materializer.materialize(*row_positions({0}), &block, context)
                        .is<ErrorCode::INTERNAL_ERROR>());
    for (const int64_t invalid_position : {-1, 2}) {
        block = output_block(columns, 1);
        EXPECT_TRUE(materializer.materialize(*row_positions({invalid_position}), &block, context)
                            .is<ErrorCode::CORRUPTION>());
    }
    block = output_block(columns, 0);
    EXPECT_TRUE(materializer.materialize(*row_positions({}), &block, context).ok());
}

TEST_F(PaimonBlobMaterializerTest, NoProjectedBlobColumnsLeaveOrdinaryOutputUnchanged) {
    ColumnDefinition id;
    id.name = "id";
    id.type = std::make_shared<DataTypeInt32>();
    auto unprojected = write_blob("image.blob", 7, 0, {"hello"});
    unprojected.__set_path((_temp_directory.path() / "absent.blob").string());
    PaimonBlobMaterializer materializer;
    ASSERT_TRUE(materializer.prepare_split(blob_split(0, 1, {unprojected}), {id}).ok());
    EXPECT_FALSE(materializer.has_blob_columns());
    auto block = output_block({id}, 1);
    auto context = read_context(1);
    EXPECT_TRUE(materializer.materialize(*row_positions({0}), &block, context).ok());
    ASSERT_EQ(block.columns(), 1);
    EXPECT_EQ((*block.get_by_position(0).column)[0].get<TYPE_INT>(), 0);
}

TEST_F(PaimonBlobMaterializerTest, ValidatesDescriptorOptionAndRestoresInlineDefaultOnInit) {
    PaimonBlobMaterializer materializer;
    for (const auto* value : {"", "TRUE", "1", "yes"}) {
        EXPECT_TRUE(materializer.init({{"blob-as-descriptor", value}})
                            .is<ErrorCode::INVALID_ARGUMENT>());
    }
    ASSERT_TRUE(materializer.init({{"blob-as-descriptor", "true"}}).ok());
    ASSERT_TRUE(materializer.init({}).ok());
    auto file = write_blob("inline.blob", 7, 0, {"hello"});
    const std::vector<ColumnDefinition> columns {blob_column("image", 7)};
    ASSERT_TRUE(materializer.prepare_split(blob_split(0, 1, {file}), columns).ok());
    auto block = output_block(columns, 1);
    auto context = read_context();
    ASSERT_TRUE(materializer.materialize(*row_positions({0}), &block, context).ok());
    expect_inline(*block.get_by_position(0).column, 0, "hello");
}

} // namespace
} // namespace doris::format::paimon
