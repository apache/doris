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
#include <optional>
#include <string>
#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "storage/predicate/predicate_creator.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "exec/common/variant_util.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;
constexpr int64_t kRootIndexId = 310001;

VariantColumnSpec variant_with_b_template(std::optional<FieldType> type) {
    VariantColumnSpec column;
    column.unique_id = kVariantUid;
    column.max_subcolumns_count = 8;
    if (type.has_value()) {
        column.predefined_paths.push_back(
                VariantPathSpec {.path = "b*",
                                 .type = type.value(),
                                 .nullable = true,
                                 .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                                 .array_item_type = {},
                                 .array_item_nullable = true});
    }
    return column;
}

std::shared_ptr<ColumnPredicate> int_equals(int32_t column_id, std::string name, int32_t value) {
    return create_comparison_predicate<PredicateType::EQ>(
            column_id, std::move(name), std::make_shared<DataTypeInt32>(),
            Field::create_field<TYPE_INT>(value), false);
}

} // namespace

// A root-level inverted index is inherited by template-typed paths. A compaction carrier keeps old
// dynamic values as JSONB and writes no physical index, so reads must scan it instead of failing.
class IndexStorageVariantCarrierIndexTest : public IndexStorageTestFixture {
protected:
    IndexTabletOptions options(const VariantColumnSpec& variant, int64_t tablet_id) {
        IndexTabletOptions result;
        result.tablet_id = tablet_id;
        result.variant_columns = {variant};
        result.inverted_indexes = {IndexSpec::column_index(kRootIndexId, "idx_v", kVariantUid)};
        return result;
    }

    Result<RowsetSharedPtr> write(int64_t version, const std::vector<std::string>& jsons) {
        IndexRowsetSpec rowset;
        rowset.version = version;
        rowset.batches.push_back(
                IndexBatch::single_variant(jsons, static_cast<int32_t>(version * 4)));
        auto written = write_rowset(rowset);
        if (written.has_value()) {
            SegmentLoader::instance()->erase_segments(*written.value()->rowset_meta());
        }
        return written;
    }

    void install_write_schema(const VariantColumnSpec& variant) {
        auto target = build_tablet_schema(options(variant, tablet()->tablet_id()));
        target->set_schema_version(tablet_schema()->schema_version() + 1);
        _tablet_schema = target;
        tablet()->tablet_meta()->_schema = target;
        tablet()->update_max_version_schema(target);
    }

    // FE sends the template-bound subcolumn; the segments keep their own rowset schemas.
    Result<IndexReadResult> query_b1(const std::vector<RowsetSharedPtr>& rowsets,
                                     FieldType template_type, bool use_index) {
        auto write_schema = _tablet_schema;
        auto read_schema = variant_util::VariantCompactionUtil::calculate_variant_extended_schema(
                rowsets, write_schema);
        if (read_schema == nullptr) {
            return ResultError(Status::InternalError("no extended schema"));
        }
        _tablet_schema = read_schema;
        const int32_t cid = column_id_by_path("v.b1");
        if (cid < 0) {
            _tablet_schema = write_schema;
            return ResultError(Status::InternalError("v.b1 is not in the read schema"));
        }
        const auto& column = _tablet_schema->column(cid);
        IndexReadOptions read;
        read.return_columns = {0, static_cast<uint32_t>(cid)};
        read.enable_inverted_index_query = use_index;
        read.inverted_index_skip_threshold = 100;
        if (template_type == FieldType::OLAP_FIELD_TYPE_INT) {
            read.target_cast_type_for_variants[column.name()] =
                    make_nullable(std::make_shared<DataTypeInt32>());
            read.predicates.push_back(int_equals(cid, column.name(), 7));
        } else {
            read.target_cast_type_for_variants[column.name()] = nullable_string_target_type();
            read.predicates.push_back(string_equals(cid, column.name(), "x1"));
        }
        auto result = read_rowsets(rowsets, read);
        _tablet_schema = write_schema;
        return result;
    }
};

TEST_F(IndexStorageVariantCarrierIndexTest, AddedTemplateCarrierScansWithoutPhysicalIndex) {
    int64_t tablet_id = 310100;
    // STRING template over old dynamic STRING b1: the same-type path still becomes a JSONB carrier.
    for (auto type : {FieldType::OLAP_FIELD_TYPE_STRING}) {
        SCOPED_TRACE(::testing::Message() << "template_type=" << static_cast<int>(type));
        ASSERT_TRUE(create_tablet(options(variant_with_b_template(std::nullopt), tablet_id++)).ok());
        auto old = write(0, {R"({"b1":"x1","z":0})", R"({"b1":"x2","z":1})"});
        ASSERT_TRUE(old.has_value()) << old.error();
        const bool is_int = type == FieldType::OLAP_FIELD_TYPE_INT;
        // Before ALTER the old dynamic string path has the inherited root index.
        if (!is_int) {
            auto before = query_b1({old.value()}, type, true);
            ASSERT_TRUE(before.has_value()) << before.error();
            EXPECT_EQ(before->rows_read, 1);
            EXPECT_TRUE(before->inverted_index_used());
        }
        install_write_schema(variant_with_b_template(type));
        auto recent = write(1, {is_int ? R"({"b1":7,"z":2})" : R"({"b1":"x1","z":2})"});
        ASSERT_TRUE(recent.has_value()) << recent.error();
        const int64_t expected = is_int ? 1 : 2;

        auto uncompacted = query_b1({old.value(), recent.value()}, type, true);
        ASSERT_TRUE(uncompacted.has_value()) << "uncompacted: " << uncompacted.error();
        EXPECT_EQ(uncompacted->rows_read, expected);

        auto output = compact_rowsets_and_reload(IndexCompactionKind::CUMULATIVE,
                                                 {old.value(), recent.value()});
        ASSERT_TRUE(output.has_value()) << output.error();
        std::vector<int64_t> rows;
        for (bool use_index : {false, true}) {
            SCOPED_TRACE(::testing::Message() << "use_index=" << use_index);
            // Before the fix both reads failed with INVERTED_INDEX_NOT_SUPPORTED on the JSONB leaf.
            auto compacted = query_b1({output.value()}, type, use_index);
            ASSERT_TRUE(compacted.has_value()) << "compacted carrier: " << compacted.error();
            // The JSONB carrier has no physical index file entry to accelerate the predicate.
            EXPECT_FALSE(compacted->inverted_index_used());
            rows.push_back(compacted->rows_read);
        }
        // Index-enabled and scan-only reads agree on the carrier segment. The row count is only
        // recorded: this harness pushes a typed predicate straight onto the JSONB leaf.
        EXPECT_EQ(rows[0], rows[1]);
        RecordProperty("carrier_rows_read", std::to_string(rows[0]));
        RecordProperty("uncompacted_rows_expected", std::to_string(expected));
    }
}

// Control: a never-altered templated column still uses the template-derived root index.
TEST_F(IndexStorageVariantCarrierIndexTest, NeverAlteredTemplateKeepsInheritedIndex) {
    ASSERT_TRUE(create_tablet(options(variant_with_b_template(FieldType::OLAP_FIELD_TYPE_STRING),
                                      310200))
                        .ok());
    auto first = write(0, {R"({"b1":"x1","z":0})", R"({"b1":"x2","z":1})"});
    ASSERT_TRUE(first.has_value()) << first.error();
    auto second = write(1, {R"({"b1":"x3","z":2})"});
    ASSERT_TRUE(second.has_value()) << second.error();
    auto output = compact_rowsets_and_reload(IndexCompactionKind::CUMULATIVE,
                                             {first.value(), second.value()});
    ASSERT_TRUE(output.has_value()) << output.error();
    for (const auto& rowsets : std::vector<std::vector<RowsetSharedPtr>> {
                 {first.value(), second.value()}, {output.value()}}) {
        auto result = query_b1(rowsets, FieldType::OLAP_FIELD_TYPE_STRING, true);
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_EQ(result->rows_read, 1);
        EXPECT_TRUE(result->inverted_index_used());
    }
}

} // namespace doris::index_storage_test
