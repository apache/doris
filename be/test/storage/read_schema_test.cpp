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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"
#include "runtime/descriptors.h"
#include "storage/binlog.h"
#include "storage/schema.h"
#include "util/json/path_in_data.h"

namespace doris {
namespace {

TabletColumnPtr create_int_column(int32_t unique_id, std::string name, bool is_key = false) {
    auto column = std::make_shared<TabletColumn>(
            FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, FieldType::OLAP_FIELD_TYPE_INT,
            false, unique_id, sizeof(int32_t));
    column->set_name(std::move(name));
    column->set_is_key(is_key);
    return column;
}

TabletColumnPtr create_struct_column(int32_t unique_id) {
    auto column = std::make_shared<TabletColumn>();
    column->set_unique_id(unique_id);
    column->set_name("s");
    column->set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    column->set_is_nullable(false);

    auto first_child = create_int_column(unique_id + 100, "a");
    auto second_child = create_int_column(unique_id + 101, "b");
    column->add_sub_column(*first_child);
    column->add_sub_column(*second_child);
    return column;
}

TabletSchemaSPtr create_row_binlog_schema_with_colliding_names() {
    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(*create_int_column(10, "key", true));
    schema->append_column(*create_int_column(11, "v"));
    schema->append_column(*create_int_column(12, "__BEFORE__v__"));
    schema->append_column(*create_int_column(13, binlog::build_before_column_name("v")));
    schema->append_column(
            *create_int_column(14, binlog::build_before_column_name("__BEFORE__v__")));
    schema->append_column(*create_int_column(15, BINLOG_TSO_COL));
    schema->append_column(*create_int_column(16, BINLOG_LSN_COL));
    schema->append_column(*create_int_column(17, BINLOG_OP_COL));
    return schema;
}

TEST(ReadSchemaTest, DefaultColumnsAreVisible) {
    std::vector<TabletColumnPtr> storage_columns {create_int_column(10, "k", true),
                                                  create_int_column(11, "dropped"),
                                                  create_struct_column(12)};

    ReadSchema read_schema(storage_columns);
    ASSERT_EQ(3, read_schema.num_block_columns());
    ASSERT_EQ(3, read_schema.num_read_columns());
    for (size_t ordinal = 0; ordinal < storage_columns.size(); ++ordinal) {
        EXPECT_EQ(storage_columns[ordinal].get(), read_schema.column(ordinal));
        EXPECT_TRUE(
                read_schema.data_type(ordinal)->equals(*storage_columns[ordinal]->get_vec_type()));
        EXPECT_EQ(static_cast<int32_t>(ordinal),
                  read_schema.ordinal_by_uid(storage_columns[ordinal]->unique_id()));
    }
}

TEST(ReadSchemaTest, ProjectionPreservesRequestedOrder) {
    std::vector<TabletColumnPtr> storage_columns {create_int_column(10, "k", true),
                                                  create_int_column(11, "dropped"),
                                                  create_struct_column(12)};

    ReadSchema read_schema(
            project_columns_by_ordinal(storage_columns, std::vector<ColumnId> {2, 0, 2}));
    ASSERT_EQ(3, read_schema.num_block_columns());
    ASSERT_EQ(3, read_schema.num_read_columns());
    EXPECT_EQ("s", read_schema.column(0)->name());
    EXPECT_EQ("k", read_schema.column(1)->name());
    EXPECT_EQ("s", read_schema.column(2)->name());
    EXPECT_EQ(0, read_schema.ordinal_by_uid(12));
    EXPECT_EQ(1, read_schema.ordinal_by_uid(10));
    EXPECT_EQ(-1, read_schema.ordinal_by_uid(11));
}

TEST(ReadSchemaTest, ExpectedTypesDefineReadBlock) {
    std::vector<TabletColumnPtr> read_columns {create_struct_column(12),
                                               create_int_column(10, "k", true)};

    auto pruned_struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt32>()}, Strings {"a"});
    auto int_type = std::make_shared<DataTypeInt32>();
    ReadSchema read_schema(std::move(read_columns),
                           std::vector<DataTypePtr> {pruned_struct_type, int_type});
    EXPECT_FALSE(read_schema.column(0)->get_vec_type()->equals(*pruned_struct_type));

    Block block = read_schema.create_read_block();
    ASSERT_EQ(2, block.columns());
    EXPECT_EQ("s", block.get_by_position(0).name);
    EXPECT_EQ("k", block.get_by_position(1).name);
    EXPECT_TRUE(block.get_by_position(0).type->equals(*pruned_struct_type));
    EXPECT_TRUE(block.get_by_position(1).type->equals(*int_type));
}

TEST(ReadSchemaTest, AppendedDroppedColumnDoesNotExtendReadBlock) {
    std::vector<TabletColumnPtr> read_columns {create_int_column(10, "k", true),
                                               create_struct_column(12)};
    auto dropped_column = create_int_column(11, "dropped", true);
    ReadSchema read_schema(std::move(read_columns));

    EXPECT_EQ(1, read_schema.num_key_columns());
    read_schema.append_dropped_columns({*dropped_column});
    ColumnId suffix_ordinal = read_schema.ordinal_by_uid(11);
    EXPECT_EQ(2, suffix_ordinal);
    EXPECT_EQ(2, read_schema.num_block_columns());
    EXPECT_EQ(3, read_schema.num_read_columns());
    EXPECT_EQ(1, read_schema.num_key_columns());
    EXPECT_EQ(suffix_ordinal, read_schema.ordinal_by_uid(11));
    EXPECT_TRUE(read_schema.data_type(suffix_ordinal)->equals(*dropped_column->get_vec_type()));
    EXPECT_EQ(2, read_schema.create_read_block().columns());
}

TEST(ReadSchemaTest, RowBinlogMappingsUsePhysicalSchemaOrdinals) {
    auto tablet_schema = create_row_binlog_schema_with_colliding_names();
    // Reorder the two AFTER values and their BEFORE companions to verify that ReadSchema stores
    // dense read ordinals resolved by unique id, rather than physical tablet column ids or names.
    ReadSchema read_schema(project_columns_by_ordinal(
            tablet_schema->columns(), std::vector<ColumnId> {0, 2, 1, 4, 3, 5, 6, 7}));

    EXPECT_TRUE(read_schema
                        .init_from_tablet_schema(*tablet_schema,
                                                 /*merge_by_sequence_mapping=*/false,
                                                 /*map_row_binlog_columns=*/true)
                        .ok());

    EXPECT_TRUE(read_schema.row_binlog_value_pairs_complete());
    EXPECT_EQ(read_schema.row_binlog_value_column_pairs(),
              (ReadSchema::RowBinlogValueColumnPairs {{2, 4}, {1, 3}}));
    EXPECT_EQ(4, read_schema.before_column_ordinal(2));
    EXPECT_EQ(3, read_schema.before_column_ordinal(1));
    for (ColumnId ordinal : {0, 3, 4, 5, 6, 7}) {
        EXPECT_EQ(ordinal, read_schema.before_column_ordinal(ordinal));
    }
}

TEST(ReadSchemaTest, MalformedRowBinlogLayoutKeepsConservativeNameMapping) {
    TabletSchema tablet_schema;
    tablet_schema.append_column(*create_int_column(10, "key", true));
    tablet_schema.append_column(*create_int_column(11, "v"));
    tablet_schema.append_column(*create_int_column(12, binlog::build_before_column_name("v")));
    tablet_schema.append_column(*create_int_column(13, "orphan"));
    tablet_schema.append_column(*create_int_column(14, BINLOG_TSO_COL));
    tablet_schema.append_column(*create_int_column(15, BINLOG_LSN_COL));
    tablet_schema.append_column(*create_int_column(16, BINLOG_OP_COL));
    ReadSchema read_schema(tablet_schema.columns());

    EXPECT_TRUE(read_schema
                        .init_from_tablet_schema(tablet_schema, /*merge_by_sequence_mapping=*/false,
                                                 /*map_row_binlog_columns=*/true)
                        .ok());

    EXPECT_FALSE(read_schema.row_binlog_value_pairs_complete());
    EXPECT_TRUE(read_schema.row_binlog_value_column_pairs().empty());
    EXPECT_EQ(2, read_schema.before_column_ordinal(1));
}

} // namespace

namespace {

// key k(10), value v(11) governed by sequence column s(12).
TabletSchemaSPtr create_sequence_mapped_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::UNIQUE_KEYS);
    for (const auto& [uid, name, is_key] : {std::tuple<int32_t, const char*, bool> {10, "k", true},
                                            {11, "v", false},
                                            {12, "s", false}}) {
        ColumnPB* column_pb = schema_pb.add_column();
        column_pb->set_unique_id(uid);
        column_pb->set_name(name);
        column_pb->set_type("INT");
        column_pb->set_length(4);
        column_pb->set_index_length(4);
        column_pb->set_is_key(is_key);
        column_pb->set_is_nullable(!is_key);
    }
    ColumnGroupPB* group = schema_pb.mutable_seq_map()->add_cg();
    group->set_sequence_column(12);
    group->add_columns_in_group(11);

    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    return tablet_schema;
}

} // namespace

// The two facts hold for the whole read: they are taken from the tablet schema, not from the
// projection, so a column group that excludes the columns they are about still reports them.
TEST(ReadSchemaTest, TabletHasSequenceMapIsIndependentOfTheProjection) {
    auto tablet_schema = create_sequence_mapped_schema();

    ReadSchema whole(tablet_schema->columns());
    ASSERT_TRUE(whole.init_from_tablet_schema(*tablet_schema,
                                              /*merge_by_sequence_mapping=*/false,
                                              /*map_row_binlog_columns=*/false)
                        .ok());
    EXPECT_TRUE(whole.tablet_has_sequence_map());

    // The key column alone: neither the sequence column nor the value it governs is projected.
    ReadSchema key_only(
            project_columns_by_ordinal(tablet_schema->columns(), std::vector<ColumnId> {0}));
    ASSERT_TRUE(key_only.init_from_tablet_schema(*tablet_schema,
                                                 /*merge_by_sequence_mapping=*/false,
                                                 /*map_row_binlog_columns=*/false)
                        .ok());
    EXPECT_TRUE(key_only.tablet_has_sequence_map());
}

TEST(ReadSchemaTest, TabletWithoutSequenceMap) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_column(10, "k", true));
    tablet_schema->append_column(*create_int_column(11, "v"));

    ReadSchema read_schema(tablet_schema->columns());
    ASSERT_TRUE(read_schema
                        .init_from_tablet_schema(*tablet_schema,
                                                 /*merge_by_sequence_mapping=*/false,
                                                 /*map_row_binlog_columns=*/false)
                        .ok());
    EXPECT_FALSE(read_schema.tablet_has_sequence_map());
    EXPECT_FALSE(read_schema.tablet_has_extracted_variant_columns());
}

TEST(ReadSchemaTest, MergeBySequenceMappingBuildsTheMap) {
    auto tablet_schema = create_sequence_mapped_schema();
    ReadSchema read_schema(tablet_schema->columns());
    ASSERT_TRUE(read_schema
                        .init_from_tablet_schema(*tablet_schema,
                                                 /*merge_by_sequence_mapping=*/true,
                                                 /*map_row_binlog_columns=*/false)
                        .ok());

    // Ordinals are this ReadSchema's, not the tablet schema's column ids.
    const auto& sequence_map = read_schema.sequence_map();
    ASSERT_EQ(1, sequence_map.size());
    auto group = sequence_map.find(2);
    ASSERT_NE(group, sequence_map.end());
    EXPECT_EQ((std::vector<ColumnId> {1}), group->second);

    // Without the flag the layout is left alone, even though the tablet has one.
    ReadSchema untouched(tablet_schema->columns());
    ASSERT_TRUE(untouched
                        .init_from_tablet_schema(*tablet_schema,
                                                 /*merge_by_sequence_mapping=*/false,
                                                 /*map_row_binlog_columns=*/false)
                        .ok());
    EXPECT_TRUE(untouched.sequence_map().empty());
}

// A tablet cannot have both a sequence column and a sequence mapping; asking to merge by the
// mapping on such a schema must fail rather than build half a layout.
TEST(ReadSchemaTest, SequenceColumnAndSequenceMapConflict) {
    auto tablet_schema = create_sequence_mapped_schema();
    tablet_schema->append_column(*create_int_column(13, SEQUENCE_COL));

    ReadSchema read_schema(tablet_schema->columns());
    auto st = read_schema.init_from_tablet_schema(*tablet_schema,
                                                  /*merge_by_sequence_mapping=*/true,
                                                  /*map_row_binlog_columns=*/false);
    EXPECT_FALSE(st.ok()) << st;
    EXPECT_TRUE(read_schema.sequence_map().empty());
}

TEST(ReadSchemaTest, HasExtractedVariantColumns) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_column(10, "k", true));

    TabletColumn variant;
    variant.set_unique_id(11);
    variant.set_name("v");
    variant.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant.set_is_nullable(true);
    tablet_schema->append_column(variant);

    ReadSchema without_extracted(tablet_schema->columns());
    ASSERT_TRUE(without_extracted
                        .init_from_tablet_schema(*tablet_schema,
                                                 /*merge_by_sequence_mapping=*/false,
                                                 /*map_row_binlog_columns=*/false)
                        .ok());
    EXPECT_FALSE(without_extracted.tablet_has_extracted_variant_columns());

    TabletColumn extracted;
    extracted.set_unique_id(-1);
    extracted.set_name("v.a");
    extracted.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    extracted.set_parent_unique_id(11);
    extracted.set_path_info(PathInData("v.a"));
    tablet_schema->append_column(extracted);

    // Projected without the extracted column, which must not change the answer.
    ReadSchema read_schema(
            project_columns_by_ordinal(tablet_schema->columns(), std::vector<ColumnId> {0}));
    ASSERT_TRUE(read_schema
                        .init_from_tablet_schema(*tablet_schema,
                                                 /*merge_by_sequence_mapping=*/false,
                                                 /*map_row_binlog_columns=*/false)
                        .ok());
    EXPECT_TRUE(read_schema.tablet_has_extracted_variant_columns());
}

namespace {

// FE sends this length for a STRING type.
constexpr int FE_STRING_LENGTH = std::numeric_limits<int32_t>::max() - 4;

TSlotDescriptor create_tslot(const DataTypePtr& type, std::string name, int32_t unique_id) {
    TSlotDescriptor tslot;
    type->to_thrift(tslot.slotType);
    tslot.__set_nullIndicatorBit(type->is_nullable() ? 0 : -1);
    tslot.__set_colName(std::move(name));
    tslot.__set_col_unique_id(unique_id);
    return tslot;
}

} // namespace

// MAP<STRING, ARRAY<INT>>: the sub columns are built and named the way FE names them.
TEST(ReadSchemaTest, TabletColumnFromSlotBuildsSubColumns) {
    auto type = make_nullable(std::make_shared<DataTypeMap>(
            make_nullable(std::make_shared<DataTypeString>(FE_STRING_LENGTH, TYPE_STRING)),
            make_nullable(std::make_shared<DataTypeArray>(
                    make_nullable(std::make_shared<DataTypeInt32>())))));
    SlotDescriptor slot(create_tslot(type, "obj", 7));

    TabletColumn column = ReadSchema::tablet_column_from_slot(slot);
    EXPECT_EQ("obj", column.name());
    EXPECT_EQ(7, column.unique_id());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_MAP, column.type());
    EXPECT_TRUE(column.is_nullable());
    EXPECT_FALSE(column.has_default_value());
    ASSERT_EQ(2, column.get_subtype_count());
    const TabletColumn& key = column.get_sub_column(0);
    EXPECT_EQ("key", key.name());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_STRING, key.type());
    EXPECT_TRUE(key.is_nullable());
    const TabletColumn& value = column.get_sub_column(1);
    EXPECT_EQ("value", value.name());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_ARRAY, value.type());
    ASSERT_EQ(1, value.get_subtype_count());
    EXPECT_EQ("item", value.get_sub_column(0).name());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_INT, value.get_sub_column(0).type());
}

// The column describes the slot's type: building the type back from it gives the same type.
TEST(ReadSchemaTest, TabletColumnFromSlotKeepsTheType) {
    const auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    const auto varchar = std::make_shared<DataTypeString>(10, TYPE_VARCHAR);
    const DataTypes types = {
            std::make_shared<DataTypeInt32>(),
            make_nullable(std::make_shared<DataTypeDecimal32>(9, 2)),
            make_nullable(std::make_shared<DataTypeDecimal128>(38, 10)),
            make_nullable(std::make_shared<DataTypeDateTimeV2>(3)),
            make_nullable(std::make_shared<DataTypeDateV2>()),
            make_nullable(std::make_shared<DataTypeIPv6>()),
            make_nullable(std::make_shared<DataTypeJsonb>()),
            std::make_shared<DataTypeBitMap>(),
            make_nullable(std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())),
            make_nullable(std::make_shared<DataTypeArray>(
                    make_nullable(std::make_shared<DataTypeArray>(make_nullable(varchar))))),
            make_nullable(std::make_shared<DataTypeMap>(make_nullable(varchar), nullable_int)),
            make_nullable(std::make_shared<DataTypeStruct>(
                    DataTypes {std::make_shared<DataTypeInt32>(), make_nullable(varchar),
                               make_nullable(std::make_shared<DataTypeMap>(make_nullable(varchar),
                                                                           nullable_int))},
                    Strings {"a", "b", "c"})),
    };
    for (const auto& type : types) {
        SlotDescriptor slot(create_tslot(type, "c", 1));
        TabletColumn column = ReadSchema::tablet_column_from_slot(slot);
        EXPECT_EQ(type->get_name(),
                  DataTypeFactory::instance().create_data_type(column)->get_name());
    }
}

// Length, precision, scale, variant settings and default value come out as for a column FE sends.
TEST(ReadSchemaTest, TabletColumnFromSlotKeepsTypeDetails) {
    TSlotDescriptor char_tslot =
            create_tslot(std::make_shared<DataTypeString>(5, TYPE_CHAR), "c", 1);
    char_tslot.__set_col_default_value("ab");
    TabletColumn char_column = ReadSchema::tablet_column_from_slot(SlotDescriptor(char_tslot));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_CHAR, char_column.type());
    EXPECT_EQ(5, char_column.length());
    EXPECT_TRUE(char_column.has_default_value());
    EXPECT_EQ("ab", char_column.default_value());

    TabletColumn varchar_column = ReadSchema::tablet_column_from_slot(SlotDescriptor(
            create_tslot(std::make_shared<DataTypeString>(10, TYPE_VARCHAR), "c", 1)));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_VARCHAR, varchar_column.type());
    EXPECT_EQ(12, varchar_column.length());

    TabletColumn string_column = ReadSchema::tablet_column_from_slot(SlotDescriptor(
            create_tslot(std::make_shared<DataTypeString>(FE_STRING_LENGTH, TYPE_STRING), "c", 1)));
    EXPECT_EQ(std::numeric_limits<int32_t>::max(), string_column.length());

    // DECIMALV2 computes as (27, 9) but keeps the declared precision and scale.
    TSlotDescriptor decimalv2_tslot = create_tslot(std::make_shared<DataTypeDecimalV2>(), "c", 1);
    decimalv2_tslot.slotType.types[0].scalar_type.__set_precision(10);
    decimalv2_tslot.slotType.types[0].scalar_type.__set_scale(2);
    TabletColumn decimalv2_column =
            ReadSchema::tablet_column_from_slot(SlotDescriptor(decimalv2_tslot));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_DECIMAL, decimalv2_column.type());
    EXPECT_EQ(10, decimalv2_column.precision());
    EXPECT_EQ(2, decimalv2_column.frac());

    TSlotDescriptor variant_tslot =
            create_tslot(make_nullable(std::make_shared<DataTypeVariantV2>()), "v", 1);
    variant_tslot.slotType.types[0].scalar_type.__set_variant_max_subcolumns_count(3);
    variant_tslot.slotType.types[0].scalar_type.__set_variant_enable_doc_mode(true);
    TabletColumn variant_column =
            ReadSchema::tablet_column_from_slot(SlotDescriptor(variant_tslot));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_VARIANT, variant_column.type());
    EXPECT_EQ(3, variant_column.variant_max_subcolumns_count());
    EXPECT_TRUE(variant_column.variant_enable_doc_mode());
}

TEST(ReadSchemaTest, AppendedRowIdExtendsReadBlockBeforeDroppedColumns) {
    std::vector<TabletColumnPtr> read_columns {create_int_column(10, "k", true)};
    auto dropped_column = create_int_column(11, "dropped");
    ReadSchema read_schema(std::move(read_columns));

    read_schema.append_rowid_column();
    read_schema.append_dropped_columns({*dropped_column});

    EXPECT_EQ(1, read_schema.rowid_ordinal());
    EXPECT_EQ(2, read_schema.num_block_columns());
    EXPECT_EQ(3, read_schema.num_read_columns());
    EXPECT_EQ(2, read_schema.ordinal_by_uid(11));
    Block block = read_schema.create_read_block();
    ASSERT_EQ(2, block.columns());
    EXPECT_EQ(BeConsts::ROWID_COL, block.get_by_position(1).name);
    EXPECT_EQ(PrimitiveType::TYPE_STRING, block.get_by_position(1).type->get_primitive_type());
}

} // namespace doris
