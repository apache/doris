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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_constants.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/data_type_serde.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/point_query_executor.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/jsonb/serialize.h"

namespace doris {

// Helper class for setting up Reusable objects to test LookupConnectionCache
class ReusableTestHelper {
public:
    static TDescriptorTable create_descriptor_tablet() {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_TINYINT)
                                       .column_name("k1")
                                       .column_pos(0)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_SMALLINT)
                                       .column_name("k2")
                                       .column_pos(1)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).column_name("k3").column_pos(2).build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k4").column_pos(3).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_LARGEINT)
                                       .column_name("k5")
                                       .column_pos(4)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_DATE).column_name("k6").column_pos(5).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATETIME)
                                       .column_name("k7")
                                       .column_pos(6)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().string_type(4).column_name("k8").column_pos(7).build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().string_type(65).column_name("k9").column_pos(8).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .decimal_type(6, 3)
                                       .column_name("k10")
                                       .column_pos(9)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATEV2)
                                       .column_name("k11")
                                       .column_pos(10)
                                       .build());

        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_TINYINT)
                                       .column_name("v1")
                                       .column_pos(11)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_SMALLINT)
                                       .column_name("v2")
                                       .column_pos(12)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .column_name("v3")
                                       .column_pos(13)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_BIGINT)
                                       .column_name("v4")
                                       .column_pos(14)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_LARGEINT)
                                       .column_name("v5")
                                       .column_pos(15)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATE)
                                       .column_name("v6")
                                       .column_pos(16)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATETIME)
                                       .column_name("v7")
                                       .column_pos(17)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(4)
                                       .column_name("v8")
                                       .column_pos(18)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65)
                                       .column_name("v9")
                                       .column_pos(19)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .decimal_type(6, 3)
                                       .column_name("v10")
                                       .column_pos(20)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATEV2)
                                       .column_name("v11")
                                       .column_pos(21)
                                       .nullable(false)
                                       .build());
        tuple_builder.build(&dtb);

        return dtb.desc_tbl();
    }

    static std::shared_ptr<Reusable> create_reusable() {
        auto obj_pool = std::make_unique<ObjectPool>();
        auto runtime_state = RuntimeState::create_unique();
        auto reusable = std::make_shared<Reusable>();

        // Create a simple TupleDescriptor for testing
        TDescriptorTable t_desc_tbl = create_descriptor_tablet();

        // Initialize Reusable
        Status st = reusable->init(t_desc_tbl, output_exprs, query_options, *tablet_schema, 2);
        if (!st.ok()) {
            return nullptr;
        }

        return reusable;
    }

    static std::vector<TExpr> output_exprs;
    static TQueryOptions query_options;
    static std::shared_ptr<TabletSchema> tablet_schema;
};

std::vector<TExpr> ReusableTestHelper::output_exprs = []() {
    std::vector<TExpr> list;
    TExpr expr;
    expr.nodes.emplace_back(TExprNode());
    expr.nodes[0].node_type = TExprNodeType::SLOT_REF;
    auto type = TTypeDesc();
    type.types.emplace_back(TTypeNode());
    type.types[0].type = TTypeNodeType::SCALAR;
    type.types[0].__isset.scalar_type = true;
    type.types[0].scalar_type.type = TPrimitiveType::BIGINT;
    expr.nodes[0].type = type;
    expr.nodes[0].num_children = 0;
    list.push_back(expr);
    return list;
}();

TQueryOptions ReusableTestHelper::query_options = TQueryOptions();
std::shared_ptr<TabletSchema> ReusableTestHelper::tablet_schema = []() {
    auto schema = std::make_shared<TabletSchema>();
    for (int i = 0; i < 11; ++i) {
        schema->append_column(TabletColumn(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                           FieldType::OLAP_FIELD_TYPE_BIGINT));
    }
    TabletColumn delete_sign;
    delete_sign.set_name(DELETE_SIGN);
    delete_sign.set_unique_id(1000);
    delete_sign.set_type(FieldType::OLAP_FIELD_TYPE_TINYINT);
    schema->append_column(delete_sign);
    return schema;
}();

TEST(PointQueryReusableTest, RejectUnverifiableNestedAccessPaths) {
    TColumnAccessPath access_path;
    access_path.type = TAccessPathType::DATA;
    TDataAccessPath data_path;
    data_path.__set_path({"k1", "leaf"});
    access_path.__set_data_access_path(data_path);

    auto init = [](const TDescriptorTable& desc_tbl) {
        Reusable reusable;
        return reusable.init(desc_tbl, ReusableTestHelper::output_exprs,
                             ReusableTestHelper::query_options, *ReusableTestHelper::tablet_schema,
                             2);
    };

    auto desc_tbl = ReusableTestHelper::create_descriptor_tablet();
    ASSERT_TRUE(init(desc_tbl).ok());

    desc_tbl.slotDescriptors[0].__set_all_access_paths({access_path});
    auto status = init(desc_tbl);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("column access paths"), std::string::npos);
    EXPECT_NE(status.to_string().find("k1"), std::string::npos);

    desc_tbl = ReusableTestHelper::create_descriptor_tablet();
    desc_tbl.slotDescriptors[0].__set_predicate_access_paths({access_path});
    status = init(desc_tbl);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("column access paths"), std::string::npos);
    EXPECT_NE(status.to_string().find("k1"), std::string::npos);
}

TEST(PointQueryReusableTest, AcceptFullStructAccessPathsFromOlderFe) {
    TabletSchema schema;
    schema.copy_from(*ReusableTestHelper::tablet_schema);
    TabletColumn struct_column;
    struct_column.set_name("typed");
    struct_column.set_unique_id(100);
    struct_column.set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    for (const auto* name : {"first", "second"}) {
        TabletColumn child;
        child.set_name(name);
        child.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        struct_column.add_sub_column(child);
    }
    schema.append_column(struct_column);

    auto desc_tbl = ReusableTestHelper::create_descriptor_tablet();
    desc_tbl.slotDescriptors.resize(1);
    desc_tbl.slotDescriptors[0].__set_colName("typed");
    desc_tbl.slotDescriptors[0].__set_col_unique_id(100);
    DataTypePtr full_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()},
            Strings {"first", "second"});
    desc_tbl.slotDescriptors[0].__set_slotType(full_type->to_thrift());
    TColumnAccessPath root_path;
    root_path.type = TAccessPathType::DATA;
    TDataAccessPath root_data_path;
    root_data_path.__set_path({"typed"});
    root_path.__set_data_access_path(root_data_path);
    desc_tbl.slotDescriptors[0].__set_all_access_paths({root_path});

    auto init = [&](const TDescriptorTable& descriptor) {
        Reusable reusable;
        return reusable.init(descriptor, ReusableTestHelper::output_exprs,
                             ReusableTestHelper::query_options, schema, 2);
    };
    ASSERT_TRUE(init(desc_tbl).ok());

    desc_tbl.slotDescriptors[0].__set_predicate_access_paths({root_path});
    ASSERT_TRUE(init(desc_tbl).ok());

    // Encode an actual full row-store value, then decode it through each accepted descriptor.
    const std::string first_value = "first-value";
    const std::string second_value = "second-value";
    auto source_column = full_type->create_column();
    auto& source_struct = assert_cast<ColumnStruct&>(*source_column);
    source_struct.get_column(0).insert_data(first_value.data(), first_value.size());
    source_struct.get_column(1).insert_data(second_value.data(), second_value.size());
    Block source_block;
    source_block.insert({std::move(source_column), full_type, "typed"});
    TabletSchema source_schema;
    source_schema.append_column(struct_column);
    auto jsonb_column = ColumnString::create();
    JsonbSerializeUtil::block_to_jsonb(source_schema, source_block, *jsonb_column, 1,
                                       DataTypeSerDeSPtrs {full_type->get_serde()}, {});

    auto expect_decodes_full_struct = [&](const TDescriptorTable& descriptor) {
        Reusable reusable;
        auto status = reusable.init(descriptor, ReusableTestHelper::output_exprs,
                                    ReusableTestHelper::query_options, schema, 2);
        ASSERT_TRUE(status.ok()) << status.to_string();
        Block decoded(reusable.tuple_desc()->slots(), 1);
        status = JsonbSerializeUtil::jsonb_to_block(
                reusable.get_data_type_serdes(), *jsonb_column, reusable.get_col_uid_to_idx(),
                decoded, reusable.get_col_default_values(), reusable.include_col_uids());
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_EQ(decoded.rows(), 1);
        auto decoded_column = decoded.get_by_position(0).unnest_nullable();
        const auto& result = assert_cast<const ColumnStruct&>(*decoded_column.column);
        EXPECT_EQ(result.get_column(0).get_data_at(0).to_string(), first_value);
        EXPECT_EQ(result.get_column(1).get_data_at(0).to_string(), second_value);
    };

    auto make_path = [](TAccessPathType::type type, const std::vector<std::string>& components) {
        TColumnAccessPath path;
        path.__set_type(type);
        if (type == TAccessPathType::DATA) {
            TDataAccessPath data;
            data.__set_path(components);
            path.__set_data_access_path(data);
        } else {
            path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
            TMetaAccessPath meta;
            meta.__set_path(components);
            path.__set_meta_access_path(meta);
        }
        return path;
    };
    auto first_path = make_path(TAccessPathType::DATA, {"typed", "first"});
    auto second_path = make_path(TAccessPathType::DATA, {"typed", "second"});
    auto typed_first_path = first_path;
    typed_first_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    auto typed_second_path = second_path;
    typed_second_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    auto null_path = make_path(TAccessPathType::META, {"typed", "NULL"});
    auto offset_path = make_path(TAccessPathType::META, {"typed", "second", "OFFSET"});
    const std::vector<TColumnAccessPaths> supported_path_sets {
            {root_path},
            {first_path, second_path},
            {typed_first_path, typed_second_path},
            {null_path},
            {first_path, offset_path}};
    for (const auto& paths : supported_path_sets) {
        desc_tbl.slotDescriptors[0].__set_all_access_paths(paths);
        desc_tbl.slotDescriptors[0].__set_predicate_access_paths({});
        expect_decodes_full_struct(desc_tbl);
        desc_tbl.slotDescriptors[0].__set_all_access_paths({});
        desc_tbl.slotDescriptors[0].__set_predicate_access_paths(paths);
        expect_decodes_full_struct(desc_tbl);
        desc_tbl.slotDescriptors[0].__set_all_access_paths({root_path});
        expect_decodes_full_struct(desc_tbl);
    }

    DataTypePtr narrowed_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeString>()}, Strings {"first"});
    DataTypePtr reordered_fields = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()},
            Strings {"second", "first"});
    for (const auto& invalid_type : DataTypes {narrowed_type, reordered_fields}) {
        SCOPED_TRACE(invalid_type->get_name());
        desc_tbl.slotDescriptors[0].__set_slotType(invalid_type->to_thrift());
        for (const auto& paths : supported_path_sets) {
            desc_tbl.slotDescriptors[0].__set_all_access_paths(paths);
            desc_tbl.slotDescriptors[0].__set_predicate_access_paths({});
            ASSERT_FALSE(init(desc_tbl).ok());
            desc_tbl.slotDescriptors[0].__set_all_access_paths({});
            desc_tbl.slotDescriptors[0].__set_predicate_access_paths(paths);
            ASSERT_FALSE(init(desc_tbl).ok());
        }
    }
}

TEST(PointQueryReusableTest, ValidateRootPathsByUidOrName) {
    TabletSchema schema;
    schema.copy_from(*ReusableTestHelper::tablet_schema);
    TabletColumn column;
    column.set_name("k1");
    column.set_unique_id(100);
    column.set_type(FieldType::OLAP_FIELD_TYPE_TINYINT);
    schema.append_column(column);

    auto desc_tbl = ReusableTestHelper::create_descriptor_tablet();
    auto& slot = desc_tbl.slotDescriptors[0];
    TColumnAccessPath root_path;
    root_path.type = TAccessPathType::DATA;
    TDataAccessPath data_path;
    data_path.__set_path({"k1"});
    root_path.__set_data_access_path(data_path);
    slot.__set_all_access_paths({root_path});

    auto init = [&](const TDescriptorTable& descriptor) {
        Reusable reusable;
        return reusable.init(descriptor, ReusableTestHelper::output_exprs,
                             ReusableTestHelper::query_options, schema, 2);
    };

    // Unset UIDs use the column name for type validation.
    slot.__set_col_unique_id(-1);
    ASSERT_TRUE(init(desc_tbl).ok());
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    slot.__set_slotType(string_type->to_thrift());
    ASSERT_FALSE(init(desc_tbl).ok());

    // An existing positive UID must also retain the full stored type.
    slot.__set_col_unique_id(100);
    ASSERT_FALSE(init(desc_tbl).ok());

    // A missing positive UID must not fall back to the same-name column's type.
    slot.__set_col_unique_id(101);
    ASSERT_TRUE(init(desc_tbl).ok());

    // A missing legacy name skips type validation as well.
    slot.__set_col_unique_id(-1);
    slot.__set_colName("fe_only");
    data_path.__set_path({"fe_only"});
    root_path.__set_data_access_path(data_path);
    slot.__set_all_access_paths({root_path});
    ASSERT_TRUE(init(desc_tbl).ok());

    // Missing columns must not bypass either access-path check.
    auto nested_path = root_path;
    data_path.__set_path({"fe_only", "leaf"});
    nested_path.__set_data_access_path(data_path);
    slot.__set_all_access_paths({nested_path});
    ASSERT_FALSE(init(desc_tbl).ok());
    slot.__set_all_access_paths({root_path});
    slot.__set_predicate_access_paths({nested_path});
    ASSERT_FALSE(init(desc_tbl).ok());
}

TEST(PointQueryReusableTest, MissingScalarMetadataUsesPayloadOrFeDefault) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const std::string stored_value = "payload";
    struct TestCase {
        std::string default_value;
        bool nullable;
        std::string suffix;
        int expected_default;
    };
    for (const auto& test : std::vector<TestCase> {{"abcd", false, "OFFSET", 4},
                                                   {"中文", false, "OFFSET", 6},
                                                   {"", true, "NULL", 1},
                                                   {"NULL", true, "NULL", 0},
                                                   {"abcd", true, "NULL", 0}}) {
        for (const bool payload_has_uid : {false, true}) {
            TabletColumn source_column;
            source_column.set_name("source");
            source_column.set_unique_id(payload_has_uid ? 100 : 99);
            source_column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
            TabletSchema source_schema;
            source_schema.append_column(source_column);
            auto values = string_type->create_column();
            values->insert_data(stored_value.data(), stored_value.size());
            Block source;
            source.insert({std::move(values), string_type, "source"});
            auto jsonb_column = ColumnString::create();
            JsonbSerializeUtil::block_to_jsonb(source_schema, source, *jsonb_column, 1,
                                               DataTypeSerDeSPtrs {string_type->get_serde()}, {});
            auto descriptor = ReusableTestHelper::create_descriptor_tablet();
            descriptor.slotDescriptors.resize(1);
            auto& slot = descriptor.slotDescriptors[0];
            slot.__set_colName("new_column");
            slot.__set_col_unique_id(100);
            slot.__set_slotType(string_type->to_thrift());
            slot.__set_nullIndicatorBit(test.nullable ? 0 : -1);
            slot.__set_col_default_value(test.default_value);

            const bool null_check = test.suffix == "NULL";
            TExpr expression;
            TExprNode function;
            function.__set_node_type(TExprNodeType::FUNCTION_CALL);
            function.__set_type(null_check ? DataTypeUInt8().to_thrift()
                                           : DataTypeInt32().to_thrift());
            function.__set_num_children(1);
            function.__set_is_nullable(false);
            TFunction fn;
            TFunctionName name;
            name.__set_function_name(null_check ? "is_null_pred" : "length");
            fn.__set_name(name);
            fn.__set_arg_types({slot.slotType});
            fn.__set_ret_type(function.type);
            function.__set_fn(fn);
            expression.nodes.push_back(function);
            TExprNode argument;
            argument.__set_node_type(TExprNodeType::SLOT_REF);
            argument.__set_type(slot.slotType);
            argument.__set_is_nullable(test.nullable);
            argument.__set_num_children(0);
            TSlotRef ref;
            ref.__set_slot_id(slot.id);
            ref.__set_tuple_id(descriptor.tupleDescriptors[0].id);
            argument.__set_slot_ref(ref);
            expression.nodes.push_back(argument);

            for (const int version : {-1, 0, 1}) {
                TColumnAccessPath path;
                const bool typed = version == 1;
                path.__set_type(typed ? TAccessPathType::META : TAccessPathType::DATA);
                if (version >= 0) {
                    path.__set_version(version);
                }
                if (typed) {
                    TMetaAccessPath meta;
                    meta.__set_path({"new_column", test.suffix});
                    path.__set_meta_access_path(meta);
                } else {
                    TDataAccessPath data;
                    data.__set_path({"new_column", test.suffix});
                    path.__set_data_access_path(data);
                }
                for (const int channel : {0, 1, 2}) {
                    SCOPED_TRACE(test.default_value + "/" + test.suffix + "/" +
                                 std::to_string(version) + "/" + std::to_string(channel) + "/" +
                                 std::to_string(payload_has_uid));
                    slot.__set_all_access_paths(channel == 1 ? TColumnAccessPaths {}
                                                             : TColumnAccessPaths {path});
                    slot.__set_predicate_access_paths(channel == 0 ? TColumnAccessPaths {}
                                                                   : TColumnAccessPaths {path});
                    Reusable reusable;
                    auto status = reusable.init(descriptor, {expression},
                                                ReusableTestHelper::query_options,
                                                *ReusableTestHelper::tablet_schema, 1);
                    ASSERT_TRUE(status.ok()) << status.to_string();
                    Block decoded(reusable.tuple_desc()->slots(), 1);
                    status = JsonbSerializeUtil::jsonb_to_block(
                            reusable.get_data_type_serdes(), *jsonb_column,
                            reusable.get_col_uid_to_idx(), decoded,
                            reusable.get_col_default_values(), {});
                    ASSERT_TRUE(status.ok()) << status.to_string();
                    ASSERT_EQ(decoded.rows(), 1);
                    int result_id = -1;
                    status = reusable.output_exprs()[0]->execute(&decoded, &result_id);
                    ASSERT_TRUE(status.ok()) << status.to_string();
                    const int expected =
                            payload_has_uid
                                    ? (null_check ? 0 : static_cast<int>(stored_value.size()))
                                    : test.expected_default;
                    EXPECT_EQ(decoded.get_by_position(result_id).column->get_int(0), expected);
                }
            }
        }
    }
}

TEST(PointQueryReusableTest, MissingScalarMetadataRejectsInvalidDialectOrType) {
    auto descriptor = ReusableTestHelper::create_descriptor_tablet();
    descriptor.slotDescriptors.resize(1);
    auto& slot = descriptor.slotDescriptors[0];
    slot.__set_colName("new_column");
    slot.__set_col_unique_id(100);
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    slot.__set_slotType(string_type->to_thrift());
    slot.__set_nullIndicatorBit(0);
    TColumnAccessPath path;
    path.__set_type(TAccessPathType::META);
    path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    TMetaAccessPath meta;
    meta.__set_path({"new_column", "OFFSET"});
    path.__set_meta_access_path(meta);
    auto rejects = [&](const TDescriptorTable& input, const TColumnAccessPath& invalid) {
        for (bool predicate : {false, true}) {
            auto desc = input;
            desc.slotDescriptors[0].__set_all_access_paths(
                    predicate ? TColumnAccessPaths {} : TColumnAccessPaths {invalid});
            desc.slotDescriptors[0].__set_predicate_access_paths(
                    predicate ? TColumnAccessPaths {invalid} : TColumnAccessPaths {});
            Reusable reusable;
            EXPECT_FALSE(reusable.init(desc, ReusableTestHelper::output_exprs,
                                       ReusableTestHelper::query_options,
                                       *ReusableTestHelper::tablet_schema, 1)
                                 .ok());
        }
    };
    auto legacy_meta = path;
    legacy_meta.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
    rejects(descriptor, legacy_meta);
    auto typed_data = path;
    typed_data.__set_type(TAccessPathType::DATA);
    TDataAccessPath data;
    data.__set_path({"new_column", "OFFSET"});
    typed_data.__set_data_access_path(data);
    rejects(descriptor, typed_data);
    auto nested = path;
    meta.__set_path({"new_column", "child", "OFFSET"});
    nested.__set_meta_access_path(meta);
    rejects(descriptor, nested);
    auto unknown_suffix = path;
    meta.__set_path({"new_column", "offset"});
    unknown_suffix.__set_meta_access_path(meta);
    rejects(descriptor, unknown_suffix);
    slot.slotType.types[0].scalar_type.__set_type(TPrimitiveType::CHAR);
    slot.slotType.types[0].scalar_type.__set_len(10);
    rejects(descriptor, path);
    slot.__set_slotType(DataTypeInt32().to_thrift());
    rejects(descriptor, path);
    auto null_path = path;
    meta.__set_path({"new_column", "NULL"});
    null_path.__set_meta_access_path(meta);
    slot.__set_nullIndicatorBit(-1);
    rejects(descriptor, null_path);
}

TEST(PointQueryReusableTest, RejectMalformedAccessPaths) {
    TabletSchema schema;
    schema.copy_from(*ReusableTestHelper::tablet_schema);
    TabletColumn column;
    column.set_name("k1");
    column.set_unique_id(100);
    column.set_type(FieldType::OLAP_FIELD_TYPE_TINYINT);
    schema.append_column(column);

    auto desc_tbl = ReusableTestHelper::create_descriptor_tablet();
    auto& slot = desc_tbl.slotDescriptors[0];
    slot.__set_col_unique_id(100);
    TColumnAccessPath root_path;
    root_path.__set_type(TAccessPathType::DATA);
    TDataAccessPath data;
    data.__set_path({"k1"});
    root_path.__set_data_access_path(data);

    auto init = [&](const TDescriptorTable& descriptor) {
        Reusable reusable;
        return reusable.init(descriptor, ReusableTestHelper::output_exprs,
                             ReusableTestHelper::query_options, schema, 2);
    };
    slot.__set_all_access_paths({root_path});
    ASSERT_TRUE(init(desc_tbl).ok());

    std::vector<TColumnAccessPath> invalid_paths;
    TColumnAccessPath missing_payload;
    missing_payload.__set_type(TAccessPathType::DATA);
    invalid_paths.push_back(missing_payload);
    missing_payload.__set_type(TAccessPathType::META);
    missing_payload.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    invalid_paths.push_back(missing_payload);

    auto invalid_path = root_path;
    data.__set_path({});
    invalid_path.__set_data_access_path(data);
    invalid_paths.push_back(invalid_path);
    data.__set_path({"other"});
    invalid_path.__set_data_access_path(data);
    invalid_paths.push_back(invalid_path);
    invalid_path = root_path;
    invalid_path.__set_type(static_cast<TAccessPathType::type>(0));
    invalid_paths.push_back(invalid_path);
    invalid_path = root_path;
    invalid_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED + 1);
    invalid_paths.push_back(invalid_path);

    TColumnAccessPath meta_path;
    meta_path.__set_type(TAccessPathType::META);
    TMetaAccessPath meta;
    meta.__set_path({"k1", "NULL"});
    meta_path.__set_meta_access_path(meta);
    invalid_paths.push_back(meta_path); // META is not part of the legacy encoding.
    meta_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    meta.__set_path({});
    meta_path.__set_meta_access_path(meta);
    invalid_paths.push_back(meta_path);
    meta.__set_path({"other", "NULL"});
    meta_path.__set_meta_access_path(meta);
    invalid_paths.push_back(meta_path);

    for (size_t i = 0; i < invalid_paths.size(); ++i) {
        SCOPED_TRACE(i);
        slot.__set_all_access_paths({invalid_paths[i]});
        slot.__set_predicate_access_paths({});
        EXPECT_FALSE(init(desc_tbl).ok());
        slot.__set_all_access_paths({root_path});
        slot.__set_predicate_access_paths({invalid_paths[i]});
        EXPECT_FALSE(init(desc_tbl).ok());
    }
}

// RowCache test class
class RowCacheTest : public testing::Test {
protected:
    void SetUp() override {
        // Create RowCache instance
        _row_cache = new RowCache(1024, 4); // 1KB cache, 4 shards
    }

    void TearDown() override { delete _row_cache; }

    RowCache* _row_cache = nullptr;
};

// Test basic RowCache functionality
TEST_F(RowCacheTest, PQTestBasicOperations) {
    // Create test data
    const int64_t tablet_id = 12345;
    const std::string key_data = "test_key";
    Slice key_slice(key_data.c_str(), key_data.size());

    const std::string value_data = "test_value";
    Slice value_slice(value_data.c_str(), value_data.size());

    RowCache::RowCacheKey cache_key(tablet_id, key_slice);

    // Test insertion
    _row_cache->insert(cache_key, value_slice);

    // Test lookup
    RowCache::CacheHandle handle;
    bool found = _row_cache->lookup(cache_key, &handle);
    ASSERT_TRUE(found) << "Cache entry should be found";
    ASSERT_TRUE(handle.valid()) << "Cache handle should be valid";

    // Verify data correctness
    Slice cached_data = handle.data();
    ASSERT_EQ(cached_data.size, value_slice.size) << "Cache data size should match";
    ASSERT_EQ(memcmp(cached_data.data, value_slice.data, value_slice.size), 0)
            << "Cache data content should match";

    // Test deletion
    _row_cache->erase(cache_key);
    RowCache::CacheHandle handle2;
    found = _row_cache->lookup(cache_key, &handle2);
    ASSERT_FALSE(found) << "Cache entry should be deleted";
    ASSERT_FALSE(handle2.valid()) << "Cache handle should be invalid";
}

// Test RowCache LRU mechanism
TEST_F(RowCacheTest, PQTestLRUEviction) {
    // Use small capacity cache to test LRU eviction
    RowCache small_cache(100, 1); // 100 bytes capacity, 1 shard

    // Create enough entries to trigger LRU eviction
    for (int i = 0; i < 10; i++) {
        const int64_t tablet_id = 12345;
        std::string key_data = "key_" + std::to_string(i);
        Slice key_slice(key_data.c_str(), key_data.size());

        // Create values large enough, about 20 bytes each
        std::string value_data = "value_" + std::to_string(i) + std::string(10, 'x');
        Slice value_slice(value_data.c_str(), value_data.size());

        RowCache::RowCacheKey cache_key(tablet_id, key_slice);
        small_cache.insert(cache_key, value_slice);
    }

    // Check if some earlier entries have been evicted
    bool at_least_one_evicted = false;
    for (int i = 0; i < 5; i++) {
        const int64_t tablet_id = 12345;
        std::string key_data = "key_" + std::to_string(i);
        Slice key_slice(key_data.c_str(), key_data.size());

        RowCache::RowCacheKey cache_key(tablet_id, key_slice);
        RowCache::CacheHandle handle;
        if (!small_cache.lookup(cache_key, &handle)) {
            at_least_one_evicted = true;
            break;
        }
    }

    ASSERT_TRUE(at_least_one_evicted) << "At least one cache entry should be evicted";

    // Ensure the most recently accessed entry is still in cache
    const int64_t tablet_id = 12345;
    std::string key_data = "key_9"; // Last inserted key
    Slice key_slice(key_data.c_str(), key_data.size());

    RowCache::RowCacheKey cache_key(tablet_id, key_slice);
    RowCache::CacheHandle handle;
    bool found = small_cache.lookup(cache_key, &handle);
    //evicted
    ASSERT_TRUE(!found);
}

// LookupConnectionCache test class
class LookupConnectionCacheTest : public testing::Test {
protected:
    void SetUp() override {
        // Create LookupConnectionCache instance
        _lookup_cache = new LookupConnectionCache(1024); // 1KB cache
    }

    void TearDown() override { delete _lookup_cache; }

    LookupConnectionCache* _lookup_cache = nullptr;
};

TEST_F(LookupConnectionCacheTest, PQTestLRUEvictionPolicy) {
    LookupConnectionCache cache(40);

    for (int i = 1; i <= 45; ++i) {
        auto reusable = std::make_shared<Reusable>();
        reusable->_block_pool.resize(10);
        cache.add(i, reusable);
    }
    EXPECT_LT(cache.get_element_count(), 45) << "capacity " << cache.get_capacity();
    auto entry2 = cache.get(10);

    auto reusable5 = std::make_shared<Reusable>();
    reusable5->_block_pool.resize(10);
    cache.add(41, reusable5);
    cache.add(42, reusable5);
    cache.add(43, reusable5);
    EXPECT_LT(cache.get_element_count(), 45) << "capacity " << cache.get_capacity();
}

// Test cache capacity boundary and LRU eviction policy
TEST_F(LookupConnectionCacheTest, PQTestCapacityBoundary) {
    LookupConnectionCache cache(40);
    const int num_entries = 45;

    // Insert 15 entries to exceed capacity
    for (int i = 0; i < num_entries; ++i) {
        auto reusable = ReusableTestHelper::create_reusable();
        cache.add(i, reusable);
    }

    // Verify at least 5 entries are evicted
    int found_count = 0;
    for (int i = 0; i < num_entries; ++i) {
        if (cache.get(i) != nullptr) found_count++;
    }
    EXPECT_LT(found_count, 45) << "LRU eviction should maintain capacity limit";
}

// Test thread safety with concurrent operations
TEST_F(LookupConnectionCacheTest, PQTestConcurrentAccess) {
    const int num_threads = 4;
    const int num_ops_per_thread = 1000;
    LookupConnectionCache cache(1024 * 1024 * 100); // 100MB buffer

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            // Mixed operations: 50% insert, 50% query
            for (int j = 0; j < num_ops_per_thread; ++j) {
                int64_t key = i * 1000 + j;
                auto reusable = ReusableTestHelper::create_reusable();

                if (j % 2 == 0) {
                    cache.add(key, reusable); // Insert
                } else {
                    auto entry = cache.get(key); // Query
                    EXPECT_TRUE(entry == nullptr || entry.use_count() == 2);
                }
            }
        });
    }

    for (auto& t : threads) t.join();
}

// Test exceptional input handling
TEST_F(LookupConnectionCacheTest, PQTestInvalidKeys) {
    LookupConnectionCache cache(1024 * 1024);

    // Null value test
    cache.add(0, nullptr);

    // Oversized entry test (1MB limit)
    auto large_reusable = std::make_shared<Reusable>();
    large_reusable->_block_pool.resize(1024 * 1024); // 1MB block
    cache.add(1, large_reusable);
}

// Test key collision handling
TEST_F(LookupConnectionCacheTest, PQTestDuplicateAdd) {
    LookupConnectionCache cache(1024 * 1024);
    auto reusable1 = ReusableTestHelper::create_reusable();
    auto reusable2 = ReusableTestHelper::create_reusable();

    // Overwrite existing key
    cache.add(123, reusable1);
    cache.add(123, reusable2);

    auto entry = cache.get(123);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.get(), reusable2.get()) << "Last write should win in key collision";
}

// Test reference counting mechanism
TEST_F(LookupConnectionCacheTest, PQTestEntryLifetime) {
    LookupConnectionCache cache(1024 * 1024);
    {
        auto reusable = ReusableTestHelper::create_reusable();
        cache.add(123, reusable);
        auto entry = cache.get(123);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry.use_count(), 3); // Cache + local reference
    }                                    // Local reference released

    // Verify cache maintains ownership
    auto entry = cache.get(123);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.use_count(), 2) << "Cache should maintain sole ownership after scope exit";
}

} // namespace doris
