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

#include <gen_cpp/Descriptors_constants.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/descriptors.pb.h>
#include <gtest/gtest.h>

#include "common/exception.h"
#include "runtime/descriptors.h"

namespace doris {

class SlotDescriptorTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    // Helper method to create a basic TSlotDescriptor
    TSlotDescriptor create_basic_slot_descriptor(int slot_id = 1,
                                                 const std::string& col_name = "test_col") {
        TSlotDescriptor tdesc;
        tdesc.__set_id(slot_id);
        tdesc.__set_parent(1);
        tdesc.__set_slotType(create_string_type());
        tdesc.__set_columnPos(0);
        tdesc.__set_colName(col_name);
        tdesc.__set_col_unique_id(slot_id);
        tdesc.__set_slotIdx(0);
        tdesc.__set_isMaterialized(true);
        tdesc.__set_is_key(false);
        tdesc.__set_nullIndicatorBit(0);
        return tdesc;
    }

    // Helper method to create a TTypeDesc for string type
    TTypeDesc create_string_type() {
        TTypeDesc type_desc;
        TTypeNode type_node;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::STRING);
        type_node.__set_type(TTypeNodeType::SCALAR);
        type_node.__set_scalar_type(scalar_type);
        type_desc.types.push_back(type_node);
        return type_desc;
    }

    // Helper method to create a TExpr with nodes
    TExpr create_virtual_column_expr(TExprNodeType::type node_type = TExprNodeType::FUNCTION_CALL) {
        TExpr expr;
        TExprNode node;
        node.__set_node_type(node_type);
        node.__set_type(create_string_type());
        expr.nodes.push_back(node);
        return expr;
    }
};

TEST_F(SlotDescriptorTest, BasicConstructor) {
    // Test basic constructor without virtual column expression
    TSlotDescriptor tdesc = create_basic_slot_descriptor();

    EXPECT_NO_THROW({
        SlotDescriptor slot_desc(tdesc);
        EXPECT_EQ(slot_desc.id(), 1);
        EXPECT_EQ(slot_desc.col_name(), "test_col");
        EXPECT_EQ(slot_desc.col_unique_id(), 1);
        EXPECT_FALSE(slot_desc.is_key());
        EXPECT_EQ(slot_desc.get_virtual_column_expr(), nullptr);
    });
}

TEST_F(SlotDescriptorTest, VirtualColumnExprValid) {
    // Test constructor with valid virtual column expression
    TSlotDescriptor tdesc = create_basic_slot_descriptor();
    TExpr virtual_expr = create_virtual_column_expr(TExprNodeType::FUNCTION_CALL);
    tdesc.__set_virtual_column_expr(virtual_expr);

    EXPECT_NO_THROW({
        SlotDescriptor slot_desc(tdesc);
        EXPECT_EQ(slot_desc.id(), 1);
        EXPECT_EQ(slot_desc.col_name(), "test_col");
        EXPECT_NE(slot_desc.get_virtual_column_expr(), nullptr);
    });
}

TEST_F(SlotDescriptorTest, VirtualColumnExprEmptyNodes) {
    // Test constructor with empty virtual column expression nodes - should throw exception
    TSlotDescriptor tdesc = create_basic_slot_descriptor(1, "virtual_col");
    TExpr virtual_expr;
    // Empty nodes list
    virtual_expr.nodes.clear();
    tdesc.__set_virtual_column_expr(virtual_expr);

    EXPECT_THROW({ SlotDescriptor slot_desc(tdesc); }, doris::Exception);

    // Test the specific exception message
    try {
        SlotDescriptor slot_desc(tdesc);
        FAIL() << "Expected doris::Exception";
    } catch (const doris::Exception& e) {
        std::string error_msg = e.what();
        EXPECT_TRUE(error_msg.find("Virtual column expr node is empty") != std::string::npos);
        EXPECT_TRUE(error_msg.find("virtual_col") != std::string::npos);
        EXPECT_TRUE(error_msg.find("col_unique_id: 1") != std::string::npos);
    }
}

TEST_F(SlotDescriptorTest, VirtualColumnExprSlotRefNode) {
    // Test constructor with SLOT_REF node type - should throw exception
    TSlotDescriptor tdesc = create_basic_slot_descriptor(2, "slot_ref_col");
    TExpr virtual_expr = create_virtual_column_expr(TExprNodeType::SLOT_REF);
    tdesc.__set_virtual_column_expr(virtual_expr);

    EXPECT_THROW({ SlotDescriptor slot_desc(tdesc); }, doris::Exception);

    // Test the specific exception message
    try {
        SlotDescriptor slot_desc(tdesc);
        FAIL() << "Expected doris::Exception";
    } catch (const doris::Exception& e) {
        std::string error_msg = e.what();
        EXPECT_TRUE(error_msg.find("Virtual column expr node is slot ref") != std::string::npos);
        EXPECT_TRUE(error_msg.find("slot_ref_col") != std::string::npos);
        EXPECT_TRUE(error_msg.find("col_unique_id: 2") != std::string::npos);
    }
}

TEST_F(SlotDescriptorTest, VirtualColumnExprVirtualSlotRefNode) {
    // Test constructor with VIRTUAL_SLOT_REF node type - should be valid
    TSlotDescriptor tdesc = create_basic_slot_descriptor();
    TExpr virtual_expr = create_virtual_column_expr(TExprNodeType::VIRTUAL_SLOT_REF);
    tdesc.__set_virtual_column_expr(virtual_expr);

    EXPECT_NO_THROW({
        SlotDescriptor slot_desc(tdesc);
        EXPECT_EQ(slot_desc.id(), 1);
        EXPECT_EQ(slot_desc.col_name(), "test_col");
        EXPECT_NE(slot_desc.get_virtual_column_expr(), nullptr);
    });
}

TEST_F(SlotDescriptorTest, OptionalFields) {
    // Test constructor with optional fields set
    TSlotDescriptor tdesc = create_basic_slot_descriptor();
    tdesc.__set_is_auto_increment(true);
    tdesc.__set_col_default_value("default_value");

    EXPECT_NO_THROW({
        SlotDescriptor slot_desc(tdesc);
        EXPECT_EQ(slot_desc.id(), 1);
        EXPECT_EQ(slot_desc.col_name(), "test_col");
    });
}

TEST_F(SlotDescriptorTest, OptionalFieldsNotSet) {
    // Test constructor with optional fields not set
    TSlotDescriptor tdesc = create_basic_slot_descriptor();
    // Don't set is_auto_increment and col_default_value

    EXPECT_NO_THROW({
        SlotDescriptor slot_desc(tdesc);
        EXPECT_EQ(slot_desc.id(), 1);
        EXPECT_EQ(slot_desc.col_name(), "test_col");
    });
}

TEST_F(SlotDescriptorTest, DebugString) {
    // Test debug string output for virtual and non-virtual columns
    TSlotDescriptor tdesc1 = create_basic_slot_descriptor(1, "normal_col");
    SlotDescriptor slot_desc1(tdesc1);
    std::string debug_str1 = slot_desc1.debug_string();
    EXPECT_TRUE(debug_str1.find("normal_col") != std::string::npos);
    EXPECT_TRUE(debug_str1.find("is_virtual=false") != std::string::npos);

    TSlotDescriptor tdesc2 = create_basic_slot_descriptor(2, "virtual_col");
    TExpr virtual_expr = create_virtual_column_expr(TExprNodeType::FUNCTION_CALL);
    tdesc2.__set_virtual_column_expr(virtual_expr);
    SlotDescriptor slot_desc2(tdesc2);
    std::string debug_str2 = slot_desc2.debug_string();
    EXPECT_TRUE(debug_str2.find("virtual_col") != std::string::npos);
    EXPECT_TRUE(debug_str2.find("is_virtual=true") != std::string::npos);
}

TEST_F(SlotDescriptorTest, AccessPathsPreservedThroughProtobuf) {
    TColumnAccessPath data_path;
    data_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    data_path.__set_type(TAccessPathType::DATA);
    TDataAccessPath data_payload;
    data_payload.__set_path({"s", "field"});
    data_path.__set_data_access_path(data_payload);

    TColumnAccessPath meta_path;
    meta_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    meta_path.__set_type(TAccessPathType::META);
    TMetaAccessPath meta_payload;
    meta_payload.__set_path({"s", "field", "NULL"});
    meta_path.__set_meta_access_path(meta_payload);

    TColumnAccessPath legacy_path;
    legacy_path.__set_type(TAccessPathType::DATA);
    data_payload.__set_path({"s", "legacy", "NULL"});
    legacy_path.__set_data_access_path(data_payload);

    TColumnAccessPath explicit_legacy_path;
    explicit_legacy_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
    explicit_legacy_path.__set_type(TAccessPathType::DATA);
    data_payload.__set_path({"s", "legacy", "VALUES"});
    explicit_legacy_path.__set_data_access_path(data_payload);

    TSlotDescriptor thrift_descriptor = create_basic_slot_descriptor();
    thrift_descriptor.__set_all_access_paths(
            {data_path, meta_path, legacy_path, explicit_legacy_path});
    thrift_descriptor.__set_predicate_access_paths({meta_path});

    SlotDescriptor original(thrift_descriptor);
    PSlotDescriptor protobuf_descriptor;
    original.to_protobuf(&protobuf_descriptor);
    ASSERT_EQ(protobuf_descriptor.all_access_paths_size(), 4);
    EXPECT_TRUE(protobuf_descriptor.all_access_paths(0).has_version());
    EXPECT_EQ(protobuf_descriptor.all_access_paths(0).version(),
              g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    EXPECT_TRUE(protobuf_descriptor.all_access_paths(1).has_version());
    EXPECT_FALSE(protobuf_descriptor.all_access_paths(2).has_version());
    EXPECT_TRUE(protobuf_descriptor.all_access_paths(3).has_version());
    EXPECT_EQ(protobuf_descriptor.all_access_paths(3).version(),
              g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
    SlotDescriptor round_trip(protobuf_descriptor);

    EXPECT_EQ(round_trip.all_access_paths(), thrift_descriptor.all_access_paths);
    EXPECT_EQ(round_trip.predicate_access_paths(), thrift_descriptor.predicate_access_paths);
    EXPECT_FALSE(round_trip.all_access_paths()[1].__isset.data_access_path);
    EXPECT_FALSE(round_trip.predicate_access_paths()[0].__isset.data_access_path);
    EXPECT_FALSE(round_trip.all_access_paths()[2].__isset.version);
    EXPECT_TRUE(round_trip.all_access_paths()[3].__isset.version);
    EXPECT_EQ(round_trip.all_access_paths()[3].version,
              g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
}

} // namespace doris
