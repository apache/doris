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

#include "vec/exprs/virtual_slot_ref.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/object_pool.h"
#include "runtime/descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

class VirtualSlotRefTest : public testing::Test {
public:
    void SetUp() override {
        _pool = std::make_unique<ObjectPool>();
        _state = std::make_unique<MockRuntimeState>();
    }

    void TearDown() override {
        _pool.reset();
        _state.reset();
    }

protected:
    // Helper method to create a TExprNode for VirtualSlotRef
    TExprNode create_virtual_slot_ref_node(int slot_id, const std::string& label = "") {
        TExprNode node;
        node.__set_node_type(TExprNodeType::VIRTUAL_SLOT_REF);

        // Set up slot reference
        TSlotRef slot_ref;
        slot_ref.__set_slot_id(slot_id);
        slot_ref.__set_is_virtual_slot(true);
        node.__set_slot_ref(slot_ref);

        // Set up type
        TTypeDesc type_desc;
        TTypeNode type_node;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::STRING);
        type_node.__set_type(TTypeNodeType::SCALAR);
        type_node.__set_scalar_type(scalar_type);
        type_desc.types.push_back(type_node);
        node.__set_type(type_desc);

        if (!label.empty()) {
            node.__set_label(label);
        }

        return node;
    }

    // Helper method to create a SlotDescriptor
    SlotDescriptor* create_slot_descriptor(
            int slot_id, const std::string& col_name,
            DataTypePtr data_type = std::make_shared<DataTypeString>()) {
        auto* slot_desc = _pool->add(new SlotDescriptor());
        slot_desc->_id = SlotId(slot_id);
        slot_desc->_col_name = col_name;
        slot_desc->_type = data_type;
        // Note: _is_materialized is const, so it's set during construction
        return slot_desc;
    }

    std::unique_ptr<ObjectPool> _pool;
    std::unique_ptr<MockRuntimeState> _state;
};

TEST_F(VirtualSlotRefTest, ConstructorWithTExprNode) {
    // Test constructor with TExprNode
    TExprNode node = create_virtual_slot_ref_node(1, "test_label");
    VirtualSlotRef virtual_slot_ref(node);

    EXPECT_EQ(virtual_slot_ref.slot_id(), 1);
    EXPECT_EQ(virtual_slot_ref.column_id(), -1); // Should be -1 initially
    EXPECT_EQ(virtual_slot_ref.expr_label(), "test_label");
}

TEST_F(VirtualSlotRefTest, ConstructorWithSlotDescriptor) {
    // Test constructor with SlotDescriptor
    auto* slot_desc = create_slot_descriptor(2, "col_name");
    VirtualSlotRef virtual_slot_ref(slot_desc);

    EXPECT_EQ(virtual_slot_ref.slot_id(), 2);
    EXPECT_EQ(virtual_slot_ref.column_id(), -1); // Should be -1 initially
}

TEST_F(VirtualSlotRefTest, EqualsFunction_SameObjects) {
    // Test equals with same objects
    TExprNode node = create_virtual_slot_ref_node(1, "test_label");
    VirtualSlotRef ref1(node);
    VirtualSlotRef ref2(node);

    EXPECT_TRUE(ref1.equals(ref2));
    EXPECT_TRUE(ref2.equals(ref1));
}

TEST_F(VirtualSlotRefTest, EqualsFunction_DifferentSlotIds) {
    // Test equals with different slot IDs
    TExprNode node1 = create_virtual_slot_ref_node(1, "test_label");
    TExprNode node2 = create_virtual_slot_ref_node(2, "test_label");

    VirtualSlotRef ref1(node1);
    VirtualSlotRef ref2(node2);

    EXPECT_FALSE(ref1.equals(ref2));
    EXPECT_FALSE(ref2.equals(ref1));
}

TEST_F(VirtualSlotRefTest, EqualsFunction_DifferentLabels) {
    // Test equals with different labels
    TExprNode node1 = create_virtual_slot_ref_node(1, "label1");
    TExprNode node2 = create_virtual_slot_ref_node(1, "label2");

    VirtualSlotRef ref1(node1);
    VirtualSlotRef ref2(node2);

    EXPECT_FALSE(ref1.equals(ref2));
    EXPECT_FALSE(ref2.equals(ref1));
}

TEST_F(VirtualSlotRefTest, EqualsFunction_SameSlotDifferentColumnId) {
    // Test equals with same slot but different column IDs
    TExprNode node = create_virtual_slot_ref_node(1, "test_label");
    VirtualSlotRef ref1(node);
    VirtualSlotRef ref2(node);

    // Initially they should be equal
    EXPECT_TRUE(ref1.equals(ref2));

    // Set different column IDs to test this specific comparison
    ref1._column_id = 0;
    ref2._column_id = 1;

    EXPECT_FALSE(ref1.equals(ref2));
    EXPECT_FALSE(ref2.equals(ref1));
}

TEST_F(VirtualSlotRefTest, EqualsFunction_WithDifferentTypes) {
    // Test equals with different expression types
    TExprNode node = create_virtual_slot_ref_node(1, "test_label");
    VirtualSlotRef virtual_ref(node);

    // Create a different type of expression (mock)
    class MockVExpr : public VExpr {
    public:
        MockVExpr() : VExpr(std::make_shared<DataTypeString>(), false) {}
        Status execute(VExprContext* context, Block* block, int* result_column_id) override {
            return Status::OK();
        }
        const std::string& expr_name() const override {
            static std::string name = "mock";
            return name;
        }
        std::string debug_string() const override { return "MockVExpr"; }
        bool equals(const VExpr& other) override { return false; }
    };

    MockVExpr mock_expr;
    EXPECT_FALSE(virtual_ref.equals(mock_expr));
    EXPECT_FALSE(mock_expr.equals(virtual_ref));
}

TEST_F(VirtualSlotRefTest, EqualsFunction_SameColumnName) {
    // Test equals with same column names
    auto* slot_desc1 = create_slot_descriptor(1, "same_name");
    auto* slot_desc2 = create_slot_descriptor(1, "same_name");

    VirtualSlotRef ref1(slot_desc1);
    VirtualSlotRef ref2(slot_desc2);

    // Set the column names to simulate prepared state
    ref1._column_name = &slot_desc1->col_name();
    ref2._column_name = &slot_desc2->col_name();

    // Since both point to strings with the same content, they should be equal
    EXPECT_TRUE(ref1.equals(ref2));
}

TEST_F(VirtualSlotRefTest, EqualsFunction_DifferentColumnNames) {
    // Test equals with different column names
    auto* slot_desc1 = create_slot_descriptor(1, "name1");
    auto* slot_desc2 = create_slot_descriptor(1, "name2");

    VirtualSlotRef ref1(slot_desc1);
    VirtualSlotRef ref2(slot_desc2);

    // Set the column names to simulate prepared state
    ref1._column_name = &slot_desc1->col_name();
    ref2._column_name = &slot_desc2->col_name();

    // Even with same slot_id, different column names should make them not equal
    EXPECT_FALSE(ref1.equals(ref2));
}

TEST_F(VirtualSlotRefTest, EqualsFunction_NullColumnNames) {
    // Test equals with null column names
    TExprNode node1 = create_virtual_slot_ref_node(1, "test_label");
    TExprNode node2 = create_virtual_slot_ref_node(1, "test_label");

    VirtualSlotRef ref1(node1);
    VirtualSlotRef ref2(node2);

    // Both have null column names (default state)
    EXPECT_TRUE(ref1.equals(ref2));
}

TEST_F(VirtualSlotRefTest, EqualsFunction_OneNullColumnName) {
    // Test equals when one has null column name and other doesn't
    auto* slot_desc = create_slot_descriptor(1, "col_name");
    TExprNode node = create_virtual_slot_ref_node(1, "test_label");

    VirtualSlotRef ref1(slot_desc);
    VirtualSlotRef ref2(node);

    // Set column name for ref1 only
    ref1._column_name = &slot_desc->col_name();
    // ref2._column_name remains nullptr (default)

    EXPECT_FALSE(ref1.equals(ref2));
    EXPECT_FALSE(ref2.equals(ref1));
}

TEST_F(VirtualSlotRefTest, EqualsFunction_ComprehensiveTest) {
    // Comprehensive test with all attributes
    TExprNode node1 = create_virtual_slot_ref_node(5, "comprehensive_label");
    TExprNode node2 = create_virtual_slot_ref_node(5, "comprehensive_label");

    VirtualSlotRef ref1(node1);
    VirtualSlotRef ref2(node2);

    // Set same attributes
    auto* slot_desc = create_slot_descriptor(5, "comprehensive_col");
    ref1._column_name = &slot_desc->col_name();
    ref2._column_name = &slot_desc->col_name();
    ref1._column_id = 3;
    ref2._column_id = 3;

    EXPECT_TRUE(ref1.equals(ref2));

    // Change one attribute at a time and verify equality fails
    ref2._column_id = 4;
    EXPECT_FALSE(ref1.equals(ref2));

    ref2._column_id = 3; // Restore
    auto* different_slot_desc = create_slot_descriptor(5, "different_col");
    ref2._column_name = &different_slot_desc->col_name();
    EXPECT_FALSE(ref1.equals(ref2));
}

TEST_F(VirtualSlotRefTest, EqualsFunction_TestAllBranches) {
    // Test all branches in the equals function specifically
    TExprNode node = create_virtual_slot_ref_node(1, "test_label");
    VirtualSlotRef ref1(node);
    VirtualSlotRef ref2(node);

    // 1. Test base VExpr::equals() call - different node types should fail
    class DifferentVExpr : public VExpr {
    public:
        DifferentVExpr() : VExpr(std::make_shared<DataTypeString>(), false) {
            _node_type = TExprNodeType::SLOT_REF; // Different from VIRTUAL_SLOT_REF
        }
        Status execute(VExprContext* context, Block* block, int* result_column_id) override {
            return Status::OK();
        }
        const std::string& expr_name() const override {
            static std::string name = "different";
            return name;
        }
        std::string debug_string() const override { return "DifferentVExpr"; }
        bool equals(const VExpr& other) override { return VExpr::equals(other); }
    };

    DifferentVExpr different_expr;
    EXPECT_FALSE(ref1.equals(different_expr));

    // 2. Test dynamic_cast failure with non-VirtualSlotRef
    class NonVirtualSlotRefExpr : public VExpr {
    public:
        NonVirtualSlotRefExpr() : VExpr(std::make_shared<DataTypeString>(), false) {
            _node_type = TExprNodeType::VIRTUAL_SLOT_REF; // Same type but different class
        }
        Status execute(VExprContext* context, Block* block, int* result_column_id) override {
            return Status::OK();
        }
        const std::string& expr_name() const override {
            static std::string name = "non_virtual_slot_ref";
            return name;
        }
        std::string debug_string() const override { return "NonVirtualSlotRefExpr"; }
        bool equals(const VExpr& other) override { return VExpr::equals(other); }
    };

    NonVirtualSlotRefExpr non_virtual_expr;
    EXPECT_FALSE(ref1.equals(non_virtual_expr));

    // 3. Test successful case with all attributes matching
    EXPECT_TRUE(ref1.equals(ref2));

    // 4. Test each attribute difference
    // Different slot_id
    ref2._slot_id = 2;
    EXPECT_FALSE(ref1.equals(ref2));
    ref2._slot_id = 1; // Restore

    // Different column_id
    ref1._column_id = 0;
    ref2._column_id = 1;
    EXPECT_FALSE(ref1.equals(ref2));
    ref1._column_id = ref2._column_id = -1; // Restore to default

    // Different column_name pointers
    std::string name1 = "name1";
    std::string name2 = "name2";
    ref1._column_name = &name1;
    ref2._column_name = &name2;
    EXPECT_FALSE(ref1.equals(ref2));
    ref1._column_name = ref2._column_name = nullptr; // Restore

    // Different column_label
    TExprNode node_different_label = create_virtual_slot_ref_node(1, "different_label");
    VirtualSlotRef ref3(node_different_label);
    EXPECT_FALSE(ref1.equals(ref3));
}

TEST_F(VirtualSlotRefTest, BasicProperties) {
    // Test basic properties and methods
    TExprNode node = create_virtual_slot_ref_node(10, "property_test");
    VirtualSlotRef virtual_ref(node);

    EXPECT_EQ(virtual_ref.slot_id(), 10);
    EXPECT_EQ(virtual_ref.column_id(), -1);
    EXPECT_EQ(virtual_ref.expr_label(), "property_test");
    EXPECT_FALSE(virtual_ref.is_constant());

    // Test debug string
    std::string debug_str = virtual_ref.debug_string();
    EXPECT_TRUE(debug_str.find("VirtualSlotRef") != std::string::npos);
    EXPECT_TRUE(debug_str.find("slot_id=10") != std::string::npos);
}

TEST_F(VirtualSlotRefTest, MemoryEstimate) {
    // Test memory estimation
    TExprNode node = create_virtual_slot_ref_node(1);
    VirtualSlotRef virtual_ref(node);

    EXPECT_EQ(virtual_ref.estimate_memory(1000), 0); // Should return 0 as per implementation
}

} // namespace doris::vectorized