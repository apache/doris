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

#include "vec/exprs/vann_topn_predicate.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>

#include <iostream>
#include <memory>

#include "common/object_pool.h"
#include "io/io_common.h"
#include "olap/rowset/segment_v2/ann_index_iterator.h"
#include "olap/rowset/segment_v2/index_iterator.h"
#include "runtime/descriptors.h"
#include "vec/columns/columns_number.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/virtual_slot_ref.h"

using ::testing::_;
using ::testing::SetArgPointee;
using ::testing::Return;

static std::string order_by_expr_thrift =
        R"xxx({"1":{"lst":["rec",12,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"l2_distance"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}},{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"l2_distance(array<double>, array<double>)"},"9":{"rec":{"1":{"str":""}}},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"29":{"tf":1}},{"1":{"i32":5},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"3":{"i32":4},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"casttoarray"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":7}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"casttoarray(array<float>)"},"9":{"rec":{"1":{"str":""}}},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"29":{"tf":0}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":7}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":1},"2":{"i32":0},"3":{"i32":1}}},"20":{"i32":-1},"29":{"tf":0},"36":{"str":"embedding"}},{"1":{"i32":21},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":8},"20":{"i32":-1},"28":{"i32":8},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":1}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":2}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":3}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":4}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":5}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":6}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":7}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":20}}},"20":{"i32":-1},"29":{"tf":0}}]}})xxx";

namespace doris::vectorized {
class MockAnnIndexIterator : public doris::segment_v2::AnnIndexIterator {
public:
    MockAnnIndexIterator()
            : doris::segment_v2::AnnIndexIterator(_io_ctx_mock, nullptr, nullptr, nullptr) {}

    ~MockAnnIndexIterator() override = default;

    IndexType type() override { return IndexType::ANN; }

    MOCK_METHOD(Status, read_from_index, (const doris::segment_v2::IndexParam& param), (override));

private:
    io::IOContext _io_ctx_mock;
};

template <typename T>
T read_from_json() {
    std::string json_str = order_by_expr_thrift;
    auto memBufferIn = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
            reinterpret_cast<uint8_t*>(const_cast<char*>(json_str.data())),
            static_cast<uint32_t>(json_str.size()));
    auto jsonProtocolIn = std::make_shared<apache::thrift::protocol::TJSONProtocol>(memBufferIn);
    T params;
    params.read(jsonProtocolIn.get());
    return params;
}

template <typename T>
void write_to_json(const std::string& path, std::string name, const T& expr) {
    auto memBuffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    auto jsonProtocol = std::make_shared<apache::thrift::protocol::TJSONProtocol>(memBuffer);

    expr.write(jsonProtocol.get());
    uint8_t* buf;
    uint32_t size;
    memBuffer->getBuffer(&buf, &size);
    std::string file_path = fmt::format("{}/{}.json", path, name);
    std::ofstream ofs(file_path, std::ios::binary);
    ofs.write(reinterpret_cast<const char*>(buf), size);
    ofs.close();
    std::cout << fmt::format("Serialized JSON written to {}\n", file_path);
}

class AnnTopNDescriptorTest : public testing::Test {
protected:
    void SetUp() override {
        TExpr _distance_function_call_thrift = read_from_json<TExpr>();
        // std::cout << "distance function call thrift:\n"
        //           << apache::thrift::ThriftDebugString(_distance_function_call_thrift)
        //           << std::endl;

        TDescriptorTable thrift_tbl;
        TTableDescriptor thrift_table_desc;
        thrift_table_desc.id = 0;
        thrift_tbl.tableDescriptors.push_back(thrift_table_desc);
        TTupleDescriptor tuple_desc;
        tuple_desc.__isset.tableId = true;
        tuple_desc.id = 0;
        tuple_desc.tableId = 0;
        thrift_tbl.tupleDescriptors.push_back(tuple_desc);
        TSlotDescriptor slot_desc;
        slot_desc.id = 0;
        slot_desc.parent = 0;
        slot_desc.isMaterialized = true;
        slot_desc.need_materialize = true;
        slot_desc.__isset.need_materialize = true;
        TTypeNode type_node;
        type_node.type = TTypeNodeType::type::SCALAR;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::DOUBLE);
        type_node.__set_scalar_type(scalar_type);
        slot_desc.slotType.types.push_back(type_node);
        slot_desc.virtual_column_expr = _distance_function_call_thrift;
        slot_desc.__isset.virtual_column_expr = true;
        thrift_tbl.slotDescriptors.push_back(slot_desc);
        slot_desc.id = 1;
        slot_desc.__isset.virtual_column_expr = false;
        thrift_tbl.slotDescriptors.push_back(slot_desc);
        thrift_tbl.__isset.slotDescriptors = true;
        // std::cout << "+++++++++++++++++++ thrift table descriptor:\n"
        //           << apache::thrift::ThriftDebugString(thrift_tbl) << std::endl;
        // std::cout << "+++++++++++++++++++ thrift table descriptor end\n";
        ASSERT_TRUE(DescriptorTbl::create(&obj_pool, thrift_tbl, &_desc_tbl).ok());
        // std::cout << "+++++++++++++++++++ desc tbl\n" << _desc_tbl->debug_string() << std::endl;
        // std::cout << "+++++++++++++++++++ desc tbl end\n";
        _runtime_state.set_desc_tbl(_desc_tbl);

        config::max_depth_of_expr_tree = 1000;
        doris::TSlotRef slot_ref;
        slot_ref.slot_id = 0;
        slot_ref.__isset.is_virtual_slot = true;
        slot_ref.is_virtual_slot = true;

        doris::TExprNode virtual_slot_ref_node;
        virtual_slot_ref_node.slot_ref = slot_ref;
        virtual_slot_ref_node.label = "virtual_slot_ref";
        virtual_slot_ref_node.node_type = TExprNodeType::SLOT_REF;
        virtual_slot_ref_node.type = TTypeDesc();
        type_node.type = TTypeNodeType::type::SCALAR;
        type_node.scalar_type.type = TPrimitiveType::DOUBLE;
        type_node.__isset.scalar_type = true;

        virtual_slot_ref_node.type.types.push_back(type_node);
        virtual_slot_ref_node.__isset.slot_ref = true;
        virtual_slot_ref_node.__isset.label = true;
        virtual_slot_ref_node.__isset.opcode = false;

        _virtual_slot_ref_expr.nodes.push_back(virtual_slot_ref_node);
        _ann_index_iterator = std::make_unique<MockAnnIndexIterator>();

        _row_desc = RowDescriptor(*_desc_tbl, {0}, {false});
        // std::cout << "++++++++++++++++++row desc:\n" << _row_desc.debug_string() << std::endl;
        // std::cout << "++++++++++++++++++row desc end:\n";
    }

    void TearDown() override {}

private:
    doris::ObjectPool obj_pool;
    RowDescriptor _row_desc;
    std::unique_ptr<MockAnnIndexIterator> _ann_index_iterator;
    vectorized::IColumn::MutablePtr _result_column;
    doris::TExpr _distance_function_call_thrift;
    doris::TExpr _virtual_slot_ref_expr;
    DescriptorTbl* _desc_tbl;
    doris::RuntimeState _runtime_state;
};

TEST_F(AnnTopNDescriptorTest, TestConstructor) {
    int limit = 10;
    std::shared_ptr<VExprContext> distanc_calcu_fn_call_ctx;
    ASSERT_TRUE(vectorized::VExpr::create_expr_tree(_distance_function_call_thrift,
                                                    distanc_calcu_fn_call_ctx)
                        .ok());

    std::shared_ptr<VExprContext> virtual_slot_expr_ctx;
    ASSERT_TRUE(vectorized::VExpr::create_expr_tree(_virtual_slot_ref_expr, virtual_slot_expr_ctx)
                        .ok());
    std::shared_ptr<VirtualSlotRef> v =
            std::dynamic_pointer_cast<VirtualSlotRef>(virtual_slot_expr_ctx->root());
    if (v == nullptr) {
        LOG(FATAL) << "VAnnTopNDescriptor::SetUp() failed";
    }

    v->set_virtual_column_expr(distanc_calcu_fn_call_ctx);

    std::shared_ptr<AnnTopNDescriptor> predicate;
    predicate = AnnTopNDescriptor::create_shared(limit, virtual_slot_expr_ctx);

    std::cout << predicate->debug_string();
}

TEST_F(AnnTopNDescriptorTest, TestPrepare) {
    int limit = 10;
    std::shared_ptr<VExprContext> distanc_calcu_fn_call_ctx;
    Status st = vectorized::VExpr::create_expr_tree(_distance_function_call_thrift,
                                                    distanc_calcu_fn_call_ctx);

    std::shared_ptr<VExprContext> virtual_slot_expr_ctx;
    st = vectorized::VExpr::create_expr_tree(_virtual_slot_ref_expr, virtual_slot_expr_ctx);
    std::shared_ptr<VirtualSlotRef> v =
            std::dynamic_pointer_cast<VirtualSlotRef>(virtual_slot_expr_ctx->root());
    if (v == nullptr) {
        LOG(FATAL) << "VAnnTopNDescriptor::SetUp() failed";
    }

    v->set_virtual_column_expr(distanc_calcu_fn_call_ctx);
    std::shared_ptr<AnnTopNDescriptor> predicate;
    predicate = AnnTopNDescriptor::create_shared(limit, virtual_slot_expr_ctx);
    st = predicate->prepare(&_runtime_state, _row_desc);
    ASSERT_TRUE(st.ok()) << fmt::format("st: {}, expr {}", st.to_string(),
                                        predicate->get_order_by_expr_ctx()->root()->debug_string());
}

TEST_F(AnnTopNDescriptorTest, TestEvaluateVectorAnnSearch) {
    int limit = 10;
    std::shared_ptr<VExprContext> distanc_calcu_fn_call_ctx;
    Status st = vectorized::VExpr::create_expr_tree(_distance_function_call_thrift,
                                                    distanc_calcu_fn_call_ctx);

    std::shared_ptr<VExprContext> virtual_slot_expr_ctx;
    st = vectorized::VExpr::create_expr_tree(_virtual_slot_ref_expr, virtual_slot_expr_ctx);
    std::shared_ptr<VirtualSlotRef> v =
            std::dynamic_pointer_cast<VirtualSlotRef>(virtual_slot_expr_ctx->root());
    if (v == nullptr) {
        LOG(FATAL) << "VAnnTopNDescriptor::SetUp() failed";
    }

    v->set_virtual_column_expr(distanc_calcu_fn_call_ctx);
    std::shared_ptr<AnnTopNDescriptor> predicate;
    predicate = AnnTopNDescriptor::create_shared(limit, virtual_slot_expr_ctx);
    st = predicate->prepare(&_runtime_state, _row_desc);
    ASSERT_TRUE(st.ok()) << fmt::format("st: {}, expr {}", st.to_string(),
                                        predicate->get_order_by_expr_ctx()->root()->debug_string());

    std::shared_ptr<std::vector<float>> query_value = std::make_shared<std::vector<float>>(10, 0.0);
    for (size_t i = 0; i < 10; ++i) {
        (*query_value)[i] = static_cast<float>(i);
    }
    roaring::Roaring row_bitmap;

    EXPECT_CALL(*_ann_index_iterator, read_from_index(testing::_))
            .Times(1)
            .WillOnce(testing::Invoke([](const segment_v2::IndexParam& value) {
                auto* ann_param = std::get<segment_v2::AnnIndexParam*>(value);
                ann_param->distance->clear();
                ann_param->distance->push_back(0);
                ann_param->distance->push_back(1);
                ann_param->distance->push_back(2);
                ann_param->distance->push_back(3);
                ann_param->distance->push_back(4);
                ann_param->distance->push_back(5);
                ann_param->distance->push_back(6);
                ann_param->distance->push_back(7);
                ann_param->distance->push_back(8);
                ann_param->distance->push_back(9);
                return Status::OK();
            }));

    _result_column = ColumnFloat64::create(0, 0);
    st = predicate->evaluate_vector_ann_search(_ann_index_iterator.get(), row_bitmap,
                                               _result_column);
    ColumnFloat64* result_column_float = assert_cast<ColumnFloat64*>(_result_column.get());
    for (size_t i = 0; i < query_value->size(); ++i) {
        EXPECT_EQ(result_column_float->get_data()[i], (*query_value)[i]);
    }
    ASSERT_TRUE(st.ok());
}

} // namespace doris::vectorized