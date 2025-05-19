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

#pragma once

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>

#include <iostream>
#include <memory>

#include "common/object_pool.h"
#include "olap/rowset/segment_v2/ann_index_iterator.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "vec/exprs/vexpr_context.h"
// Add CLucene RAM Directory header
#include <CLucene/store/RAMDirectory.h>

using doris::segment_v2::DorisCompoundReader;

namespace doris::vec_search_mock {

class MockIndexFileReader : public ::doris::segment_v2::IndexFileReader {
public:
    MockIndexFileReader()
            : IndexFileReader(doris::io::global_local_filesystem(), "",
                              doris::InvertedIndexStorageFormatPB::V2) {}

    MOCK_METHOD2(init, doris::Status(int, const doris::io::IOContext* io_ctx));

    MOCK_CONST_METHOD2(open, doris::Result<std::unique_ptr<DorisCompoundReader>>(
                                     const doris::TabletIndex*, const doris::io::IOContext*));
};

class MockTabletSchema : public doris::TabletIndex {};

class MockTabletColumn : public doris::TabletColumn {
    MOCK_METHOD(doris::FieldType, type, (), (const));
    MOCK_METHOD((const TabletColumn&), get_sub_column, (uint32_t), (const));
};

class MockTabletIndex : public doris::TabletIndex {
    MOCK_METHOD(doris::IndexType, index_type, (), (const));
    MOCK_METHOD((const std::map<string, string>&), properties, (), (const));
};

class MockIndexFileWriter : public doris::segment_v2::IndexFileWriter {
public:
    MockIndexFileWriter(doris::io::FileSystemSPtr fs)
            : doris::segment_v2::IndexFileWriter(fs, "test_index", "rowset_id", 1,
                                                 doris::InvertedIndexStorageFormatPB::V2) {}

    MOCK_METHOD(doris::Result<std::shared_ptr<doris::segment_v2::DorisFSDirectory>>, open,
                (const doris::TabletIndex* index_meta), (override));
};

class MockAnnIndexIterator : public doris::segment_v2::AnnIndexIterator {
public:
    MockAnnIndexIterator()
            : doris::segment_v2::AnnIndexIterator(_io_ctx_mock, nullptr, nullptr, nullptr) {}

    ~MockAnnIndexIterator() override = default;

    IndexType type() override { return IndexType::ANN; }

    MOCK_METHOD(Status, read_from_index, (const doris::segment_v2::IndexParam& param), (override));
    MOCK_METHOD(Status, range_search,
                (const segment_v2::RangeSearchParams& params,
                 const segment_v2::CustomSearchParams& custom_params,
                 segment_v2::RangeSearchResult* result),
                (override));

private:
    io::IOContext _io_ctx_mock;
};
} // namespace doris::vec_search_mock

namespace doris::vectorized {
static std::string order_by_expr_thrift =
        R"xxx({"1":{"lst":["rec",12,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"l2_distance"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}},{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"l2_distance(array<double>, array<double>)"},"9":{"rec":{"1":{"str":""}}},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"29":{"tf":1}},{"1":{"i32":5},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"3":{"i32":4},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"casttoarray"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":7}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"casttoarray(array<float>)"},"9":{"rec":{"1":{"str":""}}},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"29":{"tf":0}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":7}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":1},"2":{"i32":0},"3":{"i32":1}}},"20":{"i32":-1},"29":{"tf":0},"36":{"str":"embedding"}},{"1":{"i32":21},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":8},"20":{"i32":-1},"28":{"i32":8},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":1}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":2}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":3}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":4}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":5}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":6}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":7}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":20}}},"20":{"i32":-1},"29":{"tf":0}}]}})xxx";
template <typename T>
T read_from_json(const std::string& json_str) {
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

class VectorSearchTest : public ::testing::Test {
public:
    static void accumulate(double x, double y, double& sum) { sum += (x - y) * (x - y); }
    static double finalize(double sum) { return sqrt(sum); }

protected:
    void SetUp() override {
        TExpr _distance_function_call_thrift = read_from_json<TExpr>(order_by_expr_thrift);
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
        _ann_index_iterator = std::make_unique<vec_search_mock::MockAnnIndexIterator>();

        _row_desc = RowDescriptor(*_desc_tbl, {0}, {false});

        // Create CLucene RAM directory instead of mock
        _ram_dir = std::make_shared<lucene::store::RAMDirectory>();

        // Optional: Create test file to simulate index presence
        auto output = _ram_dir->createOutput("index_file");
        // Write some dummy data
        const char* dummy_data = "dummy data";
        output->writeBytes((const uint8_t*)dummy_data, strlen(dummy_data));
        output->close();
        delete output; // CLucene requires manual delete
    }

    void TearDown() override {}

private:
    doris::ObjectPool obj_pool;
    RowDescriptor _row_desc;
    std::unique_ptr<vec_search_mock::MockAnnIndexIterator> _ann_index_iterator;
    vectorized::IColumn::MutablePtr _result_column;
    doris::TExpr _distance_function_call_thrift;
    doris::TExpr _virtual_slot_ref_expr;
    DescriptorTbl* _desc_tbl;
    doris::RuntimeState _runtime_state;
    std::shared_ptr<lucene::store::RAMDirectory> _ram_dir;
};
} // namespace doris::vectorized