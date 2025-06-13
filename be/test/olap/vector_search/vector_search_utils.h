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
#include <gen_cpp/olap_file.pb.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>

#include <memory>
#include <string>
#include <utility>

#include "common/object_pool.h"
#include "olap/rowset/segment_v2/ann_index_iterator.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "vec/exprs/vexpr_context.h"
#include "vector_index.h"
// Add CLucene RAM Directory header
#include <CLucene/store/RAMDirectory.h>
#include <faiss/MetricType.h>

using doris::segment_v2::DorisCompoundReader;

namespace faiss {
class Index;
class IndexHNSWFlat;
} // namespace faiss

namespace doris::segment_v2 {
class FaissVectorIndex;
}

namespace doris::vector_search_utils {

// Generate random vectors for testing
std::vector<float> generate_random_vector(int dim);
// Generate random vectors in matrix form
std::vector<std::vector<float>> generate_test_vectors_matrix(int num_vectors, int dimension);
// Generate random vectors as a flatten vector
std::vector<float> generate_test_vectors_flatten(int num_vectors, int dimension);

// Enum for different index types
enum class IndexType {
    FLAT_L2,
    HNSW,
    // Add more index types as needed
};
std::unique_ptr<faiss::Index> create_native_index(IndexType type, int dimension, int m);
std::unique_ptr<doris::segment_v2::VectorIndex> create_doris_index(IndexType index_type,
                                                                   int dimension, int m);

// Helper function to add vectors to both Doris and native indexes
void add_vectors_to_indexes_serial_mode(segment_v2::VectorIndex* doris_index,
                                        faiss::Index* native_index,
                                        const std::vector<std::vector<float>>& vectors);

void add_vectors_to_indexes_batch_mode(segment_v2::VectorIndex* doris_index,
                                       faiss::Index* native_index, size_t num_vectors,
                                       const std::vector<float>& flatten_vectors);

void print_search_results(const vectorized::IndexSearchResult& doris_results,
                          const std::vector<float>& native_distances,
                          const std::vector<faiss::idx_t>& native_indices, int query_idx);

float get_radius_from_flatten(const float* vector, int dim,
                              const std::vector<float>& flatten_vectors, float percentile);
float get_radius_from_matrix(const float* vector, int dim,
                             const std::vector<std::vector<float>>& matrix_vectors,
                             float percentile);
// Helper function to compare search results between Doris and native Faiss
void compare_search_results(const vectorized::IndexSearchResult& doris_results,
                            const std::vector<float>& native_distances,
                            const std::vector<faiss::idx_t>& native_indices,
                            float abs_error = 1e-5);

// result is a vector of pairs, where each pair contains the labels and distance
// result is sorted by labels
std::vector<std::pair<int, float>> perform_native_index_range_search(faiss::Index* index,
                                                                     const float* query_vector,
                                                                     float radius);

std::unique_ptr<doris::vectorized::IndexSearchResult> perform_doris_index_range_search(
        segment_v2::VectorIndex* index, const float* query_vector, float radius,
        const vectorized::IndexSearchParameters& params);

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
    MOCK_METHOD((const std::map<std::string, std::string>&), properties, (), (const));
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

    MOCK_METHOD(Status, read_from_index, (const doris::segment_v2::IndexParam& param), (override));
    MOCK_METHOD(Status, range_search,
                (const vectorized::RangeSearchParams& params,
                 const VectorSearchUserParams& custom_params,
                 vectorized::RangeSearchResult* result),
                (override));

private:
    io::IOContext _io_ctx_mock;
};

class MockAnnIndexReader : public doris::segment_v2::AnnIndexReader {};

std::pair<std::unique_ptr<MockTabletIndex>, std::shared_ptr<segment_v2::AnnIndexReader>>
create_tmp_ann_index_reader(std::map<std::string, std::string> properties);

} // namespace doris::vector_search_utils

namespace doris::vectorized {

class VectorSearchTest : public ::testing::Test {
public:
    static void accumulate(double x, double y, double& sum) { sum += (x - y) * (x - y); }
    static double finalize(double sum) { return sqrt(sum); }

protected:
    void SetUp() override {
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
        slot_desc.virtual_column_expr = read_from_json<TExpr>(_distance_function_call_thrift);
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
        _ann_index_iterator = std::make_unique<vector_search_utils::MockAnnIndexIterator>();

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
    std::unique_ptr<vector_search_utils::MockAnnIndexIterator> _ann_index_iterator;
    vectorized::IColumn::MutablePtr _result_column;
    doris::TExpr _virtual_slot_ref_expr;
    DescriptorTbl* _desc_tbl;
    doris::RuntimeState _runtime_state;
    std::shared_ptr<lucene::store::RAMDirectory> _ram_dir;

    /*
    [0] TExprNode {
        num_children = 2
        fn = TFunctionName {
            name = "l2_distance_approximate"
        }
    },
    [1] TExprNode {
        num_children = 1
        fn = TFunctionName {
            name = "casttoarray"
        }
    },
    [2] TExprNode {
        num_children = 0,
        slot_ref = TSlotRef {
            slot_id = 1
        }
    },
    [3] TExprNode {
        node_type = ARRAY_LITERAL,
        num_children = 8,
    },
    [4] TExprNode {
        float_literal = TFloatLiteral{
            value = 1
        }
    },
    [5] TExprNode {
        float_literal = TFloatLiteral{
            value = 2
        }
    },
    [6] TExprNode {
        float_literal = TFloatLiteral{
            value = 3
        }
    },
    [7] TExprNode {
        float_literal = TFloatLiteral{
            value = 4
        }
    },
    [8] TExprNode {
        float_literal = TFloatLiteral{
            value = 5
        }
    },
    [9] TExprNode {
        float_literal = TFloatLiteral{
            value = 6
        }
    },
    [10] TExprNode {
        float_literal = TFloatLiteral{
            value = 7
        }
    },
    [11] TExprNode {
        float_literal = TFloatLiteral{
            value = 20
        }
    },
    */
    const std::string _distance_function_call_thrift =
            R"xxx({"1":{"lst":["rec",12,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":2},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"l2_distance_approximate"}}},"2":{"i32":0},"3":{"lst":["rec",2,{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}},{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"l2_distance_approximate(array<double>, array<double>)"},"9":{"rec":{"1":{"str":""}}},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"29":{"tf":1}},{"1":{"i32":5},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"3":{"i32":4},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"casttoarray"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":7}}}}]},"3":{"i64":-1}}]},"4":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"5":{"tf":0},"7":{"str":"casttoarray(array<float>)"},"9":{"rec":{"1":{"str":""}}},"11":{"i64":0},"13":{"tf":1},"14":{"tf":0},"15":{"tf":0},"16":{"i64":360}}},"29":{"tf":0}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":7}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":1},"2":{"i32":0},"3":{"i32":1}}},"20":{"i32":-1},"29":{"tf":0},"36":{"str":"embedding"}},{"1":{"i32":21},"2":{"rec":{"1":{"lst":["rec",2,{"1":{"i32":1},"4":{"tf":1},"5":{"lst":["tf",1,1]}},{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":8},"20":{"i32":-1},"28":{"i32":8},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":1}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":2}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":3}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":4}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":5}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":6}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":7}}},"20":{"i32":-1},"29":{"tf":0}},{"1":{"i32":8},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":8}}}}]},"3":{"i64":-1}}},"4":{"i32":0},"9":{"rec":{"1":{"dbl":20}}},"20":{"i32":-1},"29":{"tf":0}}]}})xxx";
};
} // namespace doris::vectorized