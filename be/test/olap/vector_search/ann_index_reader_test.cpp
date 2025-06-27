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

#include <gen_cpp/olap_file.pb.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <string>

#include "faiss_vector_index.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/tablet_schema.h"
#include "vector_search_utils.h"
using namespace doris::vector_search_utils;

namespace doris::vectorized {

TEST_F(VectorSearchTest, AnnIndexReaderRangeSearch) {
    size_t iterato = 25;
    for (size_t i = 0; i < iterato; ++i) {
        std::map<std::string, std::string> index_properties;
        index_properties["index_type"] = "hnsw";
        index_properties["metric_type"] = "l2";
        std::unique_ptr<doris::TabletIndex> index_meta = std::make_unique<doris::TabletIndex>();
        index_meta->_properties = index_properties;
        auto mock_index_file_reader = std::make_shared<MockIndexFileReader>();
        auto ann_index_reader = std::make_unique<segment_v2::AnnIndexReader>(
                index_meta.get(), mock_index_file_reader);
        doris::vector_search_utils::IndexType index_type =
                doris::vector_search_utils::IndexType::HNSW;
        const size_t dim = 128;
        const size_t m = 16;
        auto doris_faiss_index = doris::vector_search_utils::create_doris_index(index_type, dim, m);
        auto native_faiss_index =
                doris::vector_search_utils::create_native_index(index_type, dim, m);
        const size_t num_vectors = 1000;
        auto vectors = doris::vector_search_utils::generate_test_vectors_matrix(num_vectors, dim);
        doris::vector_search_utils::add_vectors_to_indexes_serial_mode(
                doris_faiss_index.get(), native_faiss_index.get(), vectors);
        std::ignore = doris_faiss_index->save(this->_ram_dir.get());
        std::vector<float> query_value = vectors[0];
        const float radius = doris::vector_search_utils::get_radius_from_matrix(query_value.data(),
                                                                                dim, vectors, 0.3);

        // Make sure all rows are in the roaring
        auto roaring = std::make_unique<roaring::Roaring>();
        for (size_t i = 0; i < num_vectors; ++i) {
            roaring->add(i);
        }

        doris::vectorized::RangeSearchParams params;
        params.radius = radius;
        params.query_value = query_value.data();
        params.roaring = roaring.get();
        doris::VectorSearchUserParams custom_params;
        custom_params.hnsw_ef_search = 16;
        doris::vectorized::RangeSearchResult result;
        auto doris_faiss_vector_index = std::make_unique<doris::segment_v2::FaissVectorIndex>();
        std::ignore = doris_faiss_vector_index->load(this->_ram_dir.get());
        ann_index_reader->_vector_index = std::move(doris_faiss_vector_index);
        std::ignore = ann_index_reader->range_search(params, custom_params, &result, nullptr);

        ASSERT_TRUE(result.roaring != nullptr);
        ASSERT_TRUE(result.distance != nullptr);
        ASSERT_TRUE(result.row_ids != nullptr);
        std::vector<std::pair<int, float>> doris_search_result_order_by_lables;
        for (size_t i = 0; i < result.roaring->cardinality(); ++i) {
            doris_search_result_order_by_lables.push_back(
                    {result.row_ids->at(i), result.distance[i]});
        }

        std::sort(doris_search_result_order_by_lables.begin(),
                  doris_search_result_order_by_lables.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });

        std::vector<std::pair<int, float>> native_search_result_order_by_lables =
                doris::vector_search_utils::perform_native_index_range_search(
                        native_faiss_index.get(), query_value.data(), radius);

        ASSERT_EQ(result.roaring->cardinality(), native_search_result_order_by_lables.size());

        for (size_t i = 0; i < native_search_result_order_by_lables.size(); ++i) {
            ASSERT_EQ(doris_search_result_order_by_lables[i].first,
                      native_search_result_order_by_lables[i].first);
            ASSERT_FLOAT_EQ(doris_search_result_order_by_lables[i].second,
                            native_search_result_order_by_lables[i].second);
        }
    }
};
} // namespace doris::vectorized