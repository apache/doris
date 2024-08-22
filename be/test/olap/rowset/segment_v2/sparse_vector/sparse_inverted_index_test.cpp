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

#include <random>

#include "gtest/gtest.h"
#include "gtest/gtest-message.h"
#include "gtest/gtest-test-part.h"
#include "gtest/gtest_pred_impl.h"

#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/sparse_vector/sparse_inverted_index_builder.h"
#include "olap/rowset/segment_v2/sparse_vector/sparse_inverted_index_searcher.h"

namespace doris::segment_v2 {

class SparseInvertedIndexTest : public testing::Test {
public:
    const std::string _k_test_dir = "./ut_dir/sparse_inverted_index_test";
}; // class SparseInvertedIndexTest

struct SparseItem {
    int32_t id;
    float val;

    SparseItem() = default;
    SparseItem(int32_t id, float val) : id(id), val(val) {}
};

// sort by id asc
bool compare_by_sparse_item_id(const SparseItem &s1, const SparseItem &s2) {
    return s1.id < s2.id;
}

TEST_F(SparseInvertedIndexTest, BuildAndSearchSparseVectors) {
    // ======= build =======

    // prepare data
    std::unordered_map<uint32_t, sparse::SparseRow> sparse_rows;
    uint32_t total_row_id = 0;

    // prepare test dir
    auto st = io::global_local_filesystem()->delete_directory(_k_test_dir);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(_k_test_dir);
    ASSERT_TRUE(st.ok()) << st;

    // prepare index path
    io::FileSystemSPtr fs = io::global_local_filesystem();
    auto index_path = _k_test_dir + "/" + "test_index_path.vi";

    // init index builder
    std::shared_ptr<sparse::SparseInvertedIndexBuilder> index_builder =
            std::make_shared<sparse::SparseInvertedIndexBuilder>(fs, index_path);
    ASSERT_EQ(index_builder->init(), Status::OK());

    // build
    const size_t batch_num = 5;
    const size_t batch_size = 200;
    const size_t dim = 100;

    // random generator
    std::default_random_engine generator;
    std::uniform_int_distribution<int> id_dist(0, 999);
    std::uniform_real_distribution<float> value_dist(0.0f, 1.0f);

    // sparse id and values
    for (int batch = 0; batch < batch_num; batch++) {
        std::vector<int32_t> raw_key_data;
        std::vector<float_t> raw_value_data;
        std::vector<uint64_t> offsets;

        // prepare data
        for (size_t i = 0; i < batch_size; i++) {
            offsets.push_back(raw_key_data.size());

            if (i % 9 != 0) { // not null row
                std::vector<SparseItem> row;
                for (size_t j = 0; j < dim; ++j) {
                    int id = id_dist(generator);
                    float value = value_dist(generator);

                    raw_key_data.push_back(id);
                    raw_value_data.push_back(value);

                    row.emplace_back(id, value);
                }
                std::sort(row.begin(), row.end(), compare_by_sparse_item_id);

                sparse::SparseRow sparse_row(row.size());
                for (size_t j = 0; j < row.size(); j++) {
                    sparse_row.set_at(j, row[j].id, row[j].val);
                }
                sparse_rows[total_row_id] = sparse_row;
            }

            total_row_id++;
        }
        offsets.push_back(raw_key_data.size());

        // batch write
        st = index_builder->add_map_int_to_float_values(
                reinterpret_cast<const uint8_t*>(raw_key_data.data()),
                reinterpret_cast<const uint8_t*>(raw_value_data.data()),
                batch_size,
                reinterpret_cast<const uint8_t*>(offsets.data()));

        ASSERT_EQ(st, Status::OK());
    }

    // flush
    ASSERT_EQ(index_builder->flush(), Status::OK());

    // close
    ASSERT_EQ(index_builder->close(), Status::OK());


    // ======= search =======

    // init index searcher
    std::shared_ptr<sparse::SparseInvertedIndexSearcher> index_searcher =
            std::make_shared<sparse::SparseInvertedIndexSearcher>(fs, index_path, 0.0, 1);
    ASSERT_EQ(index_searcher->init(), Status::OK());

    const size_t query_count = 10;
    const size_t topk = 10;
    const size_t tolerant_range = topk * 2;
    const size_t actual_verify_count = 3;

    // search topk
    for (int query = 0; query < query_count; query++) {
        // generate query sparse vector
        std::vector<int32_t> query_key_data;
        std::vector<float_t> query_value_data;

        std::vector<SparseItem> row;
        for (size_t j = 0; j < dim; ++j) {
            int id = id_dist(generator);
            float value = value_dist(generator);

            query_key_data.push_back(id);
            query_value_data.push_back(value);

            row.emplace_back(id, value);
        }
        std::sort(row.begin(), row.end(), compare_by_sparse_item_id);

        sparse::SparseRow query_row(row.size());
        for (size_t j = 0; j < row.size(); j++) {
            query_row.set_at(j, row[j].id, row[j].val);
        }

        // actual topk
        std::vector<int64_t> result_ids(topk);
        std::vector<float> result_distances(topk);

        roaring::Roaring row_bitmap;
        row_bitmap.addRange(0, batch_num * batch_size);

        st = index_searcher->search(
                &query_key_data,
                &query_value_data,
                topk,
                result_ids.data(),
                reinterpret_cast<uint8_t*>(result_distances.data()),
                row_bitmap);

        ASSERT_EQ(st, Status::OK());

        // expected topk
        std::vector<std::pair<uint32_t, float>> dot_results;
        for (const auto& pair : sparse_rows) {
            uint32_t row_id = pair.first;
            const sparse::SparseRow& sparse_row = pair.second;
            float dot_value = sparse_row.dot(query_row);
            dot_results.emplace_back(row_id, dot_value);
        }

        std::sort(dot_results.begin(), dot_results.end(),
                [](const std::pair<uint32_t, float>& a, const std::pair<uint32_t, float>& b) {
                    return a.second > b.second; // desc order
                });

        for (size_t i = 0; i < std::min(dot_results.size(), tolerant_range); i++) {
            if (dot_results[i].second == 0) {
                continue;
            }

            std::cout << dot_results[i].first << ":" << dot_results[i].second;
            if (i < std::min(dot_results.size(), tolerant_range) - 1) {
                std::cout << ", ";
            }
        }
        std::cout << std::endl;

        // compare actual vs expected
        for (size_t i = 0; i < result_ids.size(); i++) {
            std::cout << result_ids[i];
            if (i < result_ids.size() - 1) {
                std::cout << ", ";
            }
        }
        std::cout << std::endl;

        for (size_t i = 0; i < result_distances.size(); i++) {
            std::cout << result_distances[i];
            if (i < result_distances.size() - 1) {
                std::cout << ", ";
            }
        }
        std::cout << std::endl;

        size_t verify_count = 0;
        bool find;
        for (size_t i = 0; i < result_ids.size() && verify_count < actual_verify_count; i++) {
            if (result_ids[i] == -1) {
                continue;
            }

            find = false;

            for (size_t j = 0; j < std::min(dot_results.size(), tolerant_range); j++) {
                if (dot_results[j].first == result_ids[i]) {
                    verify_count++;
                    find = true;
                    break;
                }
            }

            if (!find) {
                FAIL() << "Row ID mismatch at index." << i;
            }
        }
    }
}

} // namespace doris::segment_v2


