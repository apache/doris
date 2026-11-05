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

// The error codes FaissVectorIndex reports through the ANN storage interface. They must be the
// ones it reported when it called CLucene itself: INVERTED_INDEX_CLUCENE_ERROR for a file that
// cannot be opened, created or closed, and IO_ERROR for bytes faiss could not write.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_files.h"
#include "storage/index/ann/faiss_ann_index.h"
#include "storage/index/ann/vector_search_utils.h"
#include "storage/index/inverted/clucene/scripted_lucene_directory.h"

namespace doris::segment_v2 {
namespace {

std::unique_ptr<FaissVectorIndex> built_index(FaissBuildParameter::IndexType type, int rows,
                                              int dim) {
    auto index = std::make_unique<FaissVectorIndex>();
    FaissBuildParameter params;
    params.dim = dim;
    params.max_degree = 16;
    params.ivf_nlist = 4;
    params.index_type = type;
    params.quantizer = FaissBuildParameter::Quantizer::FLAT;
    index->build(params);
    const std::vector<float> vectors =
            vector_search_utils::generate_test_vectors_flatten(rows, dim);
    EXPECT_TRUE(index->train(rows, vectors.data()).ok());
    EXPECT_TRUE(index->add(rows, vectors.data()).ok());
    return index;
}

void expect_error(const Status& status, int code, std::string_view text) {
    EXPECT_EQ(status.code(), code) << status.to_string();
    EXPECT_NE(status.to_string().find(text), std::string::npos) << status.to_string();
}

} // namespace

TEST(AnnIndexStorageTest, LoadReportsAMissingFileAsACluceneError) {
    auto dir = std::make_shared<ScriptedLuceneDirectory>();
    FaissVectorIndex hnsw;
    expect_error(vector_search_utils::load_from_directory(&hnsw, dir),
                 ErrorCode::INVERTED_INDEX_CLUCENE_ERROR, faiss_index_fila_name);

    // An IVF_ON_DISK index whose ann.ivfdata is gone: the metadata loads, the lists cannot, and
    // nothing of the half-loaded index survives (ASAN reports it if it leaks).
    auto ivf = built_index(FaissBuildParameter::IndexType::IVF_ON_DISK, 200, 8);
    ASSERT_TRUE(vector_search_utils::save_to_directory(ivf.get(), dir).ok());
    ASSERT_TRUE(dir->deleteFile(faiss_ivfdata_file_name));
    FaissVectorIndex loaded;
    loaded.set_type(AnnIndexType::IVF_ON_DISK);
    expect_error(vector_search_utils::load_from_directory(&loaded, dir),
                 ErrorCode::INVERTED_INDEX_CLUCENE_ERROR, faiss_ivfdata_file_name);
}

TEST(AnnIndexStorageTest, SaveReportsTheCodesItAlwaysDid) {
    // A file the sink cannot create.
    {
        auto dir = std::make_shared<ScriptedLuceneDirectory>();
        dir->fail_create = true;
        auto hnsw = built_index(FaissBuildParameter::IndexType::HNSW, 100, 8);
        expect_error(vector_search_utils::save_to_directory(hnsw.get(), dir),
                     ErrorCode::INVERTED_INDEX_CLUCENE_ERROR, "injected create failure");
    }
    // Bytes faiss cannot write: 2000 vectors of 32 floats overflow the output's 64 KiB buffer while
    // faiss is still writing, and the failed append reaches faiss as an exception.
    {
        auto dir = std::make_shared<ScriptedLuceneDirectory>();
        dir->broken_outputs.insert(faiss_index_fila_name);
        auto hnsw = built_index(FaissBuildParameter::IndexType::HNSW, 2000, 32);
        expect_error(vector_search_utils::save_to_directory(hnsw.get(), dir), ErrorCode::IO_ERROR,
                     "Failed to write vector index");
    }
    // A close that fails: a small index fits the buffer, so its bytes fail only when close()
    // flushes them. The error names the part of the index, as the SNII staging tests expect.
    {
        auto dir = std::make_shared<ScriptedLuceneDirectory>();
        dir->broken_outputs.insert(faiss_index_fila_name);
        auto hnsw = built_index(FaissBuildParameter::IndexType::HNSW, 100, 8);
        expect_error(vector_search_utils::save_to_directory(hnsw.get(), dir),
                     ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                     "Failed to close vector index output");
    }
    {
        auto dir = std::make_shared<ScriptedLuceneDirectory>();
        dir->broken_outputs.insert(faiss_ivfdata_file_name);
        auto ivf = built_index(FaissBuildParameter::IndexType::IVF_ON_DISK, 200, 8);
        expect_error(vector_search_utils::save_to_directory(ivf.get(), dir),
                     ErrorCode::INVERTED_INDEX_CLUCENE_ERROR, "Failed to close IVF data output");
    }
}

} // namespace doris::segment_v2
