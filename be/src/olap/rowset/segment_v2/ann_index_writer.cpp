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

#include "olap/rowset/segment_v2/ann_index_writer.h"

#include <cstddef>
#include <memory>
#include <string>

#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"

#ifdef BUILD_FAISS
#include "vector/faiss_vector_index.h"
#endif

namespace doris::segment_v2 {

static std::string get_or_default(const std::map<std::string, std::string>& properties,
                                  const std::string& key, const std::string& default_value) {
    auto it = properties.find(key);
    if (it != properties.end()) {
        return it->second;
    }
    return default_value;
}

AnnIndexColumnWriter::AnnIndexColumnWriter(const std::string& field_name,
                                           IndexFileWriter* index_file_writer,
                                           const TabletIndex* index_meta, const bool single_field)
        : _index_file_writer(index_file_writer), _index_meta(index_meta) {
    _field_name = StringUtil::string_to_wstring(field_name);
}

AnnIndexColumnWriter::~AnnIndexColumnWriter() {}

Status AnnIndexColumnWriter::init() {
    Result<std::shared_ptr<DorisFSDirectory>> compound_dir = _index_file_writer->open(_index_meta);

    if (!compound_dir.has_value()) {
        return Status::IOError("Failed to open index file: {}", compound_dir.error().to_string());
    }

    _dir = compound_dir.value();

    _vector_index = nullptr;
    const auto& properties = _index_meta->properties();
    const std::string index_type = get_or_default(properties, INDEX_TYPE, "hnsw");
    const std::string metric_type = get_or_default(properties, METRIC_TYPE, "l2_distance");
    const std::string quantilizer = get_or_default(properties, QUANTILIZER, "flat");
    FaissBuildParameter builderParameter;
    std::shared_ptr<FaissVectorIndex> faiss_index = std::make_shared<FaissVectorIndex>();
    builderParameter.index_type = FaissBuildParameter::string_to_index_type(index_type);
    builderParameter.d = std::stoi(get_or_default(properties, DIM, "512"));
    builderParameter.m = std::stoi(get_or_default(properties, MAX_DEGREE, "32"));
    builderParameter.pq_m = std::stoi(get_or_default(properties, PQ_M, "-1")); // -1 means not set

    builderParameter.metric_type = FaissBuildParameter::string_to_metric_type(metric_type);
    builderParameter.quantilizer = FaissBuildParameter::string_to_quantilizer(quantilizer);

    faiss_index->set_build_params(builderParameter);

    _vector_index = faiss_index;
    return Status::OK();
}

Status AnnIndexColumnWriter::add_values(const std::string fn, const void* values, size_t count) {
    return Status::OK();
}

void AnnIndexColumnWriter::close_on_error() {}

Status AnnIndexColumnWriter::add_array_values(size_t field_size, const void* value_ptr,
                                              const uint8_t* null_map, const uint8_t* offsets_ptr,
                                              size_t count) {
    // TODO: Performance optimization
    if (count == 0) {
        return Status::OK();
    }

    const auto* offsets = reinterpret_cast<const size_t*>(offsets_ptr);
    const size_t dim = _vector_index->get_dimension();
    for (int i = 1; i < count; ++i) {
        auto array_elem_size = offsets[i] - offsets[i - 1];
        if (array_elem_size != dim) {
            return Status::InvalidArgument(
                    "Ann index only support array with {} dimension, but get {}", dim,
                    array_elem_size);
        }
    }

    const float* p = reinterpret_cast<const float*>(value_ptr);
    RETURN_IF_ERROR(_vector_index->add(count, p));

    return Status::OK();
}

Status AnnIndexColumnWriter::add_array_values(size_t field_size, const CollectionValue* values,
                                              size_t count) {
    return Status::InternalError("Ann index should not be used on nullable column");
}

Status AnnIndexColumnWriter::add_nulls(uint32_t count) {
    return Status::InternalError("Ann index should not be used on nullable column");
}

Status AnnIndexColumnWriter::add_array_nulls(const uint8_t* null_map, size_t row_id) {
    return Status::InternalError("Ann index should not be used on nullable column");
}

int64_t AnnIndexColumnWriter::size() const {
    return 0; // TODO: 获取倒排索引的内存大小
}

Status AnnIndexColumnWriter::finish() {
    return _vector_index->save(_dir.get());
}

} // namespace doris::segment_v2
