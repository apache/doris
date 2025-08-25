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

#include "olap/rowset/segment_v2/ann_index/ann_index_writer.h"

#include <cstddef>
#include <memory>
#include <string>

#include "common/cast_set.h"
#include "olap/rowset/segment_v2/ann_index/faiss_ann_index.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"
static std::string get_or_default(const std::map<std::string, std::string>& properties,
                                  const std::string& key, const std::string& default_value) {
    auto it = properties.find(key);
    if (it != properties.end()) {
        return it->second;
    }
    return default_value;
}

AnnIndexColumnWriter::AnnIndexColumnWriter(IndexFileWriter* index_file_writer,
                                           const TabletIndex* index_meta)
        : _index_file_writer(index_file_writer), _index_meta(index_meta) {}

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
    FaissBuildParameter build_parameter;
    std::shared_ptr<FaissVectorIndex> faiss_index = std::make_shared<FaissVectorIndex>();
    build_parameter.index_type = FaissBuildParameter::string_to_index_type(index_type);
    build_parameter.dim = std::stoi(get_or_default(properties, DIM, "512"));
    build_parameter.max_degree = std::stoi(get_or_default(properties, MAX_DEGREE, "32"));
    build_parameter.metric_type = FaissBuildParameter::string_to_metric_type(metric_type);
    build_parameter.ef_construction = std::stoi(get_or_default(properties, EF_CONSTRUCTION, "40"));

    faiss_index->build(build_parameter);

    _vector_index = faiss_index;
    LOG_INFO(
            "Create a new faiss index, index_type {} dim {} metric_type {} max_degree {}, "
            "ef_construction {}",
            index_type, build_parameter.dim, metric_type, build_parameter.max_degree,
            build_parameter.ef_construction);
    return Status::OK();
}

Status AnnIndexColumnWriter::add_values(const std::string fn, const void* values, size_t count) {
    return Status::OK();
}

void AnnIndexColumnWriter::close_on_error() {}

Status AnnIndexColumnWriter::add_array_values(size_t field_size, const void* value_ptr,
                                              const uint8_t* null_map, const uint8_t* offsets_ptr,
                                              size_t num_rows) {
    // TODO: Performance optimization
    if (num_rows == 0) {
        return Status::OK();
    }

    const auto* offsets = reinterpret_cast<const size_t*>(offsets_ptr);
    const size_t dim = _vector_index->get_dimension();
    for (size_t i = 0; i < num_rows; ++i) {
        auto array_elem_size = offsets[i + 1] - offsets[i];
        if (array_elem_size != dim) {
            return Status::InvalidArgument("Ann index expect array with {} dim, got {}.", dim,
                                           array_elem_size);
        }
    }

    const float* p = reinterpret_cast<const float*>(value_ptr);
    RETURN_IF_ERROR(_vector_index->add(cast_set<vectorized::UInt32>(num_rows), p));

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
    return 0;
}

Status AnnIndexColumnWriter::finish() {
    return _vector_index->save(_dir.get());
}
#include "common/compile_check_end.h"
} // namespace doris::segment_v2
