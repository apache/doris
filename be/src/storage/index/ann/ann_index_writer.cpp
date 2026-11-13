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

#include "storage/index/ann/ann_index_writer.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>

#include "common/cast_set.h"
#include "common/config.h"
#include "storage/index/ann/faiss_ann_index.h"
#include "storage/index/index_fs_directory.h"

namespace doris::segment_v2 {
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

AnnIndexColumnWriter::~AnnIndexColumnWriter() = default;

Status AnnIndexColumnWriter::init() {
    // The staging directory, not necessarily a filesystem one: under SNII the
    // faiss output is held in memory until begin_close() seals it into the
    // container as a blob. Only the write side is used either way.
    Result<std::shared_ptr<lucene::store::Directory>> compound_dir =
            _index_file_writer->open_ann_directory(_index_meta);

    if (!compound_dir.has_value()) {
        return Status::IOError("Failed to open index file: {}", compound_dir.error().to_string());
    }

    _dir = compound_dir.value();

    _vector_index = nullptr;
    const auto& properties = _index_meta->properties();
    const std::string index_type = get_or_default(properties, INDEX_TYPE, "hnsw");
    const std::string metric_type = get_or_default(properties, METRIC_TYPE, "l2_distance");
    const std::string quantizer = get_or_default(properties, QUANTIZER, "flat");
    FaissBuildParameter build_parameter;
    std::shared_ptr<FaissVectorIndex> faiss_index = std::make_shared<FaissVectorIndex>();
    build_parameter.index_type = FaissBuildParameter::string_to_index_type(index_type);
    build_parameter.dim = std::stoi(get_or_default(properties, DIM, "512"));
    build_parameter.max_degree = std::stoi(get_or_default(properties, MAX_DEGREE, "32"));
    build_parameter.metric_type = FaissBuildParameter::string_to_metric_type(metric_type);
    build_parameter.ef_construction = std::stoi(get_or_default(properties, EF_CONSTRUCTION, "40"));
    build_parameter.ivf_nlist = std::stoi(get_or_default(properties, NLIST, "1024"));
    build_parameter.quantizer = FaissBuildParameter::string_to_quantizer(quantizer);
    build_parameter.pq_m = std::stoi(get_or_default(properties, PQ_M, "8"));
    build_parameter.pq_nbits = std::stoi(get_or_default(properties, PQ_NBITS, "8"));

    faiss_index->build(build_parameter);

    _vector_index = faiss_index;

    LOG_INFO(
            "Create a new faiss index, index_type {} dim {} metric_type {} max_degree {}, "
            "ef_construction {}, quantizer {}",
            index_type, build_parameter.dim, metric_type, build_parameter.max_degree,
            build_parameter.ef_construction, quantizer);

    return Status::OK();
}

Status AnnIndexColumnWriter::add_values(const uint8_t* values, size_t count) {
    return Status::OK();
}

void AnnIndexColumnWriter::close_on_error() {
    PODArray<float> empty_buffered_vectors;
    _buffered_vectors.swap(empty_buffered_vectors);
    PODArray<Int64> empty_row_ids;
    _data_row_ids_in_index.swap(empty_row_ids);
}

Status AnnIndexColumnWriter::add_array_values(size_t field_size, const uint8_t* value_ptr,
                                              const uint8_t* null_map, const uint64_t* offsets_ptr,
                                              size_t num_rows) {
    // TODO: Performance optimization
    if (num_rows == 0) {
        return Status::OK();
    }

    const auto* offsets = offsets_ptr;
    const size_t dim = _vector_index->get_dimension();
    size_t num_rows_to_index = 0;
    const bool allow_empty_arrays = config::enable_ann_index_empty_array;
    // Validate the whole batch before changing row IDs or buffered vectors. Empty arrays
    // occupy a row in the data file but have no ANN entry, regardless of the delete sign.
    for (size_t i = 0; i < num_rows; ++i) {
        const auto count = offsets[i + 1] - offsets[i];
        // For [vector, [], vector], the second vector must keep label 2, not label 1.
        if (count == 0) {
            if (!allow_empty_arrays) {
                return Status::InvalidArgument(
                        "Empty arrays in ANN-indexed columns require "
                        "enable_ann_index_empty_array=true. "
                        "Writing them prevents downgrade to versions without sparse ANN support. "
                        "Index: {}, row: {}",
                        _index_meta->index_name(), i);
            }
            continue;
        }
        if (count != dim) {
            return Status::InvalidArgument("Ann index expect array with {} dim, got {}.", dim,
                                           count);
        }
        ++num_rows_to_index;
    }

    // Without empty arrays, index row IDs equal data-file row IDs, so no mapping is needed.
    // At the first empty array, record the preceding data-file row IDs as well.
    if (num_rows_to_index != num_rows && !_has_empty_arrays) {
        for (int64_t row = 0; row < _num_rows_in_data_file; ++row) {
            _data_row_ids_in_index.push_back(row);
        }
    }
    const bool needs_explicit_row_ids = _has_empty_arrays || num_rows_to_index != num_rows;
    if (needs_explicit_row_ids) {
        for (size_t i = 0; i < num_rows; ++i) {
            if (offsets[i + 1] == offsets[i]) {
                _has_empty_arrays = true;
            } else {
                const auto data_row_id = _num_rows_in_data_file + cast_set<int64_t>(i);
                _data_row_ids_in_index.push_back(data_row_id);
            }
        }
    }
    // Empty arrays contain no items, so the surviving vectors are already contiguous.
    // An all-empty batch may have a null value pointer; never do pointer arithmetic on it.
    if (num_rows_to_index != 0) {
        const float* p = reinterpret_cast<const float*>(value_ptr);
        DCHECK(p != nullptr);
        _buffered_vectors.insert(_buffered_vectors.end(), p, p + num_rows_to_index * dim);
    }
    _num_rows_in_data_file += cast_set<int64_t>(num_rows);
    _num_rows_in_index_file += cast_set<int64_t>(num_rows_to_index);

    return Status::OK();
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
    if (_num_rows_in_data_file == 0) {
        LOG_INFO("Skip ANN index build: no data rows, index_id={}, index_name={}, writer={}",
                 _index_meta->index_id(), _index_meta->index_name(),
                 _index_file_writer->debug_string());
        return _index_file_writer->delete_index(_index_meta);
    }

    const Int64 min_train_rows = _vector_index->get_min_train_rows();
    const Int64 configured_min_rows = cast_set<Int64>(config::ann_index_build_min_segment_rows);
    const Int64 effective_min_rows = std::max(min_train_rows, configured_min_rows);
    // An all-empty segment has a complete, empty index and requires no training.
    if (_num_rows_in_index_file != 0 && _num_rows_in_index_file < effective_min_rows) {
        LOG_INFO(
                "Skip ANN index build: insufficient non-empty vectors, index_id={}, index_name={}, "
                "rows_in_data_file={}, rows_in_index_file={}, required_rows={}, min_train_rows={}, "
                "configured_min_rows={}, writer={}",
                _index_meta->index_id(), _index_meta->index_name(), _num_rows_in_data_file,
                _num_rows_in_index_file, effective_min_rows, min_train_rows, configured_min_rows,
                _index_file_writer->debug_string());
        close_on_error();
        return _index_file_writer->delete_index(_index_meta);
    }

    return _build_and_save(min_train_rows, effective_min_rows);
}

Status AnnIndexColumnWriter::_build_and_save(Int64 min_train_rows, Int64 effective_min_rows) {
    const size_t dim = _vector_index->get_dimension();
    DCHECK(_buffered_vectors.size() % dim == 0);
    const Int64 num_buffered_vectors = cast_set<Int64>(_buffered_vectors.size() / dim);
    DORIS_CHECK(num_buffered_vectors == _num_rows_in_index_file);
    DORIS_CHECK(num_buffered_vectors == 0 || num_buffered_vectors >= effective_min_rows);
    if (min_train_rows > 0 && num_buffered_vectors != 0) {
        RETURN_IF_ERROR(_vector_index->train(num_buffered_vectors, _buffered_vectors.data()));
    }
    // Index entries retain their row IDs in the data file across empty arrays and batches.
    if (!_has_empty_arrays) {
        RETURN_IF_ERROR(_vector_index->add(num_buffered_vectors, _buffered_vectors.data()));
    } else {
        DORIS_CHECK(_data_row_ids_in_index.size() == num_buffered_vectors);
        RETURN_IF_ERROR(_vector_index->add_with_ids(num_buffered_vectors, _buffered_vectors.data(),
                                                    _data_row_ids_in_index.data()));
    }
    PODArray<Int64> empty_row_ids;
    _data_row_ids_in_index.swap(empty_row_ids);
    // PODArray::clear() keeps the allocated capacity. Swap with an empty array so the
    // full-segment build buffer is released before saving the index.
    PODArray<float> empty_buffered_vectors;
    _buffered_vectors.swap(empty_buffered_vectors);
    Status status = _vector_index->save(_dir.get());
    if (!status.ok()) {
        // A failed save keeps whatever it managed to write, and under SNII that is
        // an ANN-sized staging file plus its descriptor. Nothing downstream
        // unwinds it: the segment flush returns before clear() and
        // close_inverted_index(), and ADD INDEX holds every producer until the
        // whole rowset has been closed. Drop BOTH owners of the staging area here
        // -- the index file writer's, and this writer's own.
        _index_file_writer->discard_ann_staging_directory(_index_meta);
        _dir.reset();
    }
    return status;
}
} // namespace doris::segment_v2
