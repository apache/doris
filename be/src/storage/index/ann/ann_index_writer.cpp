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

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>

#include "common/cast_set.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/ann/faiss_ann_index.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "util/slice.h"
#include "util/uid_util.h"

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

AnnIndexColumnWriter::~AnnIndexColumnWriter() {
    _delete_spool_file();
}

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

    const size_t chunk_elements = AnnIndexColumnWriter::chunk_size() * build_parameter.dim;
    const Int64 min_train_rows = _vector_index->get_min_train_rows();
    if (min_train_rows > 0) {
        _training_sample.reserve(_training_sample_rows_limit(min_train_rows) *
                                 build_parameter.dim);
    }
    _buffered_vectors.reserve(chunk_elements);
    _training_sample_seen_rows = 0;
    _training_sample_rng.seed(0);

    return Status::OK();
}

Status AnnIndexColumnWriter::add_values(const std::string fn, const void* values, size_t count) {
    return Status::OK();
}

void AnnIndexColumnWriter::close_on_error() {
    _delete_spool_file();
    _training_sample.clear();
    _buffered_vectors.clear();
}

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

    const Int64 min_train_rows = _vector_index->get_min_train_rows();
    if (min_train_rows == 0) {
        RETURN_IF_ERROR(_append_vectors_no_train(p, num_rows));
    } else {
        RETURN_IF_ERROR(_append_vectors_need_train(p, num_rows, min_train_rows));
    }
    _total_rows += cast_set<int64_t>(num_rows);

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
    if (_total_rows == 0) {
        LOG_INFO("No data to train/add for ANN index. Skipping index building.");
        Status st = _index_file_writer->delete_index(_index_meta);
        _delete_spool_file();
        return st;
    }

    const Int64 min_train_rows = _vector_index->get_min_train_rows();
    Status st = min_train_rows == 0 ? _vector_index->save(_dir.get())
                                    : _train_and_add(min_train_rows);
    _delete_spool_file();
    return st;
}

Status AnnIndexColumnWriter::_append_vectors_no_train(const float* vectors, size_t num_rows) {
    DCHECK(vectors != nullptr);
    DCHECK(num_rows > 0);
    return _vector_index->add(cast_set<Int64>(num_rows), vectors);
}

Status AnnIndexColumnWriter::_append_vectors_need_train(const float* vectors, size_t num_rows,
                                                        Int64 min_train_rows) {
    DCHECK(vectors != nullptr);
    DCHECK(num_rows > 0);

    const size_t dim = _vector_index->get_dimension();
    const size_t num_elements = num_rows * dim;
    _sample_training_vectors(vectors, num_rows, dim, _training_sample_rows_limit(min_train_rows));

    if (_spool_file_writer != nullptr) {
        return _append_to_spool_file(vectors, num_elements);
    }

    const size_t buffer_elements_limit = AnnIndexColumnWriter::chunk_size() * dim;
    if (_buffered_vectors.size() + num_elements <= buffer_elements_limit) {
        _buffered_vectors.insert(_buffered_vectors.end(), vectors, vectors + num_elements);
        return Status::OK();
    }

    RETURN_IF_ERROR(_spill_buffered_vectors());
    return _append_to_spool_file(vectors, num_elements);
}

size_t AnnIndexColumnWriter::_training_sample_rows_limit(Int64 min_train_rows) const {
    DCHECK(min_train_rows > 0);
    const Int64 bounded_rows =
            std::min<Int64>(AnnIndexColumnWriter::chunk_size(),
                            config::ann_index_build_max_train_rows);
    return cast_set<size_t>(std::max<Int64>(min_train_rows, bounded_rows));
}

void AnnIndexColumnWriter::_sample_training_vectors(const float* vectors, size_t num_rows,
                                                    size_t dim, size_t sample_rows_limit) {
    DCHECK(vectors != nullptr);
    DCHECK(num_rows > 0);
    DCHECK(dim > 0);
    DCHECK(sample_rows_limit > 0);
    DCHECK(_training_sample.size() % dim == 0);

    for (size_t row = 0; row < num_rows; ++row) {
        const float* vector = vectors + row * dim;
        ++_training_sample_seen_rows;
        const size_t sample_rows = _training_sample.size() / dim;
        if (sample_rows < sample_rows_limit) {
            _training_sample.insert(_training_sample.end(), vector, vector + dim);
            continue;
        }

        std::uniform_int_distribution<uint64_t> distribution(0, _training_sample_seen_rows - 1);
        const uint64_t selected = distribution(_training_sample_rng);
        if (selected < sample_rows_limit) {
            float* dst = _training_sample.data() + cast_set<size_t>(selected) * dim;
            std::copy(vector, vector + dim, dst);
        }
    }
}

Status AnnIndexColumnWriter::_append_to_spool_file(const float* vectors, size_t num_elements) {
    const size_t bytes = num_elements * sizeof(float);
    return _spool_file_writer->append(Slice(reinterpret_cast<const uint8_t*>(vectors), bytes));
}

Status AnnIndexColumnWriter::_spill_buffered_vectors() {
    DCHECK(_spool_file_writer == nullptr);
    DORIS_CHECK(ExecEnv::GetInstance()->get_tmp_file_dirs() != nullptr);
    _spool_file_path = ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir() /
                       fmt::format("ann_index_build_{}.spool", UniqueId::gen_uid().to_string());
    io::FileWriterOptions opts;
    opts.sync_file_data = false;
    RETURN_IF_ERROR(
            io::global_local_filesystem()->create_file(_spool_file_path, &_spool_file_writer,
                                                       &opts));
    if (!_buffered_vectors.empty()) {
        RETURN_IF_ERROR(_append_to_spool_file(_buffered_vectors.data(), _buffered_vectors.size()));
        _buffered_vectors.clear();
    }
    return Status::OK();
}

Status AnnIndexColumnWriter::_flush_spool_writer() {
    if (_spool_file_writer == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_spool_file_writer->close());
    _spool_file_writer.reset();
    return Status::OK();
}

Status AnnIndexColumnWriter::_train_and_add(Int64 min_train_rows) {
    if (_total_rows < min_train_rows) {
        LOG_INFO(
                "Total data size {} is less than minimum {} rows required for ANN index training. "
                "Skipping index building for this segment.",
                _total_rows, min_train_rows);
        RETURN_IF_ERROR(_flush_spool_writer());
        return _index_file_writer->delete_index(_index_meta);
    }

    DCHECK(_training_sample.size() % _vector_index->get_dimension() == 0);
    const Int64 train_rows =
            cast_set<Int64>(_training_sample.size() / _vector_index->get_dimension());
    DORIS_CHECK(train_rows >= min_train_rows);
    RETURN_IF_ERROR(_vector_index->train(train_rows, _training_sample.data()));
    _training_sample.clear();
    if (_spool_file_writer == nullptr) {
        RETURN_IF_ERROR(_add_buffered_vectors());
    } else {
        RETURN_IF_ERROR(_flush_spool_writer());
        RETURN_IF_ERROR(_add_spooled_vectors());
    }
    return _vector_index->save(_dir.get());
}

Status AnnIndexColumnWriter::_add_buffered_vectors() {
    const size_t dim = _vector_index->get_dimension();
    DCHECK(_buffered_vectors.size() % dim == 0);
    if (_buffered_vectors.empty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_vector_index->add(cast_set<Int64>(_buffered_vectors.size() / dim),
                                       _buffered_vectors.data()));
    _buffered_vectors.clear();
    return Status::OK();
}

Status AnnIndexColumnWriter::_add_spooled_vectors() {
    DCHECK(!_spool_file_path.empty());
    io::FileReaderSPtr reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(_spool_file_path, &reader));

    const size_t dim = _vector_index->get_dimension();
    const size_t chunk_elements = AnnIndexColumnWriter::chunk_size() * dim;
    _training_sample.resize(chunk_elements);
    const size_t buffer_bytes = chunk_elements * sizeof(float);
    size_t offset = 0;
    while (offset < reader->size()) {
        const size_t bytes_to_read = std::min(buffer_bytes, reader->size() - offset);
        DCHECK(bytes_to_read % sizeof(float) == 0);
        size_t bytes_read = 0;
        RETURN_IF_ERROR(reader->read_at(
                offset, Slice(reinterpret_cast<uint8_t*>(_training_sample.data()), bytes_to_read),
                &bytes_read));
        if (bytes_read != bytes_to_read) {
            return Status::IOError(
                    "Failed to read ANN index build spool file {}, expect {} bytes, "
                    "got {} bytes",
                    _spool_file_path.native(), bytes_to_read, bytes_read);
        }
        DCHECK((bytes_read / sizeof(float)) % dim == 0);
        RETURN_IF_ERROR(_vector_index->add(cast_set<Int64>(bytes_read / sizeof(float) / dim),
                                           _training_sample.data()));
        offset += bytes_read;
    }
    RETURN_IF_ERROR(reader->close());
    _training_sample.clear();
    return Status::OK();
}

void AnnIndexColumnWriter::_delete_spool_file() {
    if (_spool_file_writer != nullptr) {
        Status st = _spool_file_writer->close();
        if (!st.ok()) {
            LOG(WARNING) << "Failed to close ANN index build spool file "
                         << _spool_file_path.native() << ": " << st;
        }
        _spool_file_writer.reset();
    }
    if (!_spool_file_path.empty()) {
        Status st = io::global_local_filesystem()->delete_file(_spool_file_path);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to delete ANN index build spool file "
                         << _spool_file_path.native() << ": " << st;
        }
        _spool_file_path.clear();
    }
}
} // namespace doris::segment_v2
