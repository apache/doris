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
#include "storage/index/ann/ann_build_memory_budget.h"
#include "storage/index/ann/faiss_ann_index.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"

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

size_t AnnIndexColumnWriter::compute_chunk_rows(size_t dim) {
    if (dim == 0) {
        return 1;
    }
    const size_t bytes_per_row = dim * sizeof(float);
    const size_t rows_by_bytes = std::max<size_t>(
            1, cast_set<size_t>(config::ann_index_build_chunk_bytes) / bytes_per_row);
    return std::max<size_t>(1, std::min(cast_set<size_t>(chunk_size()), rows_by_bytes));
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
    const std::string quantizer = get_or_default(properties, QUANTIZER, "flat");
    std::shared_ptr<FaissVectorIndex> faiss_index = std::make_shared<FaissVectorIndex>();
    FaissBuildParameter build_parameter;
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
    _build_params = build_parameter;
    _dimension = cast_set<size_t>(build_parameter.dim);
    _chunk_rows = compute_chunk_rows(_dimension);

    LOG_INFO(
            "Create a new faiss index, index_id {} index_type {} dim {} metric_type {} "
            "max_degree {}, ef_construction {}, quantizer {}, chunk_rows {} chunk_bytes {}",
            _index_meta->index_id(), index_type, build_parameter.dim, metric_type,
            build_parameter.max_degree, build_parameter.ef_construction, quantizer, _chunk_rows,
            _chunk_rows * _dimension * sizeof(float));

    RETURN_IF_ERROR(_acquire_memory_budget(build_parameter));

    return Status::OK();
}

int64_t AnnIndexColumnWriter::_oom_wait_timeout_ms() {
    // "fail" must not wait at all; "wait" and "skip" honor the configured timeout.
    if (config::ann_index_build_on_oom_action == "fail") {
        return 0;
    }
    return config::ann_index_build_memory_wait_timeout_ms;
}

Status AnnIndexColumnWriter::_acquire_memory_budget(const FaissBuildParameter& params) {
    if (config::ann_index_build_memory_budget_bytes <= 0) {
        // Admission control disabled.
        return Status::OK();
    }
    // Initial admission reservation. expected_rows is unknown at init() time, so
    // estimate one chunk's worth as a floor. _ensure_reservation_for_rows() then
    // grows the reservation toward the real footprint as rows accumulate, so the
    // global budget reflects actual memory rather than just a per-chunk floor.
    const int64_t estimated = estimate_ann_build_memory(params, /*expected_rows=*/0, _chunk_rows);
    const int64_t timeout_ms = _oom_wait_timeout_ms();
    _reservation = AnnBuildMemoryReservation::try_acquire(estimated, timeout_ms);
    if (_reservation.active() || estimated <= 0) {
        return Status::OK();
    }
    return _apply_oom_action(estimated, timeout_ms);
}

Status AnnIndexColumnWriter::_ensure_reservation_for_rows(int64_t total_rows) {
    if (config::ann_index_build_memory_budget_bytes <= 0) {
        return Status::OK();
    }
    const int64_t target = estimate_ann_build_memory(_build_params, total_rows, _chunk_rows);
    const int64_t held = _reservation.bytes();
    if (target <= held) {
        return Status::OK();
    }
    const int64_t timeout_ms = _oom_wait_timeout_ms();
    if (_reservation.grow(target - held, timeout_ms)) {
        return Status::OK();
    }
    // Backpressure: the build cannot grow within the budget. "skip" discards the
    // partially built index at finish() (segment write still succeeds); "wait"/
    // "fail" abort the build with a diagnostic error.
    return _apply_oom_action(target, timeout_ms);
}

void AnnIndexColumnWriter::_grow_reservation_best_effort(int64_t total_rows) {
    if (config::ann_index_build_memory_budget_bytes <= 0) {
        return;
    }
    const int64_t target = estimate_ann_build_memory(_build_params, total_rows, _chunk_rows);
    const int64_t held = _reservation.bytes();
    if (target > held) {
        // finish() only adds the last partial chunk; aborting a near-complete
        // build is never worth it, so account best-effort without blocking.
        (void)_reservation.grow(target - held, /*timeout_ms=*/0);
    }
}

Status AnnIndexColumnWriter::_apply_oom_action(int64_t estimated_bytes, int64_t waited_ms) {
    const std::string action = config::ann_index_build_on_oom_action;
    const int64_t budget = config::ann_index_build_memory_budget_bytes;
    const int64_t in_use = AnnBuildMemoryBudget::instance().reserved_bytes();
    if (action == "skip") {
        LOG_WARNING(
                "Skipping ANN index {} build due to memory budget: estimated={} bytes, "
                "in_use={} bytes, budget={} bytes, waited={} ms",
                _index_meta->index_id(), estimated_bytes, in_use, budget, waited_ms);
        _skip_due_to_oom = true;
        return Status::OK();
    }
    // "wait" already exhausted its timeout inside try_acquire; treat as failure.
    return Status::RuntimeError(
            "ANN index {} build failed due to memory budget (action={}): "
            "estimated={} bytes, in_use={} bytes, budget={} bytes, waited={} ms",
            _index_meta->index_id(), action, estimated_bytes, in_use, budget, waited_ms);
}

Status AnnIndexColumnWriter::_train_once_if_needed(Int64 n, const float* vec) {
    if (_trained) {
        return Status::OK();
    }
    if (_vector_index->needs_training()) {
        RETURN_IF_ERROR(_vector_index->train(n, vec));
    }
    _trained = true;
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
    if (_skip_due_to_oom) {
        // Admission control chose to skip this index build; drop the rows
        // silently so the surrounding segment write still succeeds.
        return Status::OK();
    }

    const auto* offsets = reinterpret_cast<const size_t*>(offsets_ptr);
    const size_t dim = _dimension;
    for (size_t i = 0; i < num_rows; ++i) {
        auto array_elem_size = offsets[i + 1] - offsets[i];
        if (array_elem_size != dim) {
            return Status::InvalidArgument("Ann index expect array with {} dim, got {}.", dim,
                                           array_elem_size);
        }
    }

    const float* p = reinterpret_cast<const float*>(value_ptr);

    DCHECK(_chunk_rows > 0) << "init() must have computed _chunk_rows";
    const size_t full_elements = _current_chunk_capacity_elements();
    size_t remaining_elements = num_rows * dim;
    size_t src_offset = 0;
    while (remaining_elements > 0) {
        if (_float_array.capacity() < full_elements) {
            _float_array.reserve(full_elements);
        }
        size_t available_space = full_elements - _float_array.size();
        size_t elements_to_add = std::min(remaining_elements, available_space);

        _float_array.insert(_float_array.end(), p + src_offset, p + src_offset + elements_to_add);
        src_offset += elements_to_add;
        remaining_elements -= elements_to_add;

        if (_float_array.size() == full_elements) {
            // Grow the budget reservation to cover the rows this add() will make
            // resident before consuming more memory.
            RETURN_IF_ERROR(_ensure_reservation_for_rows(static_cast<int64_t>(_added_rows) +
                                                         static_cast<int64_t>(_chunk_rows)));
            if (_skip_due_to_oom) {
                // Backpressure chose to skip: drop the buffered chunk and stop
                // adding. finish() deletes the index entry.
                _reset_chunk_buffer(true);
                return Status::OK();
            }
            RETURN_IF_ERROR(_train_once_if_needed(_chunk_rows, _float_array.data()));
            RETURN_IF_ERROR(_vector_index->add(_chunk_rows, _float_array.data()));
            _reset_chunk_buffer(false);
            _added_rows += _chunk_rows;
            _need_save_index = true;
        }
    }

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

void AnnIndexColumnWriter::_reset_chunk_buffer(bool release_memory) {
    _float_array.clear();
    if (release_memory) {
        PODArray<float> empty;
        _float_array.swap(empty);
        return;
    }
    // Guard against target == 0 (uninitialized dim/chunk_rows), which would make
    // `target * 2 == 0` and degenerate the threshold check into a per-batch free.
    const size_t target = _current_chunk_capacity_elements();
    if (target == 0) {
        return;
    }
    if (_float_array.capacity() > target * 2) {
        PODArray<float> empty;
        _float_array.swap(empty);
    }
}

Status AnnIndexColumnWriter::finish() {
    if (_skip_due_to_oom) {
        _reset_chunk_buffer(true);
        return _index_file_writer->delete_index(_index_meta);
    }
    Int64 min_train_rows = _vector_index->get_min_train_rows();

    // Check if we have enough rows to train the index
    // train/add the remaining data
    if (_float_array.empty()) {
        if (_need_save_index) {
            // Release the input buffer before save() so the serialization workspace
            // does not overlap with a stale chunk allocation (up to chunk_bytes).
            _reset_chunk_buffer(true);
            return _vector_index->save(_dir.get());
        } else {
            // No data was added at all. This can happen if the segment has 0 rows
            // or all rows were filtered out. We need to delete the directory entry
            // to avoid writing an empty/invalid index file.
            LOG_INFO("No data to train/add for ANN index {}. Skipping index building.",
                     _index_meta->index_id());
            _reset_chunk_buffer(true);
            return _index_file_writer->delete_index(_index_meta);
        }
    } else {
        DCHECK(_float_array.size() % _dimension == 0);

        Int64 num_rows = _float_array.size() / _dimension;

        if (num_rows >= min_train_rows) {
            _grow_reservation_best_effort(static_cast<int64_t>(_added_rows) + num_rows);
            RETURN_IF_ERROR(_train_once_if_needed(num_rows, _float_array.data()));
            RETURN_IF_ERROR(_vector_index->add(num_rows, _float_array.data()));
            _reset_chunk_buffer(true);
            return _vector_index->save(_dir.get());
        } else {
            // It happens to have not enough data to train.
            // If we have data to add before, we still need to save the index.
            if (_need_save_index) {
                // For IVF indexes, adding remaining vectors without training is acceptable
                // because the quantizer was already trained on previous batches. These vectors
                // are simply added to the nearest clusters without retraining.
                _grow_reservation_best_effort(static_cast<int64_t>(_added_rows) + num_rows);
                RETURN_IF_ERROR(_vector_index->add(num_rows, _float_array.data()));
                _reset_chunk_buffer(true);
                return _vector_index->save(_dir.get());
            } else {
                // Not enough data to train and no data added before.
                // Means this is a very small segment, we can skip the index building.
                // We need to delete the directory entry from index_file_writer to avoid
                // writing an empty/invalid index file which causes "IndexInput read past EOF" error.
                LOG_INFO(
                        "Remaining data size {} is less than minimum {} rows required for ANN "
                        "index {} training. Skipping index building for this segment.",
                        num_rows, min_train_rows, _index_meta->index_id());
                _reset_chunk_buffer(true);
                return _index_file_writer->delete_index(_index_meta);
            }
        }
    }
}
} // namespace doris::segment_v2
