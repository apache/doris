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

#include "mindann_index_builder.h"

#include "common/config.h"
#include "mindann_index_utils.h"
#include "olap/tablet_schema.h"
#include "tenann/common/seq_view.h"
#include "tenann/factory/index_factory.h"
#include "tenann/util/threads.h"

namespace doris {

namespace segment_v2 {

// =============== MindAnnIndexBuilderProxy =============

Status MindAnnIndexBuilder::init(const TabletIndex* tablet_index) {
    auto meta = get_vector_meta(tablet_index, std::map<std::string, std::string> {});

    _vector_index_build_threshold = get_start_build_threshold_for_pq_index(meta);

    tenann::OmpSetNumThreads(config::config_vector_index_build_concurrency);

    if (!meta.common_params().contains("dim")) {
        return Status::InvalidArgument("Dim is needed because it's a critical common param");
    }
    _dim = meta.common_params()["dim"];

    auto meta_copy = meta;
    if (meta.index_type() == tenann::IndexType::kFaissIvfPq &&
        config::enable_vector_index_block_cache) {
        meta_copy.index_writer_options()[tenann::IndexWriterOptions::write_index_cache_key] = false;
    } else {
        meta_copy.index_writer_options()[tenann::IndexWriterOptions::write_index_cache_key] = true;
    }

    try {
        // build and write index
        _index_builder = tenann::IndexFactory::CreateBuilderFromMeta(meta_copy);
        _index_builder->index_writer()->SetIndexCache(tenann::IndexCache::GetGlobalInstance());
        if (_src_is_nullable) {
            _index_builder->EnableCustomRowId();
        }
        _index_builder->Open(_index_path);

        if (!_index_builder->is_opened()) {
            return Status::InternalError("Can not open index path in " + _index_path);
        }
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    } catch (tenann::FatalError& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }

    return Status::OK();
}

Status MindAnnIndexBuilder::add_array_float_values(const uint8_t* raw_data_ptr, const size_t count,
                                                   const uint8_t* offsets_ptr) {
    if (_src_is_nullable) {
        return add_array_float_values_nullable(raw_data_ptr, count, offsets_ptr);
    }
    return add_array_float_values_not_null(raw_data_ptr, count, offsets_ptr);
}

Status MindAnnIndexBuilder::add_array_float_values_not_null(const uint8_t* raw_data_ptr,
                                                            const size_t count,
                                                            const uint8_t* offsets_ptr) {
    try {
        if (count == 0) {
            // no values to add vector index
            return Status::OK();
        }

        auto vector_view = tenann::ArraySeqView {.data = raw_data_ptr,
                                                 .dim = _dim,
                                                 .size = static_cast<uint32_t>(count),
                                                 .elem_type = tenann::kFloatType};

        const auto* offsets = reinterpret_cast<const uint64_t*>(offsets_ptr);

        for (int i = 0; i < count; i++) {
            auto array_elem_size = offsets[i + 1] - offsets[i];
            if (array_elem_size > 0 && _dim != array_elem_size) {
                LOG(WARNING) << "index dim: " << _dim << ", written dim: " << array_elem_size;
                return Status::InvalidArgument(
                        "The dimensions of the vector written are inconsistent, index dim is"
                        " {} but data dim is {}",
                        _dim, array_elem_size);
            }
        }

        _index_builder->Add({vector_view});

        _rid += count;
        _values_added += count;
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    } catch (tenann::FatalError& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }

    return Status::OK();
}

Status MindAnnIndexBuilder::add_array_float_values_nullable(const uint8_t* raw_data_ptr,
                                                            const size_t count,
                                                            const uint8_t* offsets_ptr) {
    try {
        if (count == 0) {
            // no values to add vector index
            return Status::OK();
        }

        const auto* offsets = reinterpret_cast<const uint64_t*>(offsets_ptr);

        std::vector<int64_t> row_ids;
        for (int i = 0; i < count; i++) {
            auto array_elem_size = offsets[i + 1] - offsets[i];

            if (array_elem_size == 0) {
                // we do nothing for null row, just add _rid
            } else if (_dim == array_elem_size) {
                row_ids.push_back(_rid);
                _values_added++;
            } else {
                LOG(WARNING) << "index dim: " << _dim << ", written dim: " << array_elem_size;
                return Status::InvalidArgument(
                        "The dimensions of the vector written are inconsistent, index dim is"
                        " {} but data dim is {}",
                        _dim, array_elem_size);
            }
            _rid++;
        }

        if (!row_ids.empty()) {
            auto vector_view = tenann::ArraySeqView {.data = raw_data_ptr,
                                                     .dim = _dim,
                                                     .size = static_cast<uint32_t>(row_ids.size()),
                                                     .elem_type = tenann::kFloatType};
            _index_builder->Add({vector_view}, row_ids.data());
        }

    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    } catch (tenann::FatalError& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status MindAnnIndexBuilder::flush() {
    try {
        _index_builder->Flush();
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    } catch (tenann::FatalError& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

void MindAnnIndexBuilder::close() {
    try {
        if (_index_builder && !_index_builder->is_closed()) {
            _index_builder->Close();
        }
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
    } catch (tenann::FatalError& e) {
        LOG(WARNING) << e.what();
    }
}

void MindAnnIndexBuilder::abort() {
    close();
}

} // namespace segment_v2
} // namespace doris