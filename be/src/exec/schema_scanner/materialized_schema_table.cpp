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

#include "exec/schema_scanner/materialized_schema_table.h"

#include <utility>

#include "common/status.h"
#include "util/string_util.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
#include "common/compile_check_begin.h"

MaterializedSchemaTable::MaterializedSchemaTable(
        std::string table_name,
        std::unordered_map<std::string, std::shared_ptr<MaterializedSchemaTableDir>> stores,
        size_t ttl_s)
        : table_name_(std::move(table_name)), stores_(std::move(stores)), ttl_s_(ttl_s) {}

Status MaterializedSchemaTable::init() {
    bool exists = true;
    for (const auto& [dir, store] : stores_) {
        std::string materialized_dir = store->get_materialized_path(table_name_);
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(materialized_dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(materialized_dir));
            continue;
        }

        // 1. load materialized time slices
        std::vector<io::FileInfo> slice_files;
        RETURN_IF_ERROR(io::global_local_filesystem()->list(materialized_dir, false, &slice_files,
                                                            &exists));
        for (const auto& slice_file : slice_files) {
            if (!slice_file.is_file) {
                LOG(WARNING) << "Ignoring non-file entry in materialized schema table directory: "
                             << slice_file.file_name;
                continue;
            }
            std::vector<std::string> slice_name_parts = doris::split(slice_file.file_name, "_");
            if (slice_name_parts.size() != 2) {
                LOG(WARNING) << "Invalid materialized schema table slice file name: "
                             << slice_file.file_name;
                continue;
            }
            time_slices_.emplace_back(std::stoull(slice_name_parts[0]),
                                      std::stoull(slice_name_parts[1]), slice_file.file_size,
                                      fmt::format("{}/{}", materialized_dir, slice_file.file_name));
        }

        // sort time slices by end timestamp, small to large.
        std::sort(time_slices_.begin(), time_slices_.end(),
                  [](auto&& a, auto&& b) { return a.end_timestamp_ < b.end_timestamp_; });

        // 2. init writer
        writers_[dir] = std::make_unique<MaterializedSchemaTableWriter>(store);
    }
    return Status::OK();
}

Status MaterializedSchemaTable::put_block(vectorized::Block* block) {
    if (stores_.empty()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                "config::materialized_schema_table_storage_root_path is null");
    }

    // 1. insert time slice column in block.
    int64_t timestamp = UnixMillis();
    size_t rows = block->rows();
    auto data_type =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false);
    block->insert(0, vectorized::ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                       "time_slice"));
    Defer defer {[&]() { block->erase(0); }};

    vectorized::MutableColumnPtr mutable_col_ptr;
    mutable_col_ptr = std::move(block->get_by_position(0).column)->assume_mutable();
    vectorized::IColumn* col_ptr = mutable_col_ptr.get();
    for (size_t i = 0; i < rows; ++i) {
        std::vector<void*> datas(1);
        VecDateTimeValue src[1];
        src[0].from_unixtime(timestamp, cctz::time_zone());
        datas[0] = src;
        auto* data = datas[0];
		uint64_t num = *reinterpret_cast<uint64_t*>(data);
        assert_cast<vectorized::ColumnDateTimeV2*>(col_ptr)->insert_value(num);
    }

    // 2. merge block to active block.
    {
        std::unique_lock<std::shared_mutex> wlock(lock_);
        if (active_block_ == nullptr) {
            active_block_ = vectorized::Block::create_unique(block->clone_empty());
        }
        vectorized::MutableBlock mblock =
                vectorized::MutableBlock::build_mutable_block(active_block_.get());
        RETURN_IF_ERROR(mblock.merge(*block));
        curl_slice_timestamps_.push_back(UnixMillis());
    }

    // 3. materialized active block.
    RETURN_IF_ERROR(materialized_());
    return Status::OK();
}

std::vector<MaterializedSchemaTableWriter*> MaterializedSchemaTable::get_stores_for_flush_(
        TStorageMedium::type storage_medium) {
    std::vector<std::pair<std::string, double>> stores_with_usage;
    for (auto& [dir, store] : stores_) {
        if (store->storage_medium() == storage_medium && !store->reach_capacity_limit(0)) {
            stores_with_usage.emplace_back(dir, store->get_disk_usage(0));
        }
    }
    if (stores_with_usage.empty()) {
        return {};
    }

    std::sort(stores_with_usage.begin(), stores_with_usage.end(),
              [](auto&& a, auto&& b) { return a.second < b.second; });

    std::vector<MaterializedSchemaTableWriter*> writers;
    for (const auto& [dir, _] : stores_with_usage) {
        writers.emplace_back(writers_[dir].get());
    }
    return writers;
}

Status MaterializedSchemaTable::flush_() {
    auto data_dirs = get_stores_for_flush_(TStorageMedium::type::SSD);
    if (data_dirs.empty()) {
        data_dirs = get_stores_for_flush_(TStorageMedium::type::HDD);
    }
    if (data_dirs.empty()) {
        return Status::Error<ErrorCode::NO_AVAILABLE_ROOT_PATH>(
                "no available disk can be used for spill.");
    }
    auto* writer = data_dirs[0];

    {
        MonotonicStopWatch watch;
        watch.start();

        std::unique_lock<std::shared_mutex> wlock(lock_);
        if (active_block_->rows() == 0) {
            return Status::OK();
        }
        size_t written_bytes = 0;
        std::string path = fmt::format("{}/{}_{}", table_name_, curl_slice_timestamps_[0],
                                       curl_slice_timestamps_[-1]);
        RETURN_IF_ERROR(writer->write(active_block_.get(), written_bytes, path));
        time_slices_.emplace_back(curl_slice_timestamps_[0], curl_slice_timestamps_[-1],
                                  written_bytes, path);

        // clear
        active_block_->clear_column_data();
        curl_slice_timestamps_.clear();

        LOG(INFO) << fmt::format(
                "materialized schema table {} flush time slice: <{}>, cost time: {}", table_name_,
                time_slices_[-1].debug_string(),
                PrettyPrinter::print(watch.elapsed_time(), TUnit::TIME_NS));
    }
    return Status::OK();
}

Status MaterializedSchemaTable::materialized_() {
    if (curl_slice_timestamps_.size() > config::materialized_schema_table_flush_limit) {
        RETURN_IF_ERROR(flush_());
        clear_expired_time_slice_();
    }
    return Status::OK();
}

void MaterializedSchemaTable::clear_expired_time_slice_() {
    uint64_t expired_timestamp = UnixMillis() - ttl_s_ * 1000;
    std::vector<MaterializedSchemaTableTimeSlice> expired_time_slices;

    bool has_work = false;
    MonotonicStopWatch watch;
    watch.start();
    Defer defer {[&]() {
        if (has_work) {
            LOG(INFO) << fmt::format(
                    "materialized schema table {} clear expired time slice, expired count: {}, "
                    "cost time: {}",
                    table_name_, expired_time_slices.size(),
                    PrettyPrinter::print(watch.elapsed_time(), TUnit::TIME_NS));
        }
    }};

    {
        std::unique_lock<std::shared_mutex> wlock(lock_);
        for (auto it = time_slices_.begin(); it != time_slices_.end();) {
            if (it->end_timestamp_ < expired_timestamp) {
                expired_time_slices.push_back(*it);
                it = time_slices_.erase(it);
            } else {
                ++it;
            }
        }
    }

    for (auto& slice : expired_time_slices) {
        bool exists = false;
        has_work = true;
        auto status = io::global_local_filesystem()->exists(slice.file_path_, &exists);
        if (status.ok() && exists) {
            status = io::global_local_filesystem()->delete_file(slice.file_path_);
        }
        if (!status.ok()) {
            LOG(WARNING) << "Failed to delete expired materialized schema table slice file: "
                         << slice.file_path_ << ", error: " << status.to_string();
        }
    }
}

} // namespace doris
