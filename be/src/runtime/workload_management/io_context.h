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

#pragma once

#include "common/factory_creator.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"

class ResourceContext;

class IOContext : public std::enable_shared_from_this<IOContext> {
    ENABLE_FACTORY_CREATOR(IOContext);

public:
    /*
    * 1. operate them thread-safe.
    * 2. all tasks are unified.
    * 3. should not be operated frequently, use local variables to update Counter.
    */
    struct Stats {
        RuntimeProfile::Counter* scan_rows_counter_;
        RuntimeProfile::Counter* scan_bytes_counter_;
        RuntimeProfile::Counter* scan_bytes_from_local_storage_counter_;
        RuntimeProfile::Counter* scan_bytes_from_remote_storage_counter_;
        // number rows returned by query.
        // only set once by result sink when closing.
        RuntimeProfile::Counter* returned_rows_counter_;
        RuntimeProfile::Counter* shuffle_send_bytes_counter_;
        RuntimeProfile::Counter* shuffle_send_rows_counter_;

        RuntimeProfile::Counter* spill_write_bytes_to_local_storage_counter_;
        RuntimeProfile::Counter* spill_read_bytes_from_local_storage_counter_;

        RuntimeProfile* profile() { return profile_.get(); }
        void init_profile() {
            profile_ = std::make_unique<RuntimeProfile>("MemoryContext");
            scan_rows_counter_ = ADD_COUNTER(profile_, "ScanRows", TUnit::UNIT);
            scan_bytes_counter_ = ADD_COUNTER(profile_, "ScanBytes", TUnit::BYTES);
            scan_bytes_from_local_storage_counter_ =
                    ADD_COUNTER(profile_, "ScanBytesFromLocalStorage", TUnit::BYTES);
            scan_bytes_from_remote_storage_counter_ =
                    ADD_COUNTER(profile_, "ScanBytesFromRemoteStorage", TUnit::BYTES);
            returned_rows_counter_ = ADD_COUNTER(profile_, "ReturnedRows", TUnit::UNIT);
            shuffle_send_bytes_counter_ = ADD_COUNTER(profile_, "ShuffleSendBytes", TUnit::BYTES);
            shuffle_send_rows_counter_ =
                    ADD_COUNTER(profile_, "ShuffleSendRowsCounter_", TUnit::UNIT);
            spill_write_bytes_to_local_storage_counter_ =
                    ADD_COUNTER(profile_, "SpillWriteBytesToLocalStorage", TUnit::BYTES);
            spill_read_bytes_from_local_storage_counter_ =
                    ADD_COUNTER(profile_, "SpillReadBytesFromLocalStorage", TUnit::BYTES);
        }
        std::string debug_string() { return profile_->pretty_print(); }

    private:
        std::unique_ptr<RuntimeProfile> profile_;
    };

    IOContext() { stats_.init_profile(); }
    virtual ~IOContext() = default;

    RuntimeProfile* stats_profile() { return stats_.profile(); }

    int64_t scan_rows() const { return stats_.scan_rows_counter_->value(); }
    int64_t scan_bytes() const { return stats_.scan_bytes_counter_->value(); }
    int64_t scan_bytes_from_local_storage() const {
        return stats_.scan_bytes_from_local_storage_counter_->value();
    }
    int64_t scan_bytes_from_remote_storage() const {
        return stats_.scan_bytes_from_remote_storage_counter_->value();
    }
    int64_t returned_rows() const { return stats_.returned_rows_counter_->value(); }
    int64_t shuffle_send_bytes() const { return stats_.shuffle_send_bytes_counter_->value(); }
    int64_t shuffle_send_rows() const { return stats_.shuffle_send_rows_counter_->value(); }

    int64_t spill_write_bytes_to_local_storage() const {
        return stats_.spill_write_bytes_to_local_storage_counter_->value();
    }

    int64_t spill_read_bytes_from_local_storage() const {
        return stats_.spill_read_bytes_from_local_storage_counter_->value();
    }

    void update_scan_rows(int64_t delta) const { stats_.scan_rows_counter_->update(delta); }
    void update_scan_bytes(int64_t delta) const { stats_.scan_bytes_counter_->update(delta); }
    void update_scan_bytes_from_local_storage(int64_t delta) const {
        stats_.scan_bytes_from_local_storage_counter_->update(delta);
    }
    void update_scan_bytes_from_remote_storage(int64_t delta) const {
        stats_.scan_bytes_from_remote_storage_counter_->update(delta);
    }
    void update_returned_rows(int64_t delta) const { stats_.returned_rows_counter_->update(delta); }
    void update_shuffle_send_bytes(int64_t delta) const {
        stats_.shuffle_send_bytes_counter_->update(delta);
    }
    void update_shuffle_send_rows(int64_t delta) const {
        stats_.shuffle_send_rows_counter_->update(delta);
    }

    void update_spill_write_bytes_to_local_storage(int64_t delta) const {
        stats_.spill_write_bytes_to_local_storage_counter_->update(delta);
    }

    void update_spill_read_bytes_from_local_storage(int64_t delta) const {
        stats_.spill_read_bytes_from_local_storage_counter_->update(delta);
    }

    IOThrottle* io_throttle() {
        // TODO: get io throttle from workload group
        return nullptr;
    }

protected:
    friend class ResourceContext;

    void set_resource_ctx(ResourceContext* resource_ctx) { resource_ctx_ = resource_ctx; }

    Stats stats_;
    ResourceContext* resource_ctx_ {nullptr};
};

#include "common/compile_check_end.h"
} // namespace doris
