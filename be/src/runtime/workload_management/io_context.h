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

class IOContext : public std::enable_shared_from_this<IOContext> {
    ENABLE_FACTORY_CREATOR(IOContext);

public:
    /*
    * --------------------------------
    * |          Property            |
    * --------------------------------
    * 1. operate them thread-safe.
    * 2. all tasks are unified.
    * 3. should not be operated frequently, use local variables to update Counter.
    */

    RuntimeProfile::Counter* scan_rows_counter_;
    RuntimeProfile::Counter* scan_bytes_counter_;
    RuntimeProfile::Counter* scan_bytes_from_local_storage_counter_;
    RuntimeProfile::Counter* scan_bytes_from_remote_storage_counter_;
    // number rows returned by query.
    // only set once by result sink when closing.
    RuntimeProfile::Counter* returned_rows_counter_;
    RuntimeProfile::Counter* shuffle_send_bytes_counter_;
    RuntimeProfile::Counter* shuffle_send_rows_counter_;
    RuntimeProfile* profile() { return profile_.get(); }
    std::string debug_string() { return profile_->pretty_print(); }

    /*
    * --------------------------------
    * |           Action             |
    * --------------------------------
    */

    IOThrottle* io_throttle() {
        // TODO: get io throttle from workload group
        return nullptr;
    }

protected:
    IOContext() { init_profile(); }
    virtual ~IOContext() = default;


private:
    void init_profile() {
        profile_ = std::make_unique<RuntimeProfile>("MemoryContext");
        scan_rows_counter_ = ADD_COUNTER(profile_, "ScanRows", TUnit::UNIT);
        scan_bytes_counter_ = ADD_COUNTER(profile_, "ScanBytes", TUnit::BYTES);
        scan_bytes_from_local_storage_counter_ = ADD_COUNTER(profile_, "ScanBytesFromLocalStorage", TUnit::BYTES);
        scan_bytes_from_remote_storage_counter_ = ADD_COUNTER(profile_, "ScanBytesFromRemoteStorage", TUnit::BYTES);
        returned_rows_counter_ = ADD_COUNTER(profile_, "ReturnedRows", TUnit::UNIT);
        shuffle_send_bytes_counter_ = ADD_COUNTER(profile_, "ShuffleSendBytes", TUnit::BYTES);
        shuffle_send_rows_counter_ = ADD_COUNTER(profile_, "ShuffleSendRowsCounter_", TUnit::UNIT);
    }

    // Used to collect memory execution stats.
    std::unique_ptr<RuntimeProfile> profile_;
};

class QueryIOContext : public IOContext {
    QueryIOContext() = default;
};

class LoadIOContext : public IOContext {
    LoadIOContext() = default;
};

class CompactionIOContext : public IOContext {
    CompactionIOContext() = default;
};


} // namespace doris
