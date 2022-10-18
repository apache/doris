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

#include <gtest/gtest.h>

#include "common/config.h"
#include "olap/page_cache.h"
#include "olap/segment_loader.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "service/backend_options.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/mem_info.h"

int main(int argc, char** argv) {
    std::shared_ptr<doris::MemTrackerLimiter> process_mem_tracker =
            std::make_shared<doris::MemTrackerLimiter>(-1, "Process");
    std::shared_ptr<doris::MemTrackerLimiter> orphan_mem_tracker =
            std::make_shared<doris::MemTrackerLimiter>(-1, "Orphan", process_mem_tracker);
    std::shared_ptr<doris::MemTrackerLimiter> bthread_mem_tracker =
            std::make_shared<doris::MemTrackerLimiter>(-1, "Bthread", orphan_mem_tracker);
    doris::ExecEnv::GetInstance()->set_global_mem_tracker(process_mem_tracker, orphan_mem_tracker,
                                                          bthread_mem_tracker);
    doris::thread_context()->_thread_mem_tracker_mgr->init();
    doris::TabletSchemaCache::create_global_schema_cache();
    doris::StoragePageCache::create_global_cache(1 << 30, 10);
    doris::SegmentLoader::create_global_instance(1000);
    std::string conf = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conf.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    doris::DiskInfo::init();
    doris::MemInfo::init();
    doris::BackendOptions::init();
    return RUN_ALL_TESTS();
}
