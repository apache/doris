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

#include <stdio.h>
#include <stdlib.h>

#include <memory>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gtest/gtest.h"
#include "gtest/gtest_pred_impl.h"
#include "http/ev_http_server.h"
#include "olap/page_cache.h"
#include "olap/segment_loader.h"
#include "olap/tablet_schema_cache.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "service/http_service.h"
#include "test_util.h"
#include "testutil/http_utils.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/mem_info.h"

int main(int argc, char** argv) {
    doris::ThreadLocalHandle::create_thread_local_if_not_exits();
    doris::ExecEnv::GetInstance()->init_mem_tracker();
    doris::thread_context()->thread_mem_tracker_mgr->init();
    std::shared_ptr<doris::MemTrackerLimiter> test_tracker =
            doris::MemTrackerLimiter::create_shared(doris::MemTrackerLimiter::Type::GLOBAL,
                                                    "BE-UT");
    doris::thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(test_tracker);
    doris::ExecEnv::GetInstance()->set_cache_manager(doris::CacheManager::create_global_instance());
    doris::ExecEnv::GetInstance()->set_dummy_lru_cache(std::make_shared<doris::DummyLRUCache>());
    doris::ExecEnv::GetInstance()->set_storage_page_cache(
            doris::StoragePageCache::create_global_cache(1 << 30, 10, 0));
    doris::ExecEnv::GetInstance()->set_segment_loader(new doris::SegmentLoader(1000, 1000));
    std::string conf = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    auto st = doris::config::init(conf.c_str(), false);
    doris::ExecEnv::GetInstance()->set_tablet_schema_cache(
            doris::TabletSchemaCache::create_global_schema_cache(
                    doris::config::tablet_schema_cache_capacity));
    LOG(INFO) << "init config " << st;
    doris::Status s = doris::config::set_config("enable_stacktrace", "false");
    if (!s.ok()) {
        LOG(WARNING) << "set enable_stacktrace=false failed";
    }

    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);

    doris::CpuInfo::init();
    doris::DiskInfo::init();
    doris::MemInfo::init();
    doris::BackendOptions::init();

    auto service = std::make_unique<doris::HttpService>(doris::ExecEnv::GetInstance(), 0, 1);
    service->register_debug_point_handler();
    service->_ev_http_server->start();
    doris::global_test_http_host = "http://127.0.0.1:" + std::to_string(service->get_real_port());

    ::testing::TestEventListeners& listeners = ::testing::UnitTest::GetInstance()->listeners();
    listeners.Append(new TestListener);
    doris::ExecEnv::GetInstance()->set_tracking_memory(false);

    int res = RUN_ALL_TESTS();
    return res;
}
