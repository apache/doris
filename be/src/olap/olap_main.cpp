// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/olap_main.h"

#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>

#include "olap/command_executor.h"
#include "olap/olap_define.h"
#include "olap/olap_engine.h"
#include "olap/olap_index.h"
#include "olap/olap_server.h"
#include "olap/utils.h"

using std::string;

namespace palo {

// 初始化所有的singleton对象，同时初始化comlog，
// 这么做为了尽量把日志打到日志文件中
static bool touch_all_singleton() {
    OLAPStatus res = OLAP_SUCCESS;

    OLAPRootPath* root_path = OLAPRootPath::get_instance();
    if (NULL == root_path || OLAP_SUCCESS != (res = root_path->init())) {
        OLAP_LOG_FATAL("fail to init olap root path. [res=%d]", res);
        return false;
    }

    OLAPEngine* engine = OLAPEngine::get_instance();
    if (NULL == engine || OLAP_SUCCESS != (res = engine->init())) {
        OLAP_LOG_FATAL("fail to init olap engine. [res=%d]", res);
        return false;
    }

    OLAPSnapshot* snapshot = OLAPSnapshot::get_instance();
    if (NULL == snapshot) {
        OLAP_LOG_FATAL("fail to init olap snapshot. [res=%d]", res);
        return false;
    }

    OLAPUnusedIndex* unused_index = OLAPUnusedIndex::get_instance();
    if (NULL == unused_index || OLAP_SUCCESS != (res = unused_index->init())) {
        OLAP_LOG_FATAL("fail to init delete unused index. [res=%d]", res);
        return false;
    }

    return true;
}

#ifdef OLAP_UNIT_TEST
int olap_main(int argc, char** argv) {
#else
int olap_main(int argc, char** argv) {
#endif
#ifdef GOOGLE_PROFILER
    const char* google_profiler_output = "profiler_out";
    ProfilerStart(google_profiler_output);
    HeapProfilerStart("heap_prof");
#endif

    int ret = 0;
    OLAPServer server;
    if (!touch_all_singleton()) {
        OLAP_LOG_FATAL("fail to touch all singleton.");
        ret = 1;
        goto EXIT;
    }

    if (OLAP_SUCCESS != server.init(NULL, NULL)) {
        OLAP_LOG_FATAL("server init failed, exiting.");
        ret = 1;
        goto EXIT;
    }
    
#ifdef GOOGLE_PROFILER
    HeapProfilerStop();
    ProfilerStop();
#endif

EXIT:

    return ret;
}

}  // namespace palo
