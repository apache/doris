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

#include "runtime/load_stream_mgr.h"

#include <brpc/stream.h>
#include <olap/rowset/rowset_factory.h>
#include <olap/rowset/rowset_meta.h>
#include <olap/storage_engine.h>
#include <olap/tablet_manager.h>
#include <runtime/exec_env.h>

#include "gutil/ref_counted.h"
#include "olap/lru_cache.h"
#include "runtime/load_channel.h"
#include "runtime/load_stream.h"
#include "util/uid_util.h"

namespace doris {

LoadStreamMgr::LoadStreamMgr(uint32_t segment_file_writer_thread_num,
                             FifoThreadPool* heavy_work_pool, FifoThreadPool* light_work_pool)
        : _heavy_work_pool(heavy_work_pool), _light_work_pool(light_work_pool) {
    static_cast<void>(ThreadPoolBuilder("SegmentFileWriterThreadPool")
                              .set_min_threads(segment_file_writer_thread_num)
                              .set_max_threads(segment_file_writer_thread_num)
                              .build(&_file_writer_thread_pool));
}

LoadStreamMgr::~LoadStreamMgr() {
    _file_writer_thread_pool->shutdown();
}

Status LoadStreamMgr::open_load_stream(const POpenLoadStreamRequest* request,
                                       LoadStreamSharedPtr& load_stream) {
    UniqueId load_id(request->load_id());

    {
        std::lock_guard<decltype(_lock)> l(_lock);
        auto it = _load_streams_map.find(load_id);
        if (it != _load_streams_map.end()) {
            load_stream = it->second;
        } else {
            load_stream = std::make_shared<LoadStream>(request->load_id(), this,
                                                       request->enable_profile());
            RETURN_IF_ERROR(load_stream->init(request));
            _load_streams_map[load_id] = load_stream;
        }
        load_stream->add_source(request->src_id());
    }
    return Status::OK();
}

void LoadStreamMgr::clear_load(UniqueId load_id) {
    std::lock_guard<decltype(_lock)> l(_lock);
    _load_streams_map.erase(load_id);
}

} // namespace doris
