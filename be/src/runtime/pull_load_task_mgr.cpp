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

#include "runtime/pull_load_task_mgr.h"

#include <stdio.h>

#include <functional>
#include <vector>
#include <sstream>

#include "common/logging.h"
#include "gen_cpp/BackendService_types.h"
#include "util/defer_op.h"
#include "util/file_utils.h"
#include "util/thrift_util.h"
#include "util/debug_util.h"

namespace doris {

class PullLoadTaskCtx {
public:
    PullLoadTaskCtx();
    PullLoadTaskCtx(const TUniqueId& id, int num_senders);

    Status from_thrift(const uint8_t* buf, uint32_t len);

    const TUniqueId& id() const {
        return _task_info.id;
    }

    Status add_sub_task_info(const TPullLoadSubTaskInfo& sub_task_info, bool* finish);

    void get_task_info(TPullLoadTaskInfo* task_info) const {
        std::lock_guard<std::mutex> l(_lock);
        *task_info = _task_info;
    }

    void get_task_info(std::vector<TPullLoadTaskInfo>* task_infos) const {
        std::lock_guard<std::mutex> l(_lock);
        task_infos->push_back(_task_info);
    }

    Status serialize(ThriftSerializer* serializer) {
        std::lock_guard<std::mutex> l(_lock);
        return serializer->serialize(&_task_info);
    }

private:
    mutable std::mutex _lock;
    int _num_senders;

    std::set<int> _finished_senders;
    TPullLoadTaskInfo _task_info;
};

PullLoadTaskCtx::PullLoadTaskCtx() : _num_senders(0) {
}

PullLoadTaskCtx::PullLoadTaskCtx(const TUniqueId& id, int num_senders) 
        : _num_senders(num_senders) {
    _task_info.id = id;
    _task_info.etl_state = TEtlState::RUNNING;
}

Status PullLoadTaskCtx::from_thrift(const uint8_t* buf, uint32_t len) {
    return deserialize_thrift_msg(buf, &len, true, &_task_info);
}

Status PullLoadTaskCtx::add_sub_task_info(
        const TPullLoadSubTaskInfo& sub_task_info, bool* finish) {
    std::lock_guard<std::mutex> l(_lock);
    if (_finished_senders.count(sub_task_info.sub_task_id) > 0) {
        // Already receive this sub-task informations
        return Status::OK();
    }

    // Apply this information
    for (auto& it : sub_task_info.file_map) {
        _task_info.file_map.emplace(it.first, it.second);
    }

    if (sub_task_info.__isset.tracking_url) {
        _task_info.tracking_urls.push_back(sub_task_info.tracking_url);
    }

    _finished_senders.insert(sub_task_info.sub_task_id);
    if (_finished_senders.size() == _num_senders) {
        // We have already receive all sub-task informations.
        _task_info.etl_state = TEtlState::FINISHED;
        *finish = true;
    }

    return Status::OK();
}

PullLoadTaskMgr::PullLoadTaskMgr(const std::string& path) 
        : _path(path), _dir_exist(true) {
}

PullLoadTaskMgr::~PullLoadTaskMgr() {
}

Status PullLoadTaskMgr::init() {
    auto st = load_task_ctxes();
    if (!st.ok()) {
        _dir_exist = false;
    }
    return Status::OK();
}

Status PullLoadTaskMgr::load_task_ctxes() {
    /*
    // 1. scan all files
    std::vector<std::string> files;
    RETURN_IF_ERROR(FileUtils::scan_dir(_path, &files));

    // 2. load
    for (auto& file : files) {
        if (!is_valid_task_file(file)) {
            continue;
        }
        std::string file_path = _path + "/" + file;
        Status status = load_task_ctx(file_path);
        if (!status.ok()) {
            LOG(WARNING) << "Load one file failed. file_name:" << file_path 
                << ", status:" << status.get_error_msg();
        }
    }
    */

    return Status::InternalError("Not implemented");
}

Status PullLoadTaskMgr::load_task_ctx(const std::string& file_path) {
    FILE* fp = fopen(file_path.c_str(), "r");
    if (fp == nullptr) {
        char buf[64];
        std::stringstream ss;
        ss << "fopen(" << file_path << ") failed, because: " << strerror_r(errno, buf, 64);
        return Status::InternalError(ss.str());
    }
    DeferOp close_file(std::bind(&fclose, fp));

    // 1. read content length
    uint32_t content_len = 0;
    size_t res = fread(&content_len, 4, 1, fp);
    if (res != 1) {
        char buf[64];
        std::stringstream ss;
        ss << "fread content length failed, because: " << strerror_r(errno, buf, 64);
        return Status::InternalError(ss.str());
    }

    if (content_len > 10 * 1024 * 1024) {
        return Status::InternalError("Content is too big.");
    }

    // 2. read content
    uint8_t* content = new uint8_t[content_len];
    DeferOp close_content(std::bind<void>(std::default_delete<uint8_t[]>(), content));
    res = fread(content, content_len, 1, fp);
    if (res != 1) {
        char buf[64];
        std::stringstream ss;
        ss << "fread content failed, because: " << strerror_r(errno, buf, 64);
        return Status::InternalError(ss.str());
    }

    // 3. checksum
    uint32_t checksum = 0;
    checksum = HashUtil::crc_hash(content, content_len, checksum);

    uint32_t read_checksum = 0;
    res = fread(&read_checksum, 4, 1, fp);
    if (res != 1) {
        char buf[64];
        std::stringstream ss;
        ss << "fread content failed, because: " << strerror_r(errno, buf, 64);
        return Status::InternalError(ss.str());
    }
    if (read_checksum != checksum) {
        std::stringstream ss;
        ss << "fread checksum failed, read_checksum=" << read_checksum 
            << ", content_checksum=" << checksum;
        return Status::InternalError(ss.str());
    }

    // 4. new task context
    std::shared_ptr<PullLoadTaskCtx> task_ctx(new PullLoadTaskCtx());
    RETURN_IF_ERROR(task_ctx->from_thrift(content, content_len));

    {
        std::lock_guard<std::mutex> l(_lock);
        _task_ctx_map.emplace(task_ctx->id(), task_ctx);
    }

    LOG(INFO) << "success load task " << task_ctx->id();
    return Status::OK();
}

Status PullLoadTaskMgr::save_task_ctx(PullLoadTaskCtx* task_ctx) {
    if (!_dir_exist) {
        return Status::OK();
    }
    ThriftSerializer serializer(true, 64 * 1024);
    RETURN_IF_ERROR(task_ctx->serialize(&serializer));
    uint8_t* content = nullptr;
    uint32_t content_len = 0;
    serializer.get_buffer(&content, &content_len);

    std::string file_path = task_file_path(task_ctx->id());

    FILE* fp = fopen(file_path.c_str(), "w");
    if (fp == nullptr) {
        char buf[64];
        std::stringstream ss;
        ss << "fopen(" << file_path << ") failed, because: " << strerror_r(errno, buf, 64);
        return Status::InternalError(ss.str());
    }
    DeferOp close_file(std::bind(&fclose, fp));

    // 1. write content size
    size_t res = fwrite(&content_len, 4, 1, fp);
    if (res != 1) {
        char buf[64];
        std::stringstream ss;
        ss << "fwrite content length failed., because: " << strerror_r(errno, buf, 64);
        return Status::InternalError(ss.str());
    }

    // 2. write content
    res = fwrite(content, content_len, 1, fp);
    if (res != 1) {
        char buf[64];
        std::stringstream ss;
        ss << "fwrite content failed., because: " << strerror_r(errno, buf, 64);
        return Status::InternalError(ss.str());
    }

    // 3. checksum
    uint32_t checksum = 0;
    checksum = HashUtil::crc_hash(content, content_len, checksum);
    res = fwrite(&checksum, 4, 1, fp);
    if (res != 1) {
        char buf[64];
        std::stringstream ss;
        ss << "fwrite checksum failed., because: " << strerror_r(errno, buf, 64);
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status PullLoadTaskMgr::register_task(const TUniqueId& id, int num_senders) {
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _task_ctx_map.find(id);
        if (it != std::end(_task_ctx_map)) {
            // Do nothing
            LOG(INFO) << "Duplicate pull load task, id=" << id << " num_senders=" << num_senders;
            return Status::OK();
        }

        std::shared_ptr<PullLoadTaskCtx> task_ctx(new PullLoadTaskCtx(id, num_senders));
        _task_ctx_map.emplace(id, task_ctx);
    }
    LOG(INFO) << "Register pull load task, id=" << id << ", num_senders=" << num_senders;
    return Status::OK();
}

std::string PullLoadTaskMgr::task_file_path(const TUniqueId& id) const {
    std::stringstream ss; 
    ss << _path << "/task_" << id.hi << "_" << id.lo;
    return ss.str();
}

bool PullLoadTaskMgr::is_valid_task_file(const std::string& file_name) const {
    if (file_name.find("task_") == 0) {
        return true;
    }
    return false;
}

Status PullLoadTaskMgr::deregister_task(const TUniqueId& id) {
    std::shared_ptr<PullLoadTaskCtx> ctx;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _task_ctx_map.find(id);
        if (it == std::end(_task_ctx_map)) {
            LOG(INFO) << "Deregister unknown pull load task, id=" << id;
            return Status::OK();
        }
        _task_ctx_map.erase(it);
        ctx = it->second;
    }
    
    if (ctx != nullptr && _dir_exist) {
        std::string file_path = task_file_path(id);
        remove(file_path.c_str());
    }
    LOG(INFO) << "Deregister pull load task, id=" << id;
    
    return Status::OK();
}

Status PullLoadTaskMgr::report_sub_task_info(
        const TPullLoadSubTaskInfo& sub_task_info) {
    std::shared_ptr<PullLoadTaskCtx> ctx;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _task_ctx_map.find(sub_task_info.id);
        if (it == std::end(_task_ctx_map)) {
            std::stringstream ss;
            ss << "receive unknown pull load sub-task id=" << sub_task_info.id 
                << ", sub_id=" << sub_task_info.sub_task_id;
            return Status::InternalError(ss.str());
        }

        ctx = it->second;
    }
    bool is_finished = false;
    RETURN_IF_ERROR(ctx->add_sub_task_info(sub_task_info, &is_finished));
    if (is_finished) {
        auto st = save_task_ctx(ctx.get());
        if (!st.ok()) {
            LOG(INFO) << "Save pull load task context failed.id=" << sub_task_info.id;
        }
    }
    VLOG_RPC << "process one pull load sub-task, id=" << sub_task_info.id 
        << ", sub_id=" << sub_task_info.sub_task_id;
    return Status::OK();
}

Status PullLoadTaskMgr::fetch_task_info(const TUniqueId& tid, 
                                        TFetchPullLoadTaskInfoResult* result) {
    std::shared_ptr<PullLoadTaskCtx> ctx;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _task_ctx_map.find(tid);
        if (it == std::end(_task_ctx_map)) {
            LOG(INFO) << "Fetch unknown task info, id=" << tid;
            result->task_info.id = tid;
            result->task_info.etl_state = TEtlState::CANCELLED;
            return Status::OK();
        }

        ctx = it->second;
    }
    ctx->get_task_info(&result->task_info);
    return Status::OK();
}

Status PullLoadTaskMgr::fetch_all_task_infos(
        TFetchAllPullLoadTaskInfosResult* result) {
    std::lock_guard<std::mutex> l(_lock);
    for (auto& it : _task_ctx_map) {
        it.second->get_task_info(&result->task_infos);
    }
    return Status::OK();
}

}
