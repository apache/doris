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

#include "service/http/action/file_cache_action.h"

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/fs_file_cache_storage.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "storage/olap_define.h"
#include "storage/tablet/tablet_meta.h"
#include "util/easy_json.h"

namespace doris {

constexpr static std::string_view HEADER_JSON = "application/json";
constexpr static std::string_view OP = "op";
constexpr static std::string_view SYNC = "sync";
constexpr static std::string_view PATH = "path";
constexpr static std::string_view CLEAR = "clear";
constexpr static std::string_view RESET = "reset";
constexpr static std::string_view INFO = "info";
constexpr static std::string_view HASH = "hash";
constexpr static std::string_view LIST_CACHE = "list_cache";
constexpr static std::string_view LIST_BASE_PATHS = "list_base_paths";
constexpr static std::string_view CHECK_CONSISTENCY = "check_consistency";
constexpr static std::string_view CAPACITY = "capacity";
constexpr static std::string_view RELEASE = "release";
constexpr static std::string_view BASE_PATH = "base_path";
constexpr static std::string_view RELEASED_ELEMENTS = "released_elements";
constexpr static std::string_view DUMP = "dump";
constexpr static std::string_view VALUE = "value";
constexpr static std::string_view RELOAD = "reload";

std::string file_cache_infos_to_json(const std::vector<io::FileCacheRuntimeInfo>& infos,
                                     const std::string& message = "") {
    EasyJson json;
    json["status"] = "OK";
    if (!message.empty()) {
        json["message"] = message;
    }
    uint64_t total_capacity = 0;
    auto caches = json["caches"];
    caches.SetArray();
    auto add_queue = [](EasyJson queues, const std::string& name,
                        const io::FileCacheQueueRuntimeInfo& info) {
        auto queue = queues[name];
        queue.SetObject();
        queue["percent"] = static_cast<uint64_t>(info.percent);
        queue["max_bytes"] = static_cast<uint64_t>(info.max_size);
        queue["used_bytes"] = static_cast<uint64_t>(info.current_size);
        queue["max_elements"] = static_cast<uint64_t>(info.max_elements);
        queue["elements"] = static_cast<uint64_t>(info.current_elements);
    };
    for (const auto& info : infos) {
        total_capacity += info.capacity;
        auto cache = caches.PushBack(EasyJson::kObject);
        cache["path"] = info.path;
        cache["storage"] = info.storage;
        cache["capacity_mode"] = io::file_cache_capacity_mode_to_string(info.capacity_mode);
        cache["requested_capacity"] = info.requested_capacity;
        cache["capacity_generation"] = info.capacity_generation;
        cache["effective_capacity"] = info.capacity;
        cache["used_bytes"] = info.current_size;
        cache["pending_eviction_bytes"] = static_cast<uint64_t>(info.pending_eviction_size);
        cache["max_file_block_size"] = info.max_file_block_size;
        cache["disk_resource_limit_mode"] = info.disk_resource_limit_mode;
        cache["need_evict_in_advance"] = info.need_evict_in_advance;
        if (info.disk_state.has_value()) {
            cache["disk_total_bytes"] = info.disk_state->total_capacity;
            cache["disk_available_bytes"] = info.disk_state->available_capacity;
        } else {
            cache["disk_total_bytes"];
            cache["disk_available_bytes"];
        }
        auto last_resize = cache["last_resize"];
        last_resize.SetObject();
        last_resize["source"] = io::file_cache_resize_source_to_string(info.last_resize.source);
        last_resize["time_ms"] = info.last_resize.time_ms;
        last_resize["status"] = info.last_resize.status;
        last_resize["message"] = info.last_resize.message;
        auto queues = cache["queues"];
        queues.SetObject();
        add_queue(queues, "normal", info.queues[io::FileCacheType::NORMAL]);
        add_queue(queues, "disposable", info.queues[io::FileCacheType::DISPOSABLE]);
        add_queue(queues, "index", info.queues[io::FileCacheType::INDEX]);
        add_queue(queues, "ttl", info.queues[io::FileCacheType::TTL]);
    }
    json["total_capacity"] = total_capacity;
    return json.ToString();
}

std::string file_cache_reset_result_to_json(const io::FileCacheResetResult& result) {
    EasyJson json;
    json["status"] = "OK";
    json["total_capacity"] = result.total_capacity;
    auto caches = json["caches"];
    caches.SetArray();
    for (const auto& info : result.caches) {
        auto cache = caches.PushBack(EasyJson::kObject);
        cache["path"] = info.path;
        cache["capacity_mode"] = io::file_cache_capacity_mode_to_string(info.mode);
        cache["requested_capacity"] = info.requested_capacity;
        cache["old_capacity"] = info.old_capacity;
        cache["effective_capacity"] = info.new_capacity;
        cache["disk_total_bytes"] = info.disk_total_capacity;
        cache["disk_available_bytes"] = info.disk_available_capacity;
        cache["used_bytes"] = info.used_bytes;
        cache["pending_eviction_bytes"] = info.pending_eviction_bytes;
        cache["clamped_by_disk"] = info.clamped_by_disk;
        cache["changed"] = info.changed;
    }
    return json.ToString();
}

Status FileCacheAction::_handle_header(HttpRequest* req, std::string* json_metrics) {
    const std::string header_json(HEADER_JSON);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, header_json.c_str());
    std::string operation = req->param(std::string(OP));
    Status st = Status::OK();
    if (operation == RELEASE) {
        size_t released = 0;
        const std::string& base_path = req->param(std::string(BASE_PATH));
        if (!base_path.empty()) {
            released = io::FileCacheFactory::instance()->try_release(base_path);
        } else {
            released = io::FileCacheFactory::instance()->try_release();
        }
        EasyJson json;
        json[std::string(RELEASED_ELEMENTS)] = released;
        *json_metrics = json.ToString();
    } else if (operation == CLEAR) {
        DBUG_EXECUTE_IF("FileCacheAction._handle_header.ignore_clear", {
            LOG_WARNING("debug point FileCacheAction._handle_header.ignore_clear");
            st = Status::OK();
            return st;
        });
        const std::string& sync = req->param(std::string(SYNC));
        const std::string& segment_path = req->param(std::string(VALUE));
        if (segment_path.empty()) {
            const bool sync_clear = to_lower(sync) == "true";
            std::string clear_msg;
            RETURN_IF_ERROR(
                    io::FileCacheFactory::instance()->clear_file_caches(sync_clear, &clear_msg));
            if (sync_clear) {
                EasyJson json;
                json["status"] = "OK";
                json["msg"] = clear_msg;
                *json_metrics = json.ToString();
            }
        } else {
            io::UInt128Wrapper hash = io::BlockFileCache::hash(segment_path);
            io::BlockFileCache* cache = io::FileCacheFactory::instance()->get_by_path(hash);
            cache->remove_if_cached_async(hash);
        }
    } else if (operation == RESET) {
        std::string capacity = req->param(std::string(CAPACITY));
        int64_t new_capacity = 0;
        bool parse = true;
        try {
            size_t parsed_chars = 0;
            new_capacity = std::stoll(capacity, &parsed_chars);
            parse = parsed_chars == capacity.size();
        } catch (...) {
            parse = false;
        }
        if (!parse || new_capacity < 0) {
            st = Status::InvalidArgument(
                    "The capacity {} failed to be parsed, the capacity needs to be in "
                    "the interval [0, INT64_MAX]",
                    capacity);
        } else {
            const std::string& path = req->param(std::string(PATH));
            io::FileCacheResetResult reset_result;
            RETURN_IF_ERROR(io::FileCacheFactory::instance()->reset_capacity(
                    path, static_cast<uint64_t>(new_capacity), &reset_result));
            *json_metrics = file_cache_reset_result_to_json(reset_result);
        }
    } else if (operation == INFO) {
        const std::string& path = req->param(std::string(PATH));
        std::vector<io::FileCacheRuntimeInfo> infos;
        RETURN_IF_ERROR(io::FileCacheFactory::instance()->get_cache_infos(path, &infos));
        *json_metrics = file_cache_infos_to_json(infos);
    } else if (operation == HASH) {
        const std::string& segment_path = req->param(std::string(VALUE));
        if (segment_path.empty()) {
            st = Status::InvalidArgument("missing parameter: {} is required", VALUE);
        } else {
            io::UInt128Wrapper ret = io::BlockFileCache::hash(segment_path);
            EasyJson json;
            json[std::string(HASH)] = ret.to_string();
            *json_metrics = json.ToString();
        }
    } else if (operation == LIST_CACHE) {
        const std::string& segment_path = req->param(std::string(VALUE));
        if (segment_path.empty()) {
            st = Status::InvalidArgument("missing parameter: {} is required", VALUE);
        } else {
            io::UInt128Wrapper cache_hash = io::BlockFileCache::hash(segment_path);
            std::vector<std::string> cache_files =
                    io::FileCacheFactory::instance()->get_cache_file_by_path(cache_hash);
            if (cache_files.empty()) {
                *json_metrics = "[]";
            } else {
                EasyJson json;
                std::for_each(cache_files.begin(), cache_files.end(),
                              [&json](auto& x) { json.PushBack(x); });
                *json_metrics = json.ToString();
            }
        }
    } else if (operation == DUMP) {
        io::FileCacheFactory::instance()->dump_all_caches();
    } else if (operation == LIST_BASE_PATHS) {
        auto all_cache_base_path = io::FileCacheFactory::instance()->get_base_paths();
        EasyJson json;
        std::ranges::for_each(all_cache_base_path,
                              [&json](auto& x) { json.PushBack(std::move(x)); });
        *json_metrics = json.ToString();
    } else if (operation == CHECK_CONSISTENCY) {
        const std::string& cache_base_path = req->param(std::string(BASE_PATH));
        if (cache_base_path.empty()) {
            st = Status::InvalidArgument("missing parameter: {} is required", BASE_PATH);
        } else {
            auto* block_file_cache = io::FileCacheFactory::instance()->get_by_path(cache_base_path);
            if (block_file_cache == nullptr) {
                st = Status::InvalidArgument("file cache not found for base_path: {}",
                                             cache_base_path);
            } else {
                std::vector<std::string> inconsistencies;
                RETURN_IF_ERROR(block_file_cache->report_file_cache_inconsistency(inconsistencies));
                EasyJson json;
                std::ranges::for_each(inconsistencies,
                                      [&json](auto& x) { json.PushBack(std::move(x)); });
                *json_metrics = json.ToString();
            }
        }
    } else if (operation == RELOAD) {
#ifdef BE_TEST
        std::string doris_home = getenv("DORIS_HOME");
        std::string conffile = std::string(doris_home) + "/conf/be.conf";
        if (!doris::config::init(conffile.c_str(), true, true, true)) {
            return Status::InternalError("Error reading config file");
        }

        std::string custom_conffile = doris::config::custom_config_dir + "/be_custom.conf";
        if (!doris::config::init(custom_conffile.c_str(), true, false, false)) {
            return Status::InternalError("Error reading custom config file");
        }

        if (!doris::config::enable_file_cache) {
            return Status::InternalError("config::enbale_file_cache should be true!");
        }

        std::unordered_set<std::string> cache_path_set;
        std::vector<doris::CachePath> cache_paths;
        RETURN_IF_ERROR(doris::parse_conf_cache_paths(doris::config::file_cache_path, cache_paths));

        std::vector<CachePath> cache_paths_no_dup;
        cache_paths_no_dup.reserve(cache_paths.size());
        for (const auto& cache_path : cache_paths) {
            if (cache_path_set.contains(cache_path.path)) {
                LOG(WARNING) << fmt::format("cache path {} is duplicate", cache_path.path);
                continue;
            }
            cache_path_set.emplace(cache_path.path);
            cache_paths_no_dup.emplace_back(cache_path);
        }
        RETURN_IF_ERROR(doris::io::FileCacheFactory::instance()->reload_file_cache(cache_paths));
#else
        return Status::InternalError("Do not use reload in production environment!!!!");
#endif
    } else {
        st = Status::InternalError("invalid operation: {}", operation);
    }
    return st;
}

void FileCacheAction::handle(HttpRequest* req) {
    std::string json_metrics;
    Status status = _handle_header(req, &json_metrics);
    std::string status_result = status.to_json();
    if (status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::OK,
                                json_metrics.empty() ? status.to_json() : json_metrics);
    } else {
        const auto http_status = status.is<ErrorCode::INVALID_ARGUMENT>()
                                         ? HttpStatus::BAD_REQUEST
                                         : HttpStatus::INTERNAL_SERVER_ERROR;
        HttpChannel::send_reply(req, http_status, status_result);
    }
}

} // namespace doris
