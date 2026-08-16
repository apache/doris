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

#include "format_v2/file_scan_context.h"

#include <exception>
#include <utility>

namespace doris {

Status FileContextRegistry::get_or_create(const std::string& key, const Loader& loader,
                                          std::shared_ptr<const FileContext>* context,
                                          LookupResult* lookup_result) {
    DORIS_CHECK(context != nullptr);
    context->reset();
    if (lookup_result != nullptr) {
        *lookup_result = {};
    }
    while (true) {
        std::shared_ptr<Entry> entry;
        bool load = false;
        {
            std::lock_guard registry_lock(_lock);
            auto it = _entries.find(key);
            if (it == _entries.end()) {
                entry = std::make_shared<Entry>();
                _entries.emplace(key, entry);
                load = true;
            } else {
                entry = it->second;
                std::lock_guard entry_lock(entry->lock);
                if (!entry->loading && entry->status.ok() && entry->context.expired()) {
                    // Weak values keep a long-running scan from retaining every remote footer it
                    // has ever seen. Replace only an inactive entry so concurrent users of the
                    // same file still share one single-flight load.
                    entry = std::make_shared<Entry>();
                    it->second = entry;
                    load = true;
                }
            }
        }

        if (load) {
            if (lookup_result != nullptr) {
                lookup_result->loaded = true;
            }
            std::shared_ptr<const FileContext> loaded_context;
            Status status;
            try {
                status = loader(&loaded_context);
            } catch (const std::exception& e) {
                status = Status::InternalError("File context loader failed: {}", e.what());
            } catch (...) {
                status = Status::InternalError("File context loader failed with an unknown error");
            }
            if (status.ok() && loaded_context == nullptr) {
                status = Status::InternalError("File context loader returned a null context");
            }
            if (status.ok()) {
                *context = loaded_context;
            }
            {
                std::lock_guard lock(entry->lock);
                entry->status = status;
                entry->context = loaded_context;
                entry->loading = false;
            }
            entry->ready.notify_all();
            return status;
        }

        std::unique_lock lock(entry->lock);
        if (entry->loading) {
            if (lookup_result != nullptr) {
                lookup_result->waited = true;
            }
            entry->ready.wait(lock, [&]() { return !entry->loading; });
        }
        if (!entry->status.ok()) {
            return entry->status;
        }
        *context = entry->context.lock();
        if (*context != nullptr) {
            if (lookup_result != nullptr) {
                lookup_result->hit = true;
            }
            return Status::OK();
        }
        // The loading caller may already have released its result. Retry so this caller installs
        // a fresh single-flight entry instead of returning a null context.
    }
}

} // namespace doris
