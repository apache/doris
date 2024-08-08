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

#include <glog/logging.h>

#include <functional>

namespace doris {

struct RemotePathContext {
    std::string_view resource_id;
    int64_t path_version {0};
    int64_t num_shards {0};
};

class RemotePathHelper {
public:
    RemotePathHelper() : path_version_(0) {}
    explicit RemotePathHelper(const RemotePathContext& ctx);
    ~RemotePathHelper() = default;

    RemotePathHelper(const RemotePathHelper&) = default;
    RemotePathHelper& operator=(const RemotePathHelper&) = default;
    RemotePathHelper(RemotePathHelper&&) noexcept = default;
    RemotePathHelper& operator=(RemotePathHelper&&) = default;

    std::string remote_rowset_path_prefix(int64_t tablet_id, std::string_view rowset_id) const;

    std::string remote_segment_path(int64_t tablet_id, std::string_view rowset_id,
                                    int64_t seg_id) const;

    std::string remote_tablet_path(int64_t tablet_id) const;

    int64_t path_version() const { return path_version_; }

private:
    int64_t path_version_;
    // Map tablet_id to shard_id
    std::function<int64_t(int64_t)> shard_fn_;
};

} // namespace doris
