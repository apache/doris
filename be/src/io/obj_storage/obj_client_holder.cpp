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

#include "io/obj_storage/obj_client_holder.h"

#include <fmt/format.h>

#include <utility>

#include "common/logging.h"
#include "io/obj_storage/obj_client_factory.h"

namespace doris::io {

ObjClientHolder::ObjClientHolder(ObjClientConfig config)
        : _state(std::make_shared<ObjClientState>(ObjClientState {
                  .config = std::move(config),
                  .client = nullptr,
                  .generation = 0,
          })) {}

ObjClientHolder::~ObjClientHolder() = default;

Status ObjClientHolder::init() {
    auto current = state();
    auto client = DORIS_TRY(ObjClientFactory::instance().get_or_create(current->config));
    _publish(current->config, std::move(client));
    return Status::OK();
}

Status ObjClientHolder::reset(const ObjClientConfig& update) {
    auto current = state();
    auto merged = merge_obj_client_config(current->config, update);
    // Compare full-field equality of the merged config, not hash(): distinct configurations
    // can collide, which would skip a required client rebuild (e.g. a credential update).
    if (merged == current->config) {
        return Status::OK(); // Same config
    }

    auto client = DORIS_TRY(ObjClientFactory::instance().get_or_create(merged));

    LOG(WARNING) << "reset object storage client with new config: " << update.to_string();

    _publish(std::move(merged), std::move(client));
    return Status::OK();
}

void ObjClientHolder::_publish(ObjClientConfig config, std::shared_ptr<ObjStorageClient> client) {
    // Declared before the lock: after the swap below it holds the replaced state, and
    // possibly the last reference to its client, which is then released after unlocking.
    std::shared_ptr<const ObjClientState> next;
    std::lock_guard lock(_mtx);
    next = std::make_shared<ObjClientState>(ObjClientState {
            .config = std::move(config),
            .client = std::move(client),
            .generation = _state->generation + 1,
    });
    _state.swap(next);
}

Result<int64_t> ObjClientHolder::object_file_size(const std::string& bucket,
                                                  const std::string& key) const {
    auto client = get();
    if (!client) {
        return ResultError(Status::InvalidArgument("init s3 client error"));
    }

    auto resp = client->head_object({
            .bucket = bucket,
            .key = key,
    });

    if (resp.resp.status.code != ErrorCode::OK) {
        return ResultError(std::move(Status(resp.resp.status.code, std::move(resp.resp.status.msg))
                                             .append(fmt::format("failed to head s3 file {}",
                                                                 display_path(bucket, key)))));
    }

    return resp.file_size;
}

std::string ObjClientHolder::display_path(std::string_view bucket, std::string_view key) const {
    return fmt::format("{}/{}/{}", state()->config.endpoint(), bucket, key);
}

} // namespace doris::io
