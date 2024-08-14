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

#include "remote_path_helper.h"

#include <fmt/core.h>

#include <type_traits>

namespace doris {
namespace {

// Our hash function is MurmurHash2, 64 bit version.
// It was modified in order to provide the same result in
// big and little endian archs (endian neutral).
int64_t murmur_hash64(const void* key, int32_t len) {
    constexpr uint64_t m = 0xc6a4a7935bd1e995ULL;
    constexpr int r = 47;
    constexpr unsigned int seed = 0xadc83b19ULL;
    uint64_t h = seed ^ (len * m);
    const auto* data = (const uint8_t*)key;
    const uint8_t* end = data + (len - (len & 7));

    while (data != end) {
        uint64_t k;
        if constexpr (std::endian::native == std::endian::big) {
            k = (uint64_t)data[0];
            k |= (uint64_t)data[1] << 8;
            k |= (uint64_t)data[2] << 16;
            k |= (uint64_t)data[3] << 24;
            k |= (uint64_t)data[4] << 32;
            k |= (uint64_t)data[5] << 40;
            k |= (uint64_t)data[6] << 48;
            k |= (uint64_t)data[7] << 56;
        } else {
            k = *((uint64_t*)data);
        }

        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        data += 8;
    }

    switch (len & 7) {
    case 7:
        h ^= (uint64_t)data[6] << 48;
        [[fallthrough]];
    case 6:
        h ^= (uint64_t)data[5] << 40;
        [[fallthrough]];
    case 5:
        h ^= (uint64_t)data[4] << 32;
        [[fallthrough]];
    case 4:
        h ^= (uint64_t)data[3] << 24;
        [[fallthrough]];
    case 3:
        h ^= (uint64_t)data[2] << 16;
        [[fallthrough]];
    case 2:
        h ^= (uint64_t)data[1] << 8;
        [[fallthrough]];
    case 1:
        h ^= (uint64_t)data[0];
        h *= m;
    }

    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

} // namespace

RemotePathHelper::RemotePathHelper(const RemotePathContext& ctx) : path_version_(ctx.path_version) {
    switch (path_version_) {
    case 0:
        break;
    case 1:
        shard_fn_ = [num_shards = ctx.num_shards](int64_t tablet_id) {
            return murmur_hash64(static_cast<void*>(&tablet_id), sizeof(tablet_id)) % num_shards;
        };
        break;
    default:
        LOG(FATAL) << "unknown path version, please upgrade BE/MS or drop this storage vault. "
                   << "resource_id=" << ctx.resource_id << "path_version=" << path_version_;
    }
}

std::string RemotePathHelper::remote_rowset_path_prefix(int64_t tablet_id,
                                                        std::string_view rowset_id) const {
    switch (path_version_) {
    case 0:
        // data/${tablet_id}/${rowset_id}_
        return fmt::format("data/{}/{}_", tablet_id, rowset_id);
    case 1:
        // data/${shard_id}/${tablet_id}/${rowset_id}/
        return fmt::format("data/{}/{}/{}/", shard_fn_(tablet_id), tablet_id, rowset_id);
    default:
        __builtin_unreachable();
        exit(-1);
    }
}

std::string RemotePathHelper::remote_segment_path(int64_t tablet_id, std::string_view rowset_id,
                                                  int64_t seg_id) const {
    // v0: data/${tablet_id}/${rowset_id}_${seg_id}.dat
    // v1: data/${shard_id}/${tablet_id}/${rowset_id}/${seg_id}.dat
    return fmt::format("{}{}.dat", remote_rowset_path_prefix(tablet_id, rowset_id), seg_id);
}

std::string RemotePathHelper::remote_tablet_path(int64_t tablet_id) const {
    switch (path_version_) {
    case 0:
        // data/${tablet_id}
        return fmt::format("data/{}", tablet_id);
    case 1:
        // data/${shard_id}/${tablet_id}
        return fmt::format("data/{}/{}", shard_fn_(tablet_id), tablet_id);
    default:
        __builtin_unreachable();
        exit(-1);
    }
}

} // namespace doris
