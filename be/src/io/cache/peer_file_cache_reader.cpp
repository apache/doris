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
#include "io/cache/peer_file_cache_reader.h"

#include <brpc/controller.h>
#include <bvar/latency_recorder.h>
#include <bvar/reducer.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/brpc_client_cache.h"
#include "util/bvar_helper.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/network_util.h"
#include "util/runtime_profile.h"

namespace doris::io {
// read from peer

bvar::Adder<uint64_t> peer_cache_reader_failed_counter("peer_cache_reader", "failed_counter");
bvar::Adder<uint64_t> peer_cache_reader_succ_counter("peer_cache_reader", "succ_counter");
bvar::LatencyRecorder peer_bytes_per_read("peer_cache_reader", "bytes_per_read"); // also QPS
bvar::Adder<uint64_t> peer_cache_reader_total("peer_cache_reader", "total_num");
bvar::Adder<uint64_t> peer_cache_being_read("peer_cache_reader", "file_being_read");
bvar::Adder<uint64_t> peer_cache_reader_read_counter("peer_cache_reader", "read_at");
bvar::LatencyRecorder peer_cache_reader_latency("peer_cache_reader", "peer_latency");
bvar::PerSecond<bvar::Adder<uint64_t>> peer_get_request_qps("peer_cache_reader", "peer_get_request",
                                                            &peer_cache_reader_read_counter);
bvar::Adder<uint64_t> peer_bytes_read_total("peer_cache_reader", "bytes_read");
bvar::PerSecond<bvar::Adder<uint64_t>> peer_read_througthput("peer_cache_reader",
                                                             "peer_read_throughput",
                                                             &peer_bytes_read_total);

PeerFileCacheReader::PeerFileCacheReader(const io::Path& file_path, bool is_doris_table,
                                         std::string host, int port)
        : _path(file_path), _is_doris_table(is_doris_table), _host(host), _port(port) {
    peer_cache_reader_total << 1;
    peer_cache_being_read << 1;
}

PeerFileCacheReader::~PeerFileCacheReader() {
    peer_cache_being_read << -1;
}

Status PeerFileCacheReader::fetch_blocks(const std::vector<FileBlockSPtr>& blocks, size_t off,
                                         Slice s, size_t* bytes_read, size_t file_size,
                                         const IOContext* ctx) {
    VLOG_DEBUG << "enter PeerFileCacheReader::fetch_blocks, off=" << off
               << " bytes_read=" << *bytes_read;
    if (blocks.empty()) {
        *bytes_read = 0;
        return Status::OK();
    }
    if (!_is_doris_table) {
        return Status::NotSupported<false>("peer cache fetch only supports doris table segments");
    }

    PFetchPeerDataRequest req;
    req.set_type(PFetchPeerDataRequest_Type_PEER_FILE_CACHE_BLOCK);
    req.set_path(_path.filename().native());
    req.set_file_size(static_cast<int64_t>(file_size));
    for (const auto& blk : blocks) {
        auto* cb = req.add_cache_req();
        cb->set_block_offset(static_cast<int64_t>(blk->range().left));
        cb->set_block_size(static_cast<int64_t>(blk->range().size()));
    }

    std::string realhost = _host;
    int port = _port;

    auto dns_cache = ExecEnv::GetInstance()->dns_cache();
    if (dns_cache == nullptr) {
        LOG(WARNING) << "DNS cache is not initialized, skipping hostname resolve";
    } else if (!is_valid_ip(realhost)) {
        Status status = dns_cache->get(_host, &realhost);
        if (!status.ok()) {
            peer_cache_reader_failed_counter << 1;
            LOG(WARNING) << "failed to get ip from host " << _host << ": " << status.to_string();
            return Status::InternalError<false>("failed to get ip from host {}", _host);
        }
    }
    std::string brpc_addr = get_host_port(realhost, port);
    Status st = Status::OK();
    std::shared_ptr<PBackendService_Stub> brpc_stub =
            ExecEnv::GetInstance()->brpc_internal_client_cache()->get_new_client_no_cache(
                    brpc_addr);
    if (!brpc_stub) {
        peer_cache_reader_failed_counter << 1;
        LOG(WARNING) << "failed to get brpc stub " << brpc_addr;
        st = Status::RpcError<false>("Address {} is wrong", brpc_addr);
        return st;
    }
    LIMIT_REMOTE_SCAN_IO(bytes_read);
    int64_t begin_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
    Defer defer_latency {[&]() {
        int64_t end_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
        peer_cache_reader_latency << (end_ts - begin_ts);
    }};

    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    PFetchPeerDataResponse resp;
    peer_cache_reader_read_counter << 1;
    brpc_stub->fetch_peer_data(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError<false>(cntl.ErrorText());
    }
    if (resp.has_status()) {
        Status st2 = Status::create(resp.status());
        if (!st2.ok()) return st2;
    }

    size_t filled = 0;
    for (const auto& data : resp.datas()) {
        if (data.data().empty()) {
            peer_cache_reader_failed_counter << 1;
            LOG(WARNING) << "peer cache read empty data" << data.block_offset();
            return Status::InternalError<false>("peer cache read empty data");
        }
        int64_t block_off = data.block_offset();
        size_t rel = block_off > static_cast<int64_t>(off)
                             ? static_cast<size_t>(block_off - static_cast<int64_t>(off))
                             : 0;
        size_t can_copy = std::min(s.size - rel, static_cast<size_t>(data.data().size()));
        VLOG_DEBUG << "peer cache read data=" << data.block_offset()
                   << " size=" << data.data().size() << " off=" << rel << " can_copy=" << can_copy;
        std::memcpy(s.data + rel, data.data().data(), can_copy);
        filled += can_copy;
    }
    VLOG_DEBUG << "peer cache read filled=" << filled;
    peer_bytes_read_total << filled;
    peer_bytes_per_read << filled;
    if (filled != s.size) {
        peer_cache_reader_failed_counter << 1;
        return Status::InternalError<false>("peer cache read incomplete: need={}, got={}", s.size,
                                            filled);
    }
    peer_cache_reader_succ_counter << 1;
    *bytes_read = filled;
    return Status::OK();
}

} // namespace doris::io
