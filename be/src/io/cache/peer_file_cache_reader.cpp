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
#include <butil/iobuf.h>
#include <bvar/latency_recorder.h>
#include <bvar/reducer.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/metrics/doris_metrics.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "runtime/thread_context.h"
#include "util/brpc_client_cache.h"
#include "util/bvar_helper.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/network_util.h"

namespace doris::io {

namespace {

struct ExpectedPeerFetch {
    std::vector<FileBlock::Range> expected_ranges;
    std::vector<FileBlock::Range> pending_ranges;
    std::vector<size_t> expected_block_indexes;
    size_t expected_bytes = 0;
};

size_t clip_requested_range(const FileBlock::Range& range, size_t file_size) {
    if (range.left >= file_size) {
        return 0;
    }
    return std::min(file_size - range.left, range.size());
}

ExpectedPeerFetch build_expected_peer_fetch(const std::vector<FileBlockSPtr>& blocks,
                                            size_t file_size, PFetchPeerDataRequest* req) {
    ExpectedPeerFetch expected;
    for (size_t block_idx = 0; block_idx < blocks.size(); ++block_idx) {
        const auto& blk = blocks[block_idx];
        auto* cb = req->add_cache_req();
        cb->set_block_offset(static_cast<int64_t>(blk->range().left));
        cb->set_block_size(static_cast<int64_t>(blk->range().size()));
        const size_t clipped_size = clip_requested_range(blk->range(), file_size);
        if (clipped_size == 0) {
            continue;
        }
        expected.expected_block_indexes.push_back(block_idx);
        expected.expected_ranges.emplace_back(blk->range().left,
                                              blk->range().left + clipped_size - 1);
        expected.pending_ranges.emplace_back(expected.expected_ranges.back());
        expected.expected_bytes += clipped_size;
    }
    return expected;
}

Status cut_attachment_payload(butil::IOBuf* attachment, size_t size, butil::IOBuf* out) {
    const size_t cut_size = attachment->cutn(out, size);
    if (cut_size != size) {
        return Status::InternalError<false>(
                "peer cache read incomplete attachment: need={}, got={}", size, cut_size);
    }
    return Status::OK();
}

bool subtract_pending_range(std::vector<FileBlock::Range>& pending_ranges,
                            const FileBlock::Range& response_range) {
    for (size_t idx = 0; idx < pending_ranges.size(); ++idx) {
        auto pending_range = pending_ranges[idx];
        if (response_range.left < pending_range.left ||
            response_range.right > pending_range.right) {
            continue;
        }

        if (response_range.left == pending_range.left &&
            response_range.right == pending_range.right) {
            pending_ranges.erase(pending_ranges.begin() + idx);
        } else if (response_range.left == pending_range.left) {
            pending_ranges[idx].left = response_range.right + 1;
        } else if (response_range.right == pending_range.right) {
            pending_ranges[idx].right = response_range.left - 1;
        } else {
            auto right_remain = FileBlock::Range(response_range.right + 1, pending_range.right);
            pending_ranges[idx].right = response_range.left - 1;
            pending_ranges.insert(pending_ranges.begin() + idx + 1, right_remain);
        }
        return true;
    }
    return false;
}

int find_expected_range_idx(const std::vector<FileBlock::Range>& expected_ranges,
                            const FileBlock::Range& response_range) {
    for (size_t idx = 0; idx < expected_ranges.size(); ++idx) {
        const auto& expected_range = expected_ranges[idx];
        if (response_range.left >= expected_range.left &&
            response_range.right <= expected_range.right) {
            return static_cast<int>(idx);
        }
    }
    return -1;
}

} // namespace

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

Status PeerFileCacheReader::fetch_blocks(const std::vector<FileBlockSPtr>& blocks,
                                         PeerFetchResult* result, size_t file_size,
                                         const IOContext* ctx, bool request_fill, int64_t tablet_id,
                                         std::string resource_id) {
    (void)ctx;
    if (result == nullptr) {
        return Status::InvalidArgument("peer cache fetch requires non-null result");
    }
    result->clear();
    VLOG_DEBUG << "enter PeerFileCacheReader::fetch_blocks";
    if (blocks.empty()) {
        return Status::OK();
    }
    if (!_is_doris_table) {
        return Status::NotSupported<false>("peer cache fetch only supports doris table segments");
    }

    PFetchPeerDataRequest req;
    req.set_type(PFetchPeerDataRequest_Type_PEER_FILE_CACHE_BLOCK);
    req.set_path(_path.native());
    req.set_file_size(static_cast<int64_t>(file_size));
    auto* rowset_meta_pb = req.mutable_rowset_meta();
    rowset_meta_pb->Clear();
    // RowsetMetaPB still has deprecated proto2 required rowset_id. Set a dummy value so
    // the RPC can be serialized; current peer read/fill paths only read tablet_id/resource_id.
    rowset_meta_pb->set_rowset_id(0);
    rowset_meta_pb->set_resource_id(resource_id);
    rowset_meta_pb->set_tablet_id(tablet_id);
    if (request_fill) {
        // Ask the peer server to pull missing blocks from remote storage before serving them.
        // Only set for cross-CG reads targeting the designated fill compute group
        // (peer_cache_fill_compute_group_id). Server still gates with enable_peer_server_cache_fill.
        req.set_request_cache_fill(true);
    }
    // Always advertise attachment support. Older peers can still reply in protobuf mode.
    req.set_support_attachment(true);
    auto expected = build_expected_peer_fetch(blocks, file_size, &req);
    if (expected.expected_bytes == 0) {
        return Status::OK();
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

    size_t filled = 0;
    size_t* bytes_read = &filled;
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
    // Use a longer timeout when fill is requested: server may spend up to
    // peer_server_cache_fill_timeout_ms (default 6000ms) pulling from S3 before responding.
    cntl.set_timeout_ms(request_fill ? 7000 : 5000);
    PFetchPeerDataResponse resp;
    peer_cache_reader_read_counter << 1;
    brpc_stub->fetch_peer_data(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError<false>(cntl.ErrorText());
    }
    if (resp.has_status()) {
        Status st2 = Status::create<false>(resp.status());
        LOG_EVERY_N(WARNING, 1000) << "peer cache read failed, status=" << st2.msg();
        if (!st2.ok()) return st2;
    }

    // Metadata stays in resp.datas(); payload may come from protobuf bytes or BRPC attachment.
    const bool use_attachment = resp.has_data_in_attachment() && resp.data_in_attachment();
    butil::IOBuf remaining_attachment(cntl.response_attachment());
    result->chunks.reserve(resp.datas_size());
    for (const auto& data : resp.datas()) {
        if (data.block_offset() < 0 || data.block_size() < 0) {
            peer_cache_reader_failed_counter << 1;
            result->clear();
            return Status::InternalError<false>(
                    "peer cache read invalid block metadata: offset={}, size={}",
                    data.block_offset(), data.block_size());
        }
        const size_t block_off = static_cast<size_t>(data.block_offset());
        const size_t payload_size =
                use_attachment ? static_cast<size_t>(data.block_size()) : data.data().size();
        if (payload_size == 0) {
            continue;
        }
        const auto response_range = FileBlock::Range(block_off, block_off + payload_size - 1);
        const int expected_idx = find_expected_range_idx(expected.expected_ranges, response_range);
        if (expected_idx < 0) {
            peer_cache_reader_failed_counter << 1;
            result->clear();
            return Status::InternalError<false>(
                    "peer cache read block out of requested ranges: off={}, size={}", block_off,
                    payload_size);
        }
        // Attachment payload is a single byte stream. Consume it in resp.datas() order so the
        // peer can split or reorder requested ranges without forcing a fallback.
        if (!subtract_pending_range(expected.pending_ranges, response_range)) {
            peer_cache_reader_failed_counter << 1;
            result->clear();
            return Status::InternalError<false>("peer cache read unexpected block range: [{}, {}]",
                                                response_range.left, response_range.right);
        }

        PeerFetchChunk chunk;
        chunk.block_index = expected.expected_block_indexes[expected_idx];
        chunk.block_offset = response_range.left;
        VLOG_DEBUG << "peer cache read data=" << data.block_offset() << " size=" << payload_size
                   << " block_idx=" << chunk.block_index;
        if (use_attachment) {
            auto cut_st =
                    cut_attachment_payload(&remaining_attachment, payload_size, &chunk.payload);
            if (!cut_st.ok()) {
                peer_cache_reader_failed_counter << 1;
                result->clear();
                return cut_st;
            }
        } else if (chunk.payload.append(data.data().data(), payload_size) != 0) {
            peer_cache_reader_failed_counter << 1;
            result->clear();
            return Status::InternalError<false>(
                    "failed to append protobuf payload into iobuf: size={}", payload_size);
        }
        filled += payload_size;
        result->chunks.emplace_back(std::move(chunk));
    }
    VLOG_DEBUG << "peer cache read filled=" << filled;
    // Sparse reads are complete only when all requested block ranges are covered exactly.
    if (!expected.pending_ranges.empty() || filled != expected.expected_bytes ||
        (use_attachment && !remaining_attachment.empty())) {
        peer_cache_reader_failed_counter << 1;
        result->clear();
        return Status::InternalError<false>(
                "peer cache read incomplete: need={}, got={}, attachment_left={}",
                expected.expected_bytes, filled, remaining_attachment.size());
    }
    peer_bytes_read_total << filled;
    peer_bytes_per_read << filled;
    peer_cache_reader_succ_counter << 1;
    result->bytes_read = filled;
    return Status::OK();
}

} // namespace doris::io
