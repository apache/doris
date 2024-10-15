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

#include "show_hotspot_action.h"

#include <queue>
#include <string>

#include "cloud/cloud_tablet_mgr.h"
#include "http/http_channel.h"
#include "http/http_request.h"

namespace doris {
namespace {

enum class Metrics {
    READ_BLOCK = 0,
    WRITE = 1,
    COMPACTION = 2,
    NUM_ROWSETS = 3,
    NUM_BASE_ROWSETS = 4,
    NUM_CUMU_ROWSETS = 5,
    UNKNOWN = 100000,
};

Status check_param(HttpRequest* req, int& top_n, Metrics& metrics) {
    const std::string TOPN_PARAM = "topn";

    auto& topn_str = req->param(TOPN_PARAM);
    if (!topn_str.empty()) {
        try {
            top_n = std::stoi(topn_str);
        } catch (const std::exception& e) {
            return Status::InternalError("convert topn failed, {}", e.what());
        }
    }

    const std::string METRICS_PARAM = "metrics";
    auto& metrics_str = req->param(METRICS_PARAM);
    if (metrics_str.empty()) {
        return Status::InternalError("metrics must be specified");
    }

    if (metrics_str == "read_block") {
        metrics = Metrics::READ_BLOCK;
    } else if (metrics_str == "write") {
        metrics = Metrics::WRITE;
    } else if (metrics_str == "compaction") {
        metrics = Metrics::COMPACTION;
    } else if (metrics_str == "num_rowsets") {
        metrics = Metrics::NUM_ROWSETS;
    } else if (metrics_str == "num_cumu_rowsets") {
        metrics = Metrics::NUM_CUMU_ROWSETS;
    } else if (metrics_str == "num_base_rowsets") {
        metrics = Metrics::NUM_BASE_ROWSETS;
    } else {
        return Status::InternalError("unknown metrics: {}", metrics_str);
    }

    return Status::OK();
}

struct TabletCounter {
    int64_t tablet_id {0};
    int64_t count {0};
};

struct Comparator {
    constexpr bool operator()(const TabletCounter& lhs, const TabletCounter& rhs) const {
        return lhs.count > rhs.count;
    }
};

using MinHeap = std::priority_queue<TabletCounter, std::vector<TabletCounter>, Comparator>;

} // namespace

void ShowHotspotAction::handle(HttpRequest* req) {
    int topn = 0;
    Metrics metrics {Metrics::UNKNOWN};
    auto st = check_param(req, topn, metrics);
    if (!st.ok()) [[unlikely]] {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, st.to_string());
        return;
    }

    std::function<int64_t(CloudTablet&)> count_fn;
    switch (metrics) {
    case Metrics::READ_BLOCK:
        count_fn = [](auto&& t) { return t.read_block_count.load(std::memory_order_relaxed); };
        break;
    case Metrics::WRITE:
        count_fn = [](auto&& t) { return t.write_count.load(std::memory_order_relaxed); };
        break;
    case Metrics::COMPACTION:
        count_fn = [](auto&& t) { return t.compaction_count.load(std::memory_order_relaxed); };
        break;
    case Metrics::NUM_ROWSETS:
        count_fn = [](auto&& t) { return t.fetch_add_approximate_num_rowsets(0); };
        break;
    case Metrics::NUM_BASE_ROWSETS:
        count_fn = [](auto&& t) {
            return t.fetch_add_approximate_num_rowsets(0) -
                   t.fetch_add_approximate_cumu_num_rowsets(0);
        };
        break;
    case Metrics::NUM_CUMU_ROWSETS:
        count_fn = [](auto&& t) { return t.fetch_add_approximate_cumu_num_rowsets(0); };
        break;
    default:
        break;
    }

    if (!count_fn) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "metrics not specified");
        return;
    }

    auto tablets = _storage_engine.tablet_mgr().get_weak_tablets();
    std::vector<TabletCounter> buffer;
    buffer.reserve(tablets.size());
    for (auto&& t : tablets) {
        if (auto tablet = t.lock(); tablet) {
            buffer.push_back({tablet->tablet_id(), count_fn(*tablet)});
        }
    }

    if (topn <= 0) {
        topn = tablets.size();
    }

    MinHeap min_heap;
    for (auto&& counter : buffer) {
        min_heap.push(counter);
        if (min_heap.size() > topn) {
            min_heap.pop();
        }
    }

    buffer.resize(0);
    while (!min_heap.empty()) {
        buffer.push_back(min_heap.top());
        min_heap.pop();
    }

    std::string res;
    res.reserve(buffer.size() * 20);
    // Descending order
    std::for_each(buffer.rbegin(), buffer.rend(), [&res](auto&& counter) {
        res += fmt::format("{} {}\n", counter.tablet_id, counter.count);
    });

    HttpChannel::send_reply(req, HttpStatus::OK, res);
}

} // namespace doris
