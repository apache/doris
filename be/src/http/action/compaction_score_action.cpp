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
#include "http/action/compaction_score_action.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/status.h"
#include "http/http_channel.h"
#include "http/http_handler_with_auth.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_manager.h"
#include "util/stopwatch.hpp"

namespace doris {

const std::string TOP_N = "top_n";
const std::string SYNC_META = "sync_meta";
const std::string COMPACTION_SCORE = "compaction_score";
constexpr size_t DEFAULT_TOP_N = std::numeric_limits<size_t>::max();
constexpr bool DEFAULT_SYNC_META = false;
constexpr std::string_view TABLET_ID = "tablet_id";

template <typename T>
concept CompactionScoreAccessble = requires(T t) {
    { t.get_real_compaction_score() } -> std::same_as<uint32_t>;
};

template <CompactionScoreAccessble T>
std::vector<CompactionScoreResult> calculate_compaction_scores(
        std::span<std::shared_ptr<T>> tablets) {
    std::vector<CompactionScoreResult> result;
    result.reserve(tablets.size());
    std::ranges::transform(tablets, std::back_inserter(result),
                           [](const std::shared_ptr<T>& tablet) -> CompactionScoreResult {
                               return {.tablet_id = tablet->tablet_id(),
                                       .compaction_score = tablet->get_real_compaction_score()};
                           });
    return result;
}

struct LocalCompactionScoreAccessor final : CompactionScoresAccessor {
    LocalCompactionScoreAccessor(TabletManager* tablet_mgr) : tablet_mgr(tablet_mgr) {}

    std::vector<CompactionScoreResult> get_all_tablet_compaction_scores() override {
        auto tablets = tablet_mgr->get_all_tablet();
        std::span<TabletSharedPtr> s = {tablets.begin(), tablets.end()};
        return calculate_compaction_scores(s);
    }

    TabletManager* tablet_mgr;
};

struct CloudCompactionScoresAccessor final : CompactionScoresAccessor {
    CloudCompactionScoresAccessor(CloudTabletMgr& tablet_mgr) : tablet_mgr(tablet_mgr) {}

    std::vector<CompactionScoreResult> get_all_tablet_compaction_scores() override {
        auto tablets = get_all_tablets();
        std::span<CloudTabletSPtr> s = {tablets.begin(), tablets.end()};
        return calculate_compaction_scores(s);
    }

    Status sync_meta() {
        auto tablets = get_all_tablets();
        LOG(INFO) << "start to sync meta from ms";

        MonotonicStopWatch stopwatch;
        stopwatch.start();

        for (const auto& tablet : tablets) {
            RETURN_IF_ERROR(tablet->sync_meta());
            RETURN_IF_ERROR(tablet->sync_rowsets());
        }

        stopwatch.stop();
        LOG(INFO) << "sync meta finish, time=" << stopwatch.elapsed_time() << "ns";

        return Status::OK();
    }

    std::vector<CloudTabletSPtr> get_all_tablets() {
        auto weak_tablets = tablet_mgr.get_weak_tablets();
        std::vector<CloudTabletSPtr> tablets;
        tablets.reserve(weak_tablets.size());
        for (auto& weak_tablet : weak_tablets) {
            if (auto tablet = weak_tablet.lock();
                tablet != nullptr and tablet->tablet_state() == TABLET_RUNNING) {
                tablets.push_back(std::move(tablet));
            }
        }
        return tablets;
    }

    CloudTabletMgr& tablet_mgr;
};

static rapidjson::Value jsonfy_tablet_compaction_score(
        const CompactionScoreResult& result, rapidjson::MemoryPoolAllocator<>& allocator) {
    rapidjson::Value node;
    node.SetObject();

    rapidjson::Value tablet_id_key;
    tablet_id_key.SetString(TABLET_ID.data(), TABLET_ID.length(), allocator);
    rapidjson::Value tablet_id_val;
    auto tablet_id_str = std::to_string(result.tablet_id);
    tablet_id_val.SetString(tablet_id_str.c_str(), tablet_id_str.length(), allocator);

    rapidjson::Value score_key;
    score_key.SetString(COMPACTION_SCORE.data(), COMPACTION_SCORE.size());
    rapidjson::Value score_val;
    auto score_str = std::to_string(result.compaction_score);
    score_val.SetString(score_str.c_str(), score_str.length(), allocator);
    node.AddMember(score_key, score_val, allocator);

    node.AddMember(tablet_id_key, tablet_id_val, allocator);
    return node;
}

CompactionScoreAction::CompactionScoreAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                             TPrivilegeType::type type, TabletManager* tablet_mgr)
        : HttpHandlerWithAuth(exec_env, hier, type),
          _accessor(std::make_unique<LocalCompactionScoreAccessor>(tablet_mgr)) {}

CompactionScoreAction::CompactionScoreAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                             TPrivilegeType::type type, CloudTabletMgr& tablet_mgr)
        : HttpHandlerWithAuth(exec_env, hier, type),
          _accessor(std::make_unique<CloudCompactionScoresAccessor>(tablet_mgr)) {}

void CompactionScoreAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HttpHeaders::JsonType.data());
    auto top_n_param = req->param(TOP_N);

    size_t top_n = DEFAULT_TOP_N;
    if (!top_n_param.empty()) {
        try {
            auto tmp_top_n = std::stoll(top_n_param);
            if (tmp_top_n < 0) {
                throw std::invalid_argument("`top_n` cannot less than 0");
            }
            top_n = tmp_top_n;
        } catch (const std::exception& e) {
            LOG(WARNING) << "convert failed:" << e.what();
            auto msg = fmt::format("invalid argument: top_n={}", top_n_param);
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, msg);
            return;
        }
    }

    auto sync_meta_param = req->param(SYNC_META);
    bool sync_meta = DEFAULT_SYNC_META;
    if (!sync_meta_param.empty() and !config::is_cloud_mode()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                "param `sync_meta` is only available for cloud mode");
        return;
    }
    if (sync_meta_param == "true") {
        sync_meta = true;
    } else if (sync_meta_param == "false") {
        sync_meta = false;
    } else if (!sync_meta_param.empty()) {
        auto msg = fmt::format("invalid argument: sync_meta={}", sync_meta_param);
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, msg);
        return;
    }

    std::string result;
    if (auto st = _handle(top_n, sync_meta, &result); !st) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, st.to_json());
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::OK, result);
}

Status CompactionScoreAction::_handle(size_t top_n, bool sync_meta, std::string* result) {
    if (sync_meta) {
        DCHECK(config::is_cloud_mode());
        RETURN_IF_ERROR(static_cast<CloudCompactionScoresAccessor*>(_accessor.get())->sync_meta());
    }

    auto scores = _accessor->get_all_tablet_compaction_scores();
    top_n = std::min(top_n, scores.size());
    std::partial_sort(scores.begin(), scores.begin() + top_n, scores.end(), std::greater<>());

    rapidjson::Document root;
    root.SetArray();
    auto& allocator = root.GetAllocator();
    std::for_each(scores.begin(), scores.begin() + top_n, [&](const auto& score) {
        root.PushBack(jsonfy_tablet_compaction_score(score, allocator), allocator);
    });
    rapidjson::StringBuffer str_buf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(str_buf);
    root.Accept(writer);
    *result = str_buf.GetString();
    return Status::OK();
}

} // namespace doris
