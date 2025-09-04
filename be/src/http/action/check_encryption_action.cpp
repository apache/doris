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

#include "http/action/check_encryption_action.h"

#include <gen_cpp/olap_file.pb.h>

#include <exception>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/tablet_fwd.h"
#include "runtime/exec_env.h"

namespace doris {

const std::string TABLET_ID = "tablet_id";

CheckEncryptionAction::CheckEncryptionAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                             TPrivilegeType::type type)
        : HttpHandlerWithAuth(exec_env, hier, type) {}

Result<bool> is_tablet_encrypted(const BaseTabletSPtr& tablet) {
    auto tablet_meta = tablet->tablet_meta();
    if (tablet_meta->encryption_algorithm() == EncryptionAlgorithmPB::PLAINTEXT) {
        return false;
    }
    Status st;
    bool is_encrypted = true;
    tablet->traverse_rowsets([&st, &tablet, &is_encrypted](const RowsetSharedPtr& rs) {
        if (!st) {
            return;
        }

        auto rs_meta = rs->rowset_meta();
        if (config::is_cloud_mode() && rs_meta->start_version() == 0 &&
            rs_meta->end_version() == 1) {
            return;
        }
        auto fs = rs_meta->physical_fs();
        if (fs == nullptr) {
            st = Status::InternalError("failed to get fs for rowset: tablet={}, rs={}",
                                       tablet->tablet_id(), rs->rowset_id().to_string());
            return;
        }

        if (rs->num_segments() == 0) {
            return;
        }
        auto maybe_seg_path = rs->segment_path(0);
        if (!maybe_seg_path) {
            st = std::move(maybe_seg_path.error());
            return;
        }

        std::vector<std::string_view> file_paths;
        const auto& first_seg_path = maybe_seg_path.value();
        file_paths.emplace_back(first_seg_path);
        if (tablet->tablet_schema()->has_inverted_index() &&
            tablet->tablet_schema()->get_inverted_index_storage_format() == V2) {
            std::string inverted_index_file_path = InvertedIndexDescriptor::get_index_file_path_v2(
                    InvertedIndexDescriptor::get_index_file_path_prefix(first_seg_path));
            file_paths.emplace_back(inverted_index_file_path);
        }

        for (const auto path : file_paths) {
            io::FileReaderSPtr reader;
            st = fs->open_file(path, &reader);
            if (!st) {
                return;
            }
            std::vector<uint8_t> magic_code_buf;
            magic_code_buf.reserve(sizeof(uint64_t));
            Slice magic_code(magic_code_buf.data(), sizeof(uint64_t));
            size_t bytes_read;
            st = reader->read_at(reader->size() - sizeof(uint64_t), magic_code, &bytes_read);
            if (!st) {
                return;
            }

            std::vector<uint8_t> answer = {'A', 'B', 'C', 'D', 'E', 'A', 'B', 'C'};
            is_encrypted &= Slice::mem_equal(answer.data(), magic_code.data, magic_code.size);
            if (!is_encrypted) {
                LOG(INFO) << "found not encrypted segment, path=" << first_seg_path;
            }
        }
    });

    if (st) {
        return is_encrypted;
    }
    return st;
}

Status sync_meta(const CloudTabletSPtr& tablet) {
    RETURN_IF_ERROR(tablet->sync_meta());
    RETURN_IF_ERROR(tablet->sync_rowsets());
    return Status::OK();
}

void CheckEncryptionAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HttpHeaders::JSON_TYPE.data());
    auto tablet_id_str = req->param(TABLET_ID);

    if (tablet_id_str.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                "tablet id should be set in request params");
        return;
    }
    int64_t tablet_id;
    try {
        tablet_id = std::stoll(tablet_id_str);
    } catch (const std::exception& e) {
        LOG(WARNING) << "convert tablet id to i64 failed:" << e.what();
        auto msg = fmt::format("invalid argument: tablet_id={}", tablet_id);

        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, msg);
        return;
    }

    auto maybe_tablet = ExecEnv::get_tablet(tablet_id);
    if (!maybe_tablet) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, maybe_tablet.error().to_string());
        return;
    }
    auto tablet = maybe_tablet.value();

    if (config::is_cloud_mode()) {
        auto cloud_tablet = std::dynamic_pointer_cast<CloudTablet>(tablet);
        DCHECK_NE(cloud_tablet, nullptr);
        auto st = sync_meta(cloud_tablet);
        if (!st) {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, st.to_json());
            return;
        }
    }

    auto maybe_is_encrypted = is_tablet_encrypted(tablet);
    if (maybe_is_encrypted.has_value()) {
        HttpChannel::send_reply(
                req, HttpStatus::OK,
                maybe_is_encrypted.value() ? "all encrypted" : "some are not encrypted");
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                            maybe_is_encrypted.error().to_json());
}

} // namespace doris
