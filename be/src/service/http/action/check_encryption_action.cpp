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

#include "service/http/action/check_encryption_action.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <google/protobuf/util/json_util.h>
#include <json2pb/pb_to_json.h>

#include <cstdint>
#include <exception>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "runtime/exec_env.h"
#include "service/http/action/action_constants.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_status.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {

const std::string GET_FOOTER = "get_footer";

// An encrypted file ends with a fixed-size footer region laid out as
// [1 byte][uint64 encryption info length][encryption info][padding][8 byte magic code].
constexpr size_t ENCRYPTION_FOOTER_SIZE = 256;
constexpr size_t MAX_ENCRYPTION_INFO_SIZE =
        ENCRYPTION_FOOTER_SIZE - sizeof(uint8_t) - sizeof(uint64_t);

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
        // Must not be `physical_fs()`: packed rowsets keep their segments as slices inside a
        // shared object, and only the `PackedFileSystem` wrapper can resolve a segment path to
        // that slice. `fs()` is not an option either, since it decrypts and would hide the
        // encryption footer this check is looking for.
        auto fs = rs_meta->packed_physical_fs();
        if (fs == nullptr) {
            st = Status::InternalError("failed to get fs for rowset: tablet={}, rs={}",
                                       tablet->tablet_id(), rs->rowset_id().to_string());
            return;
        }

        if (rs->num_segments() == 0) {
            return;
        }
        auto maybe_seg_path = rs->segment(0).path();
        if (!maybe_seg_path) {
            st = std::move(maybe_seg_path.error());
            return;
        }

        // Owning strings: the V2 index path is built here and must outlive the loop below.
        std::vector<std::string> file_paths;
        const auto& first_seg_path = maybe_seg_path.value();
        file_paths.emplace_back(first_seg_path);
        if (tablet->tablet_schema()->has_inverted_index() &&
            tablet->tablet_schema()->get_inverted_index_storage_format() == V2) {
            file_paths.emplace_back(InvertedIndexDescriptor::get_index_file_path_v2(
                    InvertedIndexDescriptor::get_index_file_path_prefix(first_seg_path)));
        }

        for (const auto& path : file_paths) {
            io::FileReaderSPtr reader;
            st = fs->open_file(path, &reader);
            if (!st) {
                return;
            }
            if (reader->size() < sizeof(uint64_t)) {
                st = Status::Corruption("file is too small to hold a magic code: path={}, size={}",
                                        path, reader->size());
                return;
            }
            std::vector<uint8_t> magic_code_buf;
            magic_code_buf.resize(sizeof(uint64_t));
            Slice magic_code(magic_code_buf.data(), sizeof(uint64_t));
            size_t bytes_read;
            st = reader->read_at(reader->size() - sizeof(uint64_t), magic_code, &bytes_read);
            if (!st) {
                return;
            }
            if (bytes_read != magic_code.size) {
                st = Status::Corruption(
                        "short read of the magic code: path={}, expected={}, got={}", path,
                        magic_code.size, bytes_read);
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

Result<std::string> get_last_encrypt_footer(const BaseTabletSPtr& tablet) {
    std::shared_lock l(tablet->get_header_lock());
    auto rs = tablet->get_rowset_with_max_version();
    if (rs->num_segments() == 0) {
        return "{}";
    }
    auto maybe_seg_path = rs->segment(0).path();
    if (!maybe_seg_path) {
        return ResultError(maybe_seg_path.error());
    }
    auto rs_meta = rs->rowset_meta();
    if (config::is_cloud_mode() && rs_meta->start_version() == 0 && rs_meta->end_version() == 1) {
        return "{}";
    }
    // See the comment in `is_tablet_encrypted()` for why this is neither `physical_fs()`
    // nor `fs()`.
    auto fs = rs_meta->packed_physical_fs();
    if (fs == nullptr) {
        return ResultError(Status::InternalError("failed to get fs for rowset: tablet={}, rs={}",
                                                 tablet->tablet_id(), rs->rowset_id().to_string()));
    }
    io::FileReaderSPtr reader;
    RETURN_IF_ERROR_RESULT(fs->open_file(maybe_seg_path.value(), &reader));

    // Every offset below is relative to the end of the file, so a file shorter than the footer
    // region makes them underflow. On a packed slice such an underflow does not fail the read:
    // it wraps into a neighbouring slice, whose bytes would then be decoded as this segment's
    // footer.
    if (reader->size() < ENCRYPTION_FOOTER_SIZE) {
        return ResultError(Status::Corruption(
                "file is too small to hold an encryption footer: path={}, size={}",
                maybe_seg_path.value(), reader->size()));
    }
    const size_t footer_offset = reader->size() - ENCRYPTION_FOOTER_SIZE;

    std::vector<uint8_t> pb_len_buf;
    pb_len_buf.resize(sizeof(uint64_t));
    Slice pb_len_slice(pb_len_buf.data(), sizeof(uint64_t));
    size_t bytes_read;
    RETURN_IF_ERROR_RESULT(
            reader->read_at(footer_offset + sizeof(uint8_t), pb_len_slice, &bytes_read));
    if (bytes_read != pb_len_slice.size) {
        return ResultError(Status::Corruption(
                "short read of the encryption info length: path={}, expected={}, got={}",
                maybe_seg_path.value(), pb_len_slice.size, bytes_read));
    }
    auto info_pb_size = decode_fixed64_le(pb_len_buf.data());

    // `get_footer=true` reaches this parser even when the scan above found unencrypted files, so
    // the decoded length may well be arbitrary bytes. It can never exceed what the footer holds.
    if (info_pb_size > MAX_ENCRYPTION_INFO_SIZE) {
        return ResultError(Status::Corruption(
                "encryption info length {} exceeds the {} bytes available in the footer: path={}",
                info_pb_size, MAX_ENCRYPTION_INFO_SIZE, maybe_seg_path.value()));
    }

    std::vector<uint8_t> info_pb_buf;
    info_pb_buf.resize(info_pb_size);
    Slice pb_slice(info_pb_buf.data(), info_pb_size);
    RETURN_IF_ERROR_RESULT(reader->read_at(footer_offset + sizeof(uint8_t) + sizeof(uint64_t),
                                           pb_slice, &bytes_read));
    if (bytes_read != pb_slice.size) {
        return ResultError(Status::Corruption(
                "short read of the encryption info: path={}, expected={}, got={}",
                maybe_seg_path.value(), pb_slice.size, bytes_read));
    }

    FileEncryptionInfoPB info_pb;
    if (!info_pb.ParseFromArray(info_pb_buf.data(), static_cast<int>(info_pb_buf.size()))) {
        return ResultError(Status::Corruption("parse encryption info failed"));
    }
    std::string json;
    google::protobuf::util::JsonPrintOptions opts;
    opts.add_whitespace = false;
    opts.preserve_proto_field_names = true;
    auto st = google::protobuf::util::MessageToJsonString(info_pb, &json, opts);
    return json;
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
    int64_t tablet_id = -1;
    try {
        tablet_id = std::stoll(tablet_id_str);
    } catch (const std::exception& e) {
        LOG(WARNING) << "convert tablet id to i64 failed:" << e.what();
        auto msg = fmt::format("invalid argument: tablet_id={}", tablet_id_str);

        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, msg);
        return;
    }

    bool is_get_footer = false;
    if (auto get_footer_flag = req->param(GET_FOOTER); get_footer_flag == "true") {
        is_get_footer = true;
    } else if (get_footer_flag != "false") {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                "param `get_footer` must be a boolean type");
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
        req->add_output_header(HttpHeaders::CONTENT_TYPE, HttpHeaders::JSON_TYPE.data());
        std::string result = R"({"status":)";
        result += maybe_is_encrypted.value() ? R"("all encrypted")" : R"("some are not encrypted")";
        if (is_get_footer) {
            auto maybe_footer = get_last_encrypt_footer(tablet);
            if (!maybe_footer) {
                HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                        maybe_footer.error().to_json());
                return;
            }
            result += R"(,"footer":)";
            result += maybe_footer.value();
        }
        result += "}";

        HttpChannel::send_reply(req, HttpStatus::OK, result);
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                            maybe_is_encrypted.error().to_json());
}

} // namespace doris
