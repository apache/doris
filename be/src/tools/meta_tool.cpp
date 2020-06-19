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

#include <iostream>
#include <set>
#include <sstream>
#include <string>

#include <boost/filesystem.hpp>
#include <gflags/gflags.h>

#include "common/status.h"
#include "env/env.h" 
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "json2pb/pb_to_json.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/tablet_meta_manager.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_utils.h"

using boost::filesystem::path;
using doris::DataDir;
using doris::OLAP_SUCCESS;
using doris::OlapMeta;
using doris::OLAPStatus;
using doris::Status;
using doris::TabletMeta;
using doris::TabletMetaManager;
using doris::FileUtils;
using doris::Slice;
using doris::RandomAccessFile;
using strings::Substitute;
using doris::segment_v2::SegmentFooterPB;
using doris::segment_v2::ColumnReader;
using doris::segment_v2::BinaryPlainPageDecoder;
using doris::segment_v2::PageHandle;
using doris::segment_v2::PagePointer;
using doris::segment_v2::ColumnReaderOptions;
using doris::segment_v2::ColumnIteratorOptions;
using doris::segment_v2::PageFooterPB;

const std::string HEADER_PREFIX = "tabletmeta_";

DEFINE_string(root_path, "", "storage root path");
DEFINE_string(
    operation, "get_meta",
    "valid operation: get_meta, flag, load_meta, delete_meta, show_meta");
DEFINE_int64(tablet_id, 0, "tablet_id for tablet meta");
DEFINE_int32(schema_hash, 0, "schema_hash for tablet meta");
DEFINE_string(json_meta_path, "", "absolute json meta file path");
DEFINE_string(pb_meta_path, "", "pb meta file path");
DEFINE_string(tablet_file, "", "file to save a set of tablets");
DEFINE_string(file, "", "segment file path");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris BE Meta tool.\n";
    ss << "Stop BE first before use this tool.\n";
    ss << "Usage:\n";
    ss << "./meta_tool --operation=get_meta --root_path=/path/to/storage/path "
          "--tablet_id=tabletid --schema_hash=schemahash\n";
    ss << "./meta_tool --operation=load_meta --root_path=/path/to/storage/path "
          "--json_meta_path=path\n";
    ss << "./meta_tool --operation=delete_meta "
          "--root_path=/path/to/storage/path --tablet_id=tabletid "
          "--schema_hash=schemahash\n";
    ss << "./meta_tool --operation=delete_meta --tablet_file=file_path\n";
    ss << "./meta_tool --operation=show_meta --pb_meta_path=path\n";
    ss << "./meta_tool --operation=show_segment_footer --file=/path/to/segment/file\n";
    return ss.str();
}

void show_meta() {
    TabletMeta tablet_meta;
    OLAPStatus s = tablet_meta.create_from_file(FLAGS_pb_meta_path);
    if (s != OLAP_SUCCESS) {
        std::cout << "load pb meta file:" << FLAGS_pb_meta_path << " failed"
                  << ", status:" << s << std::endl;
        return;
    }
    std::string json_meta;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    doris::TabletMetaPB tablet_meta_pb;
    tablet_meta.to_meta_pb(&tablet_meta_pb);
    json2pb::ProtoMessageToJson(tablet_meta_pb, &json_meta, json_options);
    std::cout << json_meta << std::endl;
}

void get_meta(DataDir* data_dir) {
    std::string value;
    OLAPStatus s = TabletMetaManager::get_json_meta(data_dir, FLAGS_tablet_id,
                                                    FLAGS_schema_hash, &value);
    if (s == doris::OLAP_ERR_META_KEY_NOT_FOUND) {
        std::cout << "no tablet meta for tablet_id:" << FLAGS_tablet_id
                  << ", schema_hash:" << FLAGS_schema_hash << std::endl;
        return;
    }
    std::cout << value << std::endl;
}

void load_meta(DataDir* data_dir) {
    // load json tablet meta into meta
    OLAPStatus s =
        TabletMetaManager::load_json_meta(data_dir, FLAGS_json_meta_path);
    if (s != OLAP_SUCCESS) {
        std::cout << "load meta failed, status:" << s << std::endl;
        return;
    }
    std::cout << "load meta successfully" << std::endl;
}

void delete_meta(DataDir* data_dir) {
    OLAPStatus s =
        TabletMetaManager::remove(data_dir, FLAGS_tablet_id, FLAGS_schema_hash);
    if (s != OLAP_SUCCESS) {
        std::cout << "delete tablet meta failed for tablet_id:"
                  << FLAGS_tablet_id << ", schema_hash:" << FLAGS_schema_hash
                  << ", status:" << s << std::endl;
        return;
    }
    std::cout << "delete meta successfully" << std::endl;
}

Status init_data_dir(const std::string& dir, std::unique_ptr<DataDir>* ret) {
    std::string root_path;
    Status st = FileUtils::canonicalize(dir, &root_path);
    if (!st.ok()) {
        std::cout << "invalid root path:" << FLAGS_root_path
            << ", error: " << st.to_string() << std::endl;
        return Status::InternalError("invalid root path");
    }
    doris::StorePath path;
    auto res = parse_root_path(root_path, &path);
    if (res != OLAP_SUCCESS) {
        std::cout << "parse root path failed:" << root_path << std::endl;
        return Status::InternalError("parse root path failed");
    }

    std::unique_ptr<DataDir> p(new (std::nothrow) DataDir(path.path, path.capacity_bytes, path.storage_medium));
    if (p == nullptr) {
        std::cout << "new data dir failed" << std::endl;
        return Status::InternalError("new data dir failed");
    }
    st = p->init();
    if (!st.ok()) {
        std::cout << "data_dir load failed" << std::endl;
        return Status::InternalError("data_dir load failed");
    }

    p.swap(*ret);
    return Status::OK();
}

void batch_delete_meta(const std::string& tablet_file) {
    // each line in tablet file indicate a tablet to delete, format is:
    //      data_dir,tablet_id,schema_hash
    // eg:
    //      /data1/palo.HDD,100010,11212389324
    //      /data2/palo.HDD,100010,23049230234
    std::ifstream infile(tablet_file);
    std::string line = "";
    int err_num = 0;
    int delete_num = 0;
    int total_num = 0;
    std::unordered_map<std::string, std::unique_ptr<DataDir>> dir_map;
    while (std::getline(infile, line)) {
        total_num++;
        vector<string> v = strings::Split(line, ",");
        if (v.size() != 3) {
            std::cout << "invalid line in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }
        // 1. get dir
        std::string dir;
        Status st = FileUtils::canonicalize(v[0], &dir);
        if (!st.ok()) {
            std::cout << "invalid root dir in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }

        if (dir_map.find(dir) == dir_map.end()) {
            // new data dir, init it
            std::unique_ptr<DataDir> data_dir_p;
            Status st = init_data_dir(dir, &data_dir_p);
            if (!st.ok()) {
                std::cout << "invalid root path:" << FLAGS_root_path
                    << ", error: " << st.to_string() << std::endl;
                err_num++;
                continue;
            }
            dir_map[dir] = std::move(data_dir_p);
            std::cout << "get a new data dir: " << dir << std::endl;
        }
        DataDir* data_dir = dir_map[dir].get();
        if (data_dir == nullptr) {
            std::cout << "failed to get data dir: " << line << std::endl;
            err_num++;
            continue;
        }

        // 2. get tablet id/schema_hash
        int64_t tablet_id;
        if (!safe_strto64(v[1].c_str(), &tablet_id)) {
            std::cout << "invalid tablet id: " << line << std::endl;
            err_num++;
            continue;
        }
        int64_t schema_hash;
        if (!safe_strto64(v[2].c_str(), &schema_hash)) {
            std::cout << "invalid schema hash: " << line << std::endl;
            err_num++;
            continue;
        }

        OLAPStatus s = TabletMetaManager::remove(data_dir, tablet_id, schema_hash);
        if (s != OLAP_SUCCESS) {
            std::cout << "delete tablet meta failed for tablet_id:"
                << tablet_id << ", schema_hash:" << schema_hash
                << ", status:" << s << std::endl;
            err_num++;
            continue;
        }
        
        delete_num++;
    }

    std::cout << "total: " << total_num << ", delete: " << delete_num << ", error: " << err_num << std::endl;
    return;
}

Status get_segment_footer(RandomAccessFile* input_file, SegmentFooterPB* footer) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string file_name = input_file->file_name();
    uint64_t file_size;
    RETURN_IF_ERROR(input_file->size(&file_size));

    if (file_size < 12) {
        return Status::Corruption(Substitute("Bad segment file $0: file size $1 < 12", file_name, file_size));
    }

    uint8_t fixed_buf[12];
    RETURN_IF_ERROR(input_file->read_at(file_size - 12, Slice(fixed_buf, 12)));

    // validate magic number
    const char* k_segment_magic = "D0R1";
    const uint32_t k_segment_magic_length = 4; 
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption(Substitute("Bad segment file $0: magic number not match", file_name));
    }

    // read footer PB
    uint32_t footer_length = doris::decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption(
            Substitute("Bad segment file $0: file size $1 < $2", file_name, file_size, 12 + footer_length));
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(input_file->read_at(file_size - 12 - footer_length, footer_buf));

    // validate footer PB's checksum
    uint32_t expect_checksum = doris::decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = doris::crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
            Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2",
                       file_name, actual_checksum, expect_checksum));
    }

    // deserialize footer PB
    if (!footer->ParseFromString(footer_buf)) {
        return Status::Corruption(Substitute("Bad segment file $0: failed to parse SegmentFooterPB", file_name));
    }
    return Status::OK();
}

void show_segment_footer(const std::string& file_name) {
    std::unique_ptr<RandomAccessFile> input_file;
    Status status = doris::Env::Default()->new_random_access_file(file_name, &input_file);
    if (!status.ok()) {
        std::cout << "open file failed: " << status.to_string() << std::endl;
        return;
    }
    SegmentFooterPB footer;
    status = get_segment_footer(input_file.get(), &footer);
    if (!status.ok()) {
        std::cout << "get footer failed: " << status.to_string() << std::endl;
        return;
    }
    std::string json_footer;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool ret = json2pb::ProtoMessageToJson(footer, &json_footer, json_options);
    if (!ret) {
        std::cout << "Convert PB to json failed" << std::endl;
        return;
    }
    std::cout << json_footer << std::endl;
    return;
}

int main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "show_meta") {
        show_meta();
    } else if (FLAGS_operation == "batch_delete_meta") {
        std::string tablet_file;
        Status st = FileUtils::canonicalize(FLAGS_tablet_file, &tablet_file);
        if (!st.ok()) {
            std::cout << "invalid tablet file: " << FLAGS_tablet_file
                    << ", error: " << st.to_string() << std::endl;
            return -1;
        }

        batch_delete_meta(tablet_file);
    } else if (FLAGS_operation == "show_segment_footer") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for show dict" << std::endl;
            return -1;
        }
        show_segment_footer(FLAGS_file);
    } else {
        // operations that need root path should be written here
        std::set<std::string> valid_operations = {"get_meta", "load_meta",
                                                  "delete_meta"};
        if (valid_operations.find(FLAGS_operation) == valid_operations.end()) {
            std::cout << "invalid operation:" << FLAGS_operation << std::endl;
            return -1;
        }

        std::unique_ptr<DataDir> data_dir;
        Status st = init_data_dir(FLAGS_root_path, &data_dir);
        if (!st.ok()) {
            std::cout << "invalid root path:" << FLAGS_root_path
                      << ", error: " << st.to_string() << std::endl;
            return -1;
        }

        if (FLAGS_operation == "get_meta") {
            get_meta(data_dir.get());
        } else if (FLAGS_operation == "load_meta") {
            load_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_meta") {
            delete_meta(data_dir.get());
        } else {
            std::cout << "invalid operation: " << FLAGS_operation << "\n"
                      << usage << std::endl;
            return -1;
        }
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}
