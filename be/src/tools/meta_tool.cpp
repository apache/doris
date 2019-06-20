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

#include <string>
#include <iostream>
#include <set>
#include <sstream>
#include <boost/filesystem.hpp>
#include <gflags/gflags.h>

#include "common/status.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/data_dir.h"
#include "olap/tablet_meta_manager.h"
#include "olap/olap_define.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"
#include "json2pb/pb_to_json.h"

using boost::filesystem::canonical;
using boost::filesystem::path;
using doris::DataDir;
using doris::OLAP_SUCCESS;
using doris::OlapMeta;
using doris::OLAPStatus;
using doris::Status;
using doris::TabletMeta;
using doris::TabletMetaManager;

const std::string HEADER_PREFIX = "tabletmeta_";

DEFINE_string(root_path, "", "storage root path");
DEFINE_string(operation, "get_meta",
              "valid operation: get_meta, flag, load_meta, delete_meta, show_meta");
DEFINE_int64(tablet_id, 0, "tablet_id for tablet meta");
DEFINE_int32(schema_hash, 0, "schema_hash for tablet meta");
DEFINE_string(json_meta_path, "", "absolute json meta file path");
DEFINE_string(pb_meta_path, "", "pb meta file path");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris BE Meta tool.\n";
    ss << "Stop BE first before use this tool.\n";
    ss << "Usage:\n";
    ss << "./meta_tool --operation=get_meta --root_path=/path/to/storage/path --tablet_id=tabletid --schema_hash=schemahash\n";
    ss << "./meta_tool --operation=load_meta --root_path=/path/to/storage/path --json_meta_path=path\n";
    ss << "./meta_tool --operation=delete_meta --root_path=/path/to/storage/path --tablet_id=tabletid --schema_hash=schemahash\n";
    ss << "./meta_tool --operation=show_meta --pb_meta_path=path\n";
    return ss.str();
}

void show_meta() {
    TabletMeta tablet_meta;
    OLAPStatus s = tablet_meta.create_from_file(FLAGS_pb_meta_path);
    if (s != OLAP_SUCCESS){
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

void get_meta(DataDir *data_dir) {
    std::string value;
    OLAPStatus s = TabletMetaManager::get_json_meta(data_dir, FLAGS_tablet_id, FLAGS_schema_hash, &value);
    if (s == doris::OLAP_ERR_META_KEY_NOT_FOUND) {
        std::cout << "no tablet meta for tablet_id:" << FLAGS_tablet_id
                  << ", schema_hash:" << FLAGS_schema_hash << std::endl;
        return;
    }
    std::cout << value << std::endl;
}

void load_meta(DataDir *data_dir) {
    // load json tablet meta into meta
    OLAPStatus s = TabletMetaManager::load_json_meta(data_dir, FLAGS_json_meta_path);
    if (s != OLAP_SUCCESS) {
        std::cout << "load meta failed, status:" << s << std::endl;
        return;
    }
    std::cout << "load meta successfully" << std::endl;
}

void delete_meta(DataDir *data_dir) {
    OLAPStatus s = TabletMetaManager::remove(data_dir, FLAGS_tablet_id, FLAGS_schema_hash);
    if (s != OLAP_SUCCESS) {
        std::cout << "delete tablet meta failed for tablet_id:" << FLAGS_tablet_id
                  << ", schema_hash:" << FLAGS_schema_hash
                  << ", status:" << s << std::endl;
        return;
    }
    std::cout << "delete meta successfully" << std::endl;
}

int main(int argc, char **argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "show_meta") {
        show_meta();
    }
    else {
        // operations that need root path should be written here
        std::set<std::string> valid_operations = {
            "get_meta",
            "load_meta",
            "delete_meta"
        };
        if (valid_operations.find(FLAGS_operation) == valid_operations.end()) {
            std::cout << "invalid operation:" << FLAGS_operation << std::endl;
            return -1;
        }

        path root_path(FLAGS_root_path);
        try {
            root_path = canonical(root_path);
        }
        catch (...) {
            std::cout << "invalid root path:" << FLAGS_root_path << std::endl;
            return -1;
        }

        std::unique_ptr<DataDir> data_dir(new (std::nothrow) DataDir(root_path.string()));
        if (data_dir == nullptr) {
            std::cout << "new data dir failed" << std::endl;
            return -1;
        }
        Status st = data_dir->init();
        if (!st.ok()) {
            std::cout << "data_dir load failed" << std::endl;
            return -1;
        }

        if (FLAGS_operation == "get_meta") {
            get_meta(data_dir.get());
        } else if (FLAGS_operation == "load_meta") {
            load_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_meta") {
            delete_meta(data_dir.get());
        } else {
            std::cout << "invalid operation:" << FLAGS_operation << "\n"
                    << usage << std::endl;
            return -1;
        }
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}
