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

#include <cstdio>
#include <string>
#include <iostream>
#include <unistd.h>
#include <linux/limits.h>
#include <boost/algorithm/string/trim.hpp>
#include <gflags/gflags.h>

#include "common/status.h"
#include "olap/store.h"
#include "olap/olap_header_manager.h"
#include "olap/olap_define.h"
#include "olap/olap_header.h"
#include "olap/olap_meta.h"
#include "olap/utils.h"
#include "json2pb/pb_to_json.h"

using doris::OlapStore;
using doris::OlapMeta;
using doris::OlapHeaderManager;
using doris::OLAPHeader;
using doris::OLAPStatus;
using doris::OLAP_SUCCESS;
using doris::Status;

const std::string HEADER_PREFIX = "hdr_";

DEFINE_string(root_path, "./", "storage root path");
DEFINE_string(operation, "get_header",
        "valid operation: get_header, flag, load_header, delete_header, rollback, show_header");
DEFINE_int64(tablet_id, 0, "tablet_id for header operation");
DEFINE_int32(schema_hash, 0, "schema_hash for header operation");
DEFINE_string(json_header_path, "", "json header file path");
DEFINE_string(pb_header_path, "", "pb header file path");

void print_usage(std::string progname) {
    std::cout << progname << " is the Doris File tool." << std::endl;
    std::cout << "Usage:" << std::endl;
    std::cout << "./meta_tool --operation=get_header --tablet_id=tabletid --schema_hash=schemahash" << std::endl;
    std::cout << "./meta_tool --operation=flag" << std::endl;
    std::cout << "./meta_tool --operation=load_header --json_header_path=path" << std::endl;
    std::cout << "./meta_tool --operation=delete_header --tablet_id=tabletid --schema_hash=schemahash" << std::endl;
    std::cout << "./meta_tool --root_path=rootpath --operation=rollback" << std::endl;
    std::cout << "./meta_tool --operation=show_header --pb_header_path=path" << std::endl;
}

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    std::string root_path = FLAGS_root_path;
    if (FLAGS_root_path == "") {
        std::cout << "empty root path" << std::endl;
        print_usage(argv[0]);
        return -1;
    } else if (FLAGS_root_path.find("/") != 0) {
        // relative path
        char dir[PATH_MAX] = {0};
        readlink("/proc/self/exe", dir, PATH_MAX);
        std::string path_prefix(dir);
        path_prefix = path_prefix.substr(0, path_prefix.rfind("/") + 1);
        std::string root_path_postfix = FLAGS_root_path;
        // trim tailing /
        if (root_path_postfix.rfind("/") == (root_path_postfix.size() -1)) {
            root_path_postfix = root_path_postfix.substr(0, root_path_postfix.size() -1);
        }

        root_path = path_prefix + root_path_postfix;
    }
    std::unique_ptr<OlapStore> store(new(std::nothrow) OlapStore(root_path));
    if (store.get() == NULL) {
        std::cout << "new store failed" << std::endl;
        return -1;
    }
    Status st = store->load();
    if (!st.ok()) {
        std::cout << "store load failed" << std::endl;
        return -1;
    }

    if (FLAGS_operation == "get_header") {
        std::string value;
        OLAPStatus s = OlapHeaderManager::get_json_header(store.get(), FLAGS_tablet_id, FLAGS_schema_hash, &value);
        if (s == doris::OLAP_ERR_META_KEY_NOT_FOUND) {
            std::cout << "no header for tablet_id:" << FLAGS_tablet_id
                    << " schema_hash:" << FLAGS_schema_hash;
            return 0;
        }
        std::cout << value << std::endl;
    } else if (FLAGS_operation == "flag") {
        bool converted = false;
        OLAPStatus s = OlapHeaderManager::get_header_converted(store.get(), converted);
        if (s != OLAP_SUCCESS) {
            std::cout << "get header converted flag failed" << std::endl;
            return -1;
        }
        std::cout << "is_header_converted is " << converted << std::endl;
    } else if (FLAGS_operation == "load_header") {
        OLAPStatus s = OlapHeaderManager::load_json_header(store.get(), FLAGS_json_header_path);
        if (s != OLAP_SUCCESS) {
            std::cout << "load header failed" << std::endl;
            return -1;
        }
        std::cout << "load header successfully" << std::endl;
    } else if (FLAGS_operation == "delete_header") {
        OLAPStatus s = OlapHeaderManager::remove(store.get(), FLAGS_tablet_id, FLAGS_schema_hash);
        if (s != OLAP_SUCCESS) {
            std::cout << "delete header failed for tablet_id:" << FLAGS_tablet_id
                    << " schema_hash:" << FLAGS_schema_hash << std::endl;
            return -1;
        }
        std::cout << "delete header successfully" << std::endl;
    } else if (FLAGS_operation == "rollback") {
        auto rollback_func = [&root_path](long tablet_id,
                long schema_hash, const std::string& value) -> bool {
            OLAPHeader olap_header;
            bool parsed = olap_header.ParseFromString(value);
            if (!parsed) {
                std::cout << "parse header failed";
                return true;
            }
            std::string tablet_id_str = std::to_string(tablet_id);
            std::string schema_hash_path = root_path + "/data/" + std::to_string(olap_header.shard())
                    + "/" + tablet_id_str + "/" + std::to_string(schema_hash);
            std::string header_file_path = schema_hash_path + "/" + tablet_id_str + ".hdr";
            std::cout << "save header to path:" << header_file_path << std::endl;
            OLAPStatus s = olap_header.save(header_file_path);
            if (s != OLAP_SUCCESS) {
                std::cout << "save header file to path:" << header_file_path << " failed" << std::endl;
            }
            return true;
        };
        OlapHeaderManager::traverse_headers(store->get_meta(), rollback_func);
    } else if (FLAGS_operation == "show_header") {
        OLAPHeader header(FLAGS_pb_header_path);
        OLAPStatus s = header.load_and_init();
        if (s != OLAP_SUCCESS) {
            std::cout << "load pb header file:" << FLAGS_pb_header_path << " failed" << std::endl;
            return -1;
        }
        std::string json_header;
        json2pb::Pb2JsonOptions json_options;
        json_options.pretty_json = true;
        json2pb::ProtoMessageToJson(header, &json_header, json_options);
        std::cout << "header:" << std::endl;
        std::cout << json_header << std::endl;
    } else {
        std::cout << "invalid operation:" << FLAGS_operation << std::endl;
        print_usage(argv[0]);
        return -1;
    }
    return 0;
}
