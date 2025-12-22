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

#include <gen_cpp/cloud.pb.h>
#include <gflags/gflags.h>

#include <iostream>
#include <string>

#include "common/status.h"
#include "io/fs/packed_file_trailer.h"
#include "json2pb/pb_to_json.h"

DEFINE_string(file, "", "Path to a local merge/packed file");

int main(int argc, char** argv) {
    google::SetUsageMessage(
            "Dump packed file trailer for debugging.\n"
            "Usage: packed_file_tool --file=/path/to/merge_file");
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_file.empty()) {
        std::cerr << "Flag --file is required\n";
        return -1;
    }

    doris::cloud::PackedFileFooterPB debug_info;
    uint32_t version = 0;
    doris::Status st = doris::io::read_packed_file_trailer(FLAGS_file, &debug_info, &version);
    if (!st.ok()) {
        std::cerr << "Failed to read packed file trailer: " << st.to_string() << std::endl;
        return -1;
    }

    json2pb::Pb2JsonOptions options;
    options.pretty_json = true;
    std::string json;
    json2pb::ProtoMessageToJson(debug_info, &json, options);

    std::cout << "trailer_version: " << version << "\n" << json << std::endl;
    return 0;
}
