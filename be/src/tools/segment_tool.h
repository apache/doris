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

#pragma once

#include <gflags/gflags.h>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "env/env.h" // RandomAccessFile

namespace doris {

class SegmentTool {
public:
    static void process();
private:
    static Status _show_meta(const std::string& file_name);
    static Status _show_dict(const std::string& file_name);
    static Status _get_segment_footer(RandomAccessFile* input_file, doris::segment_v2::SegmentFooterPB* footer);
    static void _print_help();
};

}
