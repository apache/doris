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

#include "gen_cpp/wtd_file.pb.h"
#include "olap/fs/fs_util.h"
#include "olap/memory/partial_row_batch.h"

namespace doris {
namespace memory {

class WTDReader {
public:
    WTDReader(fs::ReadableBlock* rblock);
    Status open();

    std::shared_ptr<PartialRowBatch> build_batch(scoped_refptr<Schema>* schema);

private:
    Status _parse_footer();

private:
    std::string _wtd_file_path;
    fs::ReadableBlock* _rblock;

    uint64_t _data_size = 0;
    wtd_file::FileFooterPB _footer;
};

} // namespace memory
} // namespace doris