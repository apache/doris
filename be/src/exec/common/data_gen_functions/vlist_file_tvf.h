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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "cpp/obj-client/obj_storage_client.h"
#include "exec/common/data_gen_functions/vdata_gen_function_inf.h"

namespace doris {
namespace io {
class S3FileSystem;
}

// One provider page and the current output batch bound listing memory, independently of directory
// size. A populated page is returned before fetching the next one so the operator can apply LIMIT.
class VListFileTVF final : public VDataGenFunctionInf {
public:
    VListFileTVF(TupleId tuple_id, const TupleDescriptor* tuple_desc);
    ~VListFileTVF() override;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

private:
    Status _fetch_next_page(RuntimeState* state);

    std::vector<size_t> _column_indices;
    std::map<std::string, std::string> _properties;
    ObjStoragePath _path;
    std::shared_ptr<io::S3FileSystem> _filesystem;
    std::vector<ObjectMeta> _objects;
    std::string _continuation_token;
    size_t _next_index = 0;
    bool _has_more = true;
};

} // namespace doris
