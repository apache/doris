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

#include <brpc/controller.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/internal_service.pb.h>

#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/tablet_info.h" // DorisNodesInfo
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"

namespace doris {

class DorisNodesInfo;
class RuntimeState;
class TupleDescriptor;

namespace vectorized {
template <typename T>
class ColumnStr;
using ColumnString = ColumnStr<UInt32>;
class MutableBlock;
} // namespace vectorized

// fetch rows by global rowid
// tablet_id/rowset_name/segment_id/ordinal_id

struct FetchOption {
    TupleDescriptor* desc = nullptr;
    RuntimeState* runtime_state = nullptr;
    TFetchOption t_fetch_opt;
};

class RowIDFetcher {
public:
    RowIDFetcher(FetchOption fetch_opt) : _fetch_option(std::move(fetch_opt)) {}
    Status init();
    Status fetch(const vectorized::ColumnPtr& row_ids, vectorized::Block* block);

private:
    PMultiGetRequest _init_fetch_request(const vectorized::ColumnString& row_ids) const;
    Status _merge_rpc_results(const PMultiGetRequest& request,
                              const std::vector<PMultiGetResponse>& rsps,
                              const std::vector<brpc::Controller>& cntls,
                              vectorized::Block* output_block,
                              std::vector<PRowLocation>* rows_id) const;

    std::vector<std::shared_ptr<PBackendService_Stub>> _stubs;
    FetchOption _fetch_option;
};

class RowIdStorageReader {
public:
    static Status read_by_rowids(const PMultiGetRequest& request, PMultiGetResponse* response);
};

} // namespace doris
