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

#include <gen_cpp/Data_types.h>
#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <vector>

#include "common/factory_creator.h"
#include "common/global_types.h"
#include "common/status.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class TFetchSchemaTableDataRequest;
class TMetaScanRange;
class TScanRange;
class TScanRangeParams;
class TupleDescriptor;

namespace vectorized {
class Block;
class VExprContext;
class VMetaScanNode;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class VMetaScanner : public VScanner {
    ENABLE_FACTORY_CREATOR(VMetaScanner);

public:
    VMetaScanner(RuntimeState* state, VMetaScanNode* parent, int64_t tuple_id,
                 const TScanRangeParams& scan_range, int64_t limit, RuntimeProfile* profile,
                 TUserIdentity user_identity);

    VMetaScanner(RuntimeState* state, pipeline::ScanLocalStateBase* local_state, int64_t tuple_id,
                 const TScanRangeParams& scan_range, int64_t limit, RuntimeProfile* profile,
                 TUserIdentity user_identity);

    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts);

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;

private:
    Status _fill_block_with_remote_data(const std::vector<MutableColumnPtr>& columns);
    Status _fetch_metadata(const TMetaScanRange& meta_scan_range);
    Status _build_iceberg_metadata_request(const TMetaScanRange& meta_scan_range,
                                           TFetchSchemaTableDataRequest* request);
    Status _build_backends_metadata_request(const TMetaScanRange& meta_scan_range,
                                            TFetchSchemaTableDataRequest* request);
    Status _build_frontends_metadata_request(const TMetaScanRange& meta_scan_range,
                                             TFetchSchemaTableDataRequest* request);
    Status _build_frontends_disks_metadata_request(const TMetaScanRange& meta_scan_range,
                                                   TFetchSchemaTableDataRequest* request);
    Status _build_workload_groups_metadata_request(const TMetaScanRange& meta_scan_range,
                                                   TFetchSchemaTableDataRequest* request);
    Status _build_workload_sched_policy_metadata_request(const TMetaScanRange& meta_scan_range,
                                                         TFetchSchemaTableDataRequest* request);
    Status _build_catalogs_metadata_request(const TMetaScanRange& meta_scan_range,
                                            TFetchSchemaTableDataRequest* request);
    Status _build_materialized_views_metadata_request(const TMetaScanRange& meta_scan_range,
                                                      TFetchSchemaTableDataRequest* request);
    Status _build_jobs_metadata_request(const TMetaScanRange& meta_scan_range,
                                        TFetchSchemaTableDataRequest* request);
    Status _build_tasks_metadata_request(const TMetaScanRange& meta_scan_range,
                                         TFetchSchemaTableDataRequest* request);
    Status _build_queries_metadata_request(const TMetaScanRange& meta_scan_range,
                                           TFetchSchemaTableDataRequest* request);
    bool _meta_eos;
    TupleId _tuple_id;
    TUserIdentity _user_identity;
    const TupleDescriptor* _tuple_desc = nullptr;
    std::vector<TRow> _batch_data;
    const TScanRange& _scan_range;
};
} // namespace doris::vectorized
