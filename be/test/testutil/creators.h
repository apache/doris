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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/Types_types.h>

#include <memory>
#include <utility>
#include <vector>

#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "runtime/types.h"
#include "util/uid_util.h"

namespace doris {
using namespace pipeline;
inline std::shared_ptr<QueryContext> generate_one_query(const TQueryOptions& options) {
    TNetworkAddress fe_address;
    fe_address.hostname = "127.0.0.1";
    fe_address.port = 8060;
    auto query_context = QueryContext::create_shared(generate_uuid(), ExecEnv::GetInstance(),
                                                     options, TNetworkAddress {}, true, fe_address,
                                                     QuerySource::INTERNAL_FRONTEND);
    return query_context;
}

inline std::shared_ptr<QueryContext> generate_one_query() {
    TQueryOptions query_options;
    query_options.query_type = TQueryType::SELECT;
    query_options.mem_limit = 1024L * 1024 * 128;
    query_options.query_slot_count = 1;
    return generate_one_query(query_options);
}

inline std::pair<pipeline::PipelinePtr, pipeline::PipelinePtr> generate_hash_join_pipeline(
        std::shared_ptr<OperatorXBase> probe_operator,
        pipeline::DataSinkOperatorPtr probe_side_sink_operator, DataSinkOperatorPtr sink_operator,
        std::shared_ptr<OperatorXBase> build_side_source) {
    auto probe_pipeline = std::make_shared<pipeline::Pipeline>(0, 1, 1);
    auto build_pipeline = std::make_shared<pipeline::Pipeline>(1, 1, 1);

    static_cast<void>(probe_pipeline->add_operator(probe_operator, 1));
    static_cast<void>(probe_pipeline->set_sink(probe_side_sink_operator));
    static_cast<void>(build_pipeline->add_operator(build_side_source, 1));
    static_cast<void>(build_pipeline->set_sink(sink_operator));

    return {probe_pipeline, build_pipeline};
}

inline std::pair<pipeline::PipelinePtr, pipeline::PipelinePtr> generate_agg_pipeline(
        std::shared_ptr<OperatorXBase> source_operator,
        pipeline::DataSinkOperatorPtr source_side_sink_operator, DataSinkOperatorPtr sink_operator,
        std::shared_ptr<OperatorXBase> sink_side_source) {
    auto source_pipeline = std::make_shared<pipeline::Pipeline>(0, 1, 1);
    auto sink_pipeline = std::make_shared<pipeline::Pipeline>(1, 1, 1);

    static_cast<void>(source_pipeline->add_operator(source_operator, 1));
    static_cast<void>(source_pipeline->set_sink(source_side_sink_operator));
    static_cast<void>(sink_pipeline->add_operator(sink_side_source, 1));
    static_cast<void>(sink_pipeline->set_sink(sink_operator));

    return {source_pipeline, sink_pipeline};
}

inline std::pair<pipeline::PipelinePtr, pipeline::PipelinePtr> generate_sort_pipeline(
        std::shared_ptr<OperatorXBase> source_operator,
        pipeline::DataSinkOperatorPtr source_side_sink_operator, DataSinkOperatorPtr sink_operator,
        std::shared_ptr<OperatorXBase> sink_side_source) {
    return generate_agg_pipeline(source_operator, source_side_sink_operator, sink_operator,
                                 sink_side_source);
}

inline std::unique_ptr<SpillPartitionerType> create_spill_partitioner(
        RuntimeState* state, const int32_t partition_count, const std::vector<TExpr>& exprs,
        const RowDescriptor& row_desc) {
    auto partitioner = std::make_unique<SpillPartitionerType>(partition_count);
    auto st = partitioner->init(exprs);
    DCHECK(st.ok()) << "init partitioner failed: " << st.to_string();
    st = partitioner->prepare(state, row_desc);
    DCHECK(st.ok()) << "prepare partitioner failed: " << st.to_string();
    return partitioner;
}

} // namespace doris