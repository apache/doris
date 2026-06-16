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
#include <gtest/gtest.h>

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "exec/operator/operator.h"
#include "exec/operator/spill_utils.h"
#include "exec/pipeline/pipeline.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "storage/binlog.h"
#include "storage/tablet_info.h"
#include "util/uid_util.h"

namespace doris {
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

inline std::pair<PipelinePtr, PipelinePtr> generate_hash_join_pipeline(
        std::shared_ptr<OperatorXBase> probe_operator, DataSinkOperatorPtr probe_side_sink_operator,
        DataSinkOperatorPtr sink_operator, std::shared_ptr<OperatorXBase> build_side_source) {
    auto probe_pipeline = std::make_shared<Pipeline>(0, 1, 1);
    auto build_pipeline = std::make_shared<Pipeline>(1, 1, 1);

    static_cast<void>(probe_pipeline->add_operator(probe_operator, 1));
    static_cast<void>(probe_pipeline->set_sink(probe_side_sink_operator));
    static_cast<void>(build_pipeline->add_operator(build_side_source, 1));
    static_cast<void>(build_pipeline->set_sink(sink_operator));

    return {probe_pipeline, build_pipeline};
}

inline std::pair<PipelinePtr, PipelinePtr> generate_agg_pipeline(
        std::shared_ptr<OperatorXBase> source_operator,
        DataSinkOperatorPtr source_side_sink_operator, DataSinkOperatorPtr sink_operator,
        std::shared_ptr<OperatorXBase> sink_side_source) {
    auto source_pipeline = std::make_shared<Pipeline>(0, 1, 1);
    auto sink_pipeline = std::make_shared<Pipeline>(1, 1, 1);

    static_cast<void>(source_pipeline->add_operator(source_operator, 1));
    static_cast<void>(source_pipeline->set_sink(source_side_sink_operator));
    static_cast<void>(sink_pipeline->add_operator(sink_side_source, 1));
    static_cast<void>(sink_pipeline->set_sink(sink_operator));

    return {source_pipeline, sink_pipeline};
}

inline std::pair<PipelinePtr, PipelinePtr> generate_sort_pipeline(
        std::shared_ptr<OperatorXBase> source_operator,
        DataSinkOperatorPtr source_side_sink_operator, DataSinkOperatorPtr sink_operator,
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

namespace testutil {

struct DescriptorTableSlotDef {
    PrimitiveType type;
    std::string column_name;
    bool nullable = false;
};

struct CreateTabletRequestColumnDef {
    std::string column_name;
    TPrimitiveType::type type;
    bool is_key = false;
    bool has_aggregation_type = false;
    TAggregationType::type aggregation_type = TAggregationType::NONE;
};

// Append a ColumnPB to `schema_pb`. Currently only INT and STRING columns are supported.
inline ColumnPB* add_column_pb(TabletSchemaPB* schema_pb, int32_t unique_id,
                               const std::string& name, const std::string& type, bool is_key,
                               bool nullable) {
    ColumnPB* column = schema_pb->add_column();
    column->set_unique_id(unique_id);
    column->set_name(name);
    column->set_type(type);
    column->set_is_key(is_key);
    column->set_is_nullable(nullable);
    if (type == "INT") {
        column->set_length(4);
        column->set_index_length(4);
    } else if (type == "STRING") {
        column->set_length(65535);
        column->set_index_length(0);
    } else {
        DCHECK(false) << "add_column_pb only supports INT/STRING columns for now, got: " << type;
    }
    column->set_precision(0);
    column->set_frac(0);
    return column;
}

inline TabletMetaPB create_tablet_meta_pb(int64_t tablet_id, int32_t schema_hash,
                                          int64_t replica_id, int64_t table_id,
                                          int64_t partition_id) {
    TabletMetaPB tablet_meta_pb;
    tablet_meta_pb.set_tablet_id(tablet_id);
    tablet_meta_pb.set_schema_hash(schema_hash);
    tablet_meta_pb.set_replica_id(replica_id);
    tablet_meta_pb.set_table_id(table_id);
    tablet_meta_pb.set_partition_id(partition_id);
    return tablet_meta_pb;
}

inline TColumn create_tablet_column(const CreateTabletRequestColumnDef& column_def) {
    TColumn column;
    column.column_name = column_def.column_name;
    column.__set_is_key(column_def.is_key);
    column.column_type.type = column_def.type;
    if (column_def.has_aggregation_type) {
        column.__set_aggregation_type(column_def.aggregation_type);
    }
    return column;
}

inline TCreateTabletReq create_tablet_request(
        int64_t tablet_id, int32_t schema_hash, int64_t partition_id,
        int16_t short_key_column_count, TKeysType::type keys_type,
        std::initializer_list<CreateTabletRequestColumnDef> column_defs, int64_t version = 1,
        TStorageType::type storage_type = TStorageType::COLUMN,
        TStorageFormat::type storage_format = TStorageFormat::V2) {
    TCreateTabletReq request;
    request.tablet_id = tablet_id;
    request.__set_version(version);
    request.partition_id = partition_id;
    request.tablet_schema.schema_hash = schema_hash;
    request.tablet_schema.short_key_column_count = short_key_column_count;
    request.tablet_schema.keys_type = keys_type;
    request.tablet_schema.storage_type = storage_type;
    request.__set_storage_format(storage_format);
    request.tablet_schema.columns.reserve(column_defs.size());
    for (const auto& column_def : column_defs) {
        request.tablet_schema.columns.push_back(create_tablet_column(column_def));
    }
    return request;
}

inline void enable_row_binlog(TCreateTabletReq* request, int32_t row_binlog_schema_hash = 0) {
    DCHECK(request != nullptr);

    TBinlogConfig binlog_config;
    binlog_config.__set_enable(true);
    binlog_config.__set_binlog_format(TBinlogFormat::ROW);
    request->__set_binlog_config(binlog_config);

    TTabletSchema row_binlog_schema = request->tablet_schema;
    row_binlog_schema.schema_hash = row_binlog_schema_hash > 0
                                            ? row_binlog_schema_hash
                                            : request->tablet_schema.schema_hash + 1;
    row_binlog_schema.keys_type = TKeysType::DUP_KEYS;

    for (auto& col : row_binlog_schema.columns) {
        if (!col.is_key) {
            col.__set_aggregation_type(TAggregationType::NONE);
        }
    }

    row_binlog_schema.columns.push_back(
            create_tablet_column({std::string(kRowBinlogLsnColName), TPrimitiveType::LARGEINT,
                                  false, true, TAggregationType::NONE}));
    row_binlog_schema.columns.push_back(create_tablet_column(
            {"__DORIS_BINLOG_OP__", TPrimitiveType::BIGINT, false, true, TAggregationType::NONE}));
    row_binlog_schema.columns.push_back(
            create_tablet_column({std::string(kRowBinlogTimestampColName), TPrimitiveType::BIGINT,
                                  false, true, TAggregationType::NONE}));
    request->__set_row_binlog_schema(row_binlog_schema);
}

inline TDescriptorTable create_descriptor_table(
        std::initializer_list<DescriptorTableSlotDef> slot_defs) {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;
    int column_pos = 0;
    for (const auto& slot_def : slot_defs) {
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(slot_def.type)
                                       .nullable(slot_def.nullable)
                                       .column_name(slot_def.column_name)
                                       .column_pos(column_pos++)
                                       .build());
    }
    tuple_builder.build(&dtb);
    return dtb.desc_tbl();
}

inline std::shared_ptr<OlapTableSchemaParam> create_table_schema_param(
        const TDescriptorTable& tdesc_tbl, int64_t index_id, int32_t schema_hash,
        const std::vector<TColumn>& columns, int64_t row_binlog_index_id = -1,
        int32_t row_binlog_schema_hash = 0,
        const std::vector<TColumn>* row_binlog_columns = nullptr, int64_t db_id = 1,
        int64_t table_id = 2, int64_t version = 0) {
    auto param = std::make_shared<OlapTableSchemaParam>();
    TOlapTableSchemaParam tschema;
    tschema.db_id = db_id;
    tschema.table_id = table_id;
    tschema.version = version;
    tschema.slot_descs = tdesc_tbl.slotDescriptors;
    tschema.tuple_desc = tdesc_tbl.tupleDescriptors[0];
    tschema.indexes.resize(1);
    tschema.indexes[0].id = index_id;
    tschema.indexes[0].schema_hash = schema_hash;
    tschema.indexes[0].columns_desc = columns;
    for (const auto& col : columns) {
        tschema.indexes[0].columns.push_back(col.column_name);
    }
    if (row_binlog_index_id > 0) {
        tschema.indexes[0].__set_row_binlog_id(row_binlog_index_id);
        TOlapTableIndexSchema row_binlog_index_schema;
        row_binlog_index_schema.id = row_binlog_index_id;
        row_binlog_index_schema.schema_hash = row_binlog_schema_hash;
        if (row_binlog_columns != nullptr) {
            row_binlog_index_schema.columns_desc = *row_binlog_columns;
            for (const auto& col : *row_binlog_columns) {
                row_binlog_index_schema.columns.push_back(col.column_name);
            }
        }
        tschema.__set_row_binlog_index_schema(row_binlog_index_schema);
    }
    Status st = param->init(tschema);
    EXPECT_TRUE(st.ok()) << st;
    if (!st.ok()) {
        return nullptr;
    }
    return param;
}

} // namespace testutil

} // namespace doris
