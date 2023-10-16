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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/data-sink.cc
// and modified by Doris

#include "exec/data_sink.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <glog/logging.h>

#include <map>
#include <memory>
#include <ostream>
#include <string>

#include "common/config.h"
#include "vec/sink/async_writer_sink.h"
#include "vec/sink/multi_cast_data_stream_sink.h"
#include "vec/sink/vdata_stream_sender.h"
#include "vec/sink/vmemory_scratch_sink.h"
#include "vec/sink/vresult_file_sink.h"
#include "vec/sink/vresult_sink.h"
#include "vec/sink/vtablet_sink.h"
#include "vec/sink/vtablet_sink_v2.h"

namespace doris {
class DescriptorTbl;
class TExpr;

Status DataSink::create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                                  const std::vector<TExpr>& output_exprs,
                                  const TPlanFragmentExecParams& params,
                                  const RowDescriptor& row_desc, RuntimeState* state,
                                  std::unique_ptr<DataSink>* sink, DescriptorTbl& desc_tbl) {
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        if (!thrift_sink.__isset.stream_sink) {
            return Status::InternalError("Missing data stream sink.");
        }
        bool send_query_statistics_with_every_batch =
                params.__isset.send_query_statistics_with_every_batch
                        ? params.send_query_statistics_with_every_batch
                        : false;
        // TODO: figure out good buffer size based on size of output row
        sink->reset(new vectorized::VDataStreamSender(state, pool, params.sender_id, row_desc,
                                                      thrift_sink.stream_sink, params.destinations,
                                                      send_query_statistics_with_every_batch));
        // RETURN_IF_ERROR(sender->prepare(state->obj_pool(), thrift_sink.stream_sink));
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        if (!thrift_sink.__isset.result_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        // TODO: figure out good buffer size based on size of output row
        sink->reset(new doris::vectorized::VResultSink(row_desc, output_exprs,
                                                       thrift_sink.result_sink,
                                                       vectorized::RESULT_SINK_BUFFER_SIZE));
        break;
    }
    case TDataSinkType::RESULT_FILE_SINK: {
        if (!thrift_sink.__isset.result_file_sink) {
            return Status::InternalError("Missing result file sink.");
        }

        // TODO: figure out good buffer size based on size of output row
        bool send_query_statistics_with_every_batch =
                params.__isset.send_query_statistics_with_every_batch
                        ? params.send_query_statistics_with_every_batch
                        : false;
        // Result file sink is not the top sink
        if (params.__isset.destinations && params.destinations.size() > 0) {
            sink->reset(new doris::vectorized::VResultFileSink(
                    state, pool, params.sender_id, row_desc, thrift_sink.result_file_sink,
                    params.destinations, send_query_statistics_with_every_batch, output_exprs,
                    desc_tbl));
        } else {
            sink->reset(new doris::vectorized::VResultFileSink(row_desc, output_exprs));
        }
        break;
    }
    case TDataSinkType::MEMORY_SCRATCH_SINK: {
        if (!thrift_sink.__isset.memory_scratch_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        sink->reset(new vectorized::MemoryScratchSink(row_desc, output_exprs));
        break;
    }
    case TDataSinkType::MYSQL_TABLE_SINK: {
#ifdef DORIS_WITH_MYSQL
        if (!thrift_sink.__isset.mysql_table_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }
        vectorized::VMysqlTableSink* vmysql_tbl_sink =
                new vectorized::VMysqlTableSink(row_desc, output_exprs);
        sink->reset(vmysql_tbl_sink);
        break;
#else
        return Status::InternalError(
                "Don't support MySQL table, you should rebuild Doris with WITH_MYSQL option ON");
#endif
    }
    case TDataSinkType::ODBC_TABLE_SINK: {
        if (!thrift_sink.__isset.odbc_table_sink) {
            return Status::InternalError("Missing data odbc sink.");
        }
        sink->reset(new vectorized::VOdbcTableSink(row_desc, output_exprs));
        break;
    }

    case TDataSinkType::JDBC_TABLE_SINK: {
        if (!thrift_sink.__isset.jdbc_table_sink) {
            return Status::InternalError("Missing data jdbc sink.");
        }
        if (config::enable_java_support) {
            sink->reset(new vectorized::VJdbcTableSink(row_desc, output_exprs));
        } else {
            return Status::InternalError(
                    "Jdbc table sink is not enabled, you can change be config "
                    "enable_java_support to true and restart be.");
        }
        break;
    }

    case TDataSinkType::EXPORT_SINK: {
        RETURN_ERROR_IF_NON_VEC;
        break;
    }
    case TDataSinkType::OLAP_TABLE_SINK: {
        Status status = Status::OK();
        DCHECK(thrift_sink.__isset.olap_table_sink);
        OlapTableSchemaParam schema;
        schema.init(thrift_sink.olap_table_sink.schema);
        bool has_inverted_index = false;
        for (const auto& index_schema : schema.indexes()) {
            for (const auto& index : index_schema->indexes) {
                if (index->index_type() == INVERTED) {
                    has_inverted_index = true;
                    goto nested_loop_exit;
                }
            }
        }
    nested_loop_exit:
        if (state->query_options().enable_memtable_on_sink_node) {
            sink->reset(new vectorized::VOlapTableSinkV2(pool, row_desc, output_exprs, &status));
        } else {
            sink->reset(new vectorized::VOlapTableSink(pool, row_desc, output_exprs, false));
        }
        RETURN_IF_ERROR(status);
        break;
    }
    case TDataSinkType::GROUP_COMMIT_OLAP_TABLE_SINK: {
        Status status = Status::OK();
        DCHECK(thrift_sink.__isset.olap_table_sink);
        sink->reset(new vectorized::VOlapTableSink(pool, row_desc, output_exprs, true));
        RETURN_IF_ERROR(status);
        break;
    }
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK: {
        return Status::NotSupported("MULTI_CAST_DATA_STREAM_SINK only support in pipeline engine");
    }

    default: {
        std::stringstream error_msg;
        std::map<int, const char*>::const_iterator i =
                _TDataSinkType_VALUES_TO_NAMES.find(thrift_sink.type);
        const char* str = "Unknown data sink type ";

        if (i != _TDataSinkType_VALUES_TO_NAMES.end()) {
            str = i->second;
        }

        error_msg << str << " not implemented.";
        return Status::InternalError(error_msg.str());
    }
    }

    if (*sink != nullptr) {
        RETURN_IF_ERROR((*sink)->init(thrift_sink));
    }

    return Status::OK();
}

Status DataSink::create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                                  const std::vector<TExpr>& output_exprs,
                                  const TPipelineFragmentParams& params,
                                  const size_t& local_param_idx, const RowDescriptor& row_desc,
                                  RuntimeState* state, std::unique_ptr<DataSink>* sink,
                                  DescriptorTbl& desc_tbl) {
    const auto& local_params = params.local_params[local_param_idx];
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        if (!thrift_sink.__isset.stream_sink) {
            return Status::InternalError("Missing data stream sink.");
        }
        bool send_query_statistics_with_every_batch =
                params.__isset.send_query_statistics_with_every_batch
                        ? params.send_query_statistics_with_every_batch
                        : false;
        // TODO: figure out good buffer size based on size of output row
        sink->reset(new vectorized::VDataStreamSender(state, pool, local_params.sender_id, row_desc,
                                                      thrift_sink.stream_sink, params.destinations,
                                                      send_query_statistics_with_every_batch));
        // RETURN_IF_ERROR(sender->prepare(state->obj_pool(), thrift_sink.stream_sink));
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        if (!thrift_sink.__isset.result_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        // TODO: figure out good buffer size based on size of output row
        sink->reset(new doris::vectorized::VResultSink(row_desc, output_exprs,
                                                       thrift_sink.result_sink,
                                                       vectorized::RESULT_SINK_BUFFER_SIZE));
        break;
    }
    case TDataSinkType::RESULT_FILE_SINK: {
        if (!thrift_sink.__isset.result_file_sink) {
            return Status::InternalError("Missing result file sink.");
        }

        // TODO: figure out good buffer size based on size of output row
        bool send_query_statistics_with_every_batch =
                params.__isset.send_query_statistics_with_every_batch
                        ? params.send_query_statistics_with_every_batch
                        : false;
        // Result file sink is not the top sink
        if (params.__isset.destinations && params.destinations.size() > 0) {
            sink->reset(new doris::vectorized::VResultFileSink(
                    state, pool, local_params.sender_id, row_desc, thrift_sink.result_file_sink,
                    params.destinations, send_query_statistics_with_every_batch, output_exprs,
                    desc_tbl));
        } else {
            sink->reset(new doris::vectorized::VResultFileSink(row_desc, output_exprs));
        }
        break;
    }
    case TDataSinkType::MEMORY_SCRATCH_SINK: {
        if (!thrift_sink.__isset.memory_scratch_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        sink->reset(new vectorized::MemoryScratchSink(row_desc, output_exprs));
        break;
    }
    case TDataSinkType::MYSQL_TABLE_SINK: {
#ifdef DORIS_WITH_MYSQL
        if (!thrift_sink.__isset.mysql_table_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }
        vectorized::VMysqlTableSink* vmysql_tbl_sink =
                new vectorized::VMysqlTableSink(row_desc, output_exprs);
        sink->reset(vmysql_tbl_sink);
        break;
#else
        return Status::InternalError(
                "Don't support MySQL table, you should rebuild Doris with WITH_MYSQL option ON");
#endif
    }
    case TDataSinkType::ODBC_TABLE_SINK: {
        if (!thrift_sink.__isset.odbc_table_sink) {
            return Status::InternalError("Missing data odbc sink.");
        }
        sink->reset(new vectorized::VOdbcTableSink(row_desc, output_exprs));
        break;
    }

    case TDataSinkType::JDBC_TABLE_SINK: {
        if (!thrift_sink.__isset.jdbc_table_sink) {
            return Status::InternalError("Missing data jdbc sink.");
        }
        if (config::enable_java_support) {
            sink->reset(new vectorized::VJdbcTableSink(row_desc, output_exprs));
        } else {
            return Status::InternalError(
                    "Jdbc table sink is not enabled, you can change be config "
                    "enable_java_support to true and restart be.");
        }
        break;
    }

    case TDataSinkType::EXPORT_SINK: {
        RETURN_ERROR_IF_NON_VEC;
        break;
    }
    case TDataSinkType::OLAP_TABLE_SINK: {
        Status status = Status::OK();
        DCHECK(thrift_sink.__isset.olap_table_sink);
        OlapTableSchemaParam schema;
        schema.init(thrift_sink.olap_table_sink.schema);
        bool has_inverted_index = false;
        for (const auto& index_schema : schema.indexes()) {
            for (const auto& index : index_schema->indexes) {
                if (index->index_type() == INVERTED) {
                    has_inverted_index = true;
                    goto nested_loop_exit;
                }
            }
        }
    nested_loop_exit:
        if (state->query_options().enable_memtable_on_sink_node) {
            sink->reset(new vectorized::VOlapTableSinkV2(pool, row_desc, output_exprs, &status));
        } else {
            sink->reset(new vectorized::VOlapTableSink(pool, row_desc, output_exprs, false));
        }
        RETURN_IF_ERROR(status);
        break;
    }
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK: {
        DCHECK(thrift_sink.__isset.multi_cast_stream_sink);
        DCHECK_GT(thrift_sink.multi_cast_stream_sink.sinks.size(), 0);
        auto multi_cast_data_streamer = std::make_shared<pipeline::MultiCastDataStreamer>(
                row_desc, pool, thrift_sink.multi_cast_stream_sink.sinks.size());
        sink->reset(new vectorized::MultiCastDataStreamSink(multi_cast_data_streamer));
        break;
    }
    case TDataSinkType::GROUP_COMMIT_OLAP_TABLE_SINK: {
        Status status = Status::OK();
        DCHECK(thrift_sink.__isset.olap_table_sink);
        sink->reset(new vectorized::VOlapTableSink(pool, row_desc, output_exprs, true));
        RETURN_IF_ERROR(status);
        break;
    }
    default: {
        std::stringstream error_msg;
        std::map<int, const char*>::const_iterator i =
                _TDataSinkType_VALUES_TO_NAMES.find(thrift_sink.type);
        const char* str = "Unknown data sink type ";

        if (i != _TDataSinkType_VALUES_TO_NAMES.end()) {
            str = i->second;
        }

        error_msg << str << " not implemented.";
        return Status::InternalError(error_msg.str());
    }
    }

    if (*sink != nullptr) {
        RETURN_IF_ERROR((*sink)->init(thrift_sink));
        RETURN_IF_ERROR((*sink)->prepare(state));
    }

    return Status::OK();
}

Status DataSink::init(const TDataSink& thrift_sink) {
    return Status::OK();
}

Status DataSink::prepare(RuntimeState* state) {
    return Status::OK();
}

} // namespace doris
