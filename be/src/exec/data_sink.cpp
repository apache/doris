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

#include <map>
#include <memory>
#include <string>

#include "common/logging.h"
#include "exec/exec_node.h"
#include "exec/tablet_sink.h"
#include "exprs/expr.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "runtime/data_stream_sender.h"
#include "runtime/export_sink.h"
#include "runtime/memory_scratch_sink.h"
#include "runtime/mysql_table_sink.h"
#include "runtime/odbc_table_sink.h"
#include "runtime/result_file_sink.h"
#include "runtime/result_sink.h"
#include "runtime/runtime_state.h"

#include "vec/sink/result_sink.h"
#include "vec/sink/vdata_stream_sender.h"
#include "vec/sink/vmysql_table_writer.h"
#include "vec/sink/vtablet_sink.h"
#include "vec/sink/vmysql_table_sink.h"

namespace doris {

Status DataSink::create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                                  const std::vector<TExpr>& output_exprs,
                                  const TPlanFragmentExecParams& params,
                                  const RowDescriptor& row_desc, bool is_vec,
                                  std::unique_ptr<DataSink>* sink, DescriptorTbl& desc_tbl) {
    DataSink* tmp_sink = nullptr;

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
        if (is_vec) {
            tmp_sink = new doris::vectorized::VDataStreamSender(
                    pool, params.sender_id, row_desc, thrift_sink.stream_sink, params.destinations,
                    16 * 1024, send_query_statistics_with_every_batch);
        } else {
            tmp_sink = new DataStreamSender(pool, params.sender_id, row_desc,
                                            thrift_sink.stream_sink, params.destinations, 16 * 1024,
                                            send_query_statistics_with_every_batch);
        }
        // RETURN_IF_ERROR(sender->prepare(state->obj_pool(), thrift_sink.stream_sink));
        sink->reset(tmp_sink);
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        if (!thrift_sink.__isset.result_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        // TODO: figure out good buffer size based on size of output row
        if (is_vec) {
            tmp_sink = new doris::vectorized::VResultSink(row_desc, output_exprs,
                                                          thrift_sink.result_sink, 4096);
        } else {
            tmp_sink = new ResultSink(row_desc, output_exprs, thrift_sink.result_sink, 1024);
        }
        sink->reset(tmp_sink);
        break;
    }
    case TDataSinkType::RESULT_FILE_SINK: {
        if (!thrift_sink.__isset.result_file_sink) {
            return Status::InternalError("Missing result file sink.");
        }
        // Result file sink is not the top sink
        if (params.__isset.destinations && params.destinations.size() > 0) {
            tmp_sink = new ResultFileSink(row_desc, output_exprs, thrift_sink.result_file_sink,
                                          params.destinations, pool, params.sender_id, desc_tbl);
        } else {
            tmp_sink = new ResultFileSink(row_desc, output_exprs, thrift_sink.result_file_sink);
        }
        sink->reset(tmp_sink);
        break;
    }
    case TDataSinkType::MEMORY_SCRATCH_SINK: {
        if (!thrift_sink.__isset.memory_scratch_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        tmp_sink = new MemoryScratchSink(row_desc, output_exprs, thrift_sink.memory_scratch_sink);
        sink->reset(tmp_sink);
        break;
    }
    case TDataSinkType::MYSQL_TABLE_SINK: {
#ifdef DORIS_WITH_MYSQL
        if (!thrift_sink.__isset.mysql_table_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }
        if (is_vec) {
            doris::vectorized::VMysqlTableSink* vmysql_tbl_sink =
                    new doris::vectorized::VMysqlTableSink(pool, row_desc, output_exprs);
            sink->reset(vmysql_tbl_sink);
        } else {
            // TODO: figure out good buffer size based on size of output row
            MysqlTableSink* mysql_tbl_sink = new MysqlTableSink(pool, row_desc, output_exprs);
            sink->reset(mysql_tbl_sink);
        }
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
        OdbcTableSink* odbc_tbl_sink = new OdbcTableSink(pool, row_desc, output_exprs);
        sink->reset(odbc_tbl_sink);
        break;
    }

    case TDataSinkType::EXPORT_SINK: {
        if (!thrift_sink.__isset.export_sink) {
            return Status::InternalError("Missing export sink sink.");
        }

        std::unique_ptr<ExportSink> export_sink(new ExportSink(pool, row_desc, output_exprs));
        sink->reset(export_sink.release());
        break;
    }
    case TDataSinkType::OLAP_TABLE_SINK: {
        Status status;
        DCHECK(thrift_sink.__isset.olap_table_sink);
        if (is_vec) {
            sink->reset(new stream_load::VOlapTableSink(pool, row_desc, output_exprs, &status));
        } else {
            sink->reset(new stream_load::OlapTableSink(pool, row_desc, output_exprs, &status));
        }
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

    if (sink->get() != nullptr) {
        RETURN_IF_ERROR((*sink)->init(thrift_sink));
    }

    return Status::OK();
}

Status DataSink::init(const TDataSink& thrift_sink) {
    return Status::OK();
}

Status DataSink::prepare(RuntimeState* state) {
    _expr_mem_tracker =
            MemTracker::create_tracker(-1, _name + ":Expr:" + std::to_string(state->load_job_id()),
                                       state->instance_mem_tracker());
    return Status::OK();
}

} // namespace doris
