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

#include "runtime/load_stream_writer.h"

#include <brpc/controller.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <filesystem>
#include <ostream>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gutil/integral_types.h"
#include "gutil/strings/numbers.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/data_dir.h"
#include "olap/memtable.h"
#include "olap/memtable_flush_executor.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset_builder.h"
#include "olap/schema.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/txn_manager.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/memory/mem_tracker.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/debug_points.h"
#include "util/mem_info.h"
#include "util/ref_count_closure.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

LoadStreamWriter::LoadStreamWriter(WriteRequest* context, RuntimeProfile* profile)
        : _req(*context), _rowset_writer(nullptr) {
    _rowset_builder =
            std::make_unique<RowsetBuilder>(*StorageEngine::instance(), *context, profile);
}

LoadStreamWriter::~LoadStreamWriter() = default;

Status LoadStreamWriter::init() {
    RETURN_IF_ERROR(_rowset_builder->init());
    _rowset_writer = _rowset_builder->rowset_writer();
    _is_init = true;
    return Status::OK();
}

Status LoadStreamWriter::append_data(uint32_t segid, uint64_t offset, butil::IOBuf buf) {
    io::FileWriter* file_writer = nullptr;
    {
        std::lock_guard lock_guard(_lock);
        DCHECK(_is_init);
        if (segid >= _segment_file_writers.size()) {
            for (size_t i = _segment_file_writers.size(); i <= segid; i++) {
                Status st;
                io::FileWriterPtr file_writer;
                st = _rowset_writer->create_file_writer(i, file_writer);
                if (!st.ok()) {
                    _is_canceled = true;
                    return st;
                }
                _segment_file_writers.push_back(std::move(file_writer));
            }
        }

        // TODO: IOBuf to Slice
        file_writer = _segment_file_writers[segid].get();
    }
    DBUG_EXECUTE_IF("LoadStreamWriter.append_data.null_file_writer", { file_writer = nullptr; });
    VLOG_DEBUG << " file_writer " << file_writer << "seg id " << segid;
    if (file_writer == nullptr) {
        return Status::Corruption("append_data failed, file writer {} is destoryed", segid);
    }
    if (file_writer->bytes_appended() != offset) {
        return Status::Corruption(
                "append_data out-of-order in segment={}, expected offset={}, actual={}",
                file_writer->path().native(), offset, file_writer->bytes_appended());
    }
    return file_writer->append(buf.to_string());
}

Status LoadStreamWriter::close_segment(uint32_t segid) {
    io::FileWriter* file_writer = nullptr;
    {
        std::lock_guard lock_guard(_lock);
        DBUG_EXECUTE_IF("LoadStreamWriter.close_segment.uninited_writer", { _is_init = false; });
        if (!_is_init) {
            return Status::Corruption("close_segment failed, LoadStreamWriter is not inited");
        }
        DBUG_EXECUTE_IF("LoadStreamWriter.close_segment.bad_segid",
                        { segid = _segment_file_writers.size(); });
        if (segid >= _segment_file_writers.size()) {
            return Status::Corruption("close_segment failed, segment {} is never opened", segid);
        }
        file_writer = _segment_file_writers[segid].get();
    }
    DBUG_EXECUTE_IF("LoadStreamWriter.close_segment.null_file_writer", { file_writer = nullptr; });
    if (file_writer == nullptr) {
        return Status::Corruption("close_segment failed, file writer {} is destoryed", segid);
    }
    auto st = file_writer->close();
    if (!st.ok()) {
        _is_canceled = true;
        return st;
    }
    LOG(INFO) << "segment " << segid << " path " << file_writer->path().native()
              << "closed, written " << file_writer->bytes_appended() << " bytes";
    if (file_writer->bytes_appended() == 0) {
        return Status::Corruption("segment {} closed with 0 bytes", file_writer->path().native());
    }
    return Status::OK();
}

Status LoadStreamWriter::add_segment(uint32_t segid, const SegmentStatistics& stat,
                                     TabletSchemaSPtr flush_schema) {
    io::FileWriter* file_writer = nullptr;
    {
        std::lock_guard lock_guard(_lock);
        DBUG_EXECUTE_IF("LoadStreamWriter.add_segment.uninited_writer", { _is_init = false; });
        if (!_is_init) {
            return Status::Corruption("add_segment failed, LoadStreamWriter is not inited");
        }
        DBUG_EXECUTE_IF("LoadStreamWriter.add_segment.bad_segid",
                        { segid = _segment_file_writers.size(); });
        if (segid >= _segment_file_writers.size()) {
            return Status::Corruption("add_segment failed, segment {} is never opened", segid);
        }
        file_writer = _segment_file_writers[segid].get();
    }
    DBUG_EXECUTE_IF("LoadStreamWriter.add_segment.null_file_writer", { file_writer = nullptr; });
    if (file_writer == nullptr) {
        return Status::Corruption("add_segment failed, file writer {} is destoryed", segid);
    }
    if (!file_writer->is_closed()) {
        return Status::Corruption("add_segment failed, segment {} is not closed",
                                  file_writer->path().native());
    }
    if (file_writer->bytes_appended() != stat.data_size) {
        return Status::Corruption(
                "add_segment failed, segment stat {} does not match, file size={}, "
                "stat.data_size={}",
                file_writer->path().native(), file_writer->bytes_appended(), stat.data_size);
    }
    return _rowset_writer->add_segment(segid, stat, flush_schema);
}

Status LoadStreamWriter::close() {
    std::lock_guard<std::mutex> l(_lock);
    DBUG_EXECUTE_IF("LoadStreamWriter.close.uninited_writer", { _is_init = false; });
    if (!_is_init) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this LoadStreamWriter, so that it can create an empty rowset
        // for this tablet when being closed.
        RETURN_IF_ERROR(init());
    }

    DCHECK(_is_init)
            << "rowset builder is supposed be to initialized before close_wait() being called";

    if (_is_canceled) {
        return Status::InternalError("flush segment failed");
    }

    for (const auto& writer : _segment_file_writers) {
        if (!writer->is_closed()) {
            return Status::Corruption("LoadStreamWriter close failed, segment {} is not closed",
                                      writer->path().native());
        }
    }

    RETURN_IF_ERROR(_rowset_builder->build_rowset());
    RETURN_IF_ERROR(_rowset_builder->submit_calc_delete_bitmap_task());
    RETURN_IF_ERROR(_rowset_builder->wait_calc_delete_bitmap());
    RETURN_IF_ERROR(_rowset_builder->commit_txn());

    return Status::OK();
}

} // namespace doris
