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

#include "bvar/bvar.h"
#include "cloud/config.h"
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

bvar::Adder<int64_t> g_load_stream_writer_cnt("load_stream_writer_count");
bvar::Adder<int64_t> g_load_stream_file_writer_cnt("load_stream_file_writer_count");

LoadStreamWriter::LoadStreamWriter(WriteRequest* context, RuntimeProfile* profile)
        : _req(*context), _rowset_writer(nullptr) {
    g_load_stream_writer_cnt << 1;
    // TODO(plat1ko): CloudStorageEngine
    _rowset_builder = std::make_unique<RowsetBuilder>(
            ExecEnv::GetInstance()->storage_engine().to_local(), *context, profile);
    _query_thread_context.init_unlocked(); // from load stream
}

LoadStreamWriter::~LoadStreamWriter() {
    g_load_stream_writer_cnt << -1;
}

Status LoadStreamWriter::init() {
    RETURN_IF_ERROR(_rowset_builder->init());
    _rowset_writer = _rowset_builder->rowset_writer();
    _is_init = true;
    return Status::OK();
}

Status LoadStreamWriter::append_data(uint32_t segid, uint64_t offset, butil::IOBuf buf,
                                     FileType file_type) {
    SCOPED_ATTACH_TASK(_query_thread_context);
    io::FileWriter* file_writer = nullptr;
    auto& file_writers =
            file_type == FileType::SEGMENT_FILE ? _segment_file_writers : _inverted_file_writers;
    {
        std::lock_guard lock_guard(_lock);
        DCHECK(_is_init);
        if (segid >= file_writers.size()) {
            for (size_t i = file_writers.size(); i <= segid; i++) {
                Status st;
                io::FileWriterPtr file_writer;
                st = _rowset_writer->create_file_writer(i, file_writer, file_type);
                if (!st.ok()) {
                    _is_canceled = true;
                    return st;
                }
                file_writers.push_back(std::move(file_writer));
                g_load_stream_file_writer_cnt << 1;
            }
        }

        // TODO: IOBuf to Slice
        file_writer = file_writers[segid].get();
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

Status LoadStreamWriter::close_writer(uint32_t segid, FileType file_type) {
    SCOPED_ATTACH_TASK(_query_thread_context);
    io::FileWriter* file_writer = nullptr;
    auto& file_writers =
            file_type == FileType::SEGMENT_FILE ? _segment_file_writers : _inverted_file_writers;
    {
        std::lock_guard lock_guard(_lock);
        DBUG_EXECUTE_IF("LoadStreamWriter.close_writer.uninited_writer", { _is_init = false; });
        if (!_is_init) {
            return Status::Corruption("close_writer failed, LoadStreamWriter is not inited");
        }
        DBUG_EXECUTE_IF("LoadStreamWriter.close_writer.bad_segid",
                        { segid = file_writers.size(); });
        if (segid >= file_writers.size()) {
            return Status::Corruption(
                    "close_writer failed, file {} is never opened, file type is {}", segid,
                    file_type);
        }
        file_writer = file_writers[segid].get();
    }

    DBUG_EXECUTE_IF("LoadStreamWriter.close_writer.null_file_writer", { file_writer = nullptr; });
    if (file_writer == nullptr) {
        return Status::Corruption(
                "close_writer failed, file writer {} is destoryed, fiel type is {}", segid,
                file_type);
    }
    auto st = file_writer->close();
    if (!st.ok()) {
        _is_canceled = true;
        return st;
    }
    g_load_stream_file_writer_cnt << -1;
    LOG(INFO) << "file " << segid << " path " << file_writer->path().native() << "closed, written "
              << file_writer->bytes_appended() << " bytes"
              << ", file type is " << file_type;
    if (file_writer->bytes_appended() == 0) {
        return Status::Corruption("file {} closed with 0 bytes, file type is {}",
                                  file_writer->path().native(), file_type);
    }
    return Status::OK();
}

Status LoadStreamWriter::add_segment(uint32_t segid, const SegmentStatistics& stat,
                                     TabletSchemaSPtr flush_schema) {
    SCOPED_ATTACH_TASK(_query_thread_context);
    size_t segment_file_size = 0;
    size_t inverted_file_size = 0;
    {
        std::lock_guard lock_guard(_lock);
        DBUG_EXECUTE_IF("LoadStreamWriter.add_segment.uninited_writer", { _is_init = false; });
        if (!_is_init) {
            return Status::Corruption("add_segment failed, LoadStreamWriter is not inited");
        }
        DBUG_EXECUTE_IF("LoadStreamWriter.add_segment.bad_segid",
                        { segid = _segment_file_writers.size(); });
        RETURN_IF_ERROR(_calc_file_size(segid, FileType::SEGMENT_FILE, &segment_file_size));
        if (_inverted_file_writers.size() > 0) {
            RETURN_IF_ERROR(
                    _calc_file_size(segid, FileType::INVERTED_INDEX_FILE, &inverted_file_size));
        }
    }

    if (segment_file_size + inverted_file_size != stat.data_size) {
        return Status::Corruption(
                "add_segment failed, segment stat {} does not match, file size={}, inverted file "
                "size={}, stat.data_size={}, tablet id={}",
                segid, segment_file_size, inverted_file_size, stat.data_size, _req.tablet_id);
    }

    return _rowset_writer->add_segment(segid, stat, flush_schema);
}

Status LoadStreamWriter::_calc_file_size(uint32_t segid, FileType file_type, size_t* file_size) {
    io::FileWriter* file_writer = nullptr;
    auto& file_writers =
            (file_type == FileType::SEGMENT_FILE) ? _segment_file_writers : _inverted_file_writers;

    if (segid >= file_writers.size()) {
        return Status::Corruption("calc file size failed, file {} is never opened, file type is {}",
                                  segid, file_type);
    }
    file_writer = file_writers[segid].get();
    DBUG_EXECUTE_IF("LoadStreamWriter.calc_file_size.null_file_writer", { file_writer = nullptr; });
    if (file_writer == nullptr) {
        return Status::Corruption(
                "calc file size failed, file writer {} is destoryed, file type is {}", segid,
                file_type);
    }
    if (file_writer->state() != io::FileWriter::State::CLOSED) {
        return Status::Corruption("calc file size failed, file {} is not closed",
                                  file_writer->path().native());
    }
    *file_size = file_writer->bytes_appended();
    return Status::OK();
}

Status LoadStreamWriter::close() {
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_ATTACH_TASK(_query_thread_context);
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
    if (_inverted_file_writers.size() > 0 &&
        _inverted_file_writers.size() != _segment_file_writers.size()) {
        return Status::Corruption(
                "LoadStreamWriter close failed, inverted file writer size is {},"
                "segment file writer size is {}",
                _inverted_file_writers.size(), _segment_file_writers.size());
    }
    for (const auto& writer : _segment_file_writers) {
        if (writer->state() != io::FileWriter::State::CLOSED) {
            return Status::Corruption("LoadStreamWriter close failed, segment {} is not closed",
                                      writer->path().native());
        }
    }

    for (const auto& writer : _inverted_file_writers) {
        if (writer->state() != io::FileWriter::State::CLOSED) {
            return Status::Corruption(
                    "LoadStreamWriter close failed, inverted file {} is not closed",
                    writer->path().native());
        }
    }

    RETURN_IF_ERROR(_rowset_builder->build_rowset());
    RETURN_IF_ERROR(_rowset_builder->submit_calc_delete_bitmap_task());
    RETURN_IF_ERROR(_rowset_builder->wait_calc_delete_bitmap());
    // FIXME(plat1ko): No `commit_txn` operation in cloud mode, need better abstractions
    RETURN_IF_ERROR(static_cast<RowsetBuilder*>(_rowset_builder.get())->commit_txn());

    return Status::OK();
}

} // namespace doris
