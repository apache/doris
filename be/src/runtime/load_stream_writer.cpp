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

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
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
#include "util/mem_info.h"
#include "util/ref_count_closure.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

LoadStreamWriter::LoadStreamWriter(WriteRequest* req, RuntimeProfile* profile)
        : _req(*req), _rowset_builder(*req, profile), _rowset_writer(nullptr) {}

LoadStreamWriter::~LoadStreamWriter() = default;

Status LoadStreamWriter::init() {
    RETURN_IF_ERROR(_rowset_builder.init());
    _rowset_writer = _rowset_builder.rowset_writer();
    _is_init = true;
    return Status::OK();
}

Status LoadStreamWriter::append_data(uint32_t segid, butil::IOBuf buf) {
    io::FileWriter* file_writer = nullptr;
    {
        std::lock_guard lock_guard(_lock);
        if (!_is_init) {
            RETURN_IF_ERROR(init());
        }
        if (segid + 1 > _segment_file_writers.size()) {
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
    VLOG_DEBUG << " file_writer " << file_writer << "seg id " << segid;
    return file_writer->append(buf.to_string());
}

Status LoadStreamWriter::close_segment(uint32_t segid) {
    auto st = _segment_file_writers[segid]->close();
    if (!st.ok()) {
        _is_canceled = true;
        return st;
    }
    if (_segment_file_writers[segid]->bytes_appended() == 0) {
        return Status::Corruption("segment {} is zero bytes", segid);
    }
    LOG(INFO) << "segid " << segid << "path " << _segment_file_writers[segid]->path() << " written "
              << _segment_file_writers[segid]->bytes_appended() << " bytes";
    return Status::OK();
}

Status LoadStreamWriter::add_segment(uint32_t segid, const SegmentStatistics& stat) {
    if (_segment_file_writers[segid]->bytes_appended() != stat.data_size) {
        LOG(WARNING) << _segment_file_writers[segid]->path() << " is added without all data, "
                     << "actual " << _segment_file_writers[segid]->bytes_appended()
                     << " expected " << stat.data_size;
        return Status::Corruption("segment {} is added without all data, actual {} expected {}",
                                  _segment_file_writers[segid]->path().native(),
                                  _segment_file_writers[segid]->bytes_appended(),
                                  stat.data_size);
    }
    return _rowset_writer->add_segment(segid, stat);
}

Status LoadStreamWriter::close() {
    std::lock_guard<std::mutex> l(_lock);
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
        return Status::Error<ErrorCode::INTERNAL_ERROR>("flush segment failed");
    }

    for (size_t i = 0; i < _segment_file_writers.size(); i++) {
        if (!_segment_file_writers[i]->is_closed()) {
            LOG(WARNING) << _segment_file_writers[i]->path() << " is not eos";
            return Status::Corruption("segment {} is not eos",
                                      _segment_file_writers[i]->path().native());
        }
    }

    RETURN_IF_ERROR(_rowset_builder.build_rowset());
    RETURN_IF_ERROR(_rowset_builder.submit_calc_delete_bitmap_task());
    RETURN_IF_ERROR(_rowset_builder.wait_calc_delete_bitmap());
    RETURN_IF_ERROR(_rowset_builder.commit_txn());

    return Status::OK();
}

} // namespace doris
