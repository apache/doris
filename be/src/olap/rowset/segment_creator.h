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

#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <string>
#include <typeinfo>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/tablet_fwd.h"
#include "util/spinlock.h"
#include "vec/core/block.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

namespace segment_v2 {
class SegmentWriter;
class VerticalSegmentWriter;
} // namespace segment_v2

struct SegmentStatistics;
class BetaRowsetWriter;
class SegmentFileCollection;

class FileWriterCreator {
public:
    virtual ~FileWriterCreator() = default;

    virtual Status create(uint32_t segment_id, io::FileWriterPtr& file_writer,
                          FileType file_type = FileType::SEGMENT_FILE) = 0;
};

template <class T>
class FileWriterCreatorT : public FileWriterCreator {
public:
    explicit FileWriterCreatorT(T* t) : _t(t) {}

    Status create(uint32_t segment_id, io::FileWriterPtr& file_writer,
                  FileType file_type = FileType::SEGMENT_FILE) override {
        return _t->create_file_writer(segment_id, file_writer, file_type);
    }

private:
    T* _t = nullptr;
};

class SegmentCollector {
public:
    virtual ~SegmentCollector() = default;

    virtual Status add(uint32_t segment_id, SegmentStatistics& segstat,
                       TabletSchemaSPtr flush_chema) = 0;
};

template <class T>
class SegmentCollectorT : public SegmentCollector {
public:
    explicit SegmentCollectorT(T* t) : _t(t) {}

    Status add(uint32_t segment_id, SegmentStatistics& segstat,
               TabletSchemaSPtr flush_chema) override {
        return _t->add_segment(segment_id, segstat, flush_chema);
    }

private:
    T* _t = nullptr;
};

class SegmentFlusher {
public:
    SegmentFlusher(RowsetWriterContext& context, SegmentFileCollection& seg_files);

    ~SegmentFlusher();

    // Return the file size flushed to disk in "flush_size"
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block, int32_t segment_id,
                              int64_t* flush_size = nullptr);

    int64_t num_rows_written() const { return _num_rows_written; }

    // for partial update
    int64_t num_rows_updated() const { return _num_rows_updated; }
    int64_t num_rows_deleted() const { return _num_rows_deleted; }
    int64_t num_rows_new_added() const { return _num_rows_new_added; }
    int64_t num_rows_filtered() const { return _num_rows_filtered; }

    Status close();

public:
    class Writer {
        friend class SegmentFlusher;

    public:
        ~Writer();

        Status add_rows(const vectorized::Block* block, size_t row_offset, size_t input_row_num) {
            return _flusher->_add_rows(_writer, block, row_offset, input_row_num);
        }

        Status flush();

        int64_t max_row_to_add(size_t row_avg_size_in_bytes);

    private:
        Writer(SegmentFlusher* flusher, std::unique_ptr<segment_v2::SegmentWriter>& segment_writer);

        SegmentFlusher* _flusher = nullptr;
        std::unique_ptr<segment_v2::SegmentWriter> _writer;
    };

    Status create_writer(std::unique_ptr<SegmentFlusher::Writer>& writer, uint32_t segment_id);

    bool need_buffering();

private:
    Status _parse_variant_columns(vectorized::Block& block);
    Status _add_rows(std::unique_ptr<segment_v2::SegmentWriter>& segment_writer,
                     const vectorized::Block* block, size_t row_offset, size_t row_num);
    Status _add_rows(std::unique_ptr<segment_v2::VerticalSegmentWriter>& segment_writer,
                     const vectorized::Block* block, size_t row_offset, size_t row_num);
    Status _create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                  int32_t segment_id, bool no_compression = false);
    Status _create_segment_writer(std::unique_ptr<segment_v2::VerticalSegmentWriter>& writer,
                                  int32_t segment_id, bool no_compression = false);
    Status _flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                 TabletSchemaSPtr flush_schema = nullptr,
                                 int64_t* flush_size = nullptr);
    Status _flush_segment_writer(std::unique_ptr<segment_v2::VerticalSegmentWriter>& writer,
                                 TabletSchemaSPtr flush_schema = nullptr,
                                 int64_t* flush_size = nullptr);

private:
    RowsetWriterContext& _context;
    SegmentFileCollection& _seg_files;

    // written rows by add_block/add_row
    std::atomic<int64_t> _num_rows_written = 0;
    std::atomic<int64_t> _num_rows_updated = 0;
    std::atomic<int64_t> _num_rows_new_added = 0;
    std::atomic<int64_t> _num_rows_deleted = 0;
    std::atomic<int64_t> _num_rows_filtered = 0;
};

class SegmentCreator {
public:
    SegmentCreator(RowsetWriterContext& context, SegmentFileCollection& seg_files);

    ~SegmentCreator() = default;

    void set_segment_start_id(uint32_t start_id) { _next_segment_id = start_id; }

    Status add_block(const vectorized::Block* block);

    Status flush();

    int32_t allocate_segment_id() { return _next_segment_id.fetch_add(1); }

    int32_t next_segment_id() const { return _next_segment_id.load(); }

    int64_t num_rows_written() const { return _segment_flusher.num_rows_written(); }

    // for partial update
    int64_t num_rows_updated() const { return _segment_flusher.num_rows_updated(); }
    int64_t num_rows_deleted() const { return _segment_flusher.num_rows_deleted(); }
    int64_t num_rows_new_added() const { return _segment_flusher.num_rows_new_added(); }
    int64_t num_rows_filtered() const { return _segment_flusher.num_rows_filtered(); }

    // Flush a block into a single segment, with pre-allocated segment_id.
    // Return the file size flushed to disk in "flush_size"
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block, int32_t segment_id,
                              int64_t* flush_size = nullptr);

    // Flush a block into a single segment, without pre-allocated segment_id.
    // This method is thread-safe.
    Status flush_single_block(const vectorized::Block* block) {
        return flush_single_block(block, allocate_segment_id());
    }

    Status close();

private:
    std::atomic<int32_t> _next_segment_id = 0;
    SegmentFlusher _segment_flusher;
    std::unique_ptr<SegmentFlusher::Writer> _flush_writer;
    // Buffer block to num bytes before flushing
    vectorized::MutableBlock _buffer_block;
};

} // namespace doris
