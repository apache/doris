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

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "io/fs/file_writer.h"
#include "runtime/runtime_profile.h"
#include "runtime/workload_management/resource_context.h"
namespace doris {
class RuntimeState;

class SpillDataDir;
class SpillFile;
class SpillRemoteUploadBudget;

/// SpillFileWriter writes blocks to a SpillFile, automatically managing
/// part-file rotation when a part exceeds the configured size threshold
/// (config::spill_file_part_size_bytes).
///
/// Usage:
///   SpillFileWriterSPtr writer;
///   RETURN_IF_ERROR(spill_file->create_writer(state, profile, writer));
///   RETURN_IF_ERROR(writer->write_block(state, block));
///   RETURN_IF_ERROR(writer->close());
///
/// Part rotation is fully internal. Each part file has its own footer with
/// block offset metadata. Parts are named 0, 1, 2, ... within the SpillFile's
/// directory. Parts are closed non-blocking (FileWriter::close(true)) so that the
/// upload of a finished part overlaps with writing the next one; close() waits for
/// all of them.
///
/// Files are created on the SpillDataDir's file system, which is either a local disk or
/// the object storage of a cloud storage vault. For object storage the writer additionally
/// bounds in-flight upload memory through SpillRemoteUploadBudget (S3FileWriter asks the
/// budget right before it submits a buffer and gives the bytes back when the upload finished)
/// and reports request statistics.
class SpillFileWriter {
public:
    SpillFileWriter(const std::shared_ptr<SpillFile>& spill_file, RuntimeState* state,
                    RuntimeProfile* profile, SpillDataDir* data_dir, const std::string& spill_dir);

    ~SpillFileWriter();

    /// Write a block. Automatically opens the first part, splits large blocks,
    /// and rotates to a new part when the current one exceeds max_part_size.
    Status write_block(RuntimeState* state, const Block& block);

    /// Finalize: close the current part, wait for all parts, record cumulative stats
    /// in SpillFile. After close(), no more writes are allowed.
    Status close();

private:
    /// Remote only: budget bytes taken and given back for one part. Updated from the
    /// appending thread (gate) and the upload threads (done callback); shared by value with
    /// the FileWriterOptions lambdas so that it outlives the writer.
    struct PartBudgetLedger {
        std::atomic<int64_t> acquired {0};
        std::atomic<int64_t> released {0};
        std::atomic<int64_t> wait_ns {0};
    };

    /// A part whose FileWriter::close(true) has been issued but not yet confirmed.
    struct ClosingPart {
        std::unique_ptr<doris::io::FileWriter> writer;
        std::string path;
        size_t part_index = 0;
        int64_t part_bytes = 0;
        // Error of close(true) itself, if it failed synchronously, or of the footer write.
        Status close_status;
        // Remote only: upload budget accounting and request statistics of this part.
        std::shared_ptr<PartBudgetLedger> ledger;
        std::shared_ptr<doris::io::RemoteWriteStats> stats;
    };

    /// Open the next part file (spill_dir/{_current_part_index}).
    Status _open_next_part(const std::shared_ptr<SpillFile>& spill_file);

    /// Close the current part: write footer, issue non-blocking close, move it to
    /// _closing_parts and advance to the next part index.
    Status _close_current_part(const std::shared_ptr<SpillFile>& spill_file);

    /// Confirm closes of finished parts in part order. With block=true waits for all of
    /// them. Every confirmed part is removed from _closing_parts whether it succeeded or
    /// not, so budget and statistics are always reconciled. Returns the first error.
    Status _reap_closing_parts(bool block, const std::shared_ptr<SpillFile>& spill_file);

    /// Bring a part to its final state (draining in-flight uploads on failure), reconcile
    /// budget and statistics, register it with the SpillFile, and on failure abort the
    /// multipart upload of the part if there is one.
    Status _finish_part(ClosingPart& part, const std::shared_ptr<SpillFile>& spill_file,
                        Status close_status);

    /// If current part size >= _max_part_size, close it.
    Status _rotate_if_needed(const std::shared_ptr<SpillFile>& spill_file);

    /// Serialize and write a single block to the current part.
    Status _write_internal(const Block& block, const std::shared_ptr<SpillFile>& spill_file);

    struct MultipartUploadId {
        std::string path;
        std::string bucket;
        std::string key;
        std::string upload_id;
    };
    /// Identity of the multipart upload of `writer`, if it started one.
    static MultipartUploadId _multipart_upload_id(doris::io::FileWriter* writer);
    void _abort_multipart_upload(const MultipartUploadId& upload);

    // ── Back-reference ──
    std::weak_ptr<SpillFile> _spill_file_wptr; // weak ref; use lock() in close()

    // ── Configuration ──
    SpillDataDir* _data_dir = nullptr;
    std::string _spill_dir;
    int64_t _max_part_size;
    // Non-null only for remote stores.
    SpillRemoteUploadBudget* _budget = nullptr;

    // ── Current part state (reset on rotation) ──
    size_t _current_part_index = 0;
    std::string _current_part_path;
    std::unique_ptr<doris::io::FileWriter> _file_writer;
    size_t _part_written_blocks = 0;
    int64_t _part_written_bytes = 0;
    size_t _part_max_sub_block_size = 0;
    std::string _part_meta;
    std::shared_ptr<PartBudgetLedger> _part_ledger;
    std::shared_ptr<doris::io::RemoteWriteStats> _part_stats;

    // Parts closed with close(true) and not yet confirmed, in part order.
    std::vector<ClosingPart> _closing_parts;

    bool _closed = false;

    // ── Counters ──
    RuntimeProfile::Counter* _write_file_timer = nullptr;
    RuntimeProfile::Counter* _serialize_timer = nullptr;
    RuntimeProfile::Counter* _write_block_counter = nullptr;
    RuntimeProfile::Counter* _write_block_bytes_counter = nullptr;
    RuntimeProfile::Counter* _write_file_total_size = nullptr;
    RuntimeProfile::Counter* _write_file_current_size = nullptr;
    RuntimeProfile::Counter* _write_rows_counter = nullptr;
    RuntimeProfile::Counter* _memory_used_counter = nullptr;
    RuntimeProfile::Counter* _total_file_count = nullptr;
    // Remote only, may be null when the profile does not register them.
    RuntimeProfile::Counter* _remote_write_requests = nullptr;
    RuntimeProfile::Counter* _remote_upload_part_requests = nullptr;
    RuntimeProfile::Counter* _remote_upload_bytes = nullptr;
    RuntimeProfile::Counter* _remote_upload_timer = nullptr;
    RuntimeProfile::Counter* _remote_upload_wait_timer = nullptr;

    std::shared_ptr<ResourceContext> _resource_ctx = nullptr;
};
using SpillFileWriterSPtr = std::shared_ptr<SpillFileWriter>;
} // namespace doris
