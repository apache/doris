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

#include <cstdint>
#include <memory>
#include <string>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/io/file_writer.h"

// Build-time staging for one blob sub-file (design 10).
//
// The container is a PULL consumer: SniiCompoundWriter::add_blob_index registers
// a BlobFileSource and only asks for the bytes at finish(), because placement
// (cold before the metadata groups, hot after) is the container's decision, not
// the producer's. BkdBuilder, on the other hand, PUSHES into an io::FileWriter.
// Something has to sit between the two and hold the bytes meanwhile.
//
// It cannot be SpillableByteBuffer: that one exposes stream_into() -- a single
// sequential drain -- and read_fn is positional. It cannot be a plain vector
// either, because bkd_data is the COLD sub-file and is sized by the point count.
// So: a temp file, written once and then read positionally, which is the same
// shape the spilled point runs already use.
namespace doris::snii::bkd {

class StagedBlobFile final : public io::FileWriter {
public:
    // `tag` only makes the temp file recognizable to a human; uniqueness comes
    // from the pid and a process-wide counter.
    static Status create(const std::string& tag, std::unique_ptr<StagedBlobFile>* out);

    ~StagedBlobFile() override;

    StagedBlobFile(const StagedBlobFile&) = delete;
    StagedBlobFile& operator=(const StagedBlobFile&) = delete;

    // io::FileWriter. append() is the producer side; finalize() flushes and
    // switches the file to readable.
    Status append(Slice data) override;
    Status finalize() override;
    uint64_t bytes_written() const override { return bytes_written_; }

    // Positional read for BlobFileSource::read_fn. Reads EXACTLY `len` bytes or
    // fails -- a short read reported as OK would be checksummed and sealed as if
    // it were the real payload. Only valid after finalize().
    Status read_at(uint64_t offset, size_t len, uint8_t* out) const;

    // Removes the temp file. Called by the destructor too, so an abandoned build
    // leaves nothing behind.
    void remove();

    // Where the staging file lives, for diagnostics and for tests that need to
    // assert on THIS file rather than on whatever a directory scan happens to
    // find. Empty once remove() has run.
    const std::string& path() const { return path_; }

private:
    StagedBlobFile() = default;

    int fd_ = -1;
    std::string path_;
    uint64_t bytes_written_ = 0;
    bool finalized_ = false;
};

} // namespace doris::snii::bkd
