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

#include "exec/spill/spill_file_writer.h"

namespace doris {

/// S3-specific upload admission and accounting for a spill part.
class SpillS3FileWriter final : public SpillFileWriter {
public:
    SpillS3FileWriter(const std::shared_ptr<SpillFile>& spill_file, RuntimeState* state,
                      RuntimeProfile* profile, SpillDataDir* data_dir, io::FileSystemSPtr fs,
                      std::string spill_dir);
    ~SpillS3FileWriter() override;

protected:
    Status _append_to_part(const Slice& data) override;
    void _record_spill_write_bytes(int64_t bytes) override;
    void _on_part_closed(const std::shared_ptr<SpillFile>& spill_file, bool closed,
                         int64_t close_ns) override;

private:
    Status _wait_for_upload_queue();

    int64_t _part_upload_ns = 0;
    RuntimeProfile::Counter* _remote_upload_bytes = nullptr;
    RuntimeProfile::Counter* _remote_upload_timer = nullptr;
    RuntimeProfile::Counter* _remote_upload_wait_timer = nullptr;
};

} // namespace doris
