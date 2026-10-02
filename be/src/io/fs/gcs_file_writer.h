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

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"

namespace doris {
class HttpClient;

namespace io {

/** Streams one object through the Google Cloud Storage resumable-upload protocol. */
class GcsFileWriter final : public FileWriter {
public:
    static constexpr size_t MIN_CHUNK_SIZE = 256 * 1024;
    static constexpr size_t DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024;

    static Result<FileWriterPtr> create(const Path& path,
                                        const std::map<std::string, std::string>& properties,
                                        size_t chunk_size = DEFAULT_CHUNK_SIZE);

    static Status delete_object(const Path& path,
                                const std::map<std::string, std::string>& properties);

    ~GcsFileWriter() override;

    Status appendv(const Slice* data, size_t data_cnt) override;
    Status close(bool non_block = false) override;

    const Path& path() const override { return _path; }
    size_t bytes_appended() const override { return _bytes_appended; }
    State state() const override { return _state; }

private:
    struct UploadProgress {
        bool complete = false;
        int64_t committed_offset = -1;
    };

    GcsFileWriter(Path path, std::string bucket, std::string key, std::string endpoint,
                  std::string authorization, int64_t token_expiration_time_ms, size_t chunk_size);

    Status _start_upload();
    Status _upload_buffer(bool final_chunk);
    Status _upload_empty_object();
    Status _send_chunk(std::string payload, bool final_chunk);
    Result<UploadProgress> _query_progress(bool final_chunk);
    Result<UploadProgress> _progress_from_response(const HttpClient& client,
                                                   bool final_chunk) const;
    Status _prepare_client(HttpClient* client, const std::string& url) const;
    Status _validate_token() const;
    void _cancel_upload();

    Path _path;
    std::string _bucket;
    std::string _key;
    std::string _endpoint;
    std::string _authorization;
    int64_t _token_expiration_time_ms;
    size_t _chunk_size;
    size_t _bytes_appended = 0;
    size_t _bytes_uploaded = 0;
    std::string _buffer;
    std::string _session_uri;
    State _state {State::OPENED};
};

} // namespace io
} // namespace doris
