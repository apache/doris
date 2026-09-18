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
#include <orc/OrcFile.hh>
#include <string>
#include <unordered_map>
#include <vector>

#include "io/fs/buffered_reader.h"

namespace doris {

class RuntimeProfile;

namespace io {
struct IOContext;
}

namespace format::orc {

struct OrcFileInputStreamOptions {
    uint64_t natural_read_size = 8L * 1024L * 1024L;
    int64_t once_max_read_bytes = 8L * 1024L * 1024L;
    int64_t max_merge_distance_bytes = 1L * 1024L * 1024L;
};

class OrcFileInputStream final : public ::orc::InputStream {
public:
    OrcFileInputStream(std::string file_name, io::FileReaderSPtr file_reader,
                       const io::IOContext* io_ctx, RuntimeProfile* profile,
                       OrcFileInputStreamOptions options);
    ~OrcFileInputStream() override;

    uint64_t getLength() const override;
    uint64_t getNaturalReadSize() const override;
    void read(void* buf, uint64_t length, uint64_t offset) override;
    const std::string& getName() const override;

    void beforeReadStripe(std::unique_ptr<::orc::StripeInformation> current_stripe_information,
                          const std::vector<bool>& selected_columns,
                          std::unordered_map<::orc::StreamId, std::shared_ptr<::orc::InputStream>>&
                                  streams) override;

private:
    void _flush_active_clusters();
    void _add_direct_stream(
            const ::orc::StreamId& stream_id,
            std::unordered_map<::orc::StreamId, std::shared_ptr<::orc::InputStream>>& streams);
    void _add_clustered_streams(
            const std::vector<std::pair<::orc::StreamId, io::PrefetchRange>>& cluster_streams,
            const io::PrefetchRange& cluster_range,
            std::unordered_map<::orc::StreamId, std::shared_ptr<::orc::InputStream>>& streams);

    std::string _file_name;
    io::FileReaderSPtr _file_reader;
    io::FileReaderSPtr _default_reader;
    const io::IOContext* _io_ctx = nullptr;
    RuntimeProfile* _profile = nullptr;
    OrcFileInputStreamOptions _options;

    std::vector<std::shared_ptr<::orc::InputStream>> _active_stripe_streams;
    std::vector<io::FileReaderSPtr> _active_cluster_readers;
};

} // namespace format::orc
} // namespace doris
