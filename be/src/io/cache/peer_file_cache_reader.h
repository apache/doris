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

#include "common/status.h"
#include "io/cache/file_block.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_system.h"
#include "util/slice.h"

namespace doris {
class RuntimeProfile;

namespace io {
struct IOContext;

class PeerFileCacheReader final {
public:
    /**
     * Construct a peer file cache reader bound to a specific file and peer endpoint.
     *
     * Params:
     * - file_path: Path of the target file whose cache blocks will be fetched from a peer.
     * - is_doris_table: Whether the target file is a Doris table segment; only true is supported.
     * - host: Peer hostname or IP address to fetch from.
     * - port: Peer BRPC service port.
     */
    PeerFileCacheReader(const io::Path& file_path, bool is_doris_table, std::string host, int port);
    ~PeerFileCacheReader();
    /**
     * Fetch data blocks from a peer and write them into the provided buffer.
     *
     * Behavior:
     * - Supports only Doris table segment files (is_doris_table=true); otherwise returns NotSupported.
     * - Builds a BRPC request to invoke peer fetch_peer_data using the given blocks.
     * - Copies returned block data into the contiguous buffer Slice s, using 'off' as the base offset.
     * - Succeeds only if exactly s.size bytes are written; otherwise returns an Incomplete error.
     *
     * Params:
     * - blocks: List of file blocks to fetch (global file offsets, inclusive ranges).
     * - off: Base file offset corresponding to the start of Slice s.
     * - s: Destination buffer; must be large enough to hold all requested block bytes.
     * - n: Output number of bytes successfully written.
     * - ctx: IO context (kept for interface symmetry).
     *
     * Returns:
     * - OK: Successfully wrote exactly s.size bytes into the buffer.
     * - NotSupported: The file is not a Doris table segment.
     */
    Status fetch_blocks(const std::vector<FileBlockSPtr>& blocks, size_t off, Slice s,
                        size_t* bytes_read, const IOContext* ctx);

private:
    io::Path _path;
    bool _is_doris_table {false};
    std::string _host = "127.0.0.1";
    int _port = 9060;
};

} // namespace io
} // namespace doris
