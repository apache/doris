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

#include <gen_cpp/olap_file.pb.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/io_common.h"
#include "storage/index/bloom_filter/bloom_filter.h"

namespace doris::segment_v2 {

// Opens a .gpidx file and validates it (magic, version, hash strategy, length, crc32, and
// agreement with the descriptor) before returning the bloom filter.
//
// Every failure is treated as "may contain any value", never as a query error: the function always
// returns OK and leaves *out_bloom null on failure. The caller must then keep the rowset or tablet.
//
// Pass global_point_index_reader_options() as `reader_opts` so every reader of .gpidx files uses
// the file cache the same way. nullptr reads straight from storage.
Status try_load_global_point_index(const io::FileSystemSPtr& fs, const std::string& path,
                                   const ColumnPointIndexPB& desc, const io::IOContext* io_ctx,
                                   std::unique_ptr<BloomFilter>* out_bloom, int64_t* bytes_read,
                                   const io::FileReaderOptions* reader_opts = nullptr);

// Reader options for a .gpidx file. `file_size` comes from the descriptor, which saves a remote
// HEAD request. Caching is disabled for storage vault path version 1: its file names do not
// contain the rowset id, and the file cache keys on the file name, so files of different rowsets
// could share one cache key.
io::FileReaderOptions global_point_index_reader_options(int64_t tablet_id, int64_t file_size,
                                                        int64_t path_version);

} // namespace doris::segment_v2
