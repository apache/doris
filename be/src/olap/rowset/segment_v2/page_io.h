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

#include <gen_cpp/segment_v2.pb.h>

#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "io/io_common.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "util/slice.h"

namespace doris {

class BlockCompressionCodec;
struct OlapReaderStatistics;

namespace io {
class FileWriter;
class FileReader;
} // namespace io

namespace segment_v2 {
class EncodingInfo;
class PageHandle;

struct PageReadOptions {
    // whether to verify page checksum
    bool verify_checksum = true;
    // whether to use page cache in read path
    bool use_page_cache = false;
    // if true, use DURABLE CachePriority in page cache
    // currently used for in memory olap table
    bool kept_in_memory = false;
    // index_page should not be pre-decoded
    bool pre_decode = true;
    // for page cache allocation
    // page types are divided into DATA_PAGE & INDEX_PAGE
    // INDEX_PAGE including index_page, dict_page and short_key_page
    PageTypePB type;
    // block to read page
    io::FileReader* file_reader = nullptr;
    // location of the page
    PagePointer page_pointer;
    // decompressor for page body (null means page body is not compressed)
    BlockCompressionCodec* codec = nullptr;
    // used to collect IO metrics
    OlapReaderStatistics* stats = nullptr;

    const EncodingInfo* encoding_info = nullptr;

    const io::IOContext& io_ctx;

    void sanity_check() const {
        CHECK_NOTNULL(file_reader);
        CHECK_NOTNULL(stats);
    }
};

inline std::ostream& operator<<(std::ostream& os, const PageReadOptions& opt) {
    return os << "PageReadOptions { verify_checksum=" << opt.verify_checksum
              << " use_page_cache=" << opt.use_page_cache
              << " kept_in_memory=" << opt.kept_in_memory << " pre_decode=" << opt.pre_decode
              << " type=" << opt.type << " page_pointer=" << opt.page_pointer
              << " has_codec=" << (opt.codec != nullptr)
              << " has_encoding_info=" << (opt.encoding_info != nullptr) << " }";
}

// Utility class for read and write page. All types of page share the same general layout:
//     Page := PageBody, PageFooter, FooterSize(4), Checksum(4)
//     - PageBody is defined by page type and may be compressed
//     - PageFooter is serialized PageFooterPB. It contains page_type, uncompressed_body_size,
//       and other custom metadata. PageBody is not compressed when its size is equal to
//       uncompressed_body_size
//     - FooterSize stores the size of PageFooter
//     - Checksum is the crc32c checksum of all previous part
class PageIO {
public:
    // Compress `body' using `codec' into `compressed_body'.
    // The size of returned `compressed_body' is 0 when the body is not compressed, this
    // could happen when `codec' is null or space saving is less than `min_space_saving'.
    static Status compress_page_body(BlockCompressionCodec* codec, double min_space_saving,
                                     const std::vector<Slice>& body, OwnedSlice* compressed_body);

    // Encode page from `body' and `footer' and write to `file'.
    // `body' could be either uncompressed or compressed.
    // On success, the file pointer to the written page is stored in `result'.
    static Status write_page(io::FileWriter* writer, const std::vector<Slice>& body,
                             const PageFooterPB& footer, PagePointer* result);

    // Convenient function to compress page body and write page in one go.
    static Status compress_and_write_page(BlockCompressionCodec* codec, double min_space_saving,
                                          io::FileWriter* writer, const std::vector<Slice>& body,
                                          const PageFooterPB& footer, PagePointer* result) {
        DCHECK_EQ(footer.uncompressed_size(), Slice::compute_total_size(body));
        OwnedSlice compressed_body;
        RETURN_IF_ERROR(compress_page_body(codec, min_space_saving, body, &compressed_body));
        if (compressed_body.slice().empty()) { // uncompressed
            return write_page(writer, body, footer, result);
        }
        return write_page(writer, {compressed_body.slice()}, footer, result);
    }

    // Read and parse a page according to `opts'.
    // On success
    //     `handle' holds the memory of page data,
    //     `body' points to page body,
    //     `footer' stores the page footer.
    static Status read_and_decompress_page(const PageReadOptions& opts, PageHandle* handle,
                                           Slice* body, PageFooterPB* footer);
};

} // namespace segment_v2
} // namespace doris
