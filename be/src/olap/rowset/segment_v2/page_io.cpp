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

#include "olap/rowset/segment_v2/page_io.h"

#include <gen_cpp/segment_v2.pb.h>
#include <stdint.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "olap/olap_common.h"
#include "olap/page_cache.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/page_handle.h"
#include "util/block_compression.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/faststring.h"
#include "util/runtime_profile.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status PageIO::compress_page_body(BlockCompressionCodec* codec, double min_space_saving,
                                  const std::vector<Slice>& body, OwnedSlice* compressed_body) {
    size_t uncompressed_size = Slice::compute_total_size(body);
    if (codec != nullptr && !codec->exceed_max_compress_len(uncompressed_size)) {
        faststring buf;
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(codec->compress(body, uncompressed_size, &buf));
        double space_saving = 1.0 - static_cast<double>(buf.size()) / uncompressed_size;
        // return compressed body only when it saves more than min_space_saving
        if (space_saving > 0 && space_saving >= min_space_saving) {
            // shrink the buf to fit the len size to avoid taking
            // up the memory of the size MAX_COMPRESSED_SIZE
            RETURN_IF_CATCH_EXCEPTION(*compressed_body = buf.build());
            return Status::OK();
        }
    }
    // otherwise, do not compress
    OwnedSlice empty;
    *compressed_body = std::move(empty);
    return Status::OK();
}

Status PageIO::write_page(io::FileWriter* writer, const std::vector<Slice>& body,
                          const PageFooterPB& footer, PagePointer* result) {
    // sanity check of page footer
    CHECK(footer.has_type()) << "type must be set";
    CHECK(footer.has_uncompressed_size()) << "uncompressed_size must be set";
    switch (footer.type()) {
    case DATA_PAGE:
        CHECK(footer.has_data_page_footer());
        break;
    case INDEX_PAGE:
        CHECK(footer.has_index_page_footer());
        break;
    case DICTIONARY_PAGE:
        CHECK(footer.has_dict_page_footer());
        break;
    case SHORT_KEY_PAGE:
        CHECK(footer.has_short_key_page_footer());
        break;
    default:
        CHECK(false) << "Invalid page footer type: " << footer.type();
        break;
    }

    std::string footer_buf; // serialized footer + footer size
    footer.SerializeToString(&footer_buf);
    put_fixed32_le(&footer_buf, static_cast<uint32_t>(footer_buf.size()));

    std::vector<Slice> page = body;
    page.emplace_back(footer_buf);

    // checksum
    uint8_t checksum_buf[sizeof(uint32_t)];
    uint32_t checksum = crc32c::Value(page);
    encode_fixed32_le(checksum_buf, checksum);
    page.emplace_back(checksum_buf, sizeof(uint32_t));

    uint64_t offset = writer->bytes_appended();
    RETURN_IF_ERROR(writer->appendv(&page[0], page.size()));

    result->offset = offset;
    result->size = writer->bytes_appended() - offset;
    return Status::OK();
}

Status PageIO::read_and_decompress_page(const PageReadOptions& opts, PageHandle* handle,
                                        Slice* body, PageFooterPB* footer) {
    opts.sanity_check();
    opts.stats->total_pages_num++;

    auto cache = StoragePageCache::instance();
    PageCacheHandle cache_handle;
    StoragePageCache::CacheKey cache_key(opts.file_reader->path().native(),
                                         opts.file_reader->size(), opts.page_pointer.offset);
    if (opts.use_page_cache && cache->is_cache_available(opts.type) &&
        cache->lookup(cache_key, &cache_handle, opts.type)) {
        // we find page in cache, use it
        *handle = PageHandle(std::move(cache_handle));
        opts.stats->cached_pages_num++;
        // parse body and footer
        Slice page_slice = handle->data();
        uint32_t footer_size = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
        std::string footer_buf(page_slice.data + page_slice.size - 4 - footer_size, footer_size);
        if (!footer->ParseFromString(footer_buf)) {
            return Status::Corruption("Bad page: invalid footer, footer_size={}, file={}",
                                      footer_size, opts.file_reader->path().native());
        }
        *body = Slice(page_slice.data, page_slice.size - 4 - footer_size);
        return Status::OK();
    }

    // every page contains 4 bytes footer length and 4 bytes checksum
    const uint32_t page_size = opts.page_pointer.size;
    if (page_size < 8) {
        return Status::Corruption("Bad page: too small size ({}), file={}", page_size,
                                  opts.file_reader->path().native());
    }

    // hold compressed page at first, reset to decompressed page later
    std::unique_ptr<DataPage> page = std::make_unique<DataPage>(page_size);
    Slice page_slice(page->data(), page_size);
    {
        SCOPED_RAW_TIMER(&opts.stats->io_ns);
        size_t bytes_read = 0;
        RETURN_IF_ERROR(opts.file_reader->read_at(opts.page_pointer.offset, page_slice, &bytes_read,
                                                  &opts.io_ctx));
        DCHECK_EQ(bytes_read, page_size);
        opts.stats->compressed_bytes_read += page_size;
    }

    if (opts.verify_checksum) {
        uint32_t expect = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
        uint32_t actual = crc32c::Value(page_slice.data, page_slice.size - 4);
        if (expect != actual) {
            return Status::Corruption(
                    "Bad page: checksum mismatch (actual={} vs expect={}), file={}", actual, expect,
                    opts.file_reader->path().native());
        }
    }

    // remove checksum suffix
    page_slice.size -= 4;
    // parse and set footer
    uint32_t footer_size = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
    if (!footer->ParseFromArray(page_slice.data + page_slice.size - 4 - footer_size, footer_size)) {
        return Status::Corruption("Bad page: invalid footer, footer_size={}, file={}", footer_size,
                                  opts.file_reader->path().native());
    }

    uint32_t body_size = page_slice.size - 4 - footer_size;
    if (body_size != footer->uncompressed_size()) { // need decompress body
        if (opts.codec == nullptr) {
            return Status::Corruption(
                    "Bad page: page is compressed but codec is NO_COMPRESSION, file={}",
                    opts.file_reader->path().native());
        }
        SCOPED_RAW_TIMER(&opts.stats->decompress_ns);
        std::unique_ptr<DataPage> decompressed_page =
                std::make_unique<DataPage>(footer->uncompressed_size() + footer_size + 4);

        // decompress page body
        Slice compressed_body(page_slice.data, body_size);
        Slice decompressed_body(decompressed_page->data(), footer->uncompressed_size());
        RETURN_IF_ERROR(opts.codec->decompress(compressed_body, &decompressed_body));
        if (decompressed_body.size != footer->uncompressed_size()) {
            return Status::Corruption(
                    "Bad page: record uncompressed size={} vs real decompressed size={}, file={}",
                    footer->uncompressed_size(), decompressed_body.size,
                    opts.file_reader->path().native());
        }
        // append footer and footer size
        memcpy(decompressed_body.data + decompressed_body.size, page_slice.data + body_size,
               footer_size + 4);
        // free memory of compressed page
        page = std::move(decompressed_page);
        page_slice = Slice(page->data(), footer->uncompressed_size() + footer_size + 4);
        opts.stats->uncompressed_bytes_read += page_slice.size;
    } else {
        opts.stats->uncompressed_bytes_read += body_size;
    }

    if (opts.pre_decode && opts.encoding_info) {
        auto* pre_decoder = opts.encoding_info->get_data_page_pre_decoder();
        if (pre_decoder) {
            RETURN_IF_ERROR(pre_decoder->decode(
                    &page, &page_slice,
                    footer->data_page_footer().nullmap_size() + footer_size + 4));
        }
    }

    *body = Slice(page_slice.data, page_slice.size - 4 - footer_size);
    page->reset_size(page_slice.size);
    if (opts.use_page_cache && cache->is_cache_available(opts.type)) {
        // insert this page into cache and return the cache handle
        cache->insert(cache_key, page.get(), &cache_handle, opts.type, opts.kept_in_memory);
        *handle = PageHandle(std::move(cache_handle));
    } else {
        *handle = PageHandle(page.get());
    }
    page.release(); // memory now managed by handle
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
