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

#include <cstring>
#include <string>

#include "common/logging.h"
#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "olap/fs/block_manager.h"
#include "olap/page_cache.h"
#include "util/block_compression.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/faststring.h"
#include "util/runtime_profile.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status PageIO::compress_page_body(const BlockCompressionCodec* codec, double min_space_saving,
                                  const std::vector<Slice>& body, OwnedSlice* compressed_body) {
    size_t uncompressed_size = Slice::compute_total_size(body);
    if (codec != nullptr && uncompressed_size > 0) {
        size_t max_compressed_size = codec->max_compressed_len(uncompressed_size);
        if (max_compressed_size) {
            faststring buf;
            buf.resize(max_compressed_size);
            Slice compressed_slice(buf);
            RETURN_IF_ERROR(codec->compress(body, &compressed_slice));
            buf.resize(compressed_slice.get_size());

            double space_saving = 1.0 - static_cast<double>(buf.size()) / uncompressed_size;
            // return compressed body only when it saves more than min_space_saving
            if (space_saving > 0 && space_saving >= min_space_saving) {
                // shrink the buf to fit the len size to avoid taking
                // up the memory of the size MAX_COMPRESSED_SIZE
                buf.shrink_to_fit();
                *compressed_body = buf.build();
                return Status::OK();
            }
        }
    }
    // otherwise, do not compress
    OwnedSlice empty;
    *compressed_body = std::move(empty);
    return Status::OK();
}

Status PageIO::write_page(fs::WritableBlock* wblock, const std::vector<Slice>& body,
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

    uint64_t offset = wblock->bytes_appended();
    RETURN_IF_ERROR(wblock->appendv(&page[0], page.size()));

    result->offset = offset;
    result->size = wblock->bytes_appended() - offset;
    return Status::OK();
}

Status PageIO::read_and_decompress_page(const PageReadOptions& opts, PageHandle* handle,
                                        Slice* body, PageFooterPB* footer) {
    opts.sanity_check();
    opts.stats->total_pages_num++;

    auto cache = StoragePageCache::instance();
    PageCacheHandle cache_handle;
    StoragePageCache::CacheKey cache_key(opts.rblock->path_desc().filepath,
                                         opts.page_pointer.offset);
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
            return Status::Corruption("Bad page: invalid footer");
        }
        *body = Slice(page_slice.data, page_slice.size - 4 - footer_size);
        return Status::OK();
    }

    // every page contains 4 bytes footer length and 4 bytes checksum
    const uint32_t page_size = opts.page_pointer.size;
    if (page_size < 8) {
        return Status::Corruption(strings::Substitute("Bad page: too small size ($0)", page_size));
    }

    // hold compressed page at first, reset to decompressed page later
    std::unique_ptr<char[]> page(new char[page_size]);
    Slice page_slice(page.get(), page_size);
    {
        SCOPED_RAW_TIMER(&opts.stats->io_ns);
        RETURN_IF_ERROR(opts.rblock->read(opts.page_pointer.offset, page_slice));
        opts.stats->compressed_bytes_read += page_size;
    }

    if (opts.verify_checksum) {
        uint32_t expect = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
        uint32_t actual = crc32c::Value(page_slice.data, page_slice.size - 4);
        if (expect != actual) {
            return Status::Corruption(strings::Substitute(
                    "Bad page: checksum mismatch (actual=$0 vs expect=$1)", actual, expect));
        }
    }

    // remove checksum suffix
    page_slice.size -= 4;
    // parse and set footer
    uint32_t footer_size = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
    if (!footer->ParseFromArray(page_slice.data + page_slice.size - 4 - footer_size, footer_size)) {
        return Status::Corruption("Bad page: invalid footer");
    }

    uint32_t body_size = page_slice.size - 4 - footer_size;
    if (body_size != footer->uncompressed_size()) { // need decompress body
        if (opts.codec == nullptr) {
            return Status::Corruption("Bad page: page is compressed but codec is NO_COMPRESSION");
        }
        SCOPED_RAW_TIMER(&opts.stats->decompress_ns);
        std::unique_ptr<char[]> decompressed_page(
                new char[footer->uncompressed_size() + footer_size + 4]);

        // decompress page body
        Slice compressed_body(page_slice.data, body_size);
        Slice decompressed_body(decompressed_page.get(), footer->uncompressed_size());
        RETURN_IF_ERROR(opts.codec->decompress(compressed_body, &decompressed_body));
        if (decompressed_body.size != footer->uncompressed_size()) {
            return Status::Corruption(strings::Substitute(
                    "Bad page: record uncompressed size=$0 vs real decompressed size=$1",
                    footer->uncompressed_size(), decompressed_body.size));
        }
        // append footer and footer size
        memcpy(decompressed_body.data + decompressed_body.size, page_slice.data + body_size,
               footer_size + 4);
        // free memory of compressed page
        page = std::move(decompressed_page);
        page_slice = Slice(page.get(), footer->uncompressed_size() + footer_size + 4);
        opts.stats->uncompressed_bytes_read += page_slice.size;
    } else {
        opts.stats->uncompressed_bytes_read += body_size;
    }

    *body = Slice(page_slice.data, page_slice.size - 4 - footer_size);
    if (opts.use_page_cache && cache->is_cache_available(opts.type)) {
        // insert this page into cache and return the cache handle
        cache->insert(cache_key, page_slice, &cache_handle, opts.type, opts.kept_in_memory);
        *handle = PageHandle(std::move(cache_handle));
    } else {
        *handle = PageHandle(page_slice);
    }
    page.release(); // memory now managed by handle
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
