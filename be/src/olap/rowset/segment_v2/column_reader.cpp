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

#include "olap/rowset/segment_v2/column_reader.h"

#include "common/logging.h"
#include "env/env.h" // for RandomAccessFile
#include "gutil/strings/substitute.h" // for Substitute
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/page_decoder.h" // for PagePointer
#include "olap/rowset/segment_v2/page_handle.h" // for PageHandle
#include "olap/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "olap/rowset/segment_v2/page_compression.h"
#include "olap/rowset/segment_v2/options.h" // for PageDecoderOptions
#include "olap/types.h" // for TypeInfo
#include "olap/column_block.h" // for ColumnBlockView
#include "olap/page_cache.h"
#include "util/coding.h" // for get_varint32
#include "util/crc32c.h"
#include "util/rle_encoding.h" // for RleDecoder
#include "util/block_compression.h"
#include "util/file_manager.h"
#include "olap/rowset/segment_v2/binary_dict_page.h" // for BinaryDictPageDecoder
#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status ColumnReader::create(const ColumnReaderOptions& opts,
                            const ColumnMetaPB& meta,
                            uint64_t num_rows,
                            const std::string& file_name,
                            std::unique_ptr<ColumnReader>* reader) {
    std::unique_ptr<ColumnReader> reader_local(
        new ColumnReader(opts, meta, num_rows, file_name));
    RETURN_IF_ERROR(reader_local->init());
    *reader = std::move(reader_local);
    return Status::OK();
}

ColumnReader::ColumnReader(const ColumnReaderOptions& opts,
                           const ColumnMetaPB& meta,
                           uint64_t num_rows,
                           const std::string& file_name)
        : _opts(opts), _meta(meta), _num_rows(num_rows), _file_name(file_name) {
}

ColumnReader::~ColumnReader() = default;

Status ColumnReader::init() {
    _type_info = get_type_info((FieldType)_meta.type());
    if (_type_info == nullptr) {
        return Status::NotSupported(Substitute("unsupported typeinfo, type=$0", _meta.type()));
    }
    RETURN_IF_ERROR(EncodingInfo::get(_type_info, _meta.encoding(), &_encoding_info));
    RETURN_IF_ERROR(get_block_compression_codec(_meta.compression(), &_compress_codec));
    return Status::OK();
}

Status ColumnReader::new_iterator(ColumnIterator** iterator) {
    *iterator = new FileColumnIterator(this);
    return Status::OK();
}

Status ColumnReader::new_bitmap_index_iterator(BitmapIndexIterator** iterator) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    RETURN_IF_ERROR(_bitmap_index_reader->new_iterator(iterator));
    return Status::OK();
}

Status ColumnReader::read_page(const PagePointer& pp, OlapReaderStatistics* stats, PageHandle* handle) {
    OpenedFileHandle<RandomAccessFile> file_handle;
    RETURN_IF_ERROR(FileManager::instance()->open_file(_file_name, &file_handle));
    RandomAccessFile* input_file = file_handle.file();
    return read_page(input_file, pp, stats, handle);
}

Status ColumnReader::read_page(RandomAccessFile* file, const PagePointer& pp,
        OlapReaderStatistics* stats, PageHandle* handle) {
    stats->total_pages_num++;
    auto cache = StoragePageCache::instance();
    PageCacheHandle cache_handle;
    StoragePageCache::CacheKey cache_key(file->file_name(), pp.offset);
    if (cache->lookup(cache_key, &cache_handle)) {
        // we find page in cache, use it
        *handle = PageHandle(std::move(cache_handle));
        stats->cached_pages_num++;
        return Status::OK();
    }
    // Now we read this from file.
    size_t page_size = pp.size;
    if (page_size < sizeof(uint32_t)) {
        return Status::Corruption(Substitute("Bad page, page size is too small, size=$0", page_size));
    }

    // Now we use this buffer to store page from storage, if this page is compressed
    // this buffer will assigned uncompressed page, and origin content will be freed.
    std::unique_ptr<uint8_t[]> page(new uint8_t[page_size]);
    Slice page_slice(page.get(), page_size);
    {
        SCOPED_RAW_TIMER(&stats->io_ns);
        RETURN_IF_ERROR(file->read_at(pp.offset, page_slice));
        stats->compressed_bytes_read += page_size;
    }

    size_t data_size = page_size - 4;
    if (_opts.verify_checksum) {
        uint32_t expect = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
        uint32_t actual = crc32c::Value(page_slice.data, page_slice.size - 4);
        if (expect != actual) {
            return Status::Corruption(
                Substitute("Page checksum mismatch, actual=$0 vs expect=$1", actual, expect));
        }
    }

    // remove page's suffix
    page_slice.size = data_size;

    if (_compress_codec != nullptr) {
        PageDecompressor decompressor(page_slice, _compress_codec);

        Slice uncompressed_page;
        {
            SCOPED_RAW_TIMER(&stats->decompress_ns);
            RETURN_IF_ERROR(decompressor.decompress_to(&uncompressed_page));
        }

        // If decompressor create new heap memory for uncompressed data,
        // assign this uncompressed page to page and page slice
        if (uncompressed_page.data != page_slice.data) {
            page.reset((uint8_t*)uncompressed_page.data);
        }
        page_slice = uncompressed_page;
        stats->uncompressed_bytes_read += page_slice.size;
    }
    // insert this into cache and return the cache handle
    cache->insert(cache_key, page_slice, &cache_handle);
    page.release();
    *handle = PageHandle(std::move(cache_handle));

    return Status::OK();
}

Status ColumnReader::get_row_ranges_by_zone_map(CondColumn* cond_column,
                                                const std::vector<CondColumn*>& delete_conditions,
                                                std::vector<uint32_t>* delete_partial_filtered_pages,
                                                RowRanges* row_ranges) {
    RETURN_IF_ERROR(_ensure_index_loaded());

    std::vector<uint32_t> page_indexes;
    RETURN_IF_ERROR(_get_filtered_pages(cond_column, delete_conditions, delete_partial_filtered_pages, &page_indexes));
    RETURN_IF_ERROR(_calculate_row_ranges(page_indexes, row_ranges));
    return Status::OK();
}

Status ColumnReader::_get_filtered_pages(CondColumn* cond_column,
                                         const std::vector<CondColumn*>& delete_conditions,
                                         std::vector<uint32_t>* delete_partial_filtered_pages,
                                         std::vector<uint32_t>* page_indexes) {
    FieldType type = _type_info->type();
    const std::vector<ZoneMapPB>& zone_maps = _column_zone_map->get_column_zone_map();
    int32_t page_size = _column_zone_map->num_pages();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta.length()));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta.length()));
    for (int32_t i = 0; i < page_size; ++i) {
        // min value and max value are valid if has_not_null is true
        if (zone_maps[i].has_not_null()) {
            min_value->from_string(zone_maps[i].min());
            max_value->from_string(zone_maps[i].max());
        }
        // for compatible original Cond eval logic
        // TODO(hkp): optimize OlapCond
        if (zone_maps[i].has_null()) {
            // for compatible, if exist null, original logic treat null as min
            min_value->set_null();
            if (!zone_maps[i].has_not_null()) {
                // for compatible OlapCond's 'is not null'
                max_value->set_null();
            }
        }
        if (cond_column == nullptr || cond_column->eval({min_value.get(), max_value.get()})) {
            bool should_read = true;
            for (auto& col_cond : delete_conditions) {
                int state = col_cond->del_eval({min_value.get(), max_value.get()});
                if (state == DEL_SATISFIED) {
                    should_read = false;
                    break;
                } else if (state == DEL_PARTIAL_SATISFIED) {
                    delete_partial_filtered_pages->push_back(i);
                }
            }
            if (should_read) {
                page_indexes->push_back(i);
            }
        }
    }
    return Status::OK();
}

Status ColumnReader::_calculate_row_ranges(const std::vector<uint32_t>& page_indexes, RowRanges* row_ranges) {
    row_ranges->clear();
    for (auto i : page_indexes) {
        rowid_t page_first_id = _ordinal_index->get_first_row_id(i);
        rowid_t page_last_id = _ordinal_index->get_last_row_id(i);
        RowRanges page_row_ranges(RowRanges::create_single(page_first_id, page_last_id + 1));
        RowRanges::ranges_union(*row_ranges, page_row_ranges, row_ranges);
    }
    return Status::OK();
}

Status ColumnReader::get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    RowRanges bf_row_ranges;
    std::unique_ptr<BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_index_reader->new_iterator(&bf_iter));
    size_t range_size = row_ranges->range_size();
    // get covered page ids
    std::set<uint32_t> page_ids;
    for (int i = 0; i < range_size; ++i) {
        int64_t from = row_ranges->get_range_from(i);
        int64_t idx = from;
        int64_t to = row_ranges->get_range_to(i);
        auto iter = _ordinal_index->seek_at_or_before(from);
        while (idx < to) {
            page_ids.insert(iter.cur_idx());
            idx = iter.cur_page_last_row_id() + 1;
            iter.next();
        }
    }
    for (auto& pid : page_ids) {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(bf_iter->read_bloom_filter(pid, &bf));
        if (cond_column->eval(bf.get())) {
            bf_row_ranges.add(RowRange(_ordinal_index->get_first_row_id(pid),
                    _ordinal_index->get_last_row_id(pid) + 1));
        }
    }
    RowRanges::ranges_intersection(*row_ranges, bf_row_ranges, row_ranges);
    return Status::OK();
}

Status ColumnReader::_load_ordinal_index() {
    PagePointer pp = _meta.ordinal_index_page();
    PageHandle ph;
    OlapReaderStatistics stats;
    RETURN_IF_ERROR(read_page(pp, &stats, &ph));

    _ordinal_index.reset(new OrdinalPageIndex(ph.data(), _num_rows));
    RETURN_IF_ERROR(_ordinal_index->load());
    return Status::OK();
}

Status ColumnReader::_load_zone_map_index() {
    if (_meta.has_zone_map_page()) {
        PagePointer pp = _meta.zone_map_page();
        PageHandle ph;
        // tmp statistics
        OlapReaderStatistics stats;
        RETURN_IF_ERROR(read_page(pp, &stats, &ph));
        _column_zone_map.reset(new ColumnZoneMap(ph.data()));
        RETURN_IF_ERROR(_column_zone_map->load());
    } else {
        _column_zone_map.reset(nullptr);
    }
    return Status::OK();
}

Status ColumnReader::_load_bitmap_index() {
    if (_meta.has_bitmap_index()) {
        const BitmapIndexColumnPB& bitmap_index_meta = _meta.bitmap_index();
        _bitmap_index_reader.reset(new BitmapIndexReader(_file_name, bitmap_index_meta));
        RETURN_IF_ERROR(_bitmap_index_reader->load());
    } else {
        _bitmap_index_reader.reset(nullptr);
    }
    return Status::OK();
}

Status ColumnReader::_load_bloom_filter_index() {
    if (_meta.has_bloom_filter_index()) {
        const BloomFilterIndexPB& bloom_filter_index_meta = _meta.bloom_filter_index();
        _bloom_filter_index_reader.reset(new BloomFilterIndexReader(_file_name, bloom_filter_index_meta));
        RETURN_IF_ERROR(_bloom_filter_index_reader->load());
    } else {
        _bloom_filter_index_reader.reset(nullptr);
    }
    return Status::OK();
}

Status ColumnReader::seek_to_first(OrdinalPageIndexIterator* iter) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    *iter = _ordinal_index->begin();
    if (!iter->valid()) {
        return Status::NotFound("Failed to seek to first rowid");
    }
    return Status::OK();
}

Status ColumnReader::seek_at_or_before(rowid_t rowid, OrdinalPageIndexIterator* iter) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    *iter = _ordinal_index->seek_at_or_before(rowid);
    if (!iter->valid()) {
        return Status::NotFound(Substitute("Failed to seek to rowid $0, ", rowid));
    }
    return Status::OK();
}

FileColumnIterator::FileColumnIterator(ColumnReader* reader) : _reader(reader) {
}

FileColumnIterator::~FileColumnIterator() = default;

Status FileColumnIterator::seek_to_first() {
    RETURN_IF_ERROR(_reader->seek_to_first(&_page_iter));

    _page.reset(new ParsedPage());
    RETURN_IF_ERROR(_read_page(_page_iter, _page.get()));

    _seek_to_pos_in_page(_page.get(), 0);
    _current_rowid = 0;

    return Status::OK();
}

Status FileColumnIterator::seek_to_ordinal(rowid_t rid) {
    // if current page contains this row, we don't need to seek
    if (_page == nullptr || !_page->contains(rid)) {
        RETURN_IF_ERROR(_reader->seek_at_or_before(rid, &_page_iter));
        _page.reset(new ParsedPage());
        RETURN_IF_ERROR(_read_page(_page_iter, _page.get()));
    }
    _seek_to_pos_in_page(_page.get(), rid - _page->first_rowid);
    _current_rowid = rid;
    return Status::OK();
}

void FileColumnIterator::_seek_to_pos_in_page(ParsedPage* page, uint32_t offset_in_page) {
    if (page->offset_in_page == offset_in_page) {
        // fast path, do nothing
        return;
    }

    uint32_t pos_in_data = offset_in_page;
    if (_reader->is_nullable()) {
        rowid_t offset_in_data = 0;
        rowid_t skips = offset_in_page;

        if (offset_in_page > page->offset_in_page) {
            // forward, reuse null bitmap
            skips = offset_in_page - page->offset_in_page;
            offset_in_data = page->data_decoder->current_index();
        } else {
            // rewind null bitmap, and
            page->null_decoder = RleDecoder<bool>((const uint8_t*)page->null_bitmap.data, page->null_bitmap.size, 1);
        }

        auto skip_nulls = page->null_decoder.Skip(skips);
        pos_in_data = offset_in_data + skips - skip_nulls;
    }

    page->data_decoder->seek_to_position_in_page(pos_in_data);
    page->offset_in_page = offset_in_page;
}

Status FileColumnIterator::next_batch(size_t* n, ColumnBlockView* dst) {
    size_t remaining = *n;
    while (remaining > 0) {
        if (!_page->has_remaining()) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
        }

        auto iter = std::find(_delete_partial_statisfied_pages.begin(),
                _delete_partial_statisfied_pages.end(), _page->page_index);
        bool is_partial = iter != _delete_partial_statisfied_pages.end();
        if (is_partial) {
            dst->column_block()->set_delete_state(DEL_PARTIAL_SATISFIED);
        } else {
            dst->column_block()->set_delete_state(DEL_NOT_SATISFIED);
        }
        // number of rows to be read from this page
        size_t nrows_in_page = std::min(remaining, _page->remaining());
        size_t nrows_to_read = nrows_in_page;
        if (_reader->is_nullable()) {
            // when this column is nullable we read data in some runs
            // first we read null bits in the same value, if this is null, we
            // don't need to read value from page.
            // If this is not null, we read data from page in batch.
            // This would be bad in case that data is arranged one by one, which
            // will lead too many function calls to PageDecoder
            while (nrows_to_read > 0) {
                bool is_null = false;
                size_t this_run = _page->null_decoder.GetNextRun(&is_null, nrows_to_read);
                // we use num_rows only for CHECK
                size_t num_rows = this_run;
                if (!is_null) {
                    RETURN_IF_ERROR(_page->data_decoder->next_batch(&num_rows, dst));
                    DCHECK_EQ(this_run, num_rows);
                }

                // set null bits
                dst->set_null_bits(this_run, is_null);

                nrows_to_read -= this_run;
                _page->offset_in_page += this_run;
                dst->advance(this_run);
                _current_rowid += this_run;
            }
        } else {
            RETURN_IF_ERROR(_page->data_decoder->next_batch(&nrows_to_read, dst));
            DCHECK_EQ(nrows_to_read, nrows_in_page);

            if (dst->is_nullable()) {
                dst->set_null_bits(nrows_to_read, false);
            }

            _page->offset_in_page += nrows_to_read;
            dst->advance(nrows_to_read);
            _current_rowid += nrows_to_read;
        }
        remaining -= nrows_in_page;
    }
    *n -= remaining;
    // TODO(hkp): for string type, the bytes_read should be passed to page decoder
    // bytes_read = data size + null bitmap size
    _opts.stats->bytes_read += *n * dst->type_info()->size() + BitmapSize(*n);
    return Status::OK();
}

Status FileColumnIterator::_load_next_page(bool* eos) {
    _page_iter.next();
    if (!_page_iter.valid()) {
        *eos = true;
        return Status::OK();
    }
    _page.reset(new ParsedPage());
    RETURN_IF_ERROR(_read_page(_page_iter, _page.get()));
    _seek_to_pos_in_page(_page.get(), 0);
    *eos = false;
    return Status::OK();
}

// read one page from file and parse this page to make
// it ready to read
Status FileColumnIterator::_read_page(const OrdinalPageIndexIterator& iter, ParsedPage* page) {
    page->page_pointer = iter.page();
    RETURN_IF_ERROR(_reader->read_page(_file, page->page_pointer, _opts.stats, &page->page_handle));
    // TODO(zc): read page from file
    Slice data = page->page_handle.data();

    // decode first rowid
    if (!get_varint32(&data, &page->first_rowid)) {
        return Status::Corruption("Bad page, failed to decode first rowid");
    }
    // decode number rows
    if (!get_varint32(&data, &page->num_rows)) {
        return Status::Corruption("Bad page, failed to decode rows count");
    }
    if (_reader->is_nullable()) {
        uint32_t null_bitmap_size = 0;
        if (!get_varint32(&data, &null_bitmap_size)) {
            return Status::Corruption("Bad page, failed to decode null bitmap size");
        }
        if (null_bitmap_size > data.size) {
            return Status::Corruption(
                Substitute("Bad page, null bitmap too large $0 vs $1", null_bitmap_size, data.size));
        }
        page->null_decoder = RleDecoder<bool>((uint8_t*)data.data, null_bitmap_size, 1);
        page->null_bitmap = Slice(data.data, null_bitmap_size);

        // remove null bitmap
        data.remove_prefix(null_bitmap_size);
    }

    // create page data decoder
    PageDecoderOptions options;
    RETURN_IF_ERROR(_reader->encoding_info()->create_page_decoder(data, options, &page->data_decoder));
    RETURN_IF_ERROR(page->data_decoder->init());

    // lazy init dict_encoding'dict for three reasons
    // 1. a column use dictionary encoding still has non-dict-encoded data pages are seeked,load dict when necessary
    // 2. ColumnReader which is owned by Segment and Rowset can being alive even when there is no query,it should retain memory as small as possible.
    // 3. Iterators of the same column won't repeat load the dict page because of page cache.
    if (_reader->encoding_info()->encoding() == DICT_ENCODING) {
        BinaryDictPageDecoder* binary_dict_page_decoder = (BinaryDictPageDecoder*)page->data_decoder;
        if (binary_dict_page_decoder->is_dict_encoding()) {
            if (_dict_decoder == nullptr) {
                PagePointer pp = _reader->get_dict_page_pointer();
                RETURN_IF_ERROR(_reader->read_page(_file, pp, _opts.stats, &_dict_page_handle));

                _dict_decoder.reset(new BinaryPlainPageDecoder(_dict_page_handle.data()));
                RETURN_IF_ERROR(_dict_decoder->init());
            }
            binary_dict_page_decoder->set_dict_decoder(_dict_decoder.get());
        }
    }

    page->offset_in_page = 0;
    page->page_index = iter.cur_idx();
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_zone_map(CondColumn* cond_column,
                                      const std::vector<CondColumn*>& delete_conditions,
                                      RowRanges* row_ranges) {
    if (_reader->has_zone_map()) {
        RETURN_IF_ERROR(_reader->get_row_ranges_by_zone_map(cond_column, delete_conditions,
                &_delete_partial_statisfied_pages, row_ranges));
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) {
    if (cond_column != nullptr &&
            cond_column->can_do_bloom_filter() && _reader->has_bloom_filter_index()) {
        RETURN_IF_ERROR(_reader->get_row_ranges_by_bloom_filter(cond_column, row_ranges));
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    // be consistent with segment v1
    // if _has_default_value, we should create default column iterator for this column, and
    // "NULL" is a special default value which means the default value is null.
    if (_has_default_value) {
        if (_default_value == "NULL") {
            DCHECK(_is_nullable);
            _is_default_value_null = true;
        } else {
            TypeInfo* type_info = get_type_info(_type);
            _type_size = type_info->size();
            _mem_value = reinterpret_cast<void*>(_pool->allocate(_type_size));
            OLAPStatus s = OLAP_SUCCESS;
            if (_type == OLAP_FIELD_TYPE_CHAR) {
                int32_t length = _schema_length;
                char* string_buffer = reinterpret_cast<char*>(_pool->allocate(length));
                memset(string_buffer, 0, length);
                memory_copy(string_buffer, _default_value.c_str(), _default_value.length());
                ((Slice*)_mem_value)->size = length;
                ((Slice*)_mem_value)->data = string_buffer;
            } else if ( _type == OLAP_FIELD_TYPE_VARCHAR ||
                _type == OLAP_FIELD_TYPE_HLL ||
                _type == OLAP_FIELD_TYPE_OBJECT ) {
                int32_t length = _default_value.length();
                char* string_buffer = reinterpret_cast<char*>(_pool->allocate(length));
                memory_copy(string_buffer, _default_value.c_str(), length);
                ((Slice*)_mem_value)->size = length;
                ((Slice*)_mem_value)->data = string_buffer;
            } else {
                s = type_info->from_string(_mem_value, _default_value);
            }
            if (s != OLAP_SUCCESS) {
                return Status::InternalError(
                        strings::Substitute("get value of type from default value failed. status:$0", s));
            }
        }
    } else if (_is_nullable) {
        // if _has_default_value is false but _is_nullable is true, we should return null as default value.
        _is_default_value_null = true;
    } else {
        return Status::InternalError("invalid default value column for no default value and not nullable");
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::next_batch(size_t* n, ColumnBlockView* dst) {
    if (dst->is_nullable()) {
        dst->set_null_bits(*n, _is_default_value_null);
    }

    if (_is_default_value_null) {
        dst->advance(*n);
    } else {
        for (int i = 0; i < *n; ++i) {
            memcpy(dst->data(), _mem_value, _type_size);
            dst->advance(1);
        }
    }
    return Status::OK();
}

}
}
