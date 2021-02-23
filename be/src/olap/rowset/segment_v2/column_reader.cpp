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
#include "gutil/strings/substitute.h"                // for Substitute
#include "olap/column_block.h"                       // for ColumnBlockView
#include "olap/rowset/segment_v2/binary_dict_page.h" // for BinaryDictPageDecoder
#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/page_handle.h"   // for PageHandle
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "olap/types.h"                          // for TypeInfo
#include "util/block_compression.h"
#include "util/coding.h"       // for get_varint32
#include "util/rle_encoding.h" // for RleDecoder

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status ColumnReader::create(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                            uint64_t num_rows, const std::string& file_name,
                            std::unique_ptr<ColumnReader>* reader) {
    if (is_scalar_type((FieldType)meta.type())) {
        std::unique_ptr<ColumnReader> reader_local(
                new ColumnReader(opts, meta, num_rows, file_name));
        RETURN_IF_ERROR(reader_local->init());
        *reader = std::move(reader_local);
        return Status::OK();
    } else {
        auto type = (FieldType)meta.type();
        switch (type) {
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            std::unique_ptr<ColumnReader> item_reader;
            DCHECK(meta.children_columns_size() == 1);
            RETURN_IF_ERROR(ColumnReader::create(opts, meta.children_columns(0),
                                                 meta.children_columns(0).num_rows(), file_name,
                                                 &item_reader));
            RETURN_IF_ERROR(item_reader->init());

            std::unique_ptr<ColumnReader> array_reader(
                    new ColumnReader(opts, meta, num_rows, file_name));
            RETURN_IF_ERROR(array_reader->init());
            array_reader->_sub_readers.resize(1);
            array_reader->_sub_readers[0] = std::move(item_reader);
            *reader = std::move(array_reader);
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type for ColumnReader: " +
                                        std::to_string(type));
        }
    }
}

ColumnReader::ColumnReader(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                           uint64_t num_rows, const std::string& file_name)
        : _meta(meta), _opts(opts), _num_rows(num_rows), _file_name(file_name) {}

ColumnReader::~ColumnReader() = default;

Status ColumnReader::init() {
    _type_info = get_type_info(&_meta);
    if (_type_info == nullptr) {
        return Status::NotSupported(
                strings::Substitute("unsupported typeinfo, type=$0", _meta.type()));
    }
    RETURN_IF_ERROR(EncodingInfo::get(_type_info, _meta.encoding(), &_encoding_info));
    RETURN_IF_ERROR(get_block_compression_codec(_meta.compression(), &_compress_codec));

    for (int i = 0; i < _meta.indexes_size(); i++) {
        auto& index_meta = _meta.indexes(i);
        switch (index_meta.type()) {
        case ORDINAL_INDEX:
            _ordinal_index_meta = &index_meta.ordinal_index();
            break;
        case ZONE_MAP_INDEX:
            _zone_map_index_meta = &index_meta.zone_map_index();
            break;
        case BITMAP_INDEX:
            _bitmap_index_meta = &index_meta.bitmap_index();
            break;
        case BLOOM_FILTER_INDEX:
            _bf_index_meta = &index_meta.bloom_filter_index();
            break;
        default:
            return Status::Corruption(strings::Substitute(
                    "Bad file $0: invalid column index type $1", _file_name, index_meta.type()));
        }
    }
    if (_ordinal_index_meta == nullptr) {
        return Status::Corruption(strings::Substitute(
                "Bad file $0: missing ordinal index for column $1", _file_name, _meta.column_id()));
    }
    return Status::OK();
}

Status ColumnReader::new_bitmap_index_iterator(BitmapIndexIterator** iterator) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    RETURN_IF_ERROR(_bitmap_index->new_iterator(iterator));
    return Status::OK();
}

Status ColumnReader::read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp,
                               PageHandle* handle, Slice* page_body, PageFooterPB* footer) {
    iter_opts.sanity_check();
    PageReadOptions opts;
    opts.rblock = iter_opts.rblock;
    opts.page_pointer = pp;
    opts.codec = _compress_codec;
    opts.stats = iter_opts.stats;
    opts.verify_checksum = _opts.verify_checksum;
    opts.use_page_cache = iter_opts.use_page_cache;
    opts.kept_in_memory = _opts.kept_in_memory;
    opts.type = iter_opts.type;

    return PageIO::read_and_decompress_page(opts, handle, page_body, footer);
}

Status ColumnReader::get_row_ranges_by_zone_map(
        CondColumn* cond_column, CondColumn* delete_condition,
        std::unordered_set<uint32_t>* delete_partial_filtered_pages, RowRanges* row_ranges) {
    RETURN_IF_ERROR(_ensure_index_loaded());

    std::vector<uint32_t> page_indexes;
    RETURN_IF_ERROR(_get_filtered_pages(cond_column, delete_condition,
                                        delete_partial_filtered_pages, &page_indexes));
    RETURN_IF_ERROR(_calculate_row_ranges(page_indexes, row_ranges));
    return Status::OK();
}

bool ColumnReader::match_condition(CondColumn* cond) const {
    if (_zone_map_index_meta == nullptr || cond == nullptr) {
        return true;
    }
    FieldType type = _type_info->type();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta.length()));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta.length()));
    _parse_zone_map(_zone_map_index_meta->segment_zone_map(), min_value.get(), max_value.get());

    return _zone_map_match_condition(_zone_map_index_meta->segment_zone_map(), min_value.get(),
                                     max_value.get(), cond);
}

void ColumnReader::_parse_zone_map(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                                   WrapperField* max_value_container) const {
    // min value and max value are valid if has_not_null is true
    if (zone_map.has_not_null()) {
        min_value_container->from_string(zone_map.min());
        max_value_container->from_string(zone_map.max());
    }
    // for compatible original Cond eval logic
    // TODO(hkp): optimize OlapCond
    if (zone_map.has_null()) {
        // for compatible, if exist null, original logic treat null as min
        min_value_container->set_null();
        if (!zone_map.has_not_null()) {
            // for compatible OlapCond's 'is not null'
            max_value_container->set_null();
        }
    }
}

bool ColumnReader::_zone_map_match_condition(const ZoneMapPB& zone_map,
                                             WrapperField* min_value_container,
                                             WrapperField* max_value_container,
                                             CondColumn* cond) const {
    if (!zone_map.has_not_null() && !zone_map.has_null()) {
        return false; // no data in this zone
    }

    if (cond == nullptr || zone_map.pass_all()) {
        return true;
    }

    return cond->eval({min_value_container, max_value_container});
}

Status ColumnReader::_get_filtered_pages(
        CondColumn* cond_column, CondColumn* delete_condition,
        std::unordered_set<uint32_t>* delete_partial_filtered_pages,
        std::vector<uint32_t>* page_indexes) {
    FieldType type = _type_info->type();
    const std::vector<ZoneMapPB>& zone_maps = _zone_map_index->page_zone_maps();
    int32_t page_size = _zone_map_index->num_pages();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta.length()));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta.length()));
    for (int32_t i = 0; i < page_size; ++i) {
        if (zone_maps[i].pass_all()) {
            page_indexes->push_back(i);
        } else {
            _parse_zone_map(zone_maps[i], min_value.get(), max_value.get());
            if (_zone_map_match_condition(zone_maps[i], min_value.get(), max_value.get(),
                                          cond_column)) {
                bool should_read = true;
                if (delete_condition != nullptr) {
                    int state = delete_condition->del_eval({min_value.get(), max_value.get()});
                    if (state == DEL_SATISFIED) {
                        should_read = false;
                    } else if (state == DEL_PARTIAL_SATISFIED) {
                        delete_partial_filtered_pages->insert(i);
                    }
                }
                if (should_read) {
                    page_indexes->push_back(i);
                }
            }
        }
    }
    return Status::OK();
}

Status ColumnReader::_calculate_row_ranges(const std::vector<uint32_t>& page_indexes,
                                           RowRanges* row_ranges) {
    row_ranges->clear();
    for (auto i : page_indexes) {
        ordinal_t page_first_id = _ordinal_index->get_first_ordinal(i);
        ordinal_t page_last_id = _ordinal_index->get_last_ordinal(i);
        RowRanges page_row_ranges(RowRanges::create_single(page_first_id, page_last_id + 1));
        RowRanges::ranges_union(*row_ranges, page_row_ranges, row_ranges);
    }
    return Status::OK();
}

Status ColumnReader::get_row_ranges_by_bloom_filter(CondColumn* cond_column,
                                                    RowRanges* row_ranges) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    RowRanges bf_row_ranges;
    std::unique_ptr<BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_index->new_iterator(&bf_iter));
    size_t range_size = row_ranges->range_size();
    // get covered page ids
    std::set<uint32_t> page_ids;
    for (int i = 0; i < range_size; ++i) {
        int64_t from = row_ranges->get_range_from(i);
        int64_t idx = from;
        int64_t to = row_ranges->get_range_to(i);
        auto iter = _ordinal_index->seek_at_or_before(from);
        while (idx < to && iter.valid()) {
            page_ids.insert(iter.page_index());
            idx = iter.last_ordinal() + 1;
            iter.next();
        }
    }
    for (auto& pid : page_ids) {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(bf_iter->read_bloom_filter(pid, &bf));
        if (cond_column->eval(bf.get())) {
            bf_row_ranges.add(RowRange(_ordinal_index->get_first_ordinal(pid),
                                       _ordinal_index->get_last_ordinal(pid) + 1));
        }
    }
    RowRanges::ranges_intersection(*row_ranges, bf_row_ranges, row_ranges);
    return Status::OK();
}

Status ColumnReader::_load_ordinal_index(bool use_page_cache, bool kept_in_memory) {
    DCHECK(_ordinal_index_meta != nullptr);
    _ordinal_index.reset(new OrdinalIndexReader(_file_name, _ordinal_index_meta, _num_rows));
    return _ordinal_index->load(use_page_cache, kept_in_memory);
}

Status ColumnReader::_load_zone_map_index(bool use_page_cache, bool kept_in_memory) {
    if (_zone_map_index_meta != nullptr) {
        _zone_map_index.reset(new ZoneMapIndexReader(_file_name, _zone_map_index_meta));
        return _zone_map_index->load(use_page_cache, kept_in_memory);
    }
    return Status::OK();
}

Status ColumnReader::_load_bitmap_index(bool use_page_cache, bool kept_in_memory) {
    if (_bitmap_index_meta != nullptr) {
        _bitmap_index.reset(new BitmapIndexReader(_file_name, _bitmap_index_meta));
        return _bitmap_index->load(use_page_cache, kept_in_memory);
    }
    return Status::OK();
}

Status ColumnReader::_load_bloom_filter_index(bool use_page_cache, bool kept_in_memory) {
    if (_bf_index_meta != nullptr) {
        _bloom_filter_index.reset(new BloomFilterIndexReader(_file_name, _bf_index_meta));
        return _bloom_filter_index->load(use_page_cache, kept_in_memory);
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

Status ColumnReader::seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter) {
    RETURN_IF_ERROR(_ensure_index_loaded());
    *iter = _ordinal_index->seek_at_or_before(ordinal);
    if (!iter->valid()) {
        return Status::NotFound(strings::Substitute("Failed to seek to ordinal $0, ", ordinal));
    }
    return Status::OK();
}

Status ColumnReader::new_iterator(ColumnIterator** iterator) {
    if (is_scalar_type((FieldType)_meta.type())) {
        *iterator = new FileColumnIterator(this);
        return Status::OK();
    } else {
        auto type = (FieldType)_meta.type();
        switch (type) {
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            ColumnIterator* item_iterator;
            RETURN_IF_ERROR(_sub_readers[0]->new_iterator(&item_iterator));
            FileColumnIterator* offset_iterator = new FileColumnIterator(this);
            *iterator = new ArrayFileColumnIterator(offset_iterator, item_iterator);
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type to create iterator: " +
                                        std::to_string(type));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

ArrayFileColumnIterator::ArrayFileColumnIterator(FileColumnIterator* offset_reader,
                                                 ColumnIterator* item_iterator) {
    _offset_iterator.reset(offset_reader);
    _item_iterator.reset(item_iterator);
}

Status ArrayFileColumnIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(_offset_iterator->init(opts));
    RETURN_IF_ERROR(_item_iterator->init(opts));
    TypeInfo* bigint_type_info = get_scalar_type_info(FieldType::OLAP_FIELD_TYPE_BIGINT);
    RETURN_IF_ERROR(ColumnVectorBatch::create(1024, _offset_iterator->is_nullable(),
                                              bigint_type_info, nullptr, &_offset_batch));
    return Status::OK();
}

// every invoke this method, _offset_batch will be cover, so this method is not thread safe.
Status ArrayFileColumnIterator::next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) {
    // 1. read n offsets into  _offset_batch;
    _offset_batch->resize(*n + 1);
    ColumnBlock ordinal_block(_offset_batch.get(), nullptr);
    ColumnBlockView ordinal_view(&ordinal_block);
    RETURN_IF_ERROR(_offset_iterator->next_batch(n, &ordinal_view, has_null));

    if (*n == 0) {
        return Status::OK();
    }

    // 2. Because we should read n + 1 offsets, so read one more here.
    PageDecoder* offset_page_decoder = _offset_iterator->get_current_page()->data_decoder;
    if (offset_page_decoder->has_remaining()) { // not _page->has_remaining()
        size_t i = 1;
        offset_page_decoder->peek_next_batch(&i, &ordinal_view); // not null
        DCHECK(i == 1);
    } else {
        *(reinterpret_cast<ordinal_t*>(ordinal_view.data())) =
                _offset_iterator->get_current_page()->next_array_item_ordinal;
    }
    ordinal_view.set_null_bits(1, false);
    ordinal_view.advance(1);

    // 3. For nullable data，fill null ordinals from last to start: 0 N N 3 N 5 -> 0 3 3 3 5 5
    if (_offset_iterator->is_nullable()) {
        size_t j = *n + 1;
        while (--j > 0) { // j can not be less than 0
            ColumnBlockCell cell = ordinal_block.cell(j - 1);
            if (cell.is_null()) {
                ordinal_t pre =
                        *(reinterpret_cast<ordinal_t*>(ordinal_block.cell(j).mutable_cell_ptr()));
                *(reinterpret_cast<ordinal_t*>(cell.mutable_cell_ptr())) = pre;
            }
        }
    }

    // 4. read child column's data and generate collections.
    ColumnBlock* collection_block = dst->column_block();
    auto* collection_batch =
            reinterpret_cast<ArrayColumnVectorBatch*>(collection_block->vector_batch());
    size_t start_offset = dst->current_offset();
    size_t end_offset = start_offset + *n;
    auto* ordinals = reinterpret_cast<ordinal_t*>(ordinal_block.data());
    collection_batch->put_item_ordinal(ordinals, start_offset, *n + 1);

    size_t size_to_read = ordinals[*n] - ordinals[0];
    bool item_has_null = false;
    if (size_to_read > 0) {
        _item_iterator->seek_to_ordinal(ordinals[0]);
        ColumnVectorBatch* item_vector_batch = collection_batch->elements();
        RETURN_IF_ERROR(item_vector_batch->resize(collection_batch->item_offset(end_offset)));
        ColumnBlock item_block = ColumnBlock(item_vector_batch, dst->pool());
        ColumnBlockView item_view =
                ColumnBlockView(&item_block, collection_batch->item_offset(start_offset));
        size_t real_read = size_to_read;
        RETURN_IF_ERROR(_item_iterator->next_batch(&real_read, &item_view, &item_has_null));
        DCHECK(size_to_read == real_read);
    }

    if (dst->is_nullable()) {
        bool* collection_nulls =
                const_cast<bool*>(&collection_batch->null_signs()[dst->current_offset()]);
        memcpy(collection_nulls, ordinal_block.vector_batch()->null_signs(), sizeof(bool) * *n);
        dst->advance(*n);
    } else {
        dst->set_null_bits(*n, false);
        dst->advance(*n);
    }

    collection_batch->prepare_for_read(0, end_offset, item_has_null);
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////

FileColumnIterator::FileColumnIterator(ColumnReader* reader) : _reader(reader) {}

FileColumnIterator::~FileColumnIterator() = default;

Status FileColumnIterator::seek_to_first() {
    RETURN_IF_ERROR(_reader->seek_to_first(&_page_iter));
    RETURN_IF_ERROR(_read_data_page(_page_iter));

    _seek_to_pos_in_page(_page.get(), 0);
    _current_ordinal = 0;
    return Status::OK();
}

Status FileColumnIterator::seek_to_ordinal(ordinal_t ord) {
    // if current page contains this row, we don't need to seek
    if (_page == nullptr || !_page->contains(ord) || !_page_iter.valid()) {
        RETURN_IF_ERROR(_reader->seek_at_or_before(ord, &_page_iter));
        RETURN_IF_ERROR(_read_data_page(_page_iter));
    }
    _seek_to_pos_in_page(_page.get(), ord - _page->first_ordinal);
    _current_ordinal = ord;
    return Status::OK();
}

void FileColumnIterator::_seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page) {
    if (page->offset_in_page == offset_in_page) {
        // fast path, do nothing
        return;
    }

    ordinal_t pos_in_data = offset_in_page;
    if (_page->has_null) {
        ordinal_t offset_in_data = 0;
        ordinal_t skips = offset_in_page;

        if (offset_in_page > page->offset_in_page) {
            // forward, reuse null bitmap
            skips = offset_in_page - page->offset_in_page;
            offset_in_data = page->data_decoder->current_index();
        } else {
            // rewind null bitmap, and
            page->null_decoder = RleDecoder<bool>((const uint8_t*)page->null_bitmap.data,
                                                  page->null_bitmap.size, 1);
        }

        auto skip_nulls = page->null_decoder.Skip(skips);
        pos_in_data = offset_in_data + skips - skip_nulls;
    }

    page->data_decoder->seek_to_position_in_page(pos_in_data);
    page->offset_in_page = offset_in_page;
}

Status FileColumnIterator::next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) {
    size_t remaining = *n;
    *has_null = false;
    while (remaining > 0) {
        if (!_page->has_remaining()) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
        }

        auto iter = _delete_partial_satisfied_pages.find(_page->page_index);
        bool is_partial = iter != _delete_partial_satisfied_pages.end();
        if (is_partial) {
            dst->column_block()->set_delete_state(DEL_PARTIAL_SATISFIED);
        } else {
            dst->column_block()->set_delete_state(DEL_NOT_SATISFIED);
        }
        // number of rows to be read from this page
        size_t nrows_in_page = std::min(remaining, _page->remaining());
        size_t nrows_to_read = nrows_in_page;
        if (_page->has_null) {
            // when this page contains NULLs we read data in some runs
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
                } else {
                    *has_null = true;
                }

                // set null bits
                dst->set_null_bits(this_run, is_null);

                nrows_to_read -= this_run;
                _page->offset_in_page += this_run;
                dst->advance(this_run);
                _current_ordinal += this_run;
            }
        } else {
            RETURN_IF_ERROR(_page->data_decoder->next_batch(&nrows_to_read, dst));
            DCHECK_EQ(nrows_to_read, nrows_in_page);

            if (dst->is_nullable()) {
                dst->set_null_bits(nrows_to_read, false);
            }

            _page->offset_in_page += nrows_to_read;
            dst->advance(nrows_to_read);
            _current_ordinal += nrows_to_read;
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

    RETURN_IF_ERROR(_read_data_page(_page_iter));
    _seek_to_pos_in_page(_page.get(), 0);
    *eos = false;
    return Status::OK();
}

Status FileColumnIterator::_read_data_page(const OrdinalPageIndexIterator& iter) {
    PageHandle handle;
    Slice page_body;
    PageFooterPB footer;
    _opts.type = DATA_PAGE;
    RETURN_IF_ERROR(_reader->read_page(_opts, iter.page(), &handle, &page_body, &footer));
    // parse data page
    RETURN_IF_ERROR(ParsedPage::create(std::move(handle), page_body, footer.data_page_footer(),
                                       _reader->encoding_info(), iter.page(), iter.page_index(),
                                       &_page));

    // dictionary page is read when the first data page that uses it is read,
    // this is to optimize the memory usage: when there is no query on one column, we could
    // release the memory of dictionary page.
    // note that concurrent iterators for the same column won't repeatedly read dictionary page
    // because of page cache.
    if (_reader->encoding_info()->encoding() == DICT_ENCODING) {
        auto dict_page_decoder = reinterpret_cast<BinaryDictPageDecoder*>(_page->data_decoder);
        if (dict_page_decoder->is_dict_encoding()) {
            if (_dict_decoder == nullptr) {
                // read dictionary page
                Slice dict_data;
                PageFooterPB dict_footer;
                _opts.type = INDEX_PAGE;
                RETURN_IF_ERROR(_reader->read_page(_opts, _reader->get_dict_page_pointer(),
                                                   &_dict_page_handle, &dict_data, &dict_footer));
                // ignore dict_footer.dict_page_footer().encoding() due to only
                // PLAIN_ENCODING is supported for dict page right now
                _dict_decoder.reset(new BinaryPlainPageDecoder(dict_data));
                RETURN_IF_ERROR(_dict_decoder->init());
            }
            dict_page_decoder->set_dict_decoder(_dict_decoder.get());
        }
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_zone_map(CondColumn* cond_column,
                                                      CondColumn* delete_condition,
                                                      RowRanges* row_ranges) {
    if (_reader->has_zone_map()) {
        RETURN_IF_ERROR(_reader->get_row_ranges_by_zone_map(
                cond_column, delete_condition, &_delete_partial_satisfied_pages, row_ranges));
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_bloom_filter(CondColumn* cond_column,
                                                          RowRanges* row_ranges) {
    if (cond_column != nullptr && cond_column->can_do_bloom_filter() &&
        _reader->has_bloom_filter_index()) {
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
            _type_size = _type_info->size();
            _mem_value = reinterpret_cast<void*>(_pool->allocate(_type_size));
            OLAPStatus s = OLAP_SUCCESS;
            if (_type_info->type() == OLAP_FIELD_TYPE_CHAR) {
                int32_t length = _schema_length;
                char* string_buffer = reinterpret_cast<char*>(_pool->allocate(length));
                memset(string_buffer, 0, length);
                memory_copy(string_buffer, _default_value.c_str(), _default_value.length());
                ((Slice*)_mem_value)->size = length;
                ((Slice*)_mem_value)->data = string_buffer;
            } else if (_type_info->type() == OLAP_FIELD_TYPE_VARCHAR ||
                       _type_info->type() == OLAP_FIELD_TYPE_HLL ||
                       _type_info->type() == OLAP_FIELD_TYPE_OBJECT) {
                int32_t length = _default_value.length();
                char* string_buffer = reinterpret_cast<char*>(_pool->allocate(length));
                memory_copy(string_buffer, _default_value.c_str(), length);
                ((Slice*)_mem_value)->size = length;
                ((Slice*)_mem_value)->data = string_buffer;
            } else if (_type_info->type() == OLAP_FIELD_TYPE_ARRAY) {
                // TODO llj for Array default value
                return Status::NotSupported("Array default type is unsupported");
            } else {
                s = _type_info->from_string(_mem_value, _default_value);
            }
            if (s != OLAP_SUCCESS) {
                return Status::InternalError(strings::Substitute(
                        "get value of type from default value failed. status:$0", s));
            }
        }
    } else if (_is_nullable) {
        // if _has_default_value is false but _is_nullable is true, we should return null as default value.
        _is_default_value_null = true;
    } else {
        return Status::InternalError(
                "invalid default value column for no default value and not nullable");
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) {
    if (dst->is_nullable()) {
        dst->set_null_bits(*n, _is_default_value_null);
    }

    if (_is_default_value_null) {
        *has_null = true;
        dst->advance(*n);
    } else {
        *has_null = false;
        for (int i = 0; i < *n; ++i) {
            memcpy(dst->data(), _mem_value, _type_size);
            dst->advance(1);
        }
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
