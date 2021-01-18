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

#include "olap/rowset/segment_v2/column_writer.h"

#include <cstddef>

#include "common/logging.h"
#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "olap/fs/block_manager.h"
#include "olap/rowset/segment_v2/bitmap_index_writer.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/bloom_filter_index_writer.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/ordinal_page_index.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/zone_map_index.h"
#include "util/block_compression.h"
#include "util/faststring.h"
#include "util/rle_encoding.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

class NullBitmapBuilder {
public:
    NullBitmapBuilder() : _has_null(false), _bitmap_buf(512), _rle_encoder(&_bitmap_buf, 1) {}

    explicit NullBitmapBuilder(size_t reserve_bits)
            : _has_null(false),
              _bitmap_buf(BitmapSize(reserve_bits)),
              _rle_encoder(&_bitmap_buf, 1) {}

    void add_run(bool value, size_t run) {
        _has_null |= value;
        _rle_encoder.Put(value, run);
    }

    // Returns whether the building nullmap contains NULL
    bool has_null() const { return _has_null; }

    OwnedSlice finish() {
        _rle_encoder.Flush();
        return _bitmap_buf.build();
    }

    void reset() {
        _has_null = false;
        _rle_encoder.Clear();
    }

    uint64_t size() { return _bitmap_buf.size(); }

private:
    bool _has_null;
    faststring _bitmap_buf;
    RleEncoder<bool> _rle_encoder;
};

Status ColumnWriter::create(const ColumnWriterOptions& opts, const TabletColumn* column,
                            fs::WritableBlock* _wblock, std::unique_ptr<ColumnWriter>* writer) {
    std::unique_ptr<Field> field(FieldFactory::create(*column));
    DCHECK(field.get() != nullptr);
    if (is_scalar_type(column->type())) {
        std::unique_ptr<ColumnWriter> writer_local = std::unique_ptr<ColumnWriter>(
                new ScalarColumnWriter(opts, std::move(field), _wblock));
        *writer = std::move(writer_local);
        return Status::OK();
    } else {
        switch (column->type()) {
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            DCHECK(column->get_subtype_count() == 1);
            const TabletColumn& item_column = column->get_sub_column(0);

            ColumnWriterOptions item_options;
            item_options.meta = opts.meta->mutable_children_columns(0);
            item_options.need_zone_map = false;
            item_options.need_bloom_filter = item_column.is_bf_column();
            item_options.need_bitmap_index = item_column.has_bitmap_index();
            if (item_column.type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
                if (item_options.need_bloom_filter) {
                    return Status::NotSupported("Do not support bloom filter for array type");
                }
                if (item_options.need_bitmap_index) {
                    return Status::NotSupported("Do not support bitmap index for array type");
                }
            }

            std::unique_ptr<ColumnWriter> item_writer;
            RETURN_IF_ERROR(
                    ColumnWriter::create(item_options, &item_column, _wblock, &item_writer));

            std::unique_ptr<Field> bigint_field(
                    FieldFactory::create_by_type(FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT));

            ScalarColumnWriter* offset_writer =
                    new ScalarColumnWriter(opts, std::move(bigint_field), _wblock);

            std::unique_ptr<ColumnWriter> writer_local =
                    std::unique_ptr<ColumnWriter>(new ArrayColumnWriter(
                            opts, std::move(field), offset_writer, std::move(item_writer)));
            *writer = std::move(writer_local);
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type for ColumnWriter: " +
                                        std::to_string(field->type()));
        }
    }
}

Status ColumnWriter::append_nullable(const uint8_t* is_null_bits, const void* data,
                                     size_t num_rows) {
    const uint8_t* ptr = (const uint8_t*)data;
    BitmapIterator null_iter(is_null_bits, num_rows);
    bool is_null = false;
    size_t this_run = 0;
    while ((this_run = null_iter.Next(&is_null)) > 0) {
        if (is_null) {
            RETURN_IF_ERROR(append_nulls(this_run));
        } else {
            RETURN_IF_ERROR(append_data(&ptr, this_run));
        }
    }
    return Status::OK();
}

Status ColumnWriter::append_not_nulls(const void* data, size_t num_rows) {
    return append_data((const uint8_t**)&data, num_rows);
}

///////////////////////////////////////////////////////////////////////////////////

ScalarColumnWriter::ScalarColumnWriter(const ColumnWriterOptions& opts,
                                       std::unique_ptr<Field> field, fs::WritableBlock* wblock)
        : ColumnWriter(std::move(field), opts.meta->is_nullable()),
          _opts(opts),
          _wblock(wblock),
          _data_size(0) {
    // these opts.meta fields should be set by client
    DCHECK(opts.meta->has_column_id());
    DCHECK(opts.meta->has_unique_id());
    DCHECK(opts.meta->has_type());
    DCHECK(opts.meta->has_length());
    DCHECK(opts.meta->has_encoding());
    DCHECK(opts.meta->has_compression());
    DCHECK(opts.meta->has_is_nullable());
    DCHECK(wblock != nullptr);
}

ScalarColumnWriter::~ScalarColumnWriter() {
    // delete all pages
    Page* page = _pages.head;
    while (page != nullptr) {
        Page* next_page = page->next;
        delete page;
        page = next_page;
    }
}

Status ScalarColumnWriter::init() {
    RETURN_IF_ERROR(get_block_compression_codec(_opts.meta->compression(), &_compress_codec));

    PageBuilder* page_builder = nullptr;

    RETURN_IF_ERROR(
            EncodingInfo::get(get_field()->type_info(), _opts.meta->encoding(), &_encoding_info));
    _opts.meta->set_encoding(_encoding_info->encoding());
    // create page builder
    PageBuilderOptions opts;
    opts.data_page_size = _opts.data_page_size;
    RETURN_IF_ERROR(_encoding_info->create_page_builder(opts, &page_builder));
    if (page_builder == nullptr) {
        return Status::NotSupported(
                strings::Substitute("Failed to create page builder for type $0 and encoding $1",
                                    get_field()->type(), _opts.meta->encoding()));
    }
    // should store more concrete encoding type instead of DEFAULT_ENCODING
    // because the default encoding of a data type can be changed in the future
    DCHECK_NE(_opts.meta->encoding(), DEFAULT_ENCODING);
    _page_builder.reset(page_builder);
    // create ordinal builder
    _ordinal_index_builder.reset(new OrdinalIndexWriter());
    // create null bitmap builder
    if (is_nullable()) {
        _null_bitmap_builder.reset(new NullBitmapBuilder());
    }
    if (_opts.need_zone_map) {
        _zone_map_index_builder.reset(new ZoneMapIndexWriter(get_field()));
    }
    if (_opts.need_bitmap_index) {
        RETURN_IF_ERROR(
                BitmapIndexWriter::create(get_field()->type_info(), &_bitmap_index_builder));
    }
    if (_opts.need_bloom_filter) {
        RETURN_IF_ERROR(BloomFilterIndexWriter::create(
                BloomFilterOptions(), get_field()->type_info(), &_bloom_filter_index_builder));
    }
    return Status::OK();
}

Status ScalarColumnWriter::append_nulls(size_t num_rows) {
    _null_bitmap_builder->add_run(true, num_rows);
    _next_rowid += num_rows;
    if (_opts.need_zone_map) {
        _zone_map_index_builder->add_nulls(num_rows);
    }
    if (_opts.need_bitmap_index) {
        _bitmap_index_builder->add_nulls(num_rows);
    }
    if (_opts.need_bloom_filter) {
        _bloom_filter_index_builder->add_nulls(num_rows);
    }
    return Status::OK();
}

// append data to page builder. this function will make sure that
// num_rows must be written before return. And ptr will be modified
// to next data should be written
Status ScalarColumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    size_t remaining = num_rows;
    while (remaining > 0) {
        size_t num_written = remaining;
        RETURN_IF_ERROR(_page_builder->add(*ptr, &num_written));
        if (_opts.need_zone_map) {
            _zone_map_index_builder->add_values(*ptr, num_written);
        }
        if (_opts.need_bitmap_index) {
            _bitmap_index_builder->add_values(*ptr, num_written);
        }
        if (_opts.need_bloom_filter) {
            _bloom_filter_index_builder->add_values(*ptr, num_written);
        }

        bool is_page_full = (num_written < remaining);
        remaining -= num_written;
        _next_rowid += num_written;
        *ptr += get_field()->size() * num_written;
        // we must write null bits after write data, because we don't
        // know how many rows can be written into current page
        if (is_nullable()) {
            _null_bitmap_builder->add_run(false, num_written);
        }

        if (is_page_full) {
            RETURN_IF_ERROR(finish_current_page());
        }
    }
    return Status::OK();
}

uint64_t ScalarColumnWriter::estimate_buffer_size() {
    uint64_t size = _data_size;
    size += _page_builder->size();
    if (is_nullable()) {
        size += _null_bitmap_builder->size();
    }
    size += _ordinal_index_builder->size();
    if (_opts.need_zone_map) {
        size += _zone_map_index_builder->size();
    }
    if (_opts.need_bitmap_index) {
        size += _bitmap_index_builder->size();
    }
    if (_opts.need_bloom_filter) {
        size += _bloom_filter_index_builder->size();
    }
    return size;
}

Status ScalarColumnWriter::finish() {
    RETURN_IF_ERROR(finish_current_page());
    _opts.meta->set_num_rows(_next_rowid);
    return Status::OK();
}

Status ScalarColumnWriter::write_data() {
    Page* page = _pages.head;
    while (page != nullptr) {
        RETURN_IF_ERROR(_write_data_page(page));
        page = page->next;
    }
    // write column dict
    if (_encoding_info->encoding() == DICT_ENCODING) {
        OwnedSlice dict_body;
        RETURN_IF_ERROR(_page_builder->get_dictionary_page(&dict_body));

        PageFooterPB footer;
        footer.set_type(DICTIONARY_PAGE);
        footer.set_uncompressed_size(dict_body.slice().get_size());
        footer.mutable_dict_page_footer()->set_encoding(PLAIN_ENCODING);

        PagePointer dict_pp;
        RETURN_IF_ERROR(PageIO::compress_and_write_page(_compress_codec,
                                                        _opts.compression_min_space_saving, _wblock,
                                                        {dict_body.slice()}, footer, &dict_pp));
        dict_pp.to_proto(_opts.meta->mutable_dict_page());
    }
    return Status::OK();
}

Status ScalarColumnWriter::write_ordinal_index() {
    return _ordinal_index_builder->finish(_wblock, _opts.meta->add_indexes());
}

Status ScalarColumnWriter::write_zone_map() {
    if (_opts.need_zone_map) {
        return _zone_map_index_builder->finish(_wblock, _opts.meta->add_indexes());
    }
    return Status::OK();
}

Status ScalarColumnWriter::write_bitmap_index() {
    if (_opts.need_bitmap_index) {
        return _bitmap_index_builder->finish(_wblock, _opts.meta->add_indexes());
    }
    return Status::OK();
}

Status ScalarColumnWriter::write_bloom_filter_index() {
    if (_opts.need_bloom_filter) {
        return _bloom_filter_index_builder->finish(_wblock, _opts.meta->add_indexes());
    }
    return Status::OK();
}

// write a data page into file and update ordinal index
Status ScalarColumnWriter::_write_data_page(Page* page) {
    PagePointer pp;
    std::vector<Slice> compressed_body;
    for (auto& data : page->data) {
        compressed_body.push_back(data.slice());
    }
    RETURN_IF_ERROR(PageIO::write_page(_wblock, compressed_body, page->footer, &pp));
    _ordinal_index_builder->append_entry(page->footer.data_page_footer().first_ordinal(), pp);
    return Status::OK();
}

Status ScalarColumnWriter::finish_current_page() {
    if (_next_rowid == _first_rowid) {
        return Status::OK();
    }
    if (_opts.need_zone_map) {
        if (_next_rowid - _first_rowid < config::zone_map_row_num_threshold) {
            _zone_map_index_builder->reset_page_zone_map();
        }
        RETURN_IF_ERROR(_zone_map_index_builder->flush());
    }

    if (_opts.need_bloom_filter) {
        RETURN_IF_ERROR(_bloom_filter_index_builder->flush());
    }

    // build data page body : encoded values + [nullmap]
    std::vector<Slice> body;
    OwnedSlice encoded_values = _page_builder->finish();
    _page_builder->reset();
    body.push_back(encoded_values.slice());

    OwnedSlice nullmap;
    if (is_nullable() && _null_bitmap_builder->has_null()) {
        nullmap = _null_bitmap_builder->finish();
        body.push_back(nullmap.slice());
    }
    if (_null_bitmap_builder != nullptr) {
        _null_bitmap_builder->reset();
    }

    // prepare data page footer
    std::unique_ptr<Page> page(new Page());
    page->footer.set_type(DATA_PAGE);
    page->footer.set_uncompressed_size(Slice::compute_total_size(body));
    auto data_page_footer = page->footer.mutable_data_page_footer();
    data_page_footer->set_first_ordinal(_first_rowid);
    data_page_footer->set_num_values(_next_rowid - _first_rowid);
    data_page_footer->set_nullmap_size(nullmap.slice().size);
    if (_new_page_callback != nullptr) {
        _new_page_callback->put_extra_info_in_page(data_page_footer);
    }
    // trying to compress page body
    OwnedSlice compressed_body;
    RETURN_IF_ERROR(PageIO::compress_page_body(_compress_codec, _opts.compression_min_space_saving,
                                               body, &compressed_body));
    if (compressed_body.slice().empty()) {
        // page body is uncompressed
        page->data.emplace_back(std::move(encoded_values));
        page->data.emplace_back(std::move(nullmap));
    } else {
        // page body is compressed
        page->data.emplace_back(std::move(compressed_body));
    }

    _push_back_page(page.release());
    _first_rowid = _next_rowid;
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////

ArrayColumnWriter::ArrayColumnWriter(const ColumnWriterOptions& opts, std::unique_ptr<Field> field,
                                     ScalarColumnWriter* offset_writer,
                                     std::unique_ptr<ColumnWriter> item_writer)
        : ColumnWriter(std::move(field), opts.meta->is_nullable()),
          _item_writer(std::move(item_writer)) {
    _offset_writer.reset(offset_writer);
}

Status ArrayColumnWriter::init() {
    RETURN_IF_ERROR(_offset_writer->init());
    RETURN_IF_ERROR(_item_writer->init());
    _offset_writer->register_flush_page_callback(this);
    return Status::OK();
}

Status ArrayColumnWriter::put_extra_info_in_page(DataPageFooterPB* footer) {
    footer->set_next_array_item_ordinal(_item_writer->get_next_rowid());
    return Status::OK();
}

// Now we can only write data one by one.
Status ArrayColumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    size_t remaining = num_rows;
    const auto* col_cursor = reinterpret_cast<const Collection*>(*ptr);
    while (remaining > 0) {
        // TODO llj: bulk write
        size_t num_written = 1;
        ordinal_t next_item_ordinal = _item_writer->get_next_rowid();
        ordinal_t* next_item_ordinal_ptr = &next_item_ordinal;
        RETURN_IF_ERROR(
                _offset_writer->append_data((const uint8_t**)&next_item_ordinal_ptr, num_written));

        // write child item.
        if (_item_writer->is_nullable()) {
            auto* item_data_ptr = col_cursor->data;
            for (size_t i = 0; i < col_cursor->length; ++i) {
                RETURN_IF_ERROR(_item_writer->append(col_cursor->null_signs[i], item_data_ptr));
                item_data_ptr = (uint8_t*)item_data_ptr + _item_writer->get_field()->size();
            }
        } else {
            RETURN_IF_ERROR(_item_writer->append_not_nulls(col_cursor->data, col_cursor->length));
        }

        remaining -= num_written;
        col_cursor += num_written;
    }
    return Status::OK();
}

uint64_t ArrayColumnWriter::estimate_buffer_size() {
    return _offset_writer->estimate_buffer_size() + _item_writer->estimate_buffer_size();
}

Status ArrayColumnWriter::finish() {
    RETURN_IF_ERROR(_offset_writer->finish());
    RETURN_IF_ERROR(_item_writer->finish());
    return Status::OK();
}

Status ArrayColumnWriter::write_data() {
    RETURN_IF_ERROR(_offset_writer->write_data());
    RETURN_IF_ERROR(_item_writer->write_data());
    return Status::OK();
}

Status ArrayColumnWriter::write_ordinal_index() {
    RETURN_IF_ERROR(_offset_writer->write_ordinal_index());
    RETURN_IF_ERROR(_item_writer->write_ordinal_index());
    return Status::OK();
}

Status ArrayColumnWriter::append_nulls(size_t num_rows) {
    return _offset_writer->append_nulls(num_rows);
}

Status ArrayColumnWriter::finish_current_page() {
    return _offset_writer->finish_current_page();
}

} // namespace segment_v2
} // namespace doris
