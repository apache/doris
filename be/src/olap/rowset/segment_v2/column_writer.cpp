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

#include <assert.h>
#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <filesystem>

#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_writer.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bitmap_index_writer.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/bloom_filter_index_writer.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/ordinal_page_index.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "olap/rowset/segment_v2/zone_map_index.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/collection_value.h"
#include "util/block_compression.h"
#include "util/faststring.h"
#include "util/rle_encoding.h"
#include "vec/core/types.h"

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

    // Returns whether the building nullmap contains nullptr
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

//Todo(Amory): here should according nullable and offset and need sub to simply this function
Status ColumnWriter::create(const ColumnWriterOptions& opts, const TabletColumn* column,
                            io::FileWriter* file_writer, std::unique_ptr<ColumnWriter>* writer) {
    std::unique_ptr<Field> field(FieldFactory::create(*column));
    DCHECK(field.get() != nullptr);
    if (is_scalar_type(column->type())) {
        std::unique_ptr<ColumnWriter> writer_local = std::unique_ptr<ColumnWriter>(
                new ScalarColumnWriter(opts, std::move(field), file_writer));
        *writer = std::move(writer_local);
        return Status::OK();
    } else {
        switch (column->type()) {
        case FieldType::OLAP_FIELD_TYPE_STRUCT: {
            // not support empty struct
            DCHECK(column->get_subtype_count() >= 1);
            std::vector<std::unique_ptr<ColumnWriter>> sub_column_writers;
            sub_column_writers.reserve(column->get_subtype_count());
            for (uint32_t i = 0; i < column->get_subtype_count(); i++) {
                const TabletColumn& sub_column = column->get_sub_column(i);

                // create sub writer
                ColumnWriterOptions column_options;
                column_options.meta = opts.meta->mutable_children_columns(i);
                column_options.need_zone_map = false;
                column_options.need_bloom_filter = sub_column.is_bf_column();
                column_options.need_bitmap_index = sub_column.has_bitmap_index();
                column_options.inverted_index = nullptr;
                if (sub_column.type() == FieldType::OLAP_FIELD_TYPE_STRUCT) {
                    if (column_options.need_bloom_filter) {
                        return Status::NotSupported("Do not support bloom filter for struct type");
                    }
                    if (column_options.need_bitmap_index) {
                        return Status::NotSupported("Do not support bitmap index for struct type");
                    }
                }
                if (sub_column.type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
                    if (column_options.need_bloom_filter) {
                        return Status::NotSupported("Do not support bloom filter for array type");
                    }
                    if (column_options.need_bitmap_index) {
                        return Status::NotSupported("Do not support bitmap index for array type");
                    }
                }
                std::unique_ptr<ColumnWriter> sub_column_writer;
                RETURN_IF_ERROR(ColumnWriter::create(column_options, &sub_column, file_writer,
                                                     &sub_column_writer));
                sub_column_writers.push_back(std::move(sub_column_writer));
            }

            // if nullable, create null writer
            ScalarColumnWriter* null_writer = nullptr;
            if (opts.meta->is_nullable()) {
                FieldType null_type = FieldType::OLAP_FIELD_TYPE_TINYINT;
                ColumnWriterOptions null_options;
                null_options.meta = opts.meta->add_children_columns();
                null_options.meta->set_column_id(column->get_subtype_count() + 1);
                null_options.meta->set_unique_id(column->get_subtype_count() + 1);
                null_options.meta->set_type(int(null_type));
                null_options.meta->set_is_nullable(false);
                null_options.meta->set_length(
                        get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_TINYINT>()->size());
                null_options.meta->set_encoding(DEFAULT_ENCODING);
                null_options.meta->set_compression(opts.meta->compression());

                null_options.need_zone_map = false;
                null_options.need_bloom_filter = false;
                null_options.need_bitmap_index = false;

                TabletColumn null_column =
                        TabletColumn(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, null_type,
                                     null_options.meta->is_nullable(),
                                     null_options.meta->unique_id(), null_options.meta->length());
                null_column.set_name("nullable");
                null_column.set_index_length(-1); // no short key index
                std::unique_ptr<Field> null_field(FieldFactory::create(null_column));
                null_writer =
                        new ScalarColumnWriter(null_options, std::move(null_field), file_writer);
            }

            std::unique_ptr<ColumnWriter> writer_local =
                    std::unique_ptr<ColumnWriter>(new StructColumnWriter(
                            opts, std::move(field), null_writer, sub_column_writers));
            *writer = std::move(writer_local);
            return Status::OK();
        }
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            DCHECK(column->get_subtype_count() == 1);
            const TabletColumn& item_column = column->get_sub_column(0);

            // create item writer
            ColumnWriterOptions item_options;
            item_options.meta = opts.meta->mutable_children_columns(0);
            item_options.need_zone_map = false;
            item_options.need_bloom_filter = item_column.is_bf_column();
            item_options.need_bitmap_index = item_column.has_bitmap_index();
            item_options.inverted_index = nullptr;
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
                    ColumnWriter::create(item_options, &item_column, file_writer, &item_writer));

            // create length writer
            FieldType length_type = FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT;

            ColumnWriterOptions length_options;
            length_options.meta = opts.meta->add_children_columns();
            length_options.meta->set_column_id(2);
            length_options.meta->set_unique_id(2);
            length_options.meta->set_type(int(length_type));
            length_options.meta->set_is_nullable(false);
            length_options.meta->set_length(
                    get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>()->size());
            length_options.meta->set_encoding(DEFAULT_ENCODING);
            length_options.meta->set_compression(opts.meta->compression());

            length_options.need_zone_map = false;
            length_options.need_bloom_filter = false;
            length_options.need_bitmap_index = false;

            TabletColumn length_column =
                    TabletColumn(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, length_type,
                                 length_options.meta->is_nullable(),
                                 length_options.meta->unique_id(), length_options.meta->length());
            length_column.set_name("length");
            length_column.set_index_length(-1); // no short key index
            std::unique_ptr<Field> bigint_field(FieldFactory::create(length_column));
            auto* length_writer =
                    new OffsetColumnWriter(length_options, std::move(bigint_field), file_writer);

            // if nullable, create null writer
            ScalarColumnWriter* null_writer = nullptr;
            if (opts.meta->is_nullable()) {
                FieldType null_type = FieldType::OLAP_FIELD_TYPE_TINYINT;
                ColumnWriterOptions null_options;
                null_options.meta = opts.meta->add_children_columns();
                null_options.meta->set_column_id(3);
                null_options.meta->set_unique_id(3);
                null_options.meta->set_type(int(null_type));
                null_options.meta->set_is_nullable(false);
                null_options.meta->set_length(
                        get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_TINYINT>()->size());
                null_options.meta->set_encoding(DEFAULT_ENCODING);
                null_options.meta->set_compression(opts.meta->compression());

                null_options.need_zone_map = false;
                null_options.need_bloom_filter = false;
                null_options.need_bitmap_index = false;

                TabletColumn null_column =
                        TabletColumn(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, null_type,
                                     length_options.meta->is_nullable(),
                                     null_options.meta->unique_id(), null_options.meta->length());
                null_column.set_name("nullable");
                null_column.set_index_length(-1); // no short key index
                std::unique_ptr<Field> null_field(FieldFactory::create(null_column));
                null_writer =
                        new ScalarColumnWriter(null_options, std::move(null_field), file_writer);
            }

            std::unique_ptr<ColumnWriter> writer_local = std::unique_ptr<ColumnWriter>(
                    new ArrayColumnWriter(opts, std::move(field), length_writer, null_writer,
                                          std::move(item_writer)));
            *writer = std::move(writer_local);
            return Status::OK();
        }
        case FieldType::OLAP_FIELD_TYPE_MAP: {
            DCHECK(column->get_subtype_count() == 2);
            // create key & value writer
            std::vector<std::unique_ptr<ColumnWriter>> inner_writer_list;
            for (int i = 0; i < 2; ++i) {
                const TabletColumn& item_column = column->get_sub_column(i);
                // create item writer
                ColumnWriterOptions item_options;
                item_options.meta = opts.meta->mutable_children_columns(i);
                item_options.need_zone_map = false;
                item_options.need_bloom_filter = item_column.is_bf_column();
                item_options.need_bitmap_index = item_column.has_bitmap_index();
                item_options.inverted_index = nullptr;
                if (item_column.type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
                    if (item_options.need_bloom_filter) {
                        return Status::NotSupported("Do not support bloom filter for map type");
                    }
                    if (item_options.need_bitmap_index) {
                        return Status::NotSupported("Do not support bitmap index for map type");
                    }
                }
                std::unique_ptr<ColumnWriter> item_writer;
                RETURN_IF_ERROR(ColumnWriter::create(item_options, &item_column, file_writer,
                                                     &item_writer));
                inner_writer_list.push_back(std::move(item_writer));
            }

            ScalarColumnWriter* null_writer = nullptr;
            // create offset writer
            FieldType length_type = FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT;

            // Be Cautious: column unique id is used for column reader creation
            ColumnWriterOptions length_options;
            length_options.meta = opts.meta->add_children_columns();
            length_options.meta->set_column_id(column->get_subtype_count() + 1);
            length_options.meta->set_unique_id(column->get_subtype_count() + 1);
            length_options.meta->set_type(int(length_type));
            length_options.meta->set_is_nullable(false);
            length_options.meta->set_length(
                    get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>()->size());
            length_options.meta->set_encoding(DEFAULT_ENCODING);
            length_options.meta->set_compression(opts.meta->compression());

            length_options.need_zone_map = false;
            length_options.need_bloom_filter = false;
            length_options.need_bitmap_index = false;

            TabletColumn length_column =
                    TabletColumn(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, length_type,
                                 length_options.meta->is_nullable(),
                                 length_options.meta->unique_id(), length_options.meta->length());
            length_column.set_name("length");
            length_column.set_index_length(-1); // no short key index
            std::unique_ptr<Field> bigint_field(FieldFactory::create(length_column));
            auto* length_writer =
                    new OffsetColumnWriter(length_options, std::move(bigint_field), file_writer);

            // create null writer
            if (opts.meta->is_nullable()) {
                FieldType null_type = FieldType::OLAP_FIELD_TYPE_TINYINT;
                ColumnWriterOptions null_options;
                null_options.meta = opts.meta->add_children_columns();
                null_options.meta->set_column_id(column->get_subtype_count() + 2);
                null_options.meta->set_unique_id(column->get_subtype_count() + 2);
                null_options.meta->set_type(int(null_type));
                null_options.meta->set_is_nullable(false);
                null_options.meta->set_length(
                        get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_TINYINT>()->size());
                null_options.meta->set_encoding(DEFAULT_ENCODING);
                null_options.meta->set_compression(opts.meta->compression());

                null_options.need_zone_map = false;
                null_options.need_bloom_filter = false;
                null_options.need_bitmap_index = false;

                TabletColumn null_column = TabletColumn(
                        FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, null_type, false,
                        null_options.meta->unique_id(), null_options.meta->length());
                null_column.set_name("nullable");
                null_column.set_index_length(-1); // no short key index
                std::unique_ptr<Field> null_field(FieldFactory::create(null_column));
                null_writer =
                        new ScalarColumnWriter(null_options, std::move(null_field), file_writer);
            }

            // create map writer
            std::unique_ptr<ColumnWriter> sub_column_writer;
            std::unique_ptr<ColumnWriter> writer_local =
                    std::unique_ptr<ColumnWriter>(new MapColumnWriter(
                            opts, std::move(field), null_writer, length_writer, inner_writer_list));

            *writer = std::move(writer_local);
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type for ColumnWriter: {}",
                                        std::to_string(int(field->type())));
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

Status ColumnWriter::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                     size_t num_rows) {
    size_t offset = 0;
    auto next_run_step = [&]() {
        size_t step = 1;
        for (auto i = offset + 1; i < num_rows; ++i) {
            if (null_map[offset] == null_map[i]) {
                step++;
            } else {
                break;
            }
        }
        return step;
    };

    do {
        auto step = next_run_step();
        if (null_map[offset]) {
            RETURN_IF_ERROR(append_nulls(step));
            *ptr += get_field()->size() * step;
        } else {
            // TODO:
            //  1. `*ptr += get_field()->size() * step;` should do in this function, not append_data;
            //  2. support array vectorized load and ptr offset add
            RETURN_IF_ERROR(append_data(ptr, step));
        }
        offset += step;
    } while (offset < num_rows);

    return Status::OK();
}

Status ColumnWriter::append(const uint8_t* nullmap, const void* data, size_t num_rows) {
    assert(data && num_rows > 0);
    const auto* ptr = (const uint8_t*)data;
    if (nullmap) {
        return append_nullable(nullmap, &ptr, num_rows);
    } else {
        return append_data(&ptr, num_rows);
    }
}

///////////////////////////////////////////////////////////////////////////////////

ScalarColumnWriter::ScalarColumnWriter(const ColumnWriterOptions& opts,
                                       std::unique_ptr<Field> field, io::FileWriter* file_writer)
        : ColumnWriter(std::move(field), opts.meta->is_nullable()),
          _opts(opts),
          _file_writer(file_writer),
          _data_size(0) {
    // these opts.meta fields should be set by client
    DCHECK(opts.meta->has_column_id());
    DCHECK(opts.meta->has_unique_id());
    DCHECK(opts.meta->has_type());
    DCHECK(opts.meta->has_length());
    DCHECK(opts.meta->has_encoding());
    DCHECK(opts.meta->has_compression());
    DCHECK(opts.meta->has_is_nullable());
    DCHECK(file_writer != nullptr);
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
        return Status::NotSupported("Failed to create page builder for type {} and encoding {}",
                                    get_field()->type(), _opts.meta->encoding());
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
        RETURN_IF_ERROR(ZoneMapIndexWriter::create(get_field(), _zone_map_index_builder));
    }
    if (_opts.need_bitmap_index) {
        RETURN_IF_ERROR(
                BitmapIndexWriter::create(get_field()->type_info(), &_bitmap_index_builder));
    }

    if (_opts.inverted_index) {
        RETURN_IF_ERROR(InvertedIndexColumnWriter::create(
                get_field(), &_inverted_index_builder, _file_writer->path().filename().native(),
                _file_writer->path().parent_path().native(), _opts.inverted_index,
                _file_writer->fs()));
    }
    if (_opts.need_bloom_filter) {
        if (_opts.is_ngram_bf_index) {
            RETURN_IF_ERROR(NGramBloomFilterIndexWriterImpl::create(
                    BloomFilterOptions(), get_field()->type_info(), _opts.gram_size,
                    _opts.gram_bf_size, &_bloom_filter_index_builder));
        } else {
            RETURN_IF_ERROR(BloomFilterIndexWriter::create(
                    BloomFilterOptions(), get_field()->type_info(), &_bloom_filter_index_builder));
        }
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
    if (_opts.inverted_index) {
        RETURN_IF_ERROR(_inverted_index_builder->add_nulls(num_rows));
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
        RETURN_IF_ERROR(append_data_in_current_page(ptr, &num_written));

        remaining -= num_written;

        if (_page_builder->is_page_full()) {
            RETURN_IF_ERROR(finish_current_page());
        }
    }
    return Status::OK();
}

Status ScalarColumnWriter::append_data_in_current_page(const uint8_t* data, size_t* num_written) {
    RETURN_IF_ERROR(_page_builder->add(data, num_written));
    if (_opts.need_zone_map) {
        _zone_map_index_builder->add_values(data, *num_written);
    }
    if (_opts.need_bitmap_index) {
        _bitmap_index_builder->add_values(data, *num_written);
    }
    if (_opts.inverted_index) {
        RETURN_IF_ERROR(
                _inverted_index_builder->add_values(get_field()->name(), data, *num_written));
    }
    if (_opts.need_bloom_filter) {
        _bloom_filter_index_builder->add_values(data, *num_written);
    }

    _next_rowid += *num_written;

    // we must write null bits after write data, because we don't
    // know how many rows can be written into current page
    if (is_nullable()) {
        _null_bitmap_builder->add_run(false, *num_written);
    }
    return Status::OK();
}

Status ScalarColumnWriter::append_data_in_current_page(const uint8_t** data, size_t* num_written) {
    RETURN_IF_ERROR(append_data_in_current_page(*data, num_written));
    *data += get_field()->size() * (*num_written);
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
        RETURN_IF_ERROR(PageIO::compress_and_write_page(
                _compress_codec, _opts.compression_min_space_saving, _file_writer,
                {dict_body.slice()}, footer, &dict_pp));
        dict_pp.to_proto(_opts.meta->mutable_dict_page());
    }
    return Status::OK();
}

Status ScalarColumnWriter::write_ordinal_index() {
    return _ordinal_index_builder->finish(_file_writer, _opts.meta->add_indexes());
}

Status ScalarColumnWriter::write_zone_map() {
    if (_opts.need_zone_map) {
        return _zone_map_index_builder->finish(_file_writer, _opts.meta->add_indexes());
    }
    return Status::OK();
}

Status ScalarColumnWriter::write_bitmap_index() {
    if (_opts.need_bitmap_index) {
        return _bitmap_index_builder->finish(_file_writer, _opts.meta->add_indexes());
    }
    return Status::OK();
}

Status ScalarColumnWriter::write_inverted_index() {
    if (_opts.inverted_index) {
        return _inverted_index_builder->finish();
    }
    return Status::OK();
}

size_t ScalarColumnWriter::get_inverted_index_size() {
    if (_opts.inverted_index) {
        auto size = _inverted_index_builder->file_size();
        return size == -1 ? 0 : size;
    }
    return 0;
}

Status ScalarColumnWriter::write_bloom_filter_index() {
    if (_opts.need_bloom_filter) {
        return _bloom_filter_index_builder->finish(_file_writer, _opts.meta->add_indexes());
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
    RETURN_IF_ERROR(PageIO::write_page(_file_writer, compressed_body, page->footer, &pp));
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
    if (_null_bitmap_builder != nullptr) {
        if (is_nullable() && _null_bitmap_builder->has_null()) {
            nullmap = _null_bitmap_builder->finish();
            body.push_back(nullmap.slice());
        }
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

////////////////////////////////////////////////////////////////////////////////
// offset column writer
////////////////////////////////////////////////////////////////////////////////

OffsetColumnWriter::OffsetColumnWriter(const ColumnWriterOptions& opts,
                                       std::unique_ptr<Field> field, io::FileWriter* file_writer)
        : ScalarColumnWriter(opts, std::move(field), file_writer) {
    // now we only explain data in offset column as uint64
    DCHECK(get_field()->type() == FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT);
}

OffsetColumnWriter::~OffsetColumnWriter() = default;

Status OffsetColumnWriter::init() {
    RETURN_IF_ERROR(ScalarColumnWriter::init());
    register_flush_page_callback(this);
    _next_offset = 0;
    return Status::OK();
}

Status OffsetColumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    size_t remaining = num_rows;
    while (remaining > 0) {
        size_t num_written = remaining;
        RETURN_IF_ERROR(append_data_in_current_page(ptr, &num_written));
        // _next_offset after append_data_in_current_page is the offset of next data, which will used in finish_current_page() to set next_array_item_ordinal
        _next_offset = *(const uint64_t*)(*ptr);
        remaining -= num_written;

        if (_page_builder->is_page_full()) {
            // get next data for next array_item_rowid
            RETURN_IF_ERROR(finish_current_page());
        }
    }
    return Status::OK();
}

Status OffsetColumnWriter::put_extra_info_in_page(DataPageFooterPB* footer) {
    footer->set_next_array_item_ordinal(_next_offset);
    return Status::OK();
}

StructColumnWriter::StructColumnWriter(
        const ColumnWriterOptions& opts, std::unique_ptr<Field> field,
        ScalarColumnWriter* null_writer,
        std::vector<std::unique_ptr<ColumnWriter>>& sub_column_writers)
        : ColumnWriter(std::move(field), opts.meta->is_nullable()), _opts(opts) {
    for (auto& sub_column_writer : sub_column_writers) {
        _sub_column_writers.push_back(std::move(sub_column_writer));
    }
    _num_sub_column_writers = _sub_column_writers.size();
    DCHECK(_num_sub_column_writers >= 1);
    if (is_nullable()) {
        _null_writer.reset(null_writer);
    }
}

Status StructColumnWriter::init() {
    for (auto& column_writer : _sub_column_writers) {
        RETURN_IF_ERROR(column_writer->init());
    }
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->init());
    }
    return Status::OK();
}

Status StructColumnWriter::write_inverted_index() {
    if (_opts.inverted_index) {
        for (auto& column_writer : _sub_column_writers) {
            RETURN_IF_ERROR(column_writer->write_inverted_index());
        }
    }
    return Status::OK();
}

size_t StructColumnWriter::get_inverted_index_size() {
    size_t total_size = 0;
    if (_opts.inverted_index) {
        for (auto& column_writer : _sub_column_writers) {
            auto size = column_writer->get_inverted_index_size();
            total_size += (size == -1 ? 0 : size);
        }
    }
    return total_size;
}

Status StructColumnWriter::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                           size_t num_rows) {
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    RETURN_IF_ERROR(_null_writer->append_data(&null_map, num_rows));
    return Status::OK();
}

Status StructColumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    auto results = reinterpret_cast<const uint64_t*>(*ptr);
    for (size_t i = 0; i < _num_sub_column_writers; ++i) {
        auto nullmap = *(results + _num_sub_column_writers + i);
        auto data = *(results + i);
        RETURN_IF_ERROR(_sub_column_writers[i]->append(reinterpret_cast<const uint8_t*>(nullmap),
                                                       reinterpret_cast<const void*>(data),
                                                       num_rows));
    }
    return Status::OK();
}

uint64_t StructColumnWriter::estimate_buffer_size() {
    uint64_t size = 0;
    for (auto& column_writer : _sub_column_writers) {
        size += column_writer->estimate_buffer_size();
    }
    size += is_nullable() ? _null_writer->estimate_buffer_size() : 0;
    return size;
}

Status StructColumnWriter::finish() {
    for (auto& column_writer : _sub_column_writers) {
        RETURN_IF_ERROR(column_writer->finish());
    }
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->finish());
    }
    return Status::OK();
}

Status StructColumnWriter::write_data() {
    for (auto& column_writer : _sub_column_writers) {
        RETURN_IF_ERROR(column_writer->write_data());
    }
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->write_data());
    }
    return Status::OK();
}

Status StructColumnWriter::write_ordinal_index() {
    for (auto& column_writer : _sub_column_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status StructColumnWriter::append_nulls(size_t num_rows) {
    for (auto& column_writer : _sub_column_writers) {
        RETURN_IF_ERROR(column_writer->append_nulls(num_rows));
    }
    if (is_nullable()) {
        std::vector<vectorized::UInt8> null_signs(num_rows, 1);
        const uint8_t* null_sign_ptr = null_signs.data();
        RETURN_IF_ERROR(_null_writer->append_data(&null_sign_ptr, num_rows));
    }
    return Status::OK();
}

Status StructColumnWriter::finish_current_page() {
    return Status::NotSupported("struct writer has no data, can not finish_current_page");
}

////////////////////////////////////////////////////////////////////////////////

ArrayColumnWriter::ArrayColumnWriter(const ColumnWriterOptions& opts, std::unique_ptr<Field> field,
                                     OffsetColumnWriter* offset_writer,
                                     ScalarColumnWriter* null_writer,
                                     std::unique_ptr<ColumnWriter> item_writer)
        : ColumnWriter(std::move(field), opts.meta->is_nullable()),
          _item_writer(std::move(item_writer)),
          _opts(opts) {
    _offset_writer.reset(offset_writer);
    if (is_nullable()) {
        _null_writer.reset(null_writer);
    }
}

Status ArrayColumnWriter::init() {
    RETURN_IF_ERROR(_offset_writer->init());
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->init());
    }
    RETURN_IF_ERROR(_item_writer->init());
    if (_opts.inverted_index) {
        auto writer = dynamic_cast<ScalarColumnWriter*>(_item_writer.get());
        if (writer != nullptr) {
            RETURN_IF_ERROR(InvertedIndexColumnWriter::create(
                    get_field(), &_inverted_index_builder,
                    writer->_file_writer->path().filename().native(),
                    writer->_file_writer->path().parent_path().native(), _opts.inverted_index,
                    writer->_file_writer->fs()));
        }
    }
    return Status::OK();
}

Status ArrayColumnWriter::write_inverted_index() {
    if (_opts.inverted_index) {
        return _inverted_index_builder->finish();
    }
    return Status::OK();
}

size_t ArrayColumnWriter::get_inverted_index_size() {
    if (_opts.inverted_index) {
        auto size = _inverted_index_builder->file_size();
        return size == -1 ? 0 : size;
    }
    return 0;
}

// batch append data for array
Status ArrayColumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    // data_ptr contains
    // [size, offset_ptr, item_data_ptr, item_nullmap_ptr]
    auto data_ptr = reinterpret_cast<const uint64_t*>(*ptr);
    // total number length
    size_t element_cnt = size_t((unsigned long)(*data_ptr));
    auto offset_data = *(data_ptr + 1);
    const uint8_t* offsets_ptr = (const uint8_t*)offset_data;
    auto data = *(data_ptr + 2);
    auto nested_null_map = *(data_ptr + 3);
    if (element_cnt > 0) {
        RETURN_IF_ERROR(_item_writer->append(reinterpret_cast<const uint8_t*>(nested_null_map),
                                             reinterpret_cast<const void*>(data), element_cnt));
    }
    if (_opts.inverted_index) {
        auto writer = dynamic_cast<ScalarColumnWriter*>(_item_writer.get());
        // now only support nested type is scala
        if (writer != nullptr) {
            //NOTE: use array field name as index field, but item_writer size should be used when moving item_data_ptr
            RETURN_IF_ERROR(_inverted_index_builder->add_array_values(
                    _item_writer->get_field()->size(), reinterpret_cast<const void*>(data),
                    reinterpret_cast<const uint8_t*>(nested_null_map), offsets_ptr, num_rows));
        }
    }

    RETURN_IF_ERROR(_offset_writer->append_data(&offsets_ptr, num_rows));
    return Status::OK();
}

uint64_t ArrayColumnWriter::estimate_buffer_size() {
    return _offset_writer->estimate_buffer_size() +
           (is_nullable() ? _null_writer->estimate_buffer_size() : 0) +
           _item_writer->estimate_buffer_size();
}

Status ArrayColumnWriter::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                          size_t num_rows) {
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->append_data(&null_map, num_rows));
    }
    return Status::OK();
}
Status ArrayColumnWriter::finish() {
    RETURN_IF_ERROR(_offset_writer->finish());
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->finish());
    }
    RETURN_IF_ERROR(_item_writer->finish());
    return Status::OK();
}

Status ArrayColumnWriter::write_data() {
    RETURN_IF_ERROR(_offset_writer->write_data());
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->write_data());
    }
    RETURN_IF_ERROR(_item_writer->write_data());
    return Status::OK();
}

Status ArrayColumnWriter::write_ordinal_index() {
    RETURN_IF_ERROR(_offset_writer->write_ordinal_index());
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->write_ordinal_index());
    }
    if (!has_empty_items()) {
        RETURN_IF_ERROR(_item_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status ArrayColumnWriter::append_nulls(size_t num_rows) {
    size_t num_lengths = num_rows;
    const ordinal_t offset = _item_writer->get_next_rowid();
    while (num_lengths > 0) {
        // TODO llj bulk write
        const auto* offset_ptr = reinterpret_cast<const uint8_t*>(&offset);
        RETURN_IF_ERROR(_offset_writer->append_data(&offset_ptr, 1));
        --num_lengths;
    }
    return write_null_column(num_rows, true);
}

Status ArrayColumnWriter::write_null_column(size_t num_rows, bool is_null) {
    uint8_t null_sign = is_null ? 1 : 0;
    while (is_nullable() && num_rows > 0) {
        // TODO llj bulk write
        const uint8_t* null_sign_ptr = &null_sign;
        RETURN_IF_ERROR(_null_writer->append_data(&null_sign_ptr, 1));
        --num_rows;
    }
    return Status::OK();
}

Status ArrayColumnWriter::finish_current_page() {
    return Status::NotSupported("array writer has no data, can not finish_current_page");
}

/// ============================= MapColumnWriter =====================////
MapColumnWriter::MapColumnWriter(const ColumnWriterOptions& opts, std::unique_ptr<Field> field,
                                 ScalarColumnWriter* null_writer, OffsetColumnWriter* offset_writer,
                                 std::vector<std::unique_ptr<ColumnWriter>>& kv_writers)
        : ColumnWriter(std::move(field), opts.meta->is_nullable()), _opts(opts) {
    CHECK_EQ(kv_writers.size(), 2);
    _offsets_writer.reset(offset_writer);
    if (is_nullable()) {
        _null_writer.reset(null_writer);
    }
    for (auto& sub_writers : kv_writers) {
        _kv_writers.push_back(std::move(sub_writers));
    }
}

Status MapColumnWriter::init() {
    RETURN_IF_ERROR(_offsets_writer->init());
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->init());
    }
    // here register_flush_page_callback to call this.put_extra_info_in_page()
    // when finish cur data page
    for (auto& sub_writer : _kv_writers) {
        RETURN_IF_ERROR(sub_writer->init());
    }
    return Status::OK();
}

uint64_t MapColumnWriter::estimate_buffer_size() {
    size_t estimate = 0;
    for (auto& sub_writer : _kv_writers) {
        estimate += sub_writer->estimate_buffer_size();
    }
    estimate += _offsets_writer->estimate_buffer_size();
    if (is_nullable()) {
        estimate += _null_writer->estimate_buffer_size();
    }
    return estimate;
}

Status MapColumnWriter::finish() {
    RETURN_IF_ERROR(_offsets_writer->finish());
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->finish());
    }
    for (auto& sub_writer : _kv_writers) {
        RETURN_IF_ERROR(sub_writer->finish());
    }
    return Status::OK();
}

Status MapColumnWriter::append_nullable(const uint8_t* null_map, const uint8_t** ptr,
                                        size_t num_rows) {
    RETURN_IF_ERROR(append_data(ptr, num_rows));
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->append_data(&null_map, num_rows));
    }
    return Status::OK();
}

// write key value data with offsets
Status MapColumnWriter::append_data(const uint8_t** ptr, size_t num_rows) {
    // data_ptr contains
    // [size, offset_ptr, key_data_ptr, val_data_ptr, k_nullmap_ptr, v_nullmap_pr]
    // which converted results from olap_map_convertor and later will use a structure to replace it
    auto data_ptr = reinterpret_cast<const uint64_t*>(*ptr);
    // total number length
    size_t element_cnt = size_t((unsigned long)(*data_ptr));
    auto offset_data = *(data_ptr + 1);
    const uint8_t* offsets_ptr = (const uint8_t*)offset_data;

    if (element_cnt > 0) {
        for (size_t i = 0; i < 2; ++i) {
            auto data = *(data_ptr + 2 + i);
            auto nested_null_map = *(data_ptr + 2 + 2 + i);
            RETURN_IF_ERROR(
                    _kv_writers[i]->append(reinterpret_cast<const uint8_t*>(nested_null_map),
                                           reinterpret_cast<const void*>(data), element_cnt));
        }
    }
    // make sure the order : offset writer flush next_array_item_ordinal after kv_writers append_data
    // because we use _kv_writers[0]->get_next_rowid() to set next_array_item_ordinal in offset page footer
    RETURN_IF_ERROR(_offsets_writer->append_data(&offsets_ptr, num_rows));
    return Status::OK();
}

Status MapColumnWriter::write_data() {
    RETURN_IF_ERROR(_offsets_writer->write_data());
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->write_data());
    }
    for (auto& sub_writer : _kv_writers) {
        RETURN_IF_ERROR(sub_writer->write_data());
    }
    return Status::OK();
}

Status MapColumnWriter::write_ordinal_index() {
    RETURN_IF_ERROR(_offsets_writer->write_ordinal_index());
    if (is_nullable()) {
        RETURN_IF_ERROR(_null_writer->write_ordinal_index());
    }
    for (auto& sub_writer : _kv_writers) {
        if (sub_writer->get_next_rowid() != 0) {
            RETURN_IF_ERROR(sub_writer->write_ordinal_index());
        }
    }
    return Status::OK();
}

Status MapColumnWriter::append_nulls(size_t num_rows) {
    for (auto& sub_writer : _kv_writers) {
        RETURN_IF_ERROR(sub_writer->append_nulls(num_rows));
    }
    const ordinal_t offset = _kv_writers[0]->get_next_rowid();
    std::vector<vectorized::UInt8> offsets_data(num_rows, offset);
    const uint8_t* offsets_ptr = offsets_data.data();
    RETURN_IF_ERROR(_offsets_writer->append_data(&offsets_ptr, num_rows));

    if (is_nullable()) {
        std::vector<vectorized::UInt8> null_signs(num_rows, 1);
        const uint8_t* null_sign_ptr = null_signs.data();
        RETURN_IF_ERROR(_null_writer->append_data(&null_sign_ptr, num_rows));
    }
    return Status::OK();
}

Status MapColumnWriter::finish_current_page() {
    return Status::NotSupported("map writer has no data, can not finish_current_page");
}

Status MapColumnWriter::write_inverted_index() {
    if (_opts.inverted_index) {
        return _inverted_index_builder->finish();
    }
    return Status::OK();
}

size_t MapColumnWriter::get_inverted_index_size() {
    if (_opts.inverted_index) {
        auto size = _inverted_index_builder->file_size();
        return size == -1 ? 0 : size;
    }
    return 0;
}

} // namespace segment_v2
} // namespace doris
