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

#include "olap/primary_key_index.h"

#include <gen_cpp/segment_v2.pb.h>

#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/types.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

Status PrimaryKeyIndexBuilder::init() {
    // TODO(liaoxin) using the column type directly if there's only one column in unique key columns
    const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
    segment_v2::IndexedColumnWriterOptions options;
    options.write_ordinal_index = true;
    options.write_value_index = true;
    options.encoding = segment_v2::EncodingInfo::get_default_encoding(type_info, true);
    // TODO(liaoxin) test to confirm whether it needs to be compressed
    options.compression = segment_v2::NO_COMPRESSION; // currently not compressed
    _primary_key_index_builder.reset(
            new segment_v2::IndexedColumnWriter(options, type_info, _file_writer));
    RETURN_IF_ERROR(_primary_key_index_builder->init());

    return segment_v2::BloomFilterIndexWriter::create(segment_v2::BloomFilterOptions(), type_info,
                                                      &_bloom_filter_index_builder);
}

Status PrimaryKeyIndexBuilder::add_item(const Slice& key) {
    RETURN_IF_ERROR(_primary_key_index_builder->add(&key));
    Slice key_without_seq = Slice(key.get_data(), key.get_size() - _seq_col_length);
    _bloom_filter_index_builder->add_values(&key_without_seq, 1);
    // the key is already sorted, so the first key is min_key, and
    // the last key is max_key.
    if (UNLIKELY(_num_rows == 0)) {
        _min_key.append(key.get_data(), key.get_size());
    }
    _max_key.clear();
    _max_key.append(key.get_data(), key.get_size());
    _num_rows++;
    _size += key.get_size();
    return Status::OK();
}

Status PrimaryKeyIndexBuilder::finalize(segment_v2::PrimaryKeyIndexMetaPB* meta) {
    // finish primary key index
    RETURN_IF_ERROR(_primary_key_index_builder->finish(meta->mutable_primary_key_index()));

    // set min_max key, the sequence column should be removed
    meta->set_min_key(min_key().to_string());
    meta->set_max_key(max_key().to_string());

    // finish bloom filter index
    RETURN_IF_ERROR(_bloom_filter_index_builder->flush());
    return _bloom_filter_index_builder->finish(_file_writer, meta->mutable_bloom_filter_index());
}

Status PrimaryKeyIndexReader::parse_index(io::FileReaderSPtr file_reader,
                                          const segment_v2::PrimaryKeyIndexMetaPB& meta) {
    // parse primary key index
    _index_reader.reset(new segment_v2::IndexedColumnReader(file_reader, meta.primary_key_index()));
    _index_reader->set_is_pk_index(true);
    RETURN_IF_ERROR(_index_reader->load(!config::disable_storage_page_cache, false));

    _index_parsed = true;
    return Status::OK();
}

Status PrimaryKeyIndexReader::parse_bf(io::FileReaderSPtr file_reader,
                                       const segment_v2::PrimaryKeyIndexMetaPB& meta) {
    // parse bloom filter
    segment_v2::ColumnIndexMetaPB column_index_meta = meta.bloom_filter_index();
    segment_v2::BloomFilterIndexReader bf_index_reader(std::move(file_reader),
                                                       &column_index_meta.bloom_filter_index());
    RETURN_IF_ERROR(bf_index_reader.load(!config::disable_storage_page_cache, false));
    std::unique_ptr<segment_v2::BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(bf_index_reader.new_iterator(&bf_iter));
    RETURN_IF_ERROR(bf_iter->read_bloom_filter(0, &_bf));
    _bf_parsed = true;

    return Status::OK();
}

Status MergeIndexedColumnIteratorContext::init() {
    RETURN_IF_ERROR(_segment->load_pk_index_and_bf());
    auto pk_idx = _segment->get_primary_key_index();
    _index = pk_idx;
    return Status::OK();
}

std::pair<Status, Slice> MergeIndexedColumnIteratorContext::get_current_key() {
    if (_cur_pos >= _cur_size) {
        if (_cur_row_id >= _index->num_rows()) {
            return {Status::EndOfFile("Reach the end of file"), {}};
        }
        auto st = _index->new_iterator(&_iter);
        if (!st.ok()) {
            return {st, {}};
        }
        Slice last_key_slice(_last_key);
        st = _iter->seek_at_or_after(&last_key_slice, &_excat_match);
        if (!st.ok()) {
            return {st, {}};
        }
        auto current_ordinal = _iter->get_current_ordinal();
        st = _next_batch(current_ordinal);
        if (!st.ok()) {
            return {st, {}};
        }
    }
    return {Status::OK(), Slice(_index_column->get_data_at(_cur_pos).data,
                                _index_column->get_data_at(_cur_pos).size)};
}

Status MergeIndexedColumnIteratorContext::advance() {
    ++_cur_pos;
    ++_cur_row_id;
    if (_cur_row_id >= _index->num_rows()) {
        return Status::EndOfFile(fmt::format("Reach the end of file", _segment_id));
    }
    return Status::OK();
}

Status MergeIndexedColumnIteratorContext::jump_to_ge(Slice const& key) {
    RETURN_IF_ERROR(_index->new_iterator(&_iter));
    auto st = _iter->seek_at_or_after(&key, &_excat_match);
    if (st.is<ErrorCode::NOT_FOUND>()) {
        return Status::EndOfFile("Reach the end of file");
    }
    RETURN_IF_ERROR(st);
    auto current_ordinal = _iter->get_current_ordinal();
    DCHECK(current_ordinal > _cur_row_id)
            << fmt::format("current_ordinal: {} should be greater than _cur_row_id: {}",
                           current_ordinal, _cur_row_id);
    if (current_ordinal + _cur_pos < _cur_size + _cur_row_id) {
        _cur_pos = _cur_pos + current_ordinal - _cur_row_id;
        _cur_row_id = current_ordinal;
        return Status::OK();
    }
    return _next_batch(current_ordinal);
}

Status MergeIndexedColumnIteratorContext::_next_batch(size_t row_id) {
    auto total = _index->num_rows();
    if (row_id >= total) {
        return Status::EndOfFile("Reach the end of file");
    }
    auto& pk_idx = _index;
    _index_type = vectorized::DataTypeFactory::instance().create_data_type(
            pk_idx->type_info()->type(), 1, 0);
    _index_column = _index_type->create_column();
    auto remaining = total - row_id;
    size_t num_to_read = std::min(_batch_size, remaining);
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_iter->next_batch(&num_read, _index_column));
    DCHECK(num_to_read == num_read) << "num_to_read: " << num_to_read << ", num_read: " << num_read;
    _last_key = _index_column->get_data_at(num_read - 1).to_string();
    if (num_read == _batch_size && num_read != remaining) {
        num_read -= 1;
    }
    _cur_size = num_read;
    _cur_pos = 0;
    _cur_row_id = row_id;
    return Status::OK();
}

bool MergeIndexedColumnIteratorContext::Comparator::operator()(
        MergeIndexedColumnIteratorContext* node1, MergeIndexedColumnIteratorContext* node2) const {
    auto&& [st1, key1] = node1->get_current_key();
    RETURN_IF_ERROR(st1);
    auto&& [st2, key2] = node2->get_current_key();
    RETURN_IF_ERROR(st2);
    if (_seq_col_length == 0) {
        auto cmp_result = key1.compare(key2);
        return cmp_result ? (cmp_result > 0) : (node1->segment_id() < node2->segment_id());
    }
    auto key1_without_seq = Slice(key1.get_data(), key1.get_size() - _seq_col_length);
    auto key2_without_seq = Slice(key2.get_data(), key2.get_size() - _seq_col_length);
    auto cmp_result = key1_without_seq.compare(key2_without_seq);
    if (cmp_result != 0) {
        return cmp_result > 0;
    }
    auto key1_sequence_val =
            Slice(key1.get_data() + key1.get_size() - _seq_col_length, _seq_col_length);
    auto key2_sequence_val =
            Slice(key2.get_data() + key2.get_size() - _seq_col_length, _seq_col_length);
    cmp_result = key1_sequence_val.compare(key2_sequence_val);
    if (cmp_result != 0) {
        return cmp_result > 0;
    }
    return node1->segment_id() < node2->segment_id();
}

bool MergeIndexedColumnIteratorContext::Comparator::is_key_same(
        MergeIndexedColumnIteratorContext* node1, Slice const& key2) const {
    auto&& [st1, key1] = node1->get_current_key();
    RETURN_IF_ERROR(st1);
    auto key1_without_seq = Slice(key1.get_data(), key1.get_size() - _seq_col_length);
    auto key2_without_seq = Slice(key2.get_data(), key2.get_size() - _seq_col_length);
    return key1_without_seq.compare(key2_without_seq) == 0;
}

} // namespace doris
