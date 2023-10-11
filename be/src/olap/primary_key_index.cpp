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
#include "io/fs/file_writer.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"
#include "olap/rowset/segment_v2/bloom_filter_index_writer.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/types.h"

namespace doris {
PrimaryKeyIndexBuilder::~PrimaryKeyIndexBuilder() {
    _bloom_filter_index_mem_tracker->release(_bloom_filter_index_mem_tracker->consumption());
}
Status PrimaryKeyIndexBuilder::init() {
    // TODO(liaoxin) using the column type directly if there's only one column in unique key columns
    const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>();
    segment_v2::IndexedColumnWriterOptions options;
    options.write_ordinal_index = true;
    options.write_value_index = true;
    options.data_page_size = config::primary_key_data_page_size;
    options.encoding = segment_v2::EncodingInfo::get_default_encoding(type_info, true);
    options.compression = segment_v2::ZSTD;
    _primary_key_index_builder.reset(
            new segment_v2::IndexedColumnWriter(options, type_info, _file_writer));
    RETURN_IF_ERROR(_primary_key_index_builder->init());

    auto opt = segment_v2::BloomFilterOptions();
    opt.fpp = 0.01;
    _bloom_filter_index_builder.reset(
            new segment_v2::PrimaryKeyBloomFilterIndexWriterImpl(opt, type_info));
    _bloom_filter_index_mem_tracker = std::make_unique<MemTracker>("ProcessBlock-BloomFilterIndex");
    return Status::OK();
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
    _disk_size += _primary_key_index_builder->disk_size();

    // set min_max key, the sequence column should be removed
    meta->set_min_key(min_key().to_string());
    meta->set_max_key(max_key().to_string());

    // finish bloom filter index
    RETURN_IF_ERROR(_bloom_filter_index_builder->flush());
    uint64_t start_size = _file_writer->bytes_appended();
    RETURN_IF_ERROR(
            _bloom_filter_index_builder->finish(_file_writer, meta->mutable_bloom_filter_index()));
    _disk_size += _file_writer->bytes_appended() - start_size;
    _primary_key_index_builder.reset(nullptr);
    _bloom_filter_index_builder.reset(nullptr);
    return Status::OK();
}

void PrimaryKeyIndexBuilder::update_mem_tracker() {
    if (_bloom_filter_index_mem_tracker.get() != nullptr) {
        if (_bloom_filter_index_builder.get() != nullptr) {
            _bloom_filter_index_mem_tracker->set_consumption(_bloom_filter_index_builder->size());
        } else {
            _bloom_filter_index_mem_tracker->set_consumption(0);
        }
    }
}

Status PrimaryKeyIndexReader::parse_index(io::FileReaderSPtr file_reader,
                                          const segment_v2::PrimaryKeyIndexMetaPB& meta) {
    // parse primary key index
    _index_reader.reset(new segment_v2::IndexedColumnReader(file_reader, meta.primary_key_index()));
    _index_reader->set_is_pk_index(true);
    RETURN_IF_ERROR(_index_reader->load(!config::disable_pk_storage_page_cache, false));

    _index_parsed = true;
    return Status::OK();
}

Status PrimaryKeyIndexReader::parse_bf(io::FileReaderSPtr file_reader,
                                       const segment_v2::PrimaryKeyIndexMetaPB& meta) {
    // parse bloom filter
    segment_v2::ColumnIndexMetaPB column_index_meta = meta.bloom_filter_index();
    segment_v2::BloomFilterIndexReader bf_index_reader(std::move(file_reader),
                                                       column_index_meta.bloom_filter_index());
    RETURN_IF_ERROR(bf_index_reader.load(!config::disable_pk_storage_page_cache, false));
    std::unique_ptr<segment_v2::BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(bf_index_reader.new_iterator(&bf_iter));
    RETURN_IF_ERROR(bf_iter->read_bloom_filter(0, &_bf));
    _bf_parsed = true;

    return Status::OK();
}

} // namespace doris
