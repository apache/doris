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

#include "storage/index/snii/snii_bkd_index_writer.h"

#include <cstring>
#include <utility>

#include "common/check.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/snii/bkd/bkd_types.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/key_coder.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"

namespace doris::segment_v2 {

namespace {
constexpr const char* kDataFileName = "bkd_data";
constexpr const char* kIndexFileName = "bkd_index";
constexpr const char* kNullsFileName = "bkd_nulls";

// A BlobFileSource over bytes the adapter already holds in RAM. The container
// pulls positionally, so the range is bounds-checked here rather than trusted:
// a short read reported as OK would be checksummed and sealed as the payload.
::doris::snii::writer::BlobFileSource resident_source(std::string name,
                                                      std::shared_ptr<std::vector<uint8_t>> bytes) {
    ::doris::snii::writer::BlobFileSource source;
    source.name = std::move(name);
    source.length = bytes->size();
    source.read_fn = [bytes](uint64_t offset, size_t len, uint8_t* out) {
        if (offset > bytes->size() || len > bytes->size() - offset) {
            return Status::IOError("bkd blob staging read out of range");
        }
        std::memcpy(out, bytes->data() + offset, len);
        return Status::OK();
    };
    return source;
}
} // namespace

SniiBkdIndexColumnWriter::SniiBkdIndexColumnWriter(IndexFileWriter* index_file_writer,
                                                   const TabletIndex* index_meta,
                                                   FieldType value_type)
        : _index_file_writer(index_file_writer), _index_meta(index_meta), _value_type(value_type) {}

SniiBkdIndexColumnWriter::~SniiBkdIndexColumnWriter() = default;

Status SniiBkdIndexColumnWriter::init() {
    if (!field_is_numeric_type(_value_type)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII BKD index does not support field type {}", static_cast<int>(_value_type));
    }
    // Both resolved from the SAME FieldType, which is also the one recorded in
    // the index header (INV-1): the stride the source array is walked with and
    // the encoder the points are built with can never disagree.
    // field_is_numeric_type() is WIDER than the set this index can actually
    // encode: it admits UNSIGNED_TINYINT and UNSIGNED_SMALLINT, for which
    // field_type_size LOG(FATAL)s and get_key_coder returns nullptr. No FE type
    // maps to either today, so this is unreachable through DDL -- but the gate
    // must not depend on that staying true, and a null coder would be a
    // dereference rather than an error.
    _value_key_coder = get_key_coder(_value_type);
    if (_value_key_coder == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII BKD index has no key coder for field type {}", static_cast<int>(_value_type));
    }
    _value_size = cast_set<uint32_t>(field_type_size(_value_type));

    ::doris::snii::bkd::BkdBuilderOptions options;
    options.bytes_per_dim = _value_size;
    options.field_type = _value_type;
    return ::doris::snii::bkd::BkdBuilder::create(options, &_builder);
}

Status SniiBkdIndexColumnWriter::_add_value(const void* value, uint32_t docid) {
    DORIS_CHECK(_builder != nullptr);
    std::string encoded;
    _value_key_coder->full_encode_ascending(value, &encoded);
    // full_encode_ascending is length-preserving for every numeric type; a
    // disagreement here would mean the header's bytes_per_dim describes points
    // the builder never produced, so it is asserted rather than tolerated.
    DORIS_CHECK_EQ(encoded.size(), _value_size);
    return _builder->add(
            docid,
            ::doris::snii::Slice(reinterpret_cast<const uint8_t*>(encoded.data()), encoded.size()));
}

Status SniiBkdIndexColumnWriter::add_values(const std::string /*name*/, const void* values,
                                            size_t count) {
    DORIS_CHECK(values != nullptr || count == 0);
    const auto* cursor = static_cast<const uint8_t*>(values);
    for (size_t i = 0; i < count; ++i) {
        RETURN_IF_ERROR(_add_value(cursor, _rid));
        cursor += _value_size;
        ++_rid;
    }
    return Status::OK();
}

Status SniiBkdIndexColumnWriter::add_array_values(size_t field_size, const void* value_ptr,
                                                  const uint8_t* null_map,
                                                  const uint8_t* offsets_ptr, size_t count) {
    if (count == 0) {
        return Status::OK();
    }
    DORIS_CHECK(value_ptr != nullptr);
    DORIS_CHECK(offsets_ptr != nullptr);
    // The element width comes from the caller's array layout, but the points it
    // produces are the index's own field type, so a mismatch would silently
    // reinterpret the payload.
    DORIS_CHECK_EQ(field_size, _value_size);

    const auto* offsets = reinterpret_cast<const uint64_t*>(offsets_ptr);
    const auto* elements = static_cast<const uint8_t*>(value_ptr);
    size_t element = 0;
    for (size_t i = 0; i < count; ++i) {
        const size_t row_elements = offsets[i + 1] - offsets[i];
        for (size_t j = 0; j < row_elements; ++j, ++element) {
            if (null_map != nullptr && null_map[element] == 1) {
                continue;
            }
            // One row contributing several points is a first-class case; the
            // builder keys on (value, doc_id), so the row id does NOT advance
            // between them.
            RETURN_IF_ERROR(_add_value(elements + element * _value_size, _rid));
        }
        // A row that produced no point is NOT recorded as NULL here. An empty
        // array, and an array whose every element is NULL, are both non-null
        // arrays that simply cannot match a comparison -- and the index already
        // says so by holding no point for them. Marking them NULL would make
        // `col IS NULL` true for a row holding []. Array-LEVEL nulls arrive
        // separately through add_array_nulls, which is their only source.
        ++_rid;
    }
    return Status::OK();
}

Status SniiBkdIndexColumnWriter::add_nulls(uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
        _null_docids.push_back(_rid);
        ++_rid;
    }
    return Status::OK();
}

Status SniiBkdIndexColumnWriter::add_array_nulls(const uint8_t* null_map, size_t num_rows) {
    DORIS_CHECK(null_map != nullptr || num_rows == 0);
    // Called for the SAME rows add_array_values already walked, so it must not
    // advance the row id -- it only records which of those rows were NULL at the
    // array level.
    DORIS_CHECK_GE(_rid, num_rows);
    const uint32_t first_rid = _rid - cast_set<uint32_t>(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        if (null_map[i] == 1) {
            _null_docids.push_back(first_rid + cast_set<uint32_t>(i));
        }
    }
    return Status::OK();
}

Status SniiBkdIndexColumnWriter::finish() {
    DORIS_CHECK(_builder != nullptr);
    DORIS_CHECK(_index_file_writer != nullptr);

    // bkd_data is sized by the point count, so it is staged through a temp file
    // rather than held in RAM; the two hot sub-files are small by construction.
    std::unique_ptr<::doris::snii::bkd::StagedBlobFile> data;
    RETURN_IF_ERROR(::doris::snii::bkd::StagedBlobFile::create(kDataFileName, &data));

    ::doris::snii::ByteSink index_sink;
    ::doris::snii::bkd::BkdStats stats;
    RETURN_IF_ERROR(_builder->finish(data.get(), &index_sink, &stats));
    RETURN_IF_ERROR(data->finalize());

    ::doris::snii::format::NullBitmapWriter null_writer;
    null_writer.add_many(_null_docids);
    ::doris::snii::ByteSink null_sink;
    RETURN_IF_ERROR(null_writer.finish(_rid, &null_sink));

    // The container pulls at IndexFileWriter::finish_close(), long after this
    // returns, so the staging file has to outlive this object. Ownership moves
    // into the read callback itself.
    _data = std::move(data);
    std::shared_ptr<::doris::snii::bkd::StagedBlobFile> staged = _data;
    ::doris::snii::writer::BlobFileSource cold;
    cold.name = kDataFileName;
    cold.length = staged->bytes_written();
    cold.read_fn = [staged](uint64_t offset, size_t len, uint8_t* out) {
        return staged->read_at(offset, len, out);
    };

    std::vector<::doris::snii::writer::BlobFileSource> hot;
    hot.push_back(resident_source(kIndexFileName,
                                  std::make_shared<std::vector<uint8_t>>(index_sink.take())));
    hot.push_back(resident_source(kNullsFileName,
                                  std::make_shared<std::vector<uint8_t>>(null_sink.take())));

    return _index_file_writer->add_snii_blob_index(_index_meta,
                                                   ::doris::snii::format::LogicalIndexKind::kBkd,
                                                   {std::move(cold)}, std::move(hot));
}

void SniiBkdIndexColumnWriter::close_on_error() {
    // The builder unlinks its own spilled runs; the staging file removes itself
    // on destruction. Dropping both here means an aborted segment leaves no temp
    // file behind even if this writer is kept alive for a while.
    _builder.reset();
    _data.reset();
}

} // namespace doris::segment_v2
