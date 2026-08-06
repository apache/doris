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

#include "format_v2/parquet/reader/native/decoder.h"

#include <cctz/time_zone.h>
#include <gen_cpp/parquet_types.h>

#include "common/cast_set.h"
#include "format_v2/parquet/reader/native/bool_plain_decoder.h"
#include "format_v2/parquet/reader/native/bool_rle_decoder.h"
#include "format_v2/parquet/reader/native/byte_array_dict_decoder.h"
#include "format_v2/parquet/reader/native/byte_array_plain_decoder.h"
#include "format_v2/parquet/reader/native/byte_stream_split_decoder.h"
#include "format_v2/parquet/reader/native/delta_bit_pack_decoder.h"
#include "format_v2/parquet/reader/native/fix_length_dict_decoder.hpp"
#include "format_v2/parquet/reader/native/fix_length_plain_decoder.h"
#include "util/rle_encoding.h"

namespace doris::format::parquet::native {

BaseDictDecoder::BaseDictDecoder() = default;

BaseDictDecoder::~BaseDictDecoder() = default;

Status BaseDictDecoder::set_data(Slice* data) {
    if (UNLIKELY(data == nullptr || data->size == 0)) {
        return Status::Corruption("Parquet dictionary index stream is empty");
    }
    _data = data;
    _offset = 0;
    uint8_t bit_width = *data->data;
    // Dictionary indices are uint32_t; wider external widths make repeated runs overwrite the
    // decoder's four-byte state before any dictionary-bound check can run.
    if (UNLIKELY(bit_width > 32)) {
        return Status::Corruption("Parquet dictionary index bit width {} exceeds 32", bit_width);
    }
    _index_batch_decoder = std::make_unique<RleBatchDecoder<uint32_t>>(
            reinterpret_cast<uint8_t*>(data->data) + 1, static_cast<int>(data->size) - 1,
            bit_width);
    return Status::OK();
}

Status BaseDictDecoder::decode_dictionary_indices(size_t num_values,
                                                  std::vector<uint32_t>* indices) {
    DORIS_CHECK(indices != nullptr);
    indices->resize(num_values);
    const auto decoded =
            _index_batch_decoder->GetBatch(indices->data(), cast_set<uint32_t>(num_values));
    if (UNLIKELY(decoded != num_values)) {
        return Status::IOError("Can't read enough Parquet dictionary indices");
    }
    const size_t num_dictionary_values = dictionary_size();
    if (UNLIKELY(!dictionary_indices_in_bounds(indices->data(), num_values,
                                               num_dictionary_values))) {
        // The SIMD common path only computes a bound; recover the exact corrupt row for the
        // diagnostic after the batch has already been proven invalid.
        for (size_t row = 0; row < num_values; ++row) {
            if ((*indices)[row] < num_dictionary_values) {
                continue;
            }
            return Status::Corruption(
                    "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                    (*indices)[row], row, num_dictionary_values);
        }
    }
    return Status::OK();
}

Status BaseDictDecoder::decode_selected_dictionary_indices(const ParquetSelection& selection,
                                                           std::vector<uint32_t>* indices) {
    DORIS_CHECK(indices != nullptr);
    const size_t num_dictionary_values = dictionary_size();
    if (_is_fragmented_selection(selection)) {
        RETURN_IF_ERROR(_decode_fragmented_selection(selection, num_dictionary_values));
        indices->assign(_skip_indices.begin(), _skip_indices.begin() + selection.selected_values);
        return Status::OK();
    }
    indices->resize(selection.selected_values);
    size_t cursor = 0;
    size_t output = 0;
    for (const auto& range : selection.ranges) {
        DORIS_CHECK(range.first >= cursor);
        RETURN_IF_ERROR(
                _decode_and_validate_skipped(range.first - cursor, cursor, num_dictionary_values));
        const auto decoded = _index_batch_decoder->GetBatch(indices->data() + output,
                                                            cast_set<uint32_t>(range.count));
        if (UNLIKELY(decoded != range.count)) {
            return Status::IOError("Can't read enough Parquet dictionary indices");
        }
        if (UNLIKELY(!dictionary_indices_in_bounds(indices->data() + output, range.count,
                                                   num_dictionary_values))) {
            for (size_t row = 0; row < range.count; ++row) {
                if ((*indices)[output + row] < num_dictionary_values) {
                    continue;
                }
                return Status::Corruption(
                        "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                        (*indices)[output + row], range.first + row, num_dictionary_values);
            }
        }
        output += range.count;
        cursor = range.first + range.count;
    }
    DORIS_CHECK(cursor <= selection.total_values);
    RETURN_IF_ERROR(_decode_and_validate_skipped(selection.total_values - cursor, cursor,
                                                 num_dictionary_values));
    DORIS_CHECK_EQ(output, selection.selected_values);
    return Status::OK();
}

Status BaseDictDecoder::_decode_fragmented_selection(const ParquetSelection& selection,
                                                     size_t num_dictionary_values) {
    // Decode and validate the page batch once when predicate survivors alternate in tiny runs.
    // Walking each range separately turns one RLE batch into millions of decoder calls for
    // low-cardinality predicates such as TPC-DS quantity buckets.
    _skip_indices.resize(selection.total_values);
    const auto decoded = _index_batch_decoder->GetBatch(_skip_indices.data(),
                                                        cast_set<uint32_t>(selection.total_values));
    if (UNLIKELY(decoded != selection.total_values)) {
        return Status::IOError("Can't read enough Parquet dictionary indices");
    }
    if (UNLIKELY(!dictionary_indices_in_bounds(_skip_indices.data(), selection.total_values,
                                               num_dictionary_values))) {
        for (size_t row = 0; row < selection.total_values; ++row) {
            if (_skip_indices[row] < num_dictionary_values) {
                continue;
            }
            return Status::Corruption(
                    "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                    _skip_indices[row], row, num_dictionary_values);
        }
    }
    size_t output = 0;
    constexpr size_t MAX_INLINE_COPY_VALUES = 4;
    for (const auto& range : selection.ranges) {
        DORIS_CHECK(range.first + range.count <= selection.total_values);
        // Alternating predicates mostly produce one-row spans; inline tiny forward copies so
        // range compaction does not replace decoder calls with equally numerous libc calls.
        if (range.count <= MAX_INLINE_COPY_VALUES) {
            for (size_t row = 0; row < range.count; ++row) {
                _skip_indices[output + row] = _skip_indices[range.first + row];
            }
        } else {
            memmove(_skip_indices.data() + output, _skip_indices.data() + range.first,
                    range.count * sizeof(uint32_t));
        }
        output += range.count;
    }
    DORIS_CHECK_EQ(output, selection.selected_values);
    return Status::OK();
}

Status BaseDictDecoder::_decode_and_validate_skipped(size_t num_values, size_t row_offset,
                                                     size_t num_dictionary_values) {
    constexpr size_t kSkipBatchSize = 4096;
    // Skipped dictionary ids are still external input and must be bounds-checked, but keeping
    // only one bounded gap buffer avoids the page-sized scratch used by sparse selections.
    _skip_indices.resize(std::min(num_values, kSkipBatchSize));
    size_t skipped_values = 0;
    while (skipped_values < num_values) {
        const size_t batch_size = std::min(num_values - skipped_values, kSkipBatchSize);
        const auto skipped = _index_batch_decoder->GetBatch(_skip_indices.data(),
                                                            static_cast<uint32_t>(batch_size));
        if (UNLIKELY(skipped != batch_size)) {
            return Status::IOError(
                    "Can't skip enough Parquet dictionary indices at row {}: {} of {}",
                    row_offset + skipped_values, skipped, batch_size);
        }
        // Filter gaps may be huge RLE runs; validate them in bounded SIMD-sized batches.
        if (UNLIKELY(!dictionary_indices_in_bounds(_skip_indices.data(), batch_size,
                                                   num_dictionary_values))) {
            for (size_t row = 0; row < batch_size; ++row) {
                if (_skip_indices[row] < num_dictionary_values) {
                    continue;
                }
                return Status::Corruption(
                        "Parquet dictionary index {} at skipped row {} exceeds dictionary "
                        "size {}",
                        _skip_indices[row], row_offset + skipped_values + row,
                        num_dictionary_values);
            }
        }
        skipped_values += batch_size;
    }
    return Status::OK();
}

Status BaseDictDecoder::_decode_dictionary_values(size_t num_values, size_t row_offset,
                                                  size_t num_dictionary_values,
                                                  ParquetDictionaryValueConsumer& consumer) {
    constexpr size_t kLiteralBatchSize = 1024;
    size_t decoded_values = 0;
    while (decoded_values < num_values) {
        const int32_t repeats = _index_batch_decoder->NextNumRepeats();
        if (repeats > 0) {
            const size_t run = std::min<size_t>(repeats, num_values - decoded_values);
            const uint32_t index = _index_batch_decoder->GetRepeatedValue(cast_set<int32_t>(run));
            if (UNLIKELY(static_cast<size_t>(index) >= num_dictionary_values)) {
                return Status::Corruption(
                        "Parquet dictionary index {} at row {} exceeds dictionary size {}", index,
                        row_offset + decoded_values, num_dictionary_values);
            }
            RETURN_IF_ERROR(consumer.consume_repeated(index, run));
            decoded_values += run;
            continue;
        }

        const int32_t literals = _index_batch_decoder->NextNumLiterals();
        if (UNLIKELY(literals == 0)) {
            return Status::IOError("Can't read enough Parquet dictionary indices");
        }
        const size_t batch = std::min(
                {static_cast<size_t>(literals), num_values - decoded_values, kLiteralBatchSize});
        _skip_indices.resize(batch);
        if (UNLIKELY(!_index_batch_decoder->GetLiteralValues(cast_set<int32_t>(batch),
                                                             _skip_indices.data()))) {
            return Status::IOError("Can't read enough Parquet dictionary indices");
        }
        if (UNLIKELY(!dictionary_indices_in_bounds(_skip_indices.data(), batch,
                                                   num_dictionary_values))) {
            for (size_t row = 0; row < batch; ++row) {
                if (_skip_indices[row] < num_dictionary_values) {
                    continue;
                }
                return Status::Corruption(
                        "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                        _skip_indices[row], row_offset + decoded_values + row,
                        num_dictionary_values);
            }
        }
        RETURN_IF_ERROR(consumer.consume_indices(_skip_indices.data(), batch));
        decoded_values += batch;
    }
    return Status::OK();
}

namespace {
Status unsupported_type(tparquet::Type::type type, tparquet::Encoding::type encoding) {
    return Status::InternalError("Unsupported type {}(encoding={}) in parquet decoder",
                                 tparquet::to_string(type), tparquet::to_string(encoding));
}

Status create_plain_decoder(tparquet::Type::type type, std::unique_ptr<Decoder>& decoder) {
    switch (type) {
    case tparquet::Type::BOOLEAN:
        decoder = std::make_unique<BoolPlainDecoder>();
        return Status::OK();
    case tparquet::Type::BYTE_ARRAY:
        decoder = std::make_unique<ByteArrayPlainDecoder>();
        return Status::OK();
    case tparquet::Type::INT32:
    case tparquet::Type::INT64:
    case tparquet::Type::INT96:
    case tparquet::Type::FLOAT:
    case tparquet::Type::DOUBLE:
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        decoder = std::make_unique<FixLengthPlainDecoder>();
        return Status::OK();
    default:
        return unsupported_type(type, tparquet::Encoding::PLAIN);
    }
}

Status create_dictionary_decoder(tparquet::Type::type type, std::unique_ptr<Decoder>& decoder) {
    switch (type) {
    case tparquet::Type::BOOLEAN:
        return Status::InternalError("Boolean type cannot have a dictionary page");
    case tparquet::Type::BYTE_ARRAY:
        decoder = std::make_unique<ByteArrayDictDecoder>();
        return Status::OK();
    case tparquet::Type::INT32:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::INT32>>();
        return Status::OK();
    case tparquet::Type::INT64:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::INT64>>();
        return Status::OK();
    case tparquet::Type::INT96:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::INT96>>();
        return Status::OK();
    case tparquet::Type::FLOAT:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::FLOAT>>();
        return Status::OK();
    case tparquet::Type::DOUBLE:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::DOUBLE>>();
        return Status::OK();
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        decoder = std::make_unique<FixLengthDictDecoder<tparquet::Type::FIXED_LEN_BYTE_ARRAY>>();
        return Status::OK();
    default:
        return unsupported_type(type, tparquet::Encoding::RLE_DICTIONARY);
    }
}

Status create_delta_binary_decoder(tparquet::Type::type type, std::unique_ptr<Decoder>& decoder) {
    switch (type) {
    case tparquet::Type::INT32:
        decoder = std::make_unique<DeltaBitPackDecoder<int32_t>>();
        return Status::OK();
    case tparquet::Type::INT64:
        decoder = std::make_unique<DeltaBitPackDecoder<int64_t>>();
        return Status::OK();
    default:
        return Status::InternalError("DELTA_BINARY_PACKED only supports INT32 and INT64");
    }
}

Status create_byte_stream_split_decoder(tparquet::Type::type type,
                                        std::unique_ptr<Decoder>& decoder) {
    switch (type) {
    case tparquet::Type::INT32:
    case tparquet::Type::INT64:
    case tparquet::Type::INT96:
    case tparquet::Type::FLOAT:
    case tparquet::Type::DOUBLE:
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        decoder = std::make_unique<ByteStreamSplitDecoder>();
        return Status::OK();
    default:
        return unsupported_type(type, tparquet::Encoding::BYTE_STREAM_SPLIT);
    }
}
} // namespace

Status Decoder::get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                            std::unique_ptr<Decoder>& decoder) {
    switch (encoding) {
    case tparquet::Encoding::PLAIN:
        return create_plain_decoder(type, decoder);
    case tparquet::Encoding::PLAIN_DICTIONARY:
    case tparquet::Encoding::RLE_DICTIONARY:
        // PLAIN_DICTIONARY is the legacy page enum for the same RLE/bit-packed id stream; accepting
        // it here keeps every dictionary decode entry point consistent with ColumnChunkReader.
        return create_dictionary_decoder(type, decoder);
    case tparquet::Encoding::RLE:
        if (type != tparquet::Type::BOOLEAN) {
            return unsupported_type(type, encoding);
        }
        decoder = std::make_unique<BoolRLEDecoder>();
        return Status::OK();
    case tparquet::Encoding::DELTA_BINARY_PACKED:
        return create_delta_binary_decoder(type, decoder);
    case tparquet::Encoding::DELTA_BYTE_ARRAY:
        if (type != tparquet::Type::BYTE_ARRAY && type != tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return Status::InternalError(
                    "DELTA_BYTE_ARRAY only supports BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY.");
        }
        decoder = std::make_unique<DeltaByteArrayDecoder>();
        return Status::OK();
    case tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
        if (type != tparquet::Type::BYTE_ARRAY) {
            return Status::InternalError("DELTA_LENGTH_BYTE_ARRAY only supports BYTE_ARRAY.");
        }
        decoder = std::make_unique<DeltaLengthByteArrayDecoder>();
        return Status::OK();
    case tparquet::Encoding::BYTE_STREAM_SPLIT:
        return create_byte_stream_split_decoder(type, decoder);
    default:
        return Status::InternalError("Unsupported encoding {}(type={}) in parquet decoder",
                                     tparquet::to_string(encoding), tparquet::to_string(type));
    }
}

} // namespace doris::format::parquet::native
