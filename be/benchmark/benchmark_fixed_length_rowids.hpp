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

#pragma once

#include <benchmark/benchmark.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <random>
#include <vector>

#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/decimal12.h"
#include "core/value/decimalv2_value.h"
#include "storage/segment/bitshuffle_page.h"
#include "storage/segment/bitshuffle_page_pre_decoder.h"
#include "storage/segment/plain_page.h"
#include "storage/storage_layout.h"

namespace doris::segment_v2::rowid_benchmark {

// Preserve the previous decoder algorithm, including its reusable member buffer. This
// isolates selected append cost from page construction, decompression and allocation warmup.
template <FieldType F, bool BitShuffle>
class BufferedDecoder final
        : public std::conditional_t<BitShuffle, BitShufflePageDecoder<F>, PlainPageDecoder<F>> {
    using Base = std::conditional_t<BitShuffle, BitShufflePageDecoder<F>, PlainPageDecoder<F>>;
    using Storage = typename TypeTraits<F>::CppType;

public:
    BufferedDecoder(Slice page, size_t rows)
            : Base(page, {}),
              _src(page.data + (BitShuffle ? BITSHUFFLE_PAGE_HEADER_SIZE : PLAIN_PAGE_HEADER_SIZE)),
              _rows(rows) {}

    Status read_by_rowids(const rowid_t* rowids, ordinal_t base, size_t* count,
                          MutableColumnPtr& dst) override {
        size_t read_count = 0;
        _buffer.resize(*count);
        for (size_t i = 0; i < *count; ++i) {
            ordinal_t index = rowids[i] - base;
            if (UNLIKELY(index >= _rows)) {
                break;
            }
            if constexpr (BitShuffle) {
                _buffer[read_count++] =
                        *reinterpret_cast<const Storage*>(_src + index * sizeof(Storage));
            } else {
                _buffer[read_count++] = unaligned_load<Storage>(_src + index * sizeof(Storage));
            }
        }
        if (read_count > 0) {
            StorageLayout<F>::storage_to_column(reinterpret_cast<const uint8_t*>(_buffer.data()),
                                                read_count, *dst);
        }
        *count = read_count;
        return Status::OK();
    }

private:
    const char* _src;
    size_t _rows;
    std::vector<Storage> _buffer;
};

template <PrimitiveType P>
MutableColumnPtr make_column(bool nullable) {
    using Column = typename PrimitiveTypeTraits<P>::ColumnType;
    MutableColumnPtr result;
    if constexpr (is_decimal(P)) {
        result = Column::create(0, 9);
    } else {
        result = Column::create();
    }
    if (nullable) {
        result = ColumnNullable::create(std::move(result), ColumnUInt8::create());
    }
    return result;
}

template <PrimitiveType P, FieldType F, bool BitShuffle>
void run(benchmark::State& state) {
    constexpr size_t rows = 16384;
    constexpr size_t batch_size = 4096;
    constexpr rowid_t base = 1000;
    using Storage = typename TypeTraits<F>::CppType;
    using Builder = std::conditional_t<BitShuffle, BitshufflePageBuilder<F>, PlainPageBuilder<F>>;
    using Decoder = std::conditional_t<BitShuffle, BitShufflePageDecoder<F>, PlainPageDecoder<F>>;
    const bool direct = state.range(0) != 0;
    const size_t selected = rows * state.range(1) / 100;
    const bool nullable = state.range(2) != 0;
    std::vector<Storage> values(rows);
    for (size_t i = 0; i < rows; ++i) {
        if constexpr (P == TYPE_DATE) {
            values[i] = uint24_t((2024U << 9) | (2U << 5) | (i % 28 + 1));
        } else if constexpr (P == TYPE_DATETIME) {
            values[i] = 20240201010203L + (i % 28) * 1000000;
        } else if constexpr (P == TYPE_DECIMALV2) {
            values[i] = decimal12_t {static_cast<int64_t>(i) - 8000, -123456789};
        } else {
            values[i] = static_cast<Storage>(static_cast<int64_t>(i) * 17 - 8000);
        }
    }
    std::vector<rowid_t> rowids(rows);
    std::iota(rowids.begin(), rowids.end(), base);
    std::mt19937 random(42);
    std::shuffle(rowids.begin(), rowids.end(), random);
    rowids.resize(selected);
    std::sort(rowids.begin(), rowids.end());

    PageBuilderOptions options;
    options.data_page_size = rows * sizeof(Storage);
    PageBuilder* builder_raw = nullptr;
    auto status = Builder::create(&builder_raw, options);
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
        return;
    }
    std::unique_ptr<PageBuilder> builder(builder_raw);
    size_t count = rows;
    status = builder->add(reinterpret_cast<const uint8_t*>(values.data()), &count);
    CHECK(status.ok()) << status.to_string();
    CHECK_EQ(count, rows);
    OwnedSlice encoded;
    status = builder->finish(&encoded);
    CHECK(status.ok()) << status.to_string();
    Slice body = encoded.slice();
    std::unique_ptr<DataPage> decoded;
    if constexpr (BitShuffle) {
        BitShufflePagePreDecoder pre_decoder;
        status = pre_decoder.decode(&decoded, &body, 0, false, DATA_PAGE, "rowid_benchmark");
        CHECK(status.ok()) << status.to_string();
    }
    std::unique_ptr<PageDecoder> decoder;
    if (direct) {
        decoder = std::make_unique<Decoder>(body, PageDecoderOptions {});
    } else {
        decoder = std::make_unique<BufferedDecoder<F, BitShuffle>>(body, rows);
    }
    status = decoder->init();
    CHECK(status.ok()) << status.to_string();
    auto dst = make_column<P>(nullable);
    dst->reserve(selected);
    auto read = [&]() {
        dst->clear();
        auto read_values = [&](MutableColumnPtr& data) {
            for (size_t offset = 0; offset < selected; offset += batch_size) {
                size_t n = std::min(batch_size, selected - offset);
                auto result = decoder->read_by_rowids(rowids.data() + offset, base, &n, data);
                CHECK(result.ok()) << result.to_string();
            }
        };
        if (nullable) {
            auto& nullable_column = assert_cast<ColumnNullable&>(*dst);
            MutableColumnPtr data = nullable_column.get_nested_column_ptr();
            read_values(data);
            nullable_column.push_false_to_nullmap(data->size());
        } else {
            read_values(dst);
        }
    };
    read(); // Warm both the decoder buffer and destination before timing.
    CHECK_EQ(dst->size(), selected);
    auto expected = make_column<P>(nullable);
    IColumn& expected_values =
            nullable ? assert_cast<ColumnNullable&>(*expected).get_nested_column() : *expected;
    for (rowid_t rowid : rowids) {
        StorageLayout<F>::storage_to_column(reinterpret_cast<const uint8_t*>(&values[rowid - base]),
                                            1, expected_values);
    }
    const IColumn* actual_data = dst.get();
    const IColumn* expected_data = expected.get();
    if (nullable) {
        actual_data = &assert_cast<const ColumnNullable&>(*dst).get_nested_column();
        expected_data = &assert_cast<const ColumnNullable&>(*expected).get_nested_column();
    }
    for (size_t i = 0; i < selected; ++i) {
        auto actual = actual_data->get_data_at(i);
        auto wanted = expected_data->get_data_at(i);
        CHECK_EQ(actual.size, wanted.size);
        CHECK_EQ(memcmp(actual.data, wanted.data, actual.size), 0);
    }
    auto* output = dst.get();
    for (auto _ : state) {
        read();
        benchmark::DoNotOptimize(output);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * selected);
    state.SetLabel(direct ? "direct" : "reused_buffer");
}

inline void arguments(benchmark::internal::Benchmark* bench) {
    bench->ArgNames({"direct", "selectivity", "nullable"});
    for (int nullable : {0, 1}) {
        for (int selectivity : {1, 10, 50}) {
            for (int direct : {0, 1}) {
                bench->Args({direct, selectivity, nullable});
            }
        }
    }
}

#define ROWID_BENCHMARK(NAME, P, F)                                      \
    static void BM_Rowids_##NAME##_BitShuffle(benchmark::State& state) { \
        run<P, FieldType::F, true>(state);                               \
    }                                                                    \
    BENCHMARK(BM_Rowids_##NAME##_BitShuffle)->Apply(arguments);          \
    static void BM_Rowids_##NAME##_Plain(benchmark::State& state) {      \
        run<P, FieldType::F, false>(state);                              \
    }                                                                    \
    BENCHMARK(BM_Rowids_##NAME##_Plain)->Apply(arguments);

ROWID_BENCHMARK(Int, TYPE_INT, OLAP_FIELD_TYPE_INT)
ROWID_BENCHMARK(Bigint, TYPE_BIGINT, OLAP_FIELD_TYPE_BIGINT)
ROWID_BENCHMARK(Double, TYPE_DOUBLE, OLAP_FIELD_TYPE_DOUBLE)
ROWID_BENCHMARK(Date, TYPE_DATE, OLAP_FIELD_TYPE_DATE)
ROWID_BENCHMARK(Datetime, TYPE_DATETIME, OLAP_FIELD_TYPE_DATETIME)
ROWID_BENCHMARK(Decimal, TYPE_DECIMALV2, OLAP_FIELD_TYPE_DECIMAL)

#undef ROWID_BENCHMARK

} // namespace doris::segment_v2::rowid_benchmark
