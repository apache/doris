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
#include <gen_cpp/segment_v2.pb.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "storage/cache/page_cache.h"
#include "storage/segment/binary_plain_page_v2.h"
#include "storage/segment/binary_plain_page_v2_pre_decoder.h"
#include "storage/segment/binary_plain_page_v3.h"
#include "storage/segment/binary_plain_page_v3_pre_decoder.h"
#include "storage/segment/options.h"
#include "storage/segment/page_builder.h"
#include "storage/types.h"
#include "util/block_compression.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

// Build a fixed corpus of strings of `value_len` bytes each. We reuse the same
// corpus across V2 and V3 so the only variable left is the on-disk layout. A
// deterministic RNG keeps results comparable across runs.
inline std::vector<std::string> make_corpus(size_t num_elems, size_t value_len) {
    std::mt19937 rng(0xC0FFEEu);
    std::uniform_int_distribution<int> dist('a', 'z');
    std::vector<std::string> corpus;
    corpus.reserve(num_elems);
    for (size_t i = 0; i < num_elems; ++i) {
        std::string s(value_len, '\0');
        for (size_t j = 0; j < value_len; ++j) {
            s[j] = static_cast<char>(dist(rng));
        }
        corpus.emplace_back(std::move(s));
    }
    return corpus;
}

template <template <FieldType> class BuilderT>
inline OwnedSlice build_page(const std::vector<std::string>& corpus) {
    std::vector<Slice> slices;
    slices.reserve(corpus.size());
    for (const auto& s : corpus) {
        slices.emplace_back(s);
    }

    PageBuilderOptions opts;
    // Disable the size-bound check so the whole corpus lands in one page.
    opts.data_page_size = 0;
    opts.dict_page_size = 0;

    PageBuilder* raw = nullptr;
    Status st = BuilderT<FieldType::OLAP_FIELD_TYPE_VARCHAR>::create(&raw, opts);
    CHECK(st.ok()) << st;
    std::unique_ptr<PageBuilder> builder(raw);

    size_t count = slices.size();
    st = builder->add(reinterpret_cast<const uint8_t*>(slices.data()), &count);
    CHECK(st.ok()) << st;
    CHECK_EQ(count, slices.size());

    OwnedSlice out;
    st = builder->finish(&out);
    CHECK(st.ok()) << st;
    return out;
}

// Per-fixture: build the input page once outside the timed loop, then in each
// iteration restore the input Slice (since decode() rewrites it to point at the
// freshly-allocated V1 page) and measure only the decode call. We also report
// per-element throughput so V2 vs V3 are easy to compare across (N, len).
template <template <FieldType> class BuilderT, class PreDecoderT>
inline void run_pre_decode_bm(benchmark::State& state) {
    const size_t num_elems = static_cast<size_t>(state.range(0));
    const size_t value_len = static_cast<size_t>(state.range(1));

    auto corpus = make_corpus(num_elems, value_len);
    OwnedSlice owned = build_page<BuilderT>(corpus);
    const Slice original = owned.slice();

    PreDecoderT pre_decoder;

    for (auto _ : state) {
        Slice page_slice = original;
        std::unique_ptr<DataPage> decoded_page;
        Status st = pre_decoder.decode(&decoded_page, &page_slice, /*size_of_tail=*/0,
                                       /*use_cache=*/false, PageTypePB::DATA_PAGE,
                                       /*file_path=*/std::string());
        CHECK(st.ok()) << st;
        benchmark::DoNotOptimize(page_slice);
        benchmark::DoNotOptimize(decoded_page);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(num_elems));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(original.size));
    state.counters["page_bytes"] = static_cast<double>(original.size);
    state.counters["ns_per_elem"] = benchmark::Counter(
            static_cast<double>(num_elems),
            benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

inline void BM_BinaryPlainPageV2_PreDecode(benchmark::State& state) {
    run_pre_decode_bm<BinaryPlainPageV2Builder, BinaryPlainPageV2PreDecoder<false>>(state);
}

inline void BM_BinaryPlainPageV3_PreDecode(benchmark::State& state) {
    run_pre_decode_bm<BinaryPlainPageV3Builder, BinaryPlainPageV3PreDecoder<false>>(state);
}

// (num_elems, value_len) grid. Picks representative sizes: a "many small"
// case (8 byte values like compact dictionary keys), a typical varchar case
// (32, 128 bytes), and a "large value" case (1024 bytes). num_elems range
// covers small / medium / page-sized pages.
static void V2V3PreDecodeArgs(benchmark::internal::Benchmark* b) {
    for (int n : {256, 1024, 4096, 16384}) {
        for (int len : {8, 32, 128, 1024}) {
            b->Args({n, len});
        }
    }
}

BENCHMARK(BM_BinaryPlainPageV2_PreDecode)->Apply(V2V3PreDecodeArgs);
BENCHMARK(BM_BinaryPlainPageV3_PreDecode)->Apply(V2V3PreDecodeArgs);

// ---------------------------------------------------------------------------
// Fixed-page-size variants: pin to a production page size so we measure
// pre-decode cost at realistic byte counts. num_elems is derived from
// value_len so each input page lands at ~target_bytes.
//   - 64 KiB matches STORAGE_PAGE_SIZE_DEFAULT_VALUE (default data page)
//   - 256 KiB matches STORAGE_DICT_PAGE_SIZE_DEFAULT_VALUE (dict / large)
// ---------------------------------------------------------------------------

inline constexpr size_t kBenchPage64KiB = 64 * 1024;
inline constexpr size_t kBenchPage256KiB = 256 * 1024;

// Pick num_elems so that (varint_len + value_len) * num_elems ~= target_bytes.
// Varint cost: 1 byte for value_len < 128, 2 bytes for value_len < 16384.
inline size_t elems_for_target(size_t target_bytes, size_t value_len) {
    const size_t varint_bytes = value_len < 128 ? 1 : 2;
    const size_t per_entry = varint_bytes + value_len;
    return target_bytes / per_entry;
}

template <template <FieldType> class BuilderT, class PreDecoderT, size_t TargetBytes>
inline void run_pre_decode_fixed_page_bm(benchmark::State& state) {
    const size_t value_len = static_cast<size_t>(state.range(0));
    const size_t num_elems = elems_for_target(TargetBytes, value_len);

    auto corpus = make_corpus(num_elems, value_len);
    OwnedSlice owned = build_page<BuilderT>(corpus);
    const Slice original = owned.slice();

    PreDecoderT pre_decoder;
    for (auto _ : state) {
        Slice page_slice = original;
        std::unique_ptr<DataPage> decoded_page;
        Status st = pre_decoder.decode(&decoded_page, &page_slice, /*size_of_tail=*/0,
                                       /*use_cache=*/false, PageTypePB::DATA_PAGE,
                                       /*file_path=*/std::string());
        CHECK(st.ok()) << st;
        benchmark::DoNotOptimize(page_slice);
        benchmark::DoNotOptimize(decoded_page);
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(num_elems));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(original.size));
    state.counters["num_elems"] = static_cast<double>(num_elems);
    state.counters["page_bytes"] = static_cast<double>(original.size);
    state.counters["ns_per_elem"] = benchmark::Counter(
            static_cast<double>(num_elems),
            benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

inline void BM_BinaryPlainPageV2_PreDecode_64KiB(benchmark::State& state) {
    run_pre_decode_fixed_page_bm<BinaryPlainPageV2Builder, BinaryPlainPageV2PreDecoder<false>,
                                 kBenchPage64KiB>(state);
}
inline void BM_BinaryPlainPageV3_PreDecode_64KiB(benchmark::State& state) {
    run_pre_decode_fixed_page_bm<BinaryPlainPageV3Builder, BinaryPlainPageV3PreDecoder<false>,
                                 kBenchPage64KiB>(state);
}
inline void BM_BinaryPlainPageV2_PreDecode_256KiB(benchmark::State& state) {
    run_pre_decode_fixed_page_bm<BinaryPlainPageV2Builder, BinaryPlainPageV2PreDecoder<false>,
                                 kBenchPage256KiB>(state);
}
inline void BM_BinaryPlainPageV3_PreDecode_256KiB(benchmark::State& state) {
    run_pre_decode_fixed_page_bm<BinaryPlainPageV3Builder, BinaryPlainPageV3PreDecoder<false>,
                                 kBenchPage256KiB>(state);
}

// value_len sweep. Each row produces a page near the target byte count;
// num_elems is reported via the `num_elems` counter so the (N, len)
// relationship is visible alongside the timing.
static void V2V3PreDecodeFixedPageArgs(benchmark::internal::Benchmark* b) {
    for (int len : {8, 16, 32, 64, 128, 256, 512, 1024, 4096}) {
        b->Args({len});
    }
}

BENCHMARK(BM_BinaryPlainPageV2_PreDecode_64KiB)->Apply(V2V3PreDecodeFixedPageArgs);
BENCHMARK(BM_BinaryPlainPageV3_PreDecode_64KiB)->Apply(V2V3PreDecodeFixedPageArgs);
BENCHMARK(BM_BinaryPlainPageV2_PreDecode_256KiB)->Apply(V2V3PreDecodeFixedPageArgs);
BENCHMARK(BM_BinaryPlainPageV3_PreDecode_256KiB)->Apply(V2V3PreDecodeFixedPageArgs);

// ===========================================================================
// On-disk size comparison: encode a page with V2 vs V3, then ZSTD-compress the
// encoded page (mirroring the segment write path, which compresses each page
// before writing) and report the raw and ZSTD-compressed byte counts.
//
// Both V2 and V3 store the same bytes (only the layout differs), so this isolates the
// effect of the V3 layout (contiguous data + contiguous varint lengths) on ZSTD's ratio.
// Two corpora:
//   - VARCHAR (variable length, no padding).
//   - CHAR (fixed length, '\0'-padded as OlapColumnDataConvertorChar does): both V2 and V3
//     keep the padding on disk, so this measures the layout effect on highly compressible
//     padded data.
// ===========================================================================

// Build a CHAR corpus: each value is exactly `value_len` bytes (padded), with a
// random logical content length in [1, value_len] followed by '\0' padding.
inline std::vector<std::string> make_padded_char_corpus(size_t num_elems, size_t value_len) {
    std::mt19937 rng(0xC0FFEEu);
    std::uniform_int_distribution<int> ch('a', 'z');
    std::uniform_int_distribution<size_t> len_dist(1, value_len);
    std::vector<std::string> corpus;
    corpus.reserve(num_elems);
    for (size_t i = 0; i < num_elems; ++i) {
        const size_t logical = len_dist(rng);
        std::string s(value_len, '\0'); // trailing bytes stay '\0' (the padding)
        for (size_t j = 0; j < logical; ++j) {
            s[j] = static_cast<char>(ch(rng));
        }
        corpus.emplace_back(std::move(s));
    }
    return corpus;
}

template <template <FieldType> class BuilderT, FieldType Type>
inline OwnedSlice build_page_typed(const std::vector<Slice>& slices) {
    PageBuilderOptions opts;
    opts.data_page_size = 0; // single page, no size-bound check
    opts.dict_page_size = 0;

    PageBuilder* raw = nullptr;
    Status st = BuilderT<Type>::create(&raw, opts);
    CHECK(st.ok()) << st;
    std::unique_ptr<PageBuilder> builder(raw);

    size_t count = slices.size();
    st = builder->add(reinterpret_cast<const uint8_t*>(slices.data()), &count);
    CHECK(st.ok()) << st;
    CHECK_EQ(count, slices.size());

    OwnedSlice out;
    st = builder->finish(&out);
    CHECK(st.ok()) << st;
    return out;
}

inline BlockCompressionCodec* zstd_codec() {
    BlockCompressionCodec* codec = nullptr;
    Status st = get_block_compression_codec(segment_v2::CompressionTypePB::ZSTD, &codec);
    CHECK(st.ok()) << st;
    CHECK(codec != nullptr);
    return codec;
}

// Build the page once, ZSTD-compress it in the timed loop, and report raw /
// compressed byte counts plus the ratio. CharPadding selects the padded CHAR
// corpus (and the CHAR builder specialization) vs the plain VARCHAR corpus.
template <template <FieldType> class BuilderT, FieldType Type, bool CharPadding>
inline void run_zstd_size_bm(benchmark::State& state) {
    const size_t num_elems = static_cast<size_t>(state.range(0));
    const size_t value_len = static_cast<size_t>(state.range(1));

    std::vector<std::string> corpus = CharPadding ? make_padded_char_corpus(num_elems, value_len)
                                                  : make_corpus(num_elems, value_len);
    std::vector<Slice> slices;
    slices.reserve(corpus.size());
    for (const auto& s : corpus) {
        slices.emplace_back(s.data(), s.size()); // full (padded) width for CHAR
    }

    OwnedSlice page = build_page_typed<BuilderT, Type>(slices);
    const Slice raw = page.slice();

    BlockCompressionCodec* codec = zstd_codec();
    faststring compressed;
    Status st = codec->compress(raw, &compressed);
    CHECK(st.ok()) << st;
    const size_t zstd_bytes = compressed.size();

    for (auto _ : state) {
        compressed.clear();
        st = codec->compress(raw, &compressed);
        CHECK(st.ok()) << st;
        benchmark::DoNotOptimize(compressed);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(raw.size));
    state.counters["num_elems"] = static_cast<double>(num_elems);
    state.counters["raw_bytes"] = static_cast<double>(raw.size);
    state.counters["zstd_bytes"] = static_cast<double>(zstd_bytes);
    state.counters["zstd_ratio"] =
            static_cast<double>(raw.size) / static_cast<double>(std::max<size_t>(zstd_bytes, 1));
}

inline void BM_ZstdSize_V2_Varchar(benchmark::State& state) {
    run_zstd_size_bm<BinaryPlainPageV2Builder, FieldType::OLAP_FIELD_TYPE_VARCHAR, false>(state);
}
inline void BM_ZstdSize_V3_Varchar(benchmark::State& state) {
    run_zstd_size_bm<BinaryPlainPageV3Builder, FieldType::OLAP_FIELD_TYPE_VARCHAR, false>(state);
}
inline void BM_ZstdSize_V2_Char(benchmark::State& state) {
    run_zstd_size_bm<BinaryPlainPageV2Builder, FieldType::OLAP_FIELD_TYPE_CHAR, true>(state);
}
inline void BM_ZstdSize_V3_Char(benchmark::State& state) {
    run_zstd_size_bm<BinaryPlainPageV3Builder, FieldType::OLAP_FIELD_TYPE_CHAR, true>(state);
}

// (num_elems, value_len) grid.
static void V2V3ZstdSizeArgs(benchmark::internal::Benchmark* b) {
    for (int n : {1024, 16384}) {
        for (int len : {8, 32, 128, 1024}) {
            b->Args({n, len});
        }
    }
}

BENCHMARK(BM_ZstdSize_V2_Varchar)->Apply(V2V3ZstdSizeArgs);
BENCHMARK(BM_ZstdSize_V3_Varchar)->Apply(V2V3ZstdSizeArgs);
BENCHMARK(BM_ZstdSize_V2_Char)->Apply(V2V3ZstdSizeArgs);
BENCHMARK(BM_ZstdSize_V3_Char)->Apply(V2V3ZstdSizeArgs);

} // namespace segment_v2
} // namespace doris
