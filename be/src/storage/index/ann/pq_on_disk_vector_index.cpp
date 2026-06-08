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

#include "storage/index/ann/pq_on_disk_vector_index.h"

#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <crc32c/crc32c.h>

// clang-format off
#include "common/compile_check_avoid_begin.h"
#include <faiss/utils/Heap.h>
#include "common/compile_check_avoid_end.h"
// clang-format on

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

#include "common/exception.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "storage/cache/ann_index_pq_chunk_cache.h"
#include "storage/index/ann/ann_build_utils.h"
#include "storage/index/ann/ann_index_files.h"
#include "storage/index/ann/ann_search_params.h"
#include "util/time.h"

namespace doris::segment_v2 {

// ---- Magic / version constants for ann.pqmeta ----
static constexpr uint32_t kPqMetaMagic = 0x50514F44; // "PQOD"
static constexpr uint32_t kPqMetaVersion = 1;

// ---- Helper: write primitives to CLucene IndexOutput ----
namespace {

void write_uint32(lucene::store::IndexOutput* out, uint32_t val) {
    out->writeBytes(reinterpret_cast<const uint8_t*>(&val), sizeof(val));
}

class PackedCodeReader {
public:
    PackedCodeReader(const uint8_t* code, size_t code_size) : _code(code), _code_size(code_size) {}

    uint64_t read(int nbit) {
        DCHECK(static_cast<size_t>(_bit_offset + nbit) <= _code_size * 8);
        int available = 8 - (_bit_offset & 7);
        uint64_t result = _code[_bit_offset >> 3] >> (_bit_offset & 7);
        if (nbit <= available) {
            result &= (1ULL << nbit) - 1;
            _bit_offset += nbit;
            return result;
        }

        int shift = available;
        size_t byte_idx = (_bit_offset >> 3) + 1;
        _bit_offset += nbit;
        nbit -= available;
        while (nbit > 8) {
            result |= static_cast<uint64_t>(_code[byte_idx++]) << shift;
            shift += 8;
            nbit -= 8;
        }
        uint64_t last_byte = _code[byte_idx];
        last_byte &= (1ULL << nbit) - 1;
        result |= last_byte << shift;
        return result;
    }

private:
    const uint8_t* _code;
    size_t _code_size;
    int _bit_offset = 0;
};

} // namespace

// ---- Constructor / Destructor ----

PqOnDiskVectorIndex::PqOnDiskVectorIndex() : VectorIndex() {
    set_type(AnnIndexType::PQ_ON_DISK);
}

PqOnDiskVectorIndex::~PqOnDiskVectorIndex() {
    if (_ntotal > 0) {
        DorisMetrics::instance()->ann_index_in_memory_rows_cnt->increment(-_ntotal);
    }
    if (_pqdata_input != nullptr) {
        _pqdata_input->close();
        _CLDELETE(_pqdata_input);
    }
}

// ---- Build ----

void PqOnDiskVectorIndex::build(const PqOnDiskBuildParameter& params) {
    _params = params;
    _dimension = params.dim;
    _metric = params.metric;
    set_type(AnnIndexType::PQ_ON_DISK);

    _pq = std::make_unique<faiss::ProductQuantizer>(params.dim, params.pq_m, params.pq_nbits);
}

// ---- Train ----

doris::Status PqOnDiskVectorIndex::train(Int64 n, const float* x) {
    DCHECK(x != nullptr);
    DCHECK(_pq != nullptr);

    // Only train once; subsequent calls are no-ops (codebook is fixed after first training).
    if (_is_trained) {
        return doris::Status::OK();
    }

    try {
        ScopedThreadName scoped_name("pq_train_idx");
        ScopedOmpThreadBudget thread_budget;
        _pq->train(cast_set<int>(n), x);
        _is_trained = true;
    } catch (const std::exception& e) {
        return doris::Status::RuntimeError("PQ_ON_DISK training failed: {}", e.what());
    }

    return doris::Status::OK();
}

// ---- Add ----

doris::Status PqOnDiskVectorIndex::add(Int64 n, const float* x) {
    DCHECK(x != nullptr);
    DCHECK(_pq != nullptr);
    DCHECK(_is_trained) << "PQ must be trained before adding vectors";

    const size_t code_size = _pq->code_size;
    const size_t prev_size = _codes_buffer.size();
    _codes_buffer.resize(prev_size + static_cast<size_t>(n) * code_size);

    try {
        ScopedThreadName scoped_name("pq_encode_idx");
        ScopedOmpThreadBudget thread_budget;
        _pq->compute_codes(x, _codes_buffer.data() + prev_size, cast_set<size_t>(n));
    } catch (const std::exception& e) {
        return doris::Status::RuntimeError("PQ_ON_DISK encoding failed: {}", e.what());
    }

    _ntotal += n;
    DorisMetrics::instance()->ann_index_in_memory_rows_cnt->increment(n);

    return doris::Status::OK();
}

// ---- Minimum training rows ----

Int64 PqOnDiskVectorIndex::get_min_train_rows() const {
    // FAISS PQ requires at least ksub * 100 = 2^pq_nbits * 100 training vectors.
    return (1LL << _params.pq_nbits) * 100;
}

// ---- Save (write ann.pqmeta + ann.pqdata) ----

doris::Status PqOnDiskVectorIndex::save(lucene::store::Directory* dir) {
    DCHECK(_pq != nullptr);
    DCHECK(_is_trained);

    auto start_time = std::chrono::high_resolution_clock::now();

    // 1. Write ann.pqmeta: header + codebook + CRC32
    {
        lucene::store::IndexOutput* meta_output = dir->createOutput(pq_meta_file_name);

        // Build header bytes for CRC computation
        // Header: magic(4) + version(4) + dim(4) + pq_m(4) + pq_nbits(4) + metric(4) + ntotal(8) + codebook_size(4) = 36 bytes
        const size_t codebook_bytes = _pq->centroids.size() * sizeof(float);
        const size_t header_size = 36;
        const size_t payload_size = header_size + codebook_bytes;

        std::vector<uint8_t> payload(payload_size);
        uint8_t* p = payload.data();

        // Write header fields into payload buffer
        uint32_t magic = kPqMetaMagic;
        std::memcpy(p, &magic, 4);
        p += 4;
        uint32_t version = kPqMetaVersion;
        std::memcpy(p, &version, 4);
        p += 4;
        uint32_t dim = static_cast<uint32_t>(_params.dim);
        std::memcpy(p, &dim, 4);
        p += 4;
        uint32_t pq_m = static_cast<uint32_t>(_params.pq_m);
        std::memcpy(p, &pq_m, 4);
        p += 4;
        uint32_t pq_nbits = static_cast<uint32_t>(_params.pq_nbits);
        std::memcpy(p, &pq_nbits, 4);
        p += 4;
        uint32_t metric_val = (_metric == AnnIndexMetric::L2) ? 0 : 1;
        std::memcpy(p, &metric_val, 4);
        p += 4;
        uint64_t ntotal = static_cast<uint64_t>(_ntotal);
        std::memcpy(p, &ntotal, 8);
        p += 8;
        uint32_t cb_size = cast_set<uint32_t>(codebook_bytes);
        std::memcpy(p, &cb_size, 4);
        p += 4;

        // Write codebook into payload buffer
        std::memcpy(p, _pq->centroids.data(), codebook_bytes);

        // Compute CRC32 over entire payload (header + codebook)
        uint32_t crc = crc32c::Crc32c(reinterpret_cast<const char*>(payload.data()), payload_size);

        // Write payload + CRC to file
        meta_output->writeBytes(payload.data(), cast_set<Int32>(payload_size));
        write_uint32(meta_output, crc);

        meta_output->close();
        delete meta_output;
    }

    // 2. Write ann.pqdata: PQ codes in rowid order
    {
        lucene::store::IndexOutput* data_output = dir->createOutput(pq_data_file_name);
        DCHECK(_codes_buffer.size() == static_cast<size_t>(_ntotal) * _pq->code_size);

        if (!_codes_buffer.empty()) {
            // Write in chunks to respect CLucene Int32 limit
            const size_t total = _codes_buffer.size();
            const size_t kMaxChunk = static_cast<size_t>(std::numeric_limits<Int32>::max());
            size_t written = 0;
            while (written < total) {
                size_t to_write = std::min(total - written, kMaxChunk);
                data_output->writeBytes(_codes_buffer.data() + written, cast_set<Int32>(to_write));
                written += to_write;
            }
        }

        data_output->close();
        delete data_output;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    LOG_INFO("PQ_ON_DISK index saved: dim={}, M={}, nbits={}, ntotal={}, codes_bytes={}, cost={}ms",
             _params.dim, _params.pq_m, _params.pq_nbits, _ntotal, _codes_buffer.size(),
             duration.count());

    return doris::Status::OK();
}

// ---- Load (read ann.pqmeta + open ann.pqdata) ----

doris::Status PqOnDiskVectorIndex::load(lucene::store::Directory* dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // 1. Read ann.pqmeta: header + codebook + CRC32 verification
    {
        lucene::store::IndexInput* meta_input = nullptr;
        try {
            meta_input = dir->openInput(pq_meta_file_name);
        } catch (const CLuceneError& e) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "Failed to open PQ meta file: {}, error: {}", pq_meta_file_name, e.what());
        }

        // Read entire file for CRC verification
        const size_t file_size = static_cast<size_t>(meta_input->length());
        if (file_size < 40) { // minimum: 36 header + 0 codebook + 4 CRC
            meta_input->close();
            delete meta_input;
            return doris::Status::Corruption("PQ meta file too small: {} bytes", file_size);
        }

        // Read all bytes
        std::vector<uint8_t> file_data(file_size);
        meta_input->readBytes(file_data.data(), cast_set<Int32>(file_size));
        meta_input->close();
        delete meta_input;

        // Verify CRC32 (last 4 bytes are the checksum)
        const size_t payload_size = file_size - 4;
        uint32_t stored_crc = 0;
        std::memcpy(&stored_crc, file_data.data() + payload_size, 4);
        uint32_t computed_crc =
                crc32c::Crc32c(reinterpret_cast<const char*>(file_data.data()), payload_size);
        if (stored_crc != computed_crc) {
            return doris::Status::Corruption("PQ meta CRC mismatch: stored={:#x}, computed={:#x}",
                                             stored_crc, computed_crc);
        }

        // Parse header
        const uint8_t* p = file_data.data();
        uint32_t magic = 0;
        std::memcpy(&magic, p, 4);
        p += 4;
        if (magic != kPqMetaMagic) {
            return doris::Status::Corruption("PQ meta invalid magic: {:#x}", magic);
        }

        uint32_t version = 0;
        std::memcpy(&version, p, 4);
        p += 4;
        if (version != kPqMetaVersion) {
            return doris::Status::Corruption("PQ meta unsupported version: {}", version);
        }

        uint32_t dim = 0;
        std::memcpy(&dim, p, 4);
        p += 4;
        uint32_t pq_m = 0;
        std::memcpy(&pq_m, p, 4);
        p += 4;
        uint32_t pq_nbits = 0;
        std::memcpy(&pq_nbits, p, 4);
        p += 4;
        uint32_t metric_val = 0;
        std::memcpy(&metric_val, p, 4);
        p += 4;
        uint64_t ntotal = 0;
        std::memcpy(&ntotal, p, 8);
        p += 8;
        uint32_t codebook_size = 0;
        std::memcpy(&codebook_size, p, 4);
        p += 4;

        // Reconstruct parameters
        _params.dim = static_cast<int>(dim);
        _params.pq_m = static_cast<int>(pq_m);
        _params.pq_nbits = static_cast<int>(pq_nbits);
        _params.metric = (metric_val == 0) ? AnnIndexMetric::L2 : AnnIndexMetric::IP;
        _dimension = dim;
        _metric = _params.metric;
        _ntotal = static_cast<int64_t>(ntotal);

        // Reconstruct ProductQuantizer and load codebook
        _pq = std::make_unique<faiss::ProductQuantizer>(dim, pq_m, pq_nbits);

        // Verify codebook size consistency
        const size_t expected_codebook_bytes = _pq->centroids.size() * sizeof(float);
        if (codebook_size != expected_codebook_bytes) {
            return doris::Status::Corruption("PQ codebook size mismatch: stored={}, expected={}",
                                             codebook_size, expected_codebook_bytes);
        }

        // Verify we have enough data for the codebook
        const size_t header_size = 36;
        if (payload_size < header_size + codebook_size) {
            return doris::Status::Corruption("PQ meta file truncated: payload={}, need={}",
                                             payload_size, header_size + codebook_size);
        }

        // Copy codebook
        std::memcpy(_pq->centroids.data(), p, codebook_size);

        // Recompute derived values (dsub, ksub, etc.)
        _pq->set_derived_values();
        _is_trained = true;
    }

    // 2. Open ann.pqdata (keep handle open for on-demand reads)
    {
        try {
            lucene::store::IndexInput* data_input = dir->openInput(pq_data_file_name);
            _pqdata_file_size = static_cast<size_t>(data_input->length());

            // Clone the input for thread-safe access (same pattern as CachedRandomAccessReader)
            _pqdata_input = data_input->clone();
            _pqdata_input->setIoContext(nullptr);

            data_input->close();
            _CLDELETE(data_input);
        } catch (const CLuceneError& e) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "Failed to open PQ data file: {}, error: {}", pq_data_file_name, e.what());
        }

        // Verify file size consistency
        const size_t expected_size = static_cast<size_t>(_ntotal) * _pq->code_size;
        if (_pqdata_file_size != expected_size) {
            return doris::Status::Corruption(
                    "PQ data file size mismatch: actual={}, expected={} (ntotal={}, code_size={})",
                    _pqdata_file_size, expected_size, _ntotal, _pq->code_size);
        }
    }

    // Compute chunk cache parameters
    {
        const size_t code_size = _pq->code_size;
        _vecs_per_chunk = kTargetChunkSize / code_size;
        if (_vecs_per_chunk == 0) {
            _vecs_per_chunk = 1;
        }
        _chunk_size = _vecs_per_chunk * code_size;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    VLOG_DEBUG << fmt::format(
            "PQ_ON_DISK index loaded: dim={}, M={}, nbits={}, ntotal={}, pqdata_size={}, "
            "cost={}ms",
            _params.dim, _params.pq_m, _params.pq_nbits, _ntotal, _pqdata_file_size,
            duration.count());

    DorisMetrics::instance()->ann_index_in_memory_rows_cnt->increment(_ntotal);

    return doris::Status::OK();
}

void PqOnDiskVectorIndex::_quantize_dis_table(const float* dis_table, QuantizedLUT& lut) const {
    const size_t M = _pq->M;
    const size_t ksub = _pq->ksub;
    const size_t n = M * ksub;

    // Find global min/max
    float vmin = dis_table[0];
    float vmax = dis_table[0];
    for (size_t i = 1; i < n; ++i) {
        vmin = std::min(vmin, dis_table[i]);
        vmax = std::max(vmax, dis_table[i]);
    }

    // Compute scale; handle degenerate case where all values are equal
    float range = vmax - vmin;
    if (range < 1e-30f) {
        range = 1e-30f;
    }
    float inv_scale = 255.0f / range;

    lut.scale = range / 255.0f;
    lut.bias = vmin;
    lut.table.resize(n);

    for (size_t i = 0; i < n; ++i) {
        float q = (dis_table[i] - vmin) * inv_scale;
        int qi = static_cast<int>(q + 0.5f);
        qi = std::max(0, std::min(255, qi));
        lut.table[i] = static_cast<uint8_t>(qi);
    }
}

float PqOnDiskVectorIndex::_adc_compute_distance(const uint8_t* code,
                                                 const QuantizedLUT& lut) const {
    const size_t M = _pq->M;
    const size_t ksub = _pq->ksub;
    const uint8_t* qt = lut.table.data();

    uint32_t acc = 0;
    if (_params.pq_nbits == 8) {
        // Fast path: code bytes are direct sub-quantizer indices.
        // Sequential scan over M LUT regions; hardware prefetcher handles
        // the stride-ksub pattern well when the uint8 LUT fits in L2.
        for (size_t m = 0; m < M; ++m) {
            acc += qt[m * ksub + code[m]];
        }
    } else {
        // General path: unpack bit-packed codes.
        PackedCodeReader reader(code, _pq->code_size);
        for (size_t m = 0; m < M; ++m) {
            acc += qt[m * ksub + reader.read(_params.pq_nbits)];
        }
    }
    return static_cast<float>(acc) * lut.scale + static_cast<float>(M) * lut.bias;
}

// ---- Zero-copy chunk traversal template ----

template <typename Visitor>
doris::Status PqOnDiskVectorIndex::_for_each_code_in_bitmap(const roaring::Roaring& bitmap,
                                                            const io::IOContext* io_ctx,
                                                            Visitor&& visitor) {
    auto* cache = ExecEnv::GetInstance()->get_ann_index_pq_chunk_cache();
    const size_t code_size = _pq->code_size;

    roaring::Roaring::const_iterator it(bitmap);
    while (it != bitmap.end()) {
        // Collect current contiguous run
        uint32_t run_start = *it;
        uint32_t run_end = run_start;
        ++it;
        while (it != bitmap.end() && *it == run_end + 1) {
            run_end = *it;
            ++it;
        }

        if (run_end >= static_cast<uint64_t>(_ntotal)) {
            return doris::Status::InvalidArgument(
                    "PQ_ON_DISK: bitmap rowid {} exceeds indexed row count {}", run_end, _ntotal);
        }

        // Iterate chunks overlapping this run
        size_t chunk_idx_start = static_cast<size_t>(run_start) / _vecs_per_chunk;
        size_t chunk_idx_end = static_cast<size_t>(run_end) / _vecs_per_chunk;

        for (size_t ci = chunk_idx_start; ci <= chunk_idx_end; ++ci) {
            auto chunk_byte_offset = static_cast<int64_t>(ci * _chunk_size);
            size_t actual_chunk_bytes = std::min(
                    _chunk_size, _pqdata_file_size - static_cast<size_t>(chunk_byte_offset));

            // Cache lookup (handle pins the entry for this scope)
            StoragePageCache::CacheKey key(_pqdata_cache_key_prefix, _pqdata_file_size,
                                           chunk_byte_offset);
            PageCacheHandle handle;
            bool hit = (cache != nullptr) && cache->lookup(key, &handle);

            // Temporary page for cache-miss or no-cache fallback
            std::unique_ptr<DataPage> tmp_page;

            if (!hit) {
                auto tracker = cache ? cache->mem_tracker()
                                     : MemTrackerLimiter::create_shared(
                                               MemTrackerLimiter::Type::GLOBAL, "PqChunkTmp");
                tmp_page = std::make_unique<DataPage>(actual_chunk_bytes, tracker);
                {
                    std::lock_guard<std::mutex> lock(_io_mutex);
                    _pqdata_input->setIoContext(io_ctx);
                    _pqdata_input->seek(chunk_byte_offset);
                    try {
                        _pqdata_input->readBytes(reinterpret_cast<uint8_t*>(tmp_page->data()),
                                                 cast_set<Int32>(actual_chunk_bytes));
                        _pqdata_input->setIoContext(nullptr);
                    } catch (const std::exception& e) {
                        _pqdata_input->setIoContext(nullptr);
                        return doris::Status::IOError(
                                "PQ_ON_DISK: failed to read chunk at offset {}: {}",
                                chunk_byte_offset, e.what());
                    }
                }
                if (cache) {
                    // Transfer ownership to cache; handle now pins it
                    cache->insert(key, tmp_page.release(), &handle);
                }
            }

            // Pointer to chunk data: from cache handle if available, else local page.
            // After a miss with cache present, tmp_page is released to the cache
            // and handle is populated, so use tmp_page nullness as the discriminator.
            const auto* chunk_data = tmp_page
                                             ? reinterpret_cast<const uint8_t*>(tmp_page->data())
                                             : reinterpret_cast<const uint8_t*>(handle.data().data);

            // Compute overlap between run and chunk
            auto chunk_rowid_start = static_cast<uint32_t>(ci * _vecs_per_chunk);
            uint32_t chunk_rowid_end =
                    chunk_rowid_start + static_cast<uint32_t>(actual_chunk_bytes / code_size) - 1;
            uint32_t overlap_start = std::max(run_start, chunk_rowid_start);
            uint32_t overlap_end = std::min(run_end, chunk_rowid_end);

            // Zero-copy: visitor reads directly from cached chunk memory
            for (uint32_t r = overlap_start; r <= overlap_end; ++r) {
                const uint8_t* code =
                        chunk_data + static_cast<size_t>(r - chunk_rowid_start) * code_size;
                visitor(r, code);
            }
            // PageCacheHandle released here (end of loop iteration)
        }
    }
    return doris::Status::OK();
}

// ---- ADC TopN L2 ----

doris::Status PqOnDiskVectorIndex::_adc_topn_l2(const QuantizedLUT& lut, int k,
                                                const roaring::Roaring& bitmap,
                                                const io::IOContext* io_ctx,
                                                IndexSearchResult& result) {
    // max-heap: keep the k smallest distances
    std::vector<float> heap_dis(k, std::numeric_limits<float>::max());
    std::vector<faiss::idx_t> heap_ids(k, -1);

    {
        SCOPED_RAW_TIMER(&result.engine_search_ns);
        RETURN_IF_ERROR(
                _for_each_code_in_bitmap(bitmap, io_ctx, [&](uint32_t rowid, const uint8_t* code) {
                    float dist = _adc_compute_distance(code, lut);
                    // L2: smaller is better -> max-heap evicts largest
                    if (dist < heap_dis[0]) {
                        faiss::maxheap_replace_top(cast_set<size_t>(k), heap_dis.data(),
                                                   heap_ids.data(), dist,
                                                   static_cast<faiss::idx_t>(rowid));
                    }
                }));
        faiss::maxheap_reorder(cast_set<size_t>(k), heap_dis.data(), heap_ids.data());
    }

    {
        SCOPED_RAW_TIMER(&result.engine_convert_ns);
        result.roaring = std::make_shared<roaring::Roaring>();
        size_t valid_count = 0;
        for (int i = 0; i < k; ++i) {
            if (heap_dis[i] < std::numeric_limits<float>::max()) {
                valid_count++;
            }
        }
        result.distances = std::make_unique<float[]>(valid_count);
        result.row_ids = std::make_unique<std::vector<uint64_t>>(valid_count);
        for (size_t i = 0; i < valid_count; ++i) {
            result.distances[i] = std::sqrt(heap_dis[i]); // L2^2 -> L2
            (*result.row_ids)[i] = static_cast<uint64_t>(heap_ids[i]);
            result.roaring->add(cast_set<uint32_t>(heap_ids[i]));
        }
    }

    return doris::Status::OK();
}

// ---- ADC TopN IP ----

doris::Status PqOnDiskVectorIndex::_adc_topn_ip(const QuantizedLUT& lut, int k,
                                                const roaring::Roaring& bitmap,
                                                const io::IOContext* io_ctx,
                                                IndexSearchResult& result) {
    // min-heap: keep the k largest inner products
    std::vector<float> heap_dis(k, -std::numeric_limits<float>::max());
    std::vector<faiss::idx_t> heap_ids(k, -1);

    {
        SCOPED_RAW_TIMER(&result.engine_search_ns);
        RETURN_IF_ERROR(
                _for_each_code_in_bitmap(bitmap, io_ctx, [&](uint32_t rowid, const uint8_t* code) {
                    float dist = _adc_compute_distance(code, lut);
                    // IP: larger is better -> min-heap evicts smallest
                    if (dist > heap_dis[0]) {
                        faiss::minheap_replace_top(cast_set<size_t>(k), heap_dis.data(),
                                                   heap_ids.data(), dist,
                                                   static_cast<faiss::idx_t>(rowid));
                    }
                }));
        faiss::minheap_reorder(cast_set<size_t>(k), heap_dis.data(), heap_ids.data());
    }

    {
        SCOPED_RAW_TIMER(&result.engine_convert_ns);
        result.roaring = std::make_shared<roaring::Roaring>();
        size_t valid_count = 0;
        for (int i = 0; i < k; ++i) {
            if (heap_dis[i] > -std::numeric_limits<float>::max()) {
                valid_count++;
            }
        }
        result.distances = std::make_unique<float[]>(valid_count);
        result.row_ids = std::make_unique<std::vector<uint64_t>>(valid_count);
        for (size_t i = 0; i < valid_count; ++i) {
            result.distances[i] = heap_dis[i]; // IP distances used directly
            (*result.row_ids)[i] = static_cast<uint64_t>(heap_ids[i]);
            result.roaring->add(cast_set<uint32_t>(heap_ids[i]));
        }
    }

    return doris::Status::OK();
}

// ---- ADC Range Search L2 ----

doris::Status PqOnDiskVectorIndex::_adc_range_l2(const QuantizedLUT& lut, float radius,
                                                 bool is_le_or_lt, const roaring::Roaring& bitmap,
                                                 const roaring::Roaring& all_rows,
                                                 const io::IOContext* io_ctx,
                                                 IndexSearchResult& result) {
    // For L2, FAISS uses squared distance internally.
    // radius is the user-facing L2 distance; convert to squared.
    float radius_sq = (radius > 0) ? (radius * radius) : 0.0F;

    if (is_le_or_lt) {
        // L2 + le_or_lt: return rows with dist <= radius_sq
        auto matched_roaring = std::make_shared<roaring::Roaring>();
        std::vector<float> matched_dists;
        std::vector<uint64_t> matched_ids;

        RETURN_IF_ERROR(
                _for_each_code_in_bitmap(bitmap, io_ctx, [&](uint32_t rowid, const uint8_t* code) {
                    float dist = _adc_compute_distance(code, lut);
                    if (dist <= radius_sq) {
                        matched_roaring->add(rowid);
                        matched_dists.push_back(std::sqrt(dist)); // L2^2 -> L2
                        matched_ids.push_back(rowid);
                    }
                }));

        result.roaring = matched_roaring;
        result.distances = std::make_unique<float[]>(matched_dists.size());
        std::copy(matched_dists.begin(), matched_dists.end(), result.distances.get());
        result.row_ids =
                std::make_unique<std::vector<uint64_t>>(matched_ids.begin(), matched_ids.end());
    } else {
        // L2 + ge_or_gt: return rows with dist >= radius_sq -> difference set
        auto within_roaring = std::make_shared<roaring::Roaring>();

        RETURN_IF_ERROR(
                _for_each_code_in_bitmap(bitmap, io_ctx, [&](uint32_t rowid, const uint8_t* code) {
                    float dist = _adc_compute_distance(code, lut);
                    if (dist <= radius_sq) {
                        within_roaring->add(rowid);
                    }
                }));

        result.roaring = std::make_shared<roaring::Roaring>();
        *(result.roaring) = all_rows - *within_roaring;
        result.distances = nullptr;
        result.row_ids = nullptr;
    }

    return doris::Status::OK();
}

// ---- ADC Range Search IP ----

doris::Status PqOnDiskVectorIndex::_adc_range_ip(const QuantizedLUT& lut, float radius,
                                                 bool is_le_or_lt, const roaring::Roaring& bitmap,
                                                 const roaring::Roaring& all_rows,
                                                 const io::IOContext* io_ctx,
                                                 IndexSearchResult& result) {
    if (is_le_or_lt) {
        // IP + le_or_lt: complement set (rows with ip < radius)
        auto ge_roaring = std::make_shared<roaring::Roaring>();

        RETURN_IF_ERROR(
                _for_each_code_in_bitmap(bitmap, io_ctx, [&](uint32_t rowid, const uint8_t* code) {
                    float dist = _adc_compute_distance(code, lut);
                    if (dist >= radius) {
                        ge_roaring->add(rowid);
                    }
                }));

        result.roaring = std::make_shared<roaring::Roaring>();
        *(result.roaring) = all_rows - *ge_roaring;
        result.distances = nullptr;
        result.row_ids = nullptr;
    } else {
        // IP + ge_or_gt: return rows with ip >= radius
        auto matched_roaring = std::make_shared<roaring::Roaring>();
        std::vector<float> matched_dists;
        std::vector<uint64_t> matched_ids;

        RETURN_IF_ERROR(
                _for_each_code_in_bitmap(bitmap, io_ctx, [&](uint32_t rowid, const uint8_t* code) {
                    float dist = _adc_compute_distance(code, lut);
                    if (dist >= radius) {
                        matched_roaring->add(rowid);
                        matched_dists.push_back(dist);
                        matched_ids.push_back(rowid);
                    }
                }));

        result.roaring = matched_roaring;
        result.distances = std::make_unique<float[]>(matched_dists.size());
        std::copy(matched_dists.begin(), matched_dists.end(), result.distances.get());
        result.row_ids =
                std::make_unique<std::vector<uint64_t>>(matched_ids.begin(), matched_ids.end());
    }

    return doris::Status::OK();
}

// ---- TopN Search (dispatch by metric) ----

doris::Status PqOnDiskVectorIndex::ann_topn_search(const float* query_vec, int k,
                                                   const IndexSearchParameters& params,
                                                   IndexSearchResult& result) {
    DCHECK(query_vec != nullptr);
    DCHECK(_pq != nullptr);
    DCHECK(k >= 0);
    DCHECK(params.roaring != nullptr)
            << "Roaring should not be null for topN search, please set roaring in params";

    if (k == 0) {
        result.roaring = std::make_shared<roaring::Roaring>();
        result.distances = std::make_unique<float[]>(0);
        result.row_ids = std::make_unique<std::vector<uint64_t>>();
        return doris::Status::OK();
    }

    const size_t M = _pq->M;
    const size_t ksub = _pq->ksub;

    // Step 1: Build float LUT, then quantize to uint8 for cache-friendly ADC.
    // Quantization reduces LUT from M*ksub*4 bytes to M*ksub bytes (4x smaller),
    // e.g. 384KB → 96KB for M=384/ksub=256, fitting in L2 instead of thrashing L3.
    QuantizedLUT lut;
    {
        SCOPED_RAW_TIMER(&result.engine_prepare_ns);
        std::vector<float> dis_table(M * ksub);
        if (_metric == AnnIndexMetric::L2) {
            _pq->compute_distance_table(query_vec, dis_table.data());
        } else if (_metric == AnnIndexMetric::IP) {
            _pq->compute_inner_prod_table(query_vec, dis_table.data());
        } else {
            return doris::Status::InvalidArgument("Unsupported metric type for PQ_ON_DISK: {}",
                                                  static_cast<int>(_metric));
        }
        _quantize_dis_table(dis_table.data(), lut);
    }

    // Step 2+3: Read codes and ADC search with quantized LUT
    if (_metric == AnnIndexMetric::L2) {
        return _adc_topn_l2(lut, k, *params.roaring, params.io_ctx, result);
    } else {
        return _adc_topn_ip(lut, k, *params.roaring, params.io_ctx, result);
    }
}

// ---- Range Search (dispatch by metric) ----

doris::Status PqOnDiskVectorIndex::range_search(const float* query_vec, const float& radius,
                                                const IndexSearchParameters& params,
                                                IndexSearchResult& result) {
    DCHECK(query_vec != nullptr);
    DCHECK(_pq != nullptr);
    DCHECK(params.roaring != nullptr)
            << "Roaring should not be null for range search, please set roaring in params";

    const size_t M = _pq->M;
    const size_t ksub = _pq->ksub;

    // Step 1: Build float LUT, then quantize to uint8 for cache-friendly ADC
    QuantizedLUT lut;
    {
        std::vector<float> dis_table(M * ksub);
        if (_metric == AnnIndexMetric::L2) {
            _pq->compute_distance_table(query_vec, dis_table.data());
        } else if (_metric == AnnIndexMetric::IP) {
            _pq->compute_inner_prod_table(query_vec, dis_table.data());
        } else {
            return doris::Status::InvalidArgument(
                    "Unsupported metric type for PQ_ON_DISK range: {}", static_cast<int>(_metric));
        }
        _quantize_dis_table(dis_table.data(), lut);
    }

    // Step 2+3: Read codes and ADC range search with quantized LUT
    if (_metric == AnnIndexMetric::L2) {
        return _adc_range_l2(lut, radius, params.is_le_or_lt, *params.roaring, *params.roaring,
                             params.io_ctx, result);
    } else {
        return _adc_range_ip(lut, radius, params.is_le_or_lt, *params.roaring, *params.roaring,
                             params.io_ctx, result);
    }
}

} // namespace doris::segment_v2
