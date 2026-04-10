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

#include "storage/index/ann/faiss_ann_index.h"

#include <faiss/index_io.h>
#include <faiss/invlists/OnDiskInvertedLists.h>
#include <faiss/invlists/PreadInvertedLists.h>
#include <gen_cpp/segment_v2.pb.h>
#include <omp.h>
#include <pthread.h>

#include <algorithm>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>

#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "core/types.h"
#include "faiss/Index.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexHNSW.h"
#include "faiss/IndexIVF.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/IndexIVFPQ.h"
#include "faiss/IndexScalarQuantizer.h"
#include "faiss/MetricType.h"
#include "faiss/impl/FaissException.h"
#include "faiss/impl/IDSelector.h"
#include "faiss/impl/io.h"
#include "io/io_common.h"
#include "storage/cache/ann_index_ivf_list_cache.h"
#include "storage/cache/page_cache.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_files.h"
#include "storage/index/ann/ann_search_params.h"
#include "util/thread.h"
#include "util/time.h"

namespace doris::segment_v2 {

namespace {

std::mutex g_omp_thread_mutex;
std::condition_variable g_omp_thread_cv;
int g_index_threads_in_use = 0;

struct IvfOnDiskCacheStats {
    int64_t hit_cnt = 0;
    int64_t miss_cnt = 0;
};

thread_local IvfOnDiskCacheStats g_ivf_on_disk_cache_stats;

// Per-thread IOContext pointer, set by ScopedIoCtxBinding around FAISS
// search calls so that CachedRandomAccessReader::_read_clucene() can
// forward the caller's IOContext to the CLucene IndexInput.  This is
// the same thread_local trick used for g_ivf_on_disk_cache_stats:
// borrow() is a FAISS callback with a fixed signature, so there is no
// way to pass per-query context through the call chain.
thread_local const io::IOContext* g_current_io_ctx = nullptr;

// RAII guard that binds g_current_io_ctx for the lifetime of a search.
class ScopedIoCtxBinding {
public:
    explicit ScopedIoCtxBinding(const io::IOContext* ctx) { g_current_io_ctx = ctx; }
    ~ScopedIoCtxBinding() { g_current_io_ctx = nullptr; }
    ScopedIoCtxBinding(const ScopedIoCtxBinding&) = delete;
    ScopedIoCtxBinding& operator=(const ScopedIoCtxBinding&) = delete;
};

// Guard that ensures the total OpenMP threads used by concurrent index builds
// never exceed the configured omp_threads_limit.
class ScopedOmpThreadBudget {
public:
    // For each index build, reserve at most half of the remaining threads, at least 1 thread.
    ScopedOmpThreadBudget() {
        std::unique_lock<std::mutex> lock(g_omp_thread_mutex);
        auto omp_threads_limit = get_omp_threads_limit();
        // Block until there is at least one OpenMP slot available under the global cap.
        g_omp_thread_cv.wait(lock, [&] { return g_index_threads_in_use < omp_threads_limit; });
        auto thread_cap = omp_threads_limit - g_index_threads_in_use;
        // Keep headroom for other concurrent index builds: take up to half of remaining budget.
        _reserved_threads = std::max(1, thread_cap / 2);
        g_index_threads_in_use += _reserved_threads;
        DorisMetrics::instance()->ann_index_build_index_threads->increment(_reserved_threads);
        omp_set_num_threads(_reserved_threads);
        VLOG_DEBUG << fmt::format(
                "ScopedOmpThreadBudget reserve threads reserved={}, in_use={}, limit={}",
                _reserved_threads, g_index_threads_in_use, get_omp_threads_limit());
    }

    ~ScopedOmpThreadBudget() {
        std::lock_guard<std::mutex> lock(g_omp_thread_mutex);
        g_index_threads_in_use -= _reserved_threads;
        DorisMetrics::instance()->ann_index_build_index_threads->increment(-_reserved_threads);
        if (g_index_threads_in_use < 0) {
            g_index_threads_in_use = 0;
        }
        // Wake waiting index builders so they can compete for the released OpenMP budget.
        g_omp_thread_cv.notify_all();
        VLOG_DEBUG << fmt::format(
                "ScopedOmpThreadBudget release threads reserved={}, remaining_in_use={}, limit={}",
                _reserved_threads, g_index_threads_in_use, get_omp_threads_limit());
    }

    static int get_omp_threads_limit() {
        if (config::omp_threads_limit > 0) {
            return config::omp_threads_limit;
        }
        int core_cap = std::max(1, CpuInfo::num_cores());
        // Use at most 80% of the available CPU cores.
        return std::max(1, core_cap * 4 / 5);
    }

private:
    int _reserved_threads = 1;
};

// Temporarily rename the current thread so FAISS build phases are easier to spot in debuggers.
class ScopedThreadName {
public:
    explicit ScopedThreadName(const std::string& new_name) {
        // POSIX limits thread names to 15 visible chars plus the null terminator.
        char current_name[16] = {0};
        int ret = pthread_getname_np(pthread_self(), current_name, sizeof(current_name));
        if (ret == 0) {
            _has_previous_name = true;
            _previous_name = current_name;
        }
        Thread::set_self_name(new_name);
    }

    ~ScopedThreadName() {
        if (_has_previous_name) {
            Thread::set_self_name(_previous_name);
        }
    }

private:
    bool _has_previous_name = false;
    std::string _previous_name;
};

class IDSelectorRoaring : public faiss::IDSelector {
public:
    explicit IDSelectorRoaring(const roaring::Roaring* roaring) : _roaring(roaring) {
        DCHECK(_roaring != nullptr);
    }

    bool is_member(faiss::idx_t id) const final {
        if (id < 0) {
            return false;
        }
        return _roaring->contains(cast_set<UInt32>(id));
    }

private:
    const roaring::Roaring* _roaring;
};

// TODO(dynamic nprobe): SPANN-style query-aware dynamic nprobe pruning.
// After coarse quantization, if the distance from the query vector to subsequent
// centroids is much larger than the distance to the closest centroid, those
// far-away clusters are unlikely to contain true nearest neighbors and can be
// skipped. This adaptively determines the actual nprobe per query based on a
// distance ratio threshold, avoiding unnecessary inverted list scans.
// Reference: Chen et al., "SPANN: Highly-efficient Billion-scale Approximate
// Nearest Neighbor Search", NeurIPS 2021.

} // namespace
std::unique_ptr<faiss::IDSelector> FaissVectorIndex::roaring_to_faiss_selector(
        const roaring::Roaring& roaring) {
    // Wrap the roaring bitmap directly to avoid copying ids into an intermediate buffer.
    return std::unique_ptr<faiss::IDSelector>(new IDSelectorRoaring(&roaring));
}

void FaissVectorIndex::update_roaring(const faiss::idx_t* labels, const size_t n,
                                      roaring::Roaring& roaring) {
    // make sure roaring is empty before adding new elements
    DCHECK(roaring.cardinality() == 0);
    for (size_t i = 0; i < n; ++i) {
        if (labels[i] >= 0) {
            roaring.add(cast_set<UInt32>(labels[i]));
        }
    }
}

FaissVectorIndex::FaissVectorIndex() : VectorIndex(), _index(nullptr) {}

FaissVectorIndex::~FaissVectorIndex() {
    if (_index != nullptr) {
        DorisMetrics::instance()->ann_index_in_memory_rows_cnt->increment(-_index->ntotal);
    }
}

struct FaissIndexWriter : faiss::IOWriter {
public:
    FaissIndexWriter() = default;
    FaissIndexWriter(lucene::store::IndexOutput* output) : _output(output) {}
    ~FaissIndexWriter() override {
        if (_output != nullptr) {
            _output->close();
            delete _output;
        }
    }

    size_t operator()(const void* ptr, size_t size, size_t nitems) override {
        size_t bytes = size * nitems;
        if (bytes > 0) {
            const auto* data = reinterpret_cast<const uint8_t*>(ptr);
            // CLucene IndexOutput::writeBytes accepts at most Int32 bytes at a time.
            const size_t kMaxChunk = static_cast<size_t>(std::numeric_limits<Int32>::max());
            size_t written = 0;
            while (written < bytes) {
                size_t to_write = bytes - written;
                if (to_write > kMaxChunk) to_write = kMaxChunk;
                try {
                    _output->writeBytes(data + written, cast_set<Int32>(to_write));
                } catch (const std::exception& e) {
                    throw doris::Exception(doris::ErrorCode::IO_ERROR,
                                           "Failed to write vector index {}", e.what());
                }
                written += to_write;
            }
        }
        return nitems;
    };

    lucene::store::IndexOutput* _output = nullptr;
};

struct FaissIndexReader : faiss::IOReader {
public:
    FaissIndexReader() = default;
    FaissIndexReader(lucene::store::IndexInput* input) : _input(input) {}
    ~FaissIndexReader() override {
        if (_input != nullptr) {
            _input->close();
            delete _input;
        }
    }
    size_t operator()(void* ptr, size_t size, size_t nitems) override {
        size_t bytes = size * nitems;
        if (bytes > 0) {
            auto* data = reinterpret_cast<uint8_t*>(ptr);
            const size_t kMaxChunk = static_cast<size_t>(std::numeric_limits<Int32>::max());
            size_t read = 0;
            while (read < bytes) {
                size_t to_read = bytes - read;
                if (to_read > kMaxChunk) to_read = kMaxChunk;
                try {
                    _input->readBytes(data + read, cast_set<Int32>(to_read));
                } catch (const std::exception& e) {
                    throw doris::Exception(doris::ErrorCode::IO_ERROR,
                                           "Failed to read vector index {}", e.what());
                }
                read += to_read;
            }
        }
        return nitems;
    };

    lucene::store::IndexInput* _input = nullptr;
};

/**
 * RandomAccessReader backed by a CLucene IndexInput with per-range caching.
 *
 * Cache granularity matches the IVF access pattern exactly: one entry per
 * list's codes region and one per list's ids region, keyed by
 * (file_prefix, file_size, byte_offset).  Every repeated borrow() on the
 * same list is a guaranteed zero-copy cache hit.
 *
 * Thread-safety: cache lookups / inserts are lock-free (the LRU cache is
 * internally sharded).  Disk reads are serialised by _io_mutex because
 * the CLucene IndexInput is stateful (seek + readBytes).
 */
struct CachedRandomAccessReader : faiss::RandomAccessReader {
    CachedRandomAccessReader(lucene::store::IndexInput* input, std::string cache_key_prefix,
                             size_t file_size)
            : _input(input->clone()),
              _cache_key_prefix(std::move(cache_key_prefix)),
              _file_size(file_size) {
        // Clear the inherited IOContext to prevent use-after-free on the
        // caller's stale pointer.  Per-read io_ctx is bound from
        // g_current_io_ctx inside _read_clucene().
        _input->setIoContext(nullptr);
    }

    ~CachedRandomAccessReader() override {
        if (_input != nullptr) {
            _input->close();
            _CLDELETE(_input);
        }
    }

    // ---- faiss::RandomAccessReader interface ----

    void read_at(size_t offset, void* ptr, size_t nbytes) const override {
        if (nbytes == 0) {
            return;
        }
        auto ref = borrow(offset, nbytes);
        DCHECK(ref != nullptr);
        ::memcpy(ptr, ref->data(), nbytes);
    }

    std::unique_ptr<faiss::ReadRef> borrow(size_t offset, size_t nbytes) const override {
        if (nbytes == 0) {
            return nullptr;
        }

        auto* cache = AnnIndexIVFListCache::instance();
        if (!cache) {
            return RandomAccessReader::borrow(offset, nbytes);
        }

        AnnIndexIVFListCache::CacheKey key(_cache_key_prefix, _file_size,
                                           static_cast<int64_t>(offset));

        // Fast path: cache hit — zero-copy, lock-free.
        PageCacheHandle handle;
        if (cache->lookup(key, &handle)) {
            ++g_ivf_on_disk_cache_stats.hit_cnt;
            return _make_pinned_ref(std::move(handle), nbytes);
        }

        // Slow path: cache miss — read from disk, then insert.
        auto page = _fetch_from_disk(offset, nbytes, cache->mem_tracker());
        cache->insert(key, page.get(), &handle);
        page.release(); // ownership transferred to cache
        return _make_pinned_ref(std::move(handle), nbytes);
    }

private:
    // ---- ReadRef that pins a cache entry ----

    struct PinnedReadRef : faiss::ReadRef {
        explicit PinnedReadRef(PageCacheHandle handle, const uint8_t* ptr, size_t len)
                : _handle(std::move(handle)) {
            data_ = ptr;
            size_ = len;
        }

    private:
        PageCacheHandle _handle;
    };

    static std::unique_ptr<faiss::ReadRef> _make_pinned_ref(PageCacheHandle handle, size_t nbytes) {
        Slice data = handle.data();
        return std::make_unique<PinnedReadRef>(std::move(handle),
                                               reinterpret_cast<const uint8_t*>(data.data), nbytes);
    }

    // ---- Disk I/O + metrics ----

    /// Read a region from CLucene, wrapped in a DataPage, and record fetch metrics.
    std::unique_ptr<DataPage> _fetch_from_disk(
            size_t offset, size_t nbytes, std::shared_ptr<MemTrackerLimiter> mem_tracker) const {
        auto page = std::make_unique<DataPage>(nbytes, std::move(mem_tracker));

        const int64_t start_ns = MonotonicNanos();
        {
            std::lock_guard<std::mutex> lock(_io_mutex);
            _read_clucene(offset, page->data(), nbytes);
        }
        const int64_t cost_ns = MonotonicNanos() - start_ns;

        ++g_ivf_on_disk_cache_stats.miss_cnt;
        DorisMetrics::instance()->ann_ivf_on_disk_fetch_page_cnt->increment(1);
        DorisMetrics::instance()->ann_ivf_on_disk_fetch_page_costs_ms->increment(
                static_cast<int64_t>(static_cast<double>(cost_ns) / 1000.0));

        return page;
    }

    /// Low-level CLucene sequential read.  Must be called under _io_mutex.
    void _read_clucene(size_t offset, char* buf, size_t nbytes) const {
        // Temporarily bind the caller's IOContext so that file-cache
        // statistics are correctly attributed to the current query.
        _input->setIoContext(g_current_io_ctx);
        _input->seek(static_cast<int64_t>(offset));
        const size_t kMaxChunk = static_cast<size_t>(std::numeric_limits<Int32>::max());
        size_t done = 0;
        while (done < nbytes) {
            const size_t to_read = std::min(nbytes - done, kMaxChunk);
            try {
                _input->readBytes(reinterpret_cast<uint8_t*>(buf + done), cast_set<Int32>(to_read));
            } catch (const std::exception& e) {
                _input->setIoContext(nullptr);
                throw doris::Exception(doris::ErrorCode::IO_ERROR,
                                       "CachedRandomAccessReader: read failed at offset {}: {}",
                                       offset + done, e.what());
            }
            done += to_read;
        }
        _input->setIoContext(nullptr);
    }

    // ---- Members ----

    mutable std::mutex _io_mutex;
    mutable lucene::store::IndexInput* _input = nullptr;
    std::string _cache_key_prefix;
    size_t _file_size;
};

doris::Status FaissVectorIndex::train(Int64 n, const float* vec) {
    DCHECK(vec != nullptr);
    DCHECK(_index != nullptr);

    ScopedThreadName scoped_name("faiss_train_idx");
    // Reserve OpenMP threads globally so concurrent builds stay under omp_threads_limit.
    ScopedOmpThreadBudget thread_budget;

    try {
        _index->train(n, vec);
    } catch (faiss::FaissException& e) {
        return doris::Status::RuntimeError("exception occurred during training: {}", e.what());
    }

    return doris::Status::OK();
}

/** Add n vectors of dimension d to the index.
*
* Vectors are implicitly assigned labels ntotal .. ntotal + n - 1
* This function slices the input vectors in chunks smaller than
* blocksize_add and calls add_core.
* @param n      number of vectors
* @param x      input matrix, size n * d
*/
doris::Status FaissVectorIndex::add(Int64 n, const float* vec) {
    DCHECK(vec != nullptr);
    DCHECK(_index != nullptr);

    ScopedThreadName scoped_name("faiss_build_idx");
    // Apply the same thread budget when adding vectors to limit concurrency.
    ScopedOmpThreadBudget thread_budget;

    try {
        DorisMetrics::instance()->ann_index_construction->increment(1);
        _index->add(n, vec);
        DorisMetrics::instance()->ann_index_in_memory_rows_cnt->increment(n);
        DorisMetrics::instance()->ann_index_construction->increment(-1);
    } catch (faiss::FaissException& e) {
        return doris::Status::RuntimeError("exception occurred during adding: {}", e.what());
    }

    return doris::Status::OK();
}

Int64 FaissVectorIndex::get_min_train_rows() const {
    // For IVF indexes, the minimum number of training points should be at least
    // equal to the number of clusters (nlist). FAISS requires this for k-means clustering.
    Int64 ivf_min = 0;
    if (_params.index_type == FaissBuildParameter::IndexType::IVF) {
        ivf_min = _params.ivf_nlist;
    }

    // Calculate minimum training rows required by the quantizer
    Int64 quantizer_min = 0;
    if (_params.quantizer == FaissBuildParameter::Quantizer::PQ) {
        // For PQ, FAISS uses ksub = 2^pq_nbits and recommends ksub * 100 training vectors.
        // This threshold depends on pq_nbits only (independent of pq_m).
        // See code from contrib/faiss/faiss/impl/ProductQuantizer.cpp::65
        quantizer_min = (1LL << _params.pq_nbits) * 100;
    } else if (_params.quantizer == FaissBuildParameter::Quantizer::SQ4 ||
               _params.quantizer == FaissBuildParameter::Quantizer::SQ8) {
        // For SQ, minimal training requirement as scalar quantization is simpler
        quantizer_min = 1;
    }
    // For FLAT, no minimum training data required

    // Return the maximum of IVF and quantizer requirements
    return std::max(ivf_min, quantizer_min);
}

void FaissVectorIndex::build(const FaissBuildParameter& params) {
    _params = params;
    _dimension = params.dim;
    switch (params.metric_type) {
    case FaissBuildParameter::MetricType::L2:
        _metric = AnnIndexMetric::L2;
        break;
    case FaissBuildParameter::MetricType::IP:
        _metric = AnnIndexMetric::IP;
        break;
    default:
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported metric type: {}",
                               static_cast<int>(params.metric_type));
        break;
    }

    if (params.index_type == FaissBuildParameter::IndexType::HNSW) {
        set_type(AnnIndexType::HNSW);
        std::unique_ptr<faiss::IndexHNSW> hnsw_index;
        if (params.quantizer == FaissBuildParameter::Quantizer::SQ4) {
            if (params.metric_type == FaissBuildParameter::MetricType::L2) {
                hnsw_index = std::make_unique<faiss::IndexHNSWSQ>(
                        params.dim, faiss::ScalarQuantizer::QT_4bit, params.max_degree,
                        faiss::METRIC_L2);
            } else {
                hnsw_index = std::make_unique<faiss::IndexHNSWSQ>(
                        params.dim, faiss::ScalarQuantizer::QT_4bit, params.max_degree,
                        faiss::METRIC_INNER_PRODUCT);
            }
        }
        if (params.quantizer == FaissBuildParameter::Quantizer::SQ8) {
            if (params.metric_type == FaissBuildParameter::MetricType::L2) {
                hnsw_index = std::make_unique<faiss::IndexHNSWSQ>(
                        params.dim, faiss::ScalarQuantizer::QT_8bit, params.max_degree,
                        faiss::METRIC_L2);
            } else {
                hnsw_index = std::make_unique<faiss::IndexHNSWSQ>(
                        params.dim, faiss::ScalarQuantizer::QT_8bit, params.max_degree,
                        faiss::METRIC_INNER_PRODUCT);
            }
        }
        if (params.quantizer == FaissBuildParameter::Quantizer::PQ) {
            if (params.metric_type == FaissBuildParameter::MetricType::L2) {
                hnsw_index = std::make_unique<faiss::IndexHNSWPQ>(
                        params.dim, params.pq_m, params.max_degree, params.pq_nbits,
                        faiss::METRIC_L2);
            } else {
                hnsw_index = std::make_unique<faiss::IndexHNSWPQ>(
                        params.dim, params.pq_m, params.max_degree, params.pq_nbits,
                        faiss::METRIC_INNER_PRODUCT);
            }
        }
        if (params.quantizer == FaissBuildParameter::Quantizer::FLAT) {
            if (params.metric_type == FaissBuildParameter::MetricType::L2) {
                hnsw_index = std::make_unique<faiss::IndexHNSWFlat>(params.dim, params.max_degree,
                                                                    faiss::METRIC_L2);
            } else {
                hnsw_index = std::make_unique<faiss::IndexHNSWFlat>(params.dim, params.max_degree,
                                                                    faiss::METRIC_INNER_PRODUCT);
            }
        }
        hnsw_index->hnsw.efConstruction = params.ef_construction;

        _index = std::move(hnsw_index);
    } else if (params.index_type == FaissBuildParameter::IndexType::IVF ||
               params.index_type == FaissBuildParameter::IndexType::IVF_ON_DISK) {
        if (params.index_type == FaissBuildParameter::IndexType::IVF) {
            set_type(AnnIndexType::IVF);
        } else {
            set_type(AnnIndexType::IVF_ON_DISK);
        }
        std::unique_ptr<faiss::Index> ivf_index;
        if (params.metric_type == FaissBuildParameter::MetricType::L2) {
            _quantizer = std::make_unique<faiss::IndexFlat>(params.dim, faiss::METRIC_L2);
        } else {
            _quantizer =
                    std::make_unique<faiss::IndexFlat>(params.dim, faiss::METRIC_INNER_PRODUCT);
        }

        if (params.quantizer == FaissBuildParameter::Quantizer::FLAT) {
            if (params.metric_type == FaissBuildParameter::MetricType::L2) {
                ivf_index = std::make_unique<faiss::IndexIVFFlat>(
                        _quantizer.get(), params.dim, params.ivf_nlist, faiss::METRIC_L2);
            } else {
                ivf_index = std::make_unique<faiss::IndexIVFFlat>(_quantizer.get(), params.dim,
                                                                  params.ivf_nlist,
                                                                  faiss::METRIC_INNER_PRODUCT);
            }
        } else if (params.quantizer == FaissBuildParameter::Quantizer::SQ4) {
            if (params.metric_type == FaissBuildParameter::MetricType::L2) {
                ivf_index = std::make_unique<faiss::IndexIVFScalarQuantizer>(
                        _quantizer.get(), params.dim, params.ivf_nlist,
                        faiss::ScalarQuantizer::QT_4bit, faiss::METRIC_L2);
            } else {
                ivf_index = std::make_unique<faiss::IndexIVFScalarQuantizer>(
                        _quantizer.get(), params.dim, params.ivf_nlist,
                        faiss::ScalarQuantizer::QT_4bit, faiss::METRIC_INNER_PRODUCT);
            }
        } else if (params.quantizer == FaissBuildParameter::Quantizer::SQ8) {
            if (params.metric_type == FaissBuildParameter::MetricType::L2) {
                ivf_index = std::make_unique<faiss::IndexIVFScalarQuantizer>(
                        _quantizer.get(), params.dim, params.ivf_nlist,
                        faiss::ScalarQuantizer::QT_8bit, faiss::METRIC_L2);
            } else {
                ivf_index = std::make_unique<faiss::IndexIVFScalarQuantizer>(
                        _quantizer.get(), params.dim, params.ivf_nlist,
                        faiss::ScalarQuantizer::QT_8bit, faiss::METRIC_INNER_PRODUCT);
            }
        } else if (params.quantizer == FaissBuildParameter::Quantizer::PQ) {
            if (params.metric_type == FaissBuildParameter::MetricType::L2) {
                ivf_index = std::make_unique<faiss::IndexIVFPQ>(_quantizer.get(), params.dim,
                                                                params.ivf_nlist, params.pq_m,
                                                                params.pq_nbits, faiss::METRIC_L2);
            } else {
                ivf_index = std::make_unique<faiss::IndexIVFPQ>(
                        _quantizer.get(), params.dim, params.ivf_nlist, params.pq_m,
                        params.pq_nbits, faiss::METRIC_INNER_PRODUCT);
            }
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported quantizer for IVF: {}",
                                   static_cast<int>(params.quantizer));
        }

        _index = std::move(ivf_index);
    } else {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Unsupported index type: {}",
                               static_cast<int>(params.index_type));
    }
}

// TODO: Support batch search
doris::Status FaissVectorIndex::ann_topn_search(const float* query_vec, int k,
                                                const segment_v2::IndexSearchParameters& params,
                                                segment_v2::IndexSearchResult& result) {
    const int64_t cache_hit_cnt_before = g_ivf_on_disk_cache_stats.hit_cnt;
    const int64_t cache_miss_cnt_before = g_ivf_on_disk_cache_stats.miss_cnt;
    std::unique_ptr<float[]> distances_ptr;
    std::unique_ptr<std::vector<faiss::idx_t>> labels_ptr;
    {
        SCOPED_RAW_TIMER(&result.engine_prepare_ns);
        distances_ptr = std::make_unique<float[]>(k);
        // Initialize labels with -1
        // Even if there are N vectors in the index, limit N search in faiss could return less than N(eg, HNSW)
        // so we need to initialize labels with -1 to tell the end of the result ids.
        labels_ptr = std::make_unique<std::vector<faiss::idx_t>>(k, -1);
    }
    float* distances = distances_ptr.get();
    faiss::idx_t* labels = (*labels_ptr).data();
    DCHECK(params.roaring != nullptr)
            << "Roaring should not be null for topN search, please set roaring in params";

    std::unique_ptr<faiss::SearchParameters> search_param;
    std::unique_ptr<faiss::IDSelector> id_sel = nullptr;
    // Costs of roaring to faiss selector is very high especially when the cardinality is very high.
    if (params.roaring->cardinality() != params.rows_of_segment) {
        SCOPED_RAW_TIMER(&result.engine_prepare_ns);
        id_sel = roaring_to_faiss_selector(*params.roaring);
    }

    if (_index_type == AnnIndexType::HNSW) {
        const HNSWSearchParameters* hnsw_params =
                dynamic_cast<const HNSWSearchParameters*>(&params);
        if (hnsw_params == nullptr) {
            return doris::Status::InvalidArgument(
                    "HNSW search parameters should not be null for HNSW index");
        }
        faiss::SearchParametersHNSW* param = new faiss::SearchParametersHNSW();
        param->efSearch = hnsw_params->ef_search;
        param->check_relative_distance = hnsw_params->check_relative_distance;
        param->bounded_queue = hnsw_params->bounded_queue;
        param->sel = id_sel.get();
        search_param.reset(param);
    } else if (_index_type == AnnIndexType::IVF || _index_type == AnnIndexType::IVF_ON_DISK) {
        const IVFSearchParameters* ivf_params = dynamic_cast<const IVFSearchParameters*>(&params);
        if (ivf_params == nullptr) {
            return doris::Status::InvalidArgument(
                    "IVF search parameters should not be null for IVF index");
        }
        faiss::SearchParametersIVF* param = new faiss::SearchParametersIVF();
        param->nprobe = ivf_params->nprobe;
        param->sel = id_sel.get();
        search_param.reset(param);
    } else {
        return doris::Status::InvalidArgument("Unsupported index type for search");
    }

    {
        SCOPED_RAW_TIMER(&result.engine_search_ns);
        ScopedIoCtxBinding io_ctx_binding(params.io_ctx);
        try {
            _index->search(1, query_vec, k, distances, labels, search_param.get());
        } catch (faiss::FaissException& e) {
            return doris::Status::RuntimeError("exception occurred during ann topn search: {}",
                                               e.what());
        }
    }
    {
        SCOPED_RAW_TIMER(&result.engine_convert_ns);
        result.roaring = std::make_shared<roaring::Roaring>();
        update_roaring(labels, k, *result.roaring);
        size_t roaring_cardinality = result.roaring->cardinality();
        result.distances = std::make_unique<float[]>(roaring_cardinality);
        result.row_ids = std::make_unique<std::vector<uint64_t>>();
        result.row_ids->resize(roaring_cardinality);

        if (_metric == AnnIndexMetric::L2) {
            // For l2_distance, we need to convert the distance to the actual distance.
            // The distance returned by Faiss is actually the squared distance.
            // So we need to take the square root of the squared distance.
            for (size_t i = 0; i < roaring_cardinality; ++i) {
                (*result.row_ids)[i] = labels[i];
                result.distances[i] = std::sqrt(distances[i]);
            }
        } else if (_metric == AnnIndexMetric::IP) {
            // For inner product, we can use the distance directly.
            for (size_t i = 0; i < roaring_cardinality; ++i) {
                (*result.row_ids)[i] = labels[i];
                result.distances[i] = distances[i];
            }
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported metric type: {}", static_cast<int>(_metric));
        }

        DCHECK(result.row_ids->size() == result.roaring->cardinality())
                << "Row ids size: " << result.row_ids->size()
                << ", roaring size: " << result.roaring->cardinality();
    }
    if (_index_type == AnnIndexType::IVF_ON_DISK) {
        result.ivf_on_disk_cache_hit_cnt = g_ivf_on_disk_cache_stats.hit_cnt - cache_hit_cnt_before;
        result.ivf_on_disk_cache_miss_cnt =
                g_ivf_on_disk_cache_stats.miss_cnt - cache_miss_cnt_before;
    }
    // distance/row_ids conversion above already timed via SCOPED_RAW_TIMER
    return doris::Status::OK();
}

// For l2 distance, range search radius is the squared distance.
// For inner product, range search radius is the actual distance.
// range search on inner product returns all vectors with inner product greater than or equal to the radius.
// For l2 distance, range search returns all vectors with squared distance less than or equal to the radius.
doris::Status FaissVectorIndex::range_search(const float* query_vec, const float& radius,
                                             const segment_v2::IndexSearchParameters& params,
                                             segment_v2::IndexSearchResult& result) {
    const int64_t cache_hit_cnt_before = g_ivf_on_disk_cache_stats.hit_cnt;
    const int64_t cache_miss_cnt_before = g_ivf_on_disk_cache_stats.miss_cnt;
    DCHECK(_index != nullptr);
    DCHECK(query_vec != nullptr);
    DCHECK(params.roaring != nullptr)
            << "Roaring should not be null for range search, please set roaring in params";
    std::unique_ptr<faiss::SearchParameters> search_param;
    const HNSWSearchParameters* hnsw_params = dynamic_cast<const HNSWSearchParameters*>(&params);
    const IVFSearchParameters* ivf_params = dynamic_cast<const IVFSearchParameters*>(&params);
    if (hnsw_params != nullptr) {
        faiss::SearchParametersHNSW* param = new faiss::SearchParametersHNSW();
        {
            // Engine prepare: set search parameters and bind selector
            SCOPED_RAW_TIMER(&result.engine_prepare_ns);
            param->efSearch = hnsw_params->ef_search;
            param->check_relative_distance = hnsw_params->check_relative_distance;
            param->bounded_queue = hnsw_params->bounded_queue;
        }
        search_param.reset(param);
    } else if (ivf_params != nullptr) {
        faiss::SearchParametersIVF* param = new faiss::SearchParametersIVF();
        {
            // Engine prepare: set search parameters and bind selector
            SCOPED_RAW_TIMER(&result.engine_prepare_ns);
            param->nprobe = ivf_params->nprobe;
        }
        search_param.reset(param);
    } else {
        return doris::Status::InvalidArgument("Unsupported index type for range search");
    }
    std::unique_ptr<faiss::IDSelector> sel;
    {
        // Engine prepare: convert roaring bitmap to FAISS selector
        SCOPED_RAW_TIMER(&result.engine_prepare_ns);
        if (params.roaring->cardinality() != params.rows_of_segment) {
            sel = roaring_to_faiss_selector(*params.roaring);
            search_param->sel = sel.get();
        } else {
            search_param->sel = nullptr;
        }
    }

    faiss::RangeSearchResult native_search_result(1, true);
    {
        // Engine search: FAISS range_search
        SCOPED_RAW_TIMER(&result.engine_search_ns);
        ScopedIoCtxBinding io_ctx_binding(params.io_ctx);
        try {
            if (_metric == AnnIndexMetric::L2) {
                if (radius <= 0) {
                    _index->range_search(1, query_vec, 0.0f, &native_search_result,
                                         search_param.get());
                } else {
                    _index->range_search(1, query_vec, radius * radius, &native_search_result,
                                         search_param.get());
                }
            } else if (_metric == AnnIndexMetric::IP) {
                _index->range_search(1, query_vec, radius, &native_search_result,
                                     search_param.get());
            }
        } catch (faiss::FaissException& e) {
            return doris::Status::RuntimeError("exception occurred during ann range search: {}",
                                               e.what());
        }
    }

    size_t begin = native_search_result.lims[0];
    size_t end = native_search_result.lims[1];
    auto row_ids = std::make_unique<std::vector<uint64_t>>();
    row_ids->resize(end - begin);
    if (params.is_le_or_lt) {
        if (_metric == AnnIndexMetric::L2) {
            std::unique_ptr<float[]> distances_ptr;
            float* distances = nullptr;
            auto roaring = std::make_shared<roaring::Roaring>();
            {
                // Engine convert: build roaring, row_ids, distances from FAISS result
                SCOPED_RAW_TIMER(&result.engine_convert_ns);
                distances_ptr = std::make_unique<float[]>(end - begin);
                distances = distances_ptr.get();
                // The distance returned by Faiss is actually the squared distance.
                // So we need to take the square root of the squared distance.
                for (size_t i = begin; i < end; ++i) {
                    (*row_ids)[i] = native_search_result.labels[i];
                    roaring->add(cast_set<UInt32>(native_search_result.labels[i]));
                    distances[i - begin] = sqrt(native_search_result.distances[i]);
                }
            }
            result.distances = std::move(distances_ptr);
            result.row_ids = std::move(row_ids);
            result.roaring = roaring;

            DCHECK(result.row_ids->size() == result.roaring->cardinality())
                    << "row_ids size: " << result.row_ids->size()
                    << ", roaring size: " << result.roaring->cardinality();
        } else if (_metric == AnnIndexMetric::IP) {
            // For IP, we can not use the distance directly.
            // range search on ip gets all vectors with inner product greater than or equal to the radius.
            // so we need to do a convertion.
            const roaring::Roaring& origin_row_ids = *params.roaring;
            std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
            {
                // Engine convert: compute roaring difference
                SCOPED_RAW_TIMER(&result.engine_convert_ns);
                for (size_t i = begin; i < end; ++i) {
                    roaring->add(cast_set<UInt32>(native_search_result.labels[i]));
                }
                result.roaring = std::make_shared<roaring::Roaring>();
                // remove all rows that should not be included.
                *(result.roaring) = origin_row_ids - *roaring;
                // Just update the roaring. distance can not be used.
                result.distances = nullptr;
                result.row_ids = nullptr;
            }
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported metric type: {}", static_cast<int>(_metric));
        }
    } else {
        if (_metric == AnnIndexMetric::L2) {
            // Faiss can only return labels in the range of radius.
            // If the precidate is not less than, we need to to a convertion.
            const roaring::Roaring& origin_row_ids = *params.roaring;
            std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
            {
                // Engine convert: compute roaring difference
                SCOPED_RAW_TIMER(&result.engine_convert_ns);
                for (size_t i = begin; i < end; ++i) {
                    roaring->add(cast_set<UInt32>(native_search_result.labels[i]));
                }
                result.roaring = std::make_shared<roaring::Roaring>();
                *(result.roaring) = origin_row_ids - *roaring;
                result.distances = nullptr;
                result.row_ids = nullptr;
            }
        } else if (_metric == AnnIndexMetric::IP) {
            // For inner product, we can use the distance directly.
            // range search on ip gets all vectors with inner product greater than or equal to the radius.
            // when query condition is not le_or_lt, we can use the roaring and distance directly.
            std::unique_ptr<float[]> distances_ptr = std::make_unique<float[]>(end - begin);
            float* distances = distances_ptr.get();
            auto roaring = std::make_shared<roaring::Roaring>();
            // The distance returned by Faiss is actually the squared distance.
            // So we need to take the square root of the squared distance.
            for (size_t i = begin; i < end; ++i) {
                (*row_ids)[i] = native_search_result.labels[i];
                roaring->add(cast_set<UInt32>(native_search_result.labels[i]));
                distances[i - begin] = native_search_result.distances[i];
            }
            result.distances = std::move(distances_ptr);
            result.row_ids = std::move(row_ids);
            result.roaring = roaring;

            DCHECK(result.row_ids->size() == result.roaring->cardinality())
                    << "row_ids size: " << result.row_ids->size()
                    << ", roaring size: " << result.roaring->cardinality();

        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Unsupported metric type: {}", static_cast<int>(_metric));
        }
    }

    if (_index_type == AnnIndexType::IVF_ON_DISK) {
        result.ivf_on_disk_cache_hit_cnt = g_ivf_on_disk_cache_stats.hit_cnt - cache_hit_cnt_before;
        result.ivf_on_disk_cache_miss_cnt =
                g_ivf_on_disk_cache_stats.miss_cnt - cache_miss_cnt_before;
    }

    return Status::OK();
}

doris::Status FaissVectorIndex::save(lucene::store::Directory* dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    if (_index_type == AnnIndexType::IVF_ON_DISK) {
        // IVF_ON_DISK: write ivf data to a separate file, then write index metadata.
        //
        // Why do we replace invlists here in save() instead of at build() time?
        // During build/train/add, IndexIVF needs a writable ArrayInvertedLists to
        // receive vectors via add_entries(). PreadInvertedLists inherits from
        // ReadOnlyInvertedLists and does not support writes. The original
        // OnDiskInvertedLists does support writes but requires mmap on a real file,
        // which is unavailable at build time (Directory is only passed to save()).
        // So the standard faiss pattern is: build in-memory with ArrayInvertedLists,
        // then convert to on-disk format at serialization time. The replace_invlists
        // call in Step 2 is purely a serialization format switch (to emit "ilod"
        // fourcc instead of "ilar"), not a runtime data structure change.
        //
        // Step 1: The in-memory index has ArrayInvertedLists. Write them to ann.ivfdata
        //         by converting to OnDiskInvertedLists format:
        //         For each list: [codes: capacity*code_size][ids: capacity*sizeof(idx_t)]
        auto* ivf = dynamic_cast<faiss::IndexIVF*>(_index.get());
        DCHECK(ivf != nullptr);
        auto* ails = dynamic_cast<faiss::ArrayInvertedLists*>(ivf->invlists);
        DCHECK(ails != nullptr);

        const size_t nlist = ails->nlist;
        const size_t code_size = ails->code_size;

        // Build OnDiskOneList metadata and write data to ann.ivfdata
        std::vector<faiss::OnDiskOneList> lists(nlist);
        lucene::store::IndexOutput* ivfdata_output = dir->createOutput(faiss_ivfdata_file_name);
        size_t offset = 0;
        for (size_t i = 0; i < nlist; i++) {
            size_t list_size = ails->list_size(i);
            lists[i].size = list_size;
            lists[i].capacity = list_size;
            lists[i].offset = offset;

            if (list_size > 0) {
                // Write codes
                const uint8_t* codes = ails->get_codes(i);
                size_t codes_bytes = list_size * code_size;
                ivfdata_output->writeBytes(reinterpret_cast<const uint8_t*>(codes),
                                           cast_set<Int32>(codes_bytes));

                // Write ids
                const faiss::idx_t* ids = ails->get_ids(i);
                size_t ids_bytes = list_size * sizeof(faiss::idx_t);
                ivfdata_output->writeBytes(reinterpret_cast<const uint8_t*>(ids),
                                           cast_set<Int32>(ids_bytes));
            }

            offset += list_size * (code_size + sizeof(faiss::idx_t));
        }
        size_t totsize = offset;
        ivfdata_output->close();
        delete ivfdata_output;

        // Step 2: Replace ArrayInvertedLists with OnDiskInvertedLists so that
        //         write_index serializes in "ilod" format (metadata only).
        auto* od = new faiss::OnDiskInvertedLists();
        od->nlist = nlist;
        od->code_size = code_size;
        od->lists = std::move(lists);
        od->totsize = totsize;
        od->ptr = nullptr;
        od->read_only = true;
        // filename is not used during load (we use separate ivfdata file),
        // but write it for format completeness.
        od->filename = faiss_ivfdata_file_name;
        ivf->replace_invlists(od, true);

        // Step 3: Write index metadata to ann.faiss (includes "ilod" fourcc)
        lucene::store::IndexOutput* idx_output = dir->createOutput(faiss_index_fila_name);
        auto writer = std::make_unique<FaissIndexWriter>(idx_output);
        faiss::write_index(_index.get(), writer.get());

    } else {
        // HNSW / IVF: write the full index to ann.faiss
        lucene::store::IndexOutput* idx_output = dir->createOutput(faiss_index_fila_name);
        auto writer = std::make_unique<FaissIndexWriter>(idx_output);
        faiss::write_index(_index.get(), writer.get());
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    LOG_INFO(fmt::format("Faiss index saved to {}, {}, rows {}, cost {} ms", dir->toString(),
                         faiss_index_fila_name, _index->ntotal, duration.count()));

    return doris::Status::OK();
}

doris::Status FaissVectorIndex::load(lucene::store::Directory* dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    if (_index_type == AnnIndexType::IVF_ON_DISK) {
        // IVF_ON_DISK load:
        // 1. Read index metadata from ann.faiss with IO_FLAG_SKIP_IVF_DATA.
        //    This reads "ilod" metadata (lists[], slots[], etc.) without mmap.
        // 2. Replace the OnDiskInvertedLists with PreadInvertedLists.
        // 3. Open ann.ivfdata via CLucene IndexInput and bind as RandomAccessReader.
        lucene::store::IndexInput* idx_input = nullptr;
        try {
            idx_input = dir->openInput(faiss_index_fila_name);
        } catch (const CLuceneError& e) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "Failed to open index file: {}, error: {}", faiss_index_fila_name, e.what());
        }

        auto reader = std::make_unique<FaissIndexReader>(idx_input);
        // IO_FLAG_SKIP_IVF_DATA: read only index metadata (quantizer, PQ codebook,
        //   inverted-list slot table) without loading the bulk inverted-list data.
        //
        // IO_FLAG_SKIP_PRECOMPUTE_TABLE: skip rebuilding the IVFPQ precomputed
        //   distance table.  The table is NOT serialized on disk; FAISS recomputes
        //   it from quantizer centroids and PQ codebook on every read_index() call
        //   (see index_read.cpp read_ivfpq(), IndexIVFPQ.cpp precompute_table()).
        //
        //   Per-segment table memory:
        //     logical_elems = nlist * pq.M * pq.ksub         (floats)
        //     logical_bytes = logical_elems * sizeof(float)   (bytes)
        //     actual_bytes  = next_power_of_2(logical_elems) * sizeof(float)
        //                     (AlignedTable rounds up to next power of two)
        //
        //   Concrete example  (nlist=1024, pq_m=384, pq_nbits=8, ksub=2^8=256):
        //     logical_elems = 1024 * 384 * 256 = 100,663,296
        //     logical_bytes = 100,663,296 * 4  = 402,653,184  (384 MiB)
        //     next_pow2     = 2^27             = 134,217,728
        //     actual_bytes  = 134,217,728 * 4  = 536,870,912  (512 MiB per segment)
        //
        //   With 146 segments loaded simultaneously:
        //     total = 146 * 512 MiB = 74,752 MiB  (~73 GiB untracked RSS)
        //
        //   FAISS has a per-table guard (precomputed_table_max_bytes = 2 GiB) but
        //   it checks only the single-table logical size (384 MiB << 2 GiB), so it
        //   cannot prevent the multi-segment accumulation.
        //
        //   For IVF_ON_DISK the search bottleneck is disk/network I/O, not per-list
        //   CPU distance computation.  Skipping the table makes search fall back to
        //   on-the-fly compute_residual() + compute_distance_table() per (query,
        //   inverted-list) pair, which is functionally identical and correct.
        constexpr int kIVFOnDiskReadFlags =
                faiss::IO_FLAG_SKIP_IVF_DATA | faiss::IO_FLAG_SKIP_PRECOMPUTE_TABLE;
        faiss::Index* idx = faiss::read_index(reader.get(), kIVFOnDiskReadFlags);

        // Replace OnDiskInvertedLists (metadata-only) with PreadInvertedLists
        faiss::PreadInvertedLists* pread = faiss::replace_with_pread_invlists(idx);

        // Open ann.ivfdata and bind the cached random access reader
        lucene::store::IndexInput* ivfdata_input = nullptr;
        try {
            ivfdata_input = dir->openInput(faiss_ivfdata_file_name);
        } catch (const CLuceneError& e) {
            delete idx;
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "Failed to open ivfdata file: {}, error: {}", faiss_ivfdata_file_name,
                    e.what());
        }
        const size_t ivfdata_size = static_cast<size_t>(ivfdata_input->length());
        pread->set_reader(std::make_unique<CachedRandomAccessReader>(
                ivfdata_input, _ivfdata_cache_key_prefix, ivfdata_size));

        // Close the original input (CachedRandomAccessReader cloned it)
        ivfdata_input->close();
        _CLDELETE(ivfdata_input);

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        VLOG_DEBUG << fmt::format("Load IVF_ON_DISK index from {} costs {} ms, rows {}",
                                  dir->getObjectName(), duration.count(), idx->ntotal);
        _index.reset(idx);
        DorisMetrics::instance()->ann_index_in_memory_rows_cnt->increment(_index->ntotal);
    } else {
        // HNSW / IVF: load the full index from ann.faiss
        lucene::store::IndexInput* idx_input = nullptr;
        try {
            idx_input = dir->openInput(faiss_index_fila_name);
        } catch (const CLuceneError& e) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "Failed to open index file: {}, error: {}", faiss_index_fila_name, e.what());
        }

        auto reader = std::make_unique<FaissIndexReader>(idx_input);
        // IO_FLAG_SKIP_PRECOMPUTE_TABLE: skip rebuilding IVFPQ precomputed
        // distance tables for in-memory IVF.  Same rationale as IVF_ON_DISK –
        // each segment's table can be hundreds of MiBs (see calculation above),
        // and the per-list on-the-fly compute_residual() + compute_distance_table()
        // is negligible compared to overall search cost.
        // The flag is a no-op for HNSW (no IVF code path is reached).
        const int read_flags =
                (_index_type == AnnIndexType::IVF) ? faiss::IO_FLAG_SKIP_PRECOMPUTE_TABLE : 0;
        faiss::Index* idx = faiss::read_index(reader.get(), read_flags);
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        VLOG_DEBUG << fmt::format("Load index from {} costs {} ms, rows {}", dir->getObjectName(),
                                  duration.count(), idx->ntotal);
        _index.reset(idx);
        DorisMetrics::instance()->ann_index_in_memory_rows_cnt->increment(_index->ntotal);
    }

    return doris::Status::OK();
}

} // namespace doris::segment_v2
