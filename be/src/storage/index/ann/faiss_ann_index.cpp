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
#include <faiss/invlists/OnDiskInvertedListsV2.h>
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
#include "storage/cache/page_cache.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_files.h"
#include "storage/index/ann/ann_search_params.h"
#include "util/thread.h"
#include "util/time.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

namespace {

std::mutex g_omp_thread_mutex;
int g_index_threads_in_use = 0;

struct IvfOnDiskCacheStats {
    int64_t hit_cnt = 0;
    int64_t miss_cnt = 0;
};

thread_local IvfOnDiskCacheStats g_ivf_on_disk_cache_stats;

// Guard that ensures the total OpenMP threads used by concurrent index builds
// never exceed the configured omp_threads_limit.
class ScopedOmpThreadBudget {
public:
    // For each index build, reserve at most half of the remaining threads, at least 1 thread.
    ScopedOmpThreadBudget() {
        std::unique_lock<std::mutex> lock(g_omp_thread_mutex);
        auto thread_cap = config::omp_threads_limit - g_index_threads_in_use;
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
 * RandomAccessReader backed by a CLucene IndexInput with block-level caching.
 *
 * Each read_at() request is fulfilled block by block.  Blocks are cached in
 * Doris's global StoragePageCache (INDEX_PAGE type) so that repeated reads
 * to the same region (very common during IVF search) hit cheap memcpy paths
 * instead of going through the CLucene I/O stack.
 *
 * Thread-safety: cache hits are lock-free (the LRU cache is internally
 * thread-safe and the returned PageCacheHandle pins the data).  Cache misses
 * are serialised by _io_mutex because the underlying CLucene IndexInput is
 * not thread-safe (seek + readBytes).
 */
struct CachedRandomAccessReader : faiss::RandomAccessReader {
    static constexpr size_t kDefaultBlockSize = 256 * 1024; // 256 KB

    /**
     * @param input           CLucene IndexInput for the ivfdata sub-file.
     *                        The reader clones it (caller may close original).
     * @param cache_key_prefix  Unique string identifying this ivfdata file,
     *                          e.g. the index-file path from IndexFileReader.
     * @param file_size       Total byte length of the ivfdata sub-file.
     * @param block_size      Cache block size (default 256 KB).
     */
    CachedRandomAccessReader(lucene::store::IndexInput* input, std::string cache_key_prefix,
                             size_t file_size, size_t block_size = kDefaultBlockSize)
            : _input(input->clone()),
              _cache_key_prefix(std::move(cache_key_prefix)),
              _file_size(file_size),
              _block_size(block_size) {
        // IndexFileReader::init() called _stream->setIoContext(io_ctx) where
        // io_ctx.file_cache_stats points into BlockReader::_stats (a value member
        // inside BlockReader).  CachedRandomAccessReader outlives the BlockReader,
        // so that pointer becomes dangling when BlockReader is freed.  The cloned
        // CSIndexInput's _io_ctx is nullptr, meaning CSIndexInput::readInternal()
        // never calls base->setIoContext(), leaving the stale file_cache_stats on
        // the shared base FSIndexInput.  Fix: give the clone a non-null _io_ctx
        // that owns no short-lived pointers.  CSIndexInput::readInternal() will
        // then transiently set _local_io_ctx (file_cache_stats=nullptr) on the
        // base FSIndexInput before each read and clear it after, preventing UAF.
        _input->setIoContext(&_local_io_ctx);
    }

    ~CachedRandomAccessReader() override {
        if (_input != nullptr) {
            _input->close();
            _CLDELETE(_input);
        }
    }

    void read_at(size_t offset, void* ptr, size_t nbytes) const override {
        if (nbytes == 0) {
            return;
        }
        auto* dst = static_cast<uint8_t*>(ptr);
        size_t remaining = nbytes;
        size_t cur_offset = offset;

        while (remaining > 0) {
            // Which block does cur_offset fall into?
            const size_t block_no = cur_offset / _block_size;
            const size_t block_start = block_no * _block_size;
            const size_t offset_in_block = cur_offset - block_start;
            const size_t can_read = std::min(remaining, _block_size - offset_in_block);

            auto* cache = StoragePageCache::instance();
            StoragePageCache::CacheKey cache_key(_cache_key_prefix, _file_size,
                                                 static_cast<int64_t>(block_start));
            PageCacheHandle handle;

            if (cache && cache->lookup(cache_key, &handle, segment_v2::DATA_PAGE)) {
                // Cache hit – just memcpy
                Slice data = handle.data();
                ::memcpy(dst, data.data + offset_in_block, can_read);
                ++g_ivf_on_disk_cache_stats.hit_cnt;
            } else {
                // Cache miss – read the whole block from CLucene under a lock.
                const size_t actual_block_size =
                        std::min(_block_size, _file_size > block_start ? _file_size - block_start
                                                                       : static_cast<size_t>(0));

                auto page = std::make_unique<DataPage>(actual_block_size, /*use_cache=*/true,
                                                       segment_v2::DATA_PAGE);

                const int64_t fetch_start_ns = MonotonicNanos();
                {
                    std::lock_guard<std::mutex> lock(_io_mutex);
                    _read_from_input(block_start, page->data(), actual_block_size);
                }
                const int64_t fetch_costs_ns = MonotonicNanos() - fetch_start_ns;

                ::memcpy(dst, page->data() + offset_in_block, can_read);

                if (cache) {
                    cache->insert(cache_key, page.get(), &handle, segment_v2::DATA_PAGE);
                    page.release(); // cache owns the page now
                }
                ++g_ivf_on_disk_cache_stats.miss_cnt;
                DorisMetrics::instance()->ann_ivf_on_disk_fetch_page_cnt->increment(1);
                double fetch_costs_ms = static_cast<double>(fetch_costs_ns) / 1000.0;
                DorisMetrics::instance()->ann_ivf_on_disk_fetch_page_costs_ms->increment(
                        static_cast<int64_t>(fetch_costs_ms));
            }

            dst += can_read;
            cur_offset += can_read;
            remaining -= can_read;
        }
    }

private:
    void _read_from_input(size_t offset, char* buf, size_t nbytes) const {
        _input->seek(static_cast<int64_t>(offset));
        const size_t kMaxChunk = static_cast<size_t>(std::numeric_limits<Int32>::max());
        size_t done = 0;
        while (done < nbytes) {
            const size_t to_read = std::min(nbytes - done, kMaxChunk);
            try {
                _input->readBytes(reinterpret_cast<uint8_t*>(buf + done), cast_set<Int32>(to_read));
            } catch (const std::exception& e) {
                throw doris::Exception(doris::ErrorCode::IO_ERROR,
                                       "CachedRandomAccessReader::read_at failed: {}", e.what());
            }
            done += to_read;
        }
    }

    mutable std::mutex _io_mutex;
    // Safe local IOContext with file_cache_stats=nullptr.  Its address is passed
    // to the cloned CSIndexInput via setIoContext() in the constructor so that
    // CSIndexInput::readInternal() installs it on the base FSIndexInput for the
    // duration of each read, preventing access to any expired pointer.
    io::IOContext _local_io_ctx;
    mutable lucene::store::IndexInput* _input = nullptr;
    std::string _cache_key_prefix;
    size_t _file_size;
    size_t _block_size;
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
        // receive vectors via add_entries(). OnDiskInvertedListsV2 inherits from
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
        // 2. Replace the OnDiskInvertedLists with OnDiskInvertedListsV2.
        // 3. Open ann.ivfdata via CLucene IndexInput and bind as RandomAccessReader.
        lucene::store::IndexInput* idx_input = nullptr;
        try {
            idx_input = dir->openInput(faiss_index_fila_name);
        } catch (const CLuceneError& e) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "Failed to open index file: {}, error: {}", faiss_index_fila_name, e.what());
        }

        auto reader = std::make_unique<FaissIndexReader>(idx_input);
        faiss::Index* idx = faiss::read_index(reader.get(), faiss::IO_FLAG_SKIP_IVF_DATA);

        // Replace OnDiskInvertedLists (metadata-only) with V2
        faiss::OnDiskInvertedListsV2* v2 = faiss::replace_ondisk_invlists_with_v2(idx);

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
        v2->set_reader(std::make_unique<CachedRandomAccessReader>(
                ivfdata_input, _ivfdata_cache_key_prefix, ivfdata_size));

        // Plug Doris's tracked allocator so that iterator buffers are
        // accounted in the MemTracker hierarchy.
        faiss::MemoryAllocator mem_alloc;
        mem_alloc.alloc = [](size_t nbytes) -> void* { return Allocator<false>().alloc(nbytes); };
        mem_alloc.free = [](void* ptr, size_t nbytes) { Allocator<false>().free(ptr, nbytes); };
        v2->set_allocator(std::move(mem_alloc));

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
        faiss::Index* idx = faiss::read_index(reader.get());
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
