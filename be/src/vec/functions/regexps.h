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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/Regexps.h
// and modified by Doris

#pragma once

#include <hs/hs.h>
#include <hs/hs_common.h>

#include <boost/container_hash/hash.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "vec/common/string_ref.h"

namespace doris::vectorized::multiregexps {

template <typename Deleter, Deleter deleter>
struct HyperscanDeleter {
    template <typename T>
    void operator()(T* ptr) const {
        deleter(ptr);
    }
};

/// Helper unique pointers to correctly delete the allocated space when hyperscan cannot compile something and we throw an exception.
using CompilerError =
        std::unique_ptr<hs_compile_error_t,
                        HyperscanDeleter<decltype(&hs_free_compile_error), &hs_free_compile_error>>;
using ScratchPtr = std::unique_ptr<hs_scratch_t,
                                   HyperscanDeleter<decltype(&hs_free_scratch), &hs_free_scratch>>;
using DataBasePtr =
        std::unique_ptr<hs_database_t,
                        HyperscanDeleter<decltype(&hs_free_database), &hs_free_database>>;

/// Database is thread safe across multiple threads and Scratch is not but we can copy it whenever we use it in the searcher.
class Regexps {
public:
    Regexps(hs_database_t* db_, hs_scratch_t* scratch_) : db {db_}, scratch {scratch_} {}

    hs_database_t* getDB() const { return db.get(); }
    hs_scratch_t* getScratch() const { return scratch.get(); }

private:
    DataBasePtr db;
    ScratchPtr scratch;
};

class DeferredConstructedRegexps {
public:
    explicit DeferredConstructedRegexps(std::function<Regexps()> constructor_)
            : constructor(std::move(constructor_)) {}

    Regexps* get() {
        std::lock_guard lock(mutex);
        if (regexps) {
            return &*regexps;
        }
        regexps = constructor();
        return &*regexps;
    }

private:
    std::mutex mutex;
    std::function<Regexps()> constructor;
    std::optional<Regexps> regexps;
};

using DeferredConstructedRegexpsPtr = std::shared_ptr<DeferredConstructedRegexps>;

template <bool save_indices, bool WithEditDistance>
Regexps constructRegexps(const std::vector<String>& str_patterns,
                         [[maybe_unused]] std::optional<UInt32> edit_distance) {
    /// Common pointers
    std::vector<const char*> patterns;
    std::vector<unsigned int> flags;

    /// Pointer for external edit distance compilation
    std::vector<hs_expr_ext> ext_exprs;
    std::vector<const hs_expr_ext*> ext_exprs_ptrs;

    patterns.reserve(str_patterns.size());
    flags.reserve(str_patterns.size());

    if constexpr (WithEditDistance) {
        ext_exprs.reserve(str_patterns.size());
        ext_exprs_ptrs.reserve(str_patterns.size());
    }

    for (const auto& ref : str_patterns) {
        patterns.push_back(ref.data());
        /* Flags below are the pattern matching flags.
         * HS_FLAG_DOTALL is a compile flag where matching a . will not exclude newlines. This is a good
         * performance practice according to Hyperscan API. https://intel.github.io/hyperscan/dev-reference/performance.html#dot-all-mode
         * HS_FLAG_ALLOWEMPTY is a compile flag where empty strings are allowed to match.
         * HS_FLAG_UTF8 is a flag where UTF8 literals are matched.
         * HS_FLAG_SINGLEMATCH is a compile flag where each pattern match will be returned only once. it is a good performance practice
         * as it is said in the Hyperscan documentation. https://intel.github.io/hyperscan/dev-reference/performance.html#single-match-flag
         */
        flags.push_back(HS_FLAG_DOTALL | HS_FLAG_SINGLEMATCH | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8);
        if constexpr (WithEditDistance) {
            /// Hyperscan currently does not support UTF8 matching with edit distance.
            flags.back() &= ~HS_FLAG_UTF8;
            ext_exprs.emplace_back();
            /// HS_EXT_FLAG_EDIT_DISTANCE is a compile flag responsible for Levenstein distance.
            ext_exprs.back().flags = HS_EXT_FLAG_EDIT_DISTANCE;
            ext_exprs.back().edit_distance = edit_distance.value();
            ext_exprs_ptrs.push_back(&ext_exprs.back());
        }
    }
    hs_database_t* db = nullptr;
    hs_compile_error_t* compile_error = nullptr;

    std::unique_ptr<unsigned int[]> ids;

    /// We mark the patterns to provide the callback results.
    if constexpr (save_indices) {
        ids.reset(new unsigned int[patterns.size()]);
        for (size_t i = 0; i < patterns.size(); ++i) {
            ids[i] = static_cast<unsigned>(i + 1);
        }
    }

    for (auto& pattern : patterns) {
        LOG(INFO) << "pattern: " << pattern << "\n";
    }

    hs_error_t err;
    if constexpr (!WithEditDistance) {
        err = hs_compile_multi(patterns.data(), flags.data(), ids.get(),
                               static_cast<unsigned>(patterns.size()), HS_MODE_BLOCK, nullptr, &db,
                               &compile_error);
    } else {
        err = hs_compile_ext_multi(patterns.data(), flags.data(), ids.get(), ext_exprs_ptrs.data(),
                                   static_cast<unsigned>(patterns.size()), HS_MODE_BLOCK, nullptr,
                                   &db, &compile_error);
    }

    if (err != HS_SUCCESS) [[unlikely]] {
        /// CompilerError is a unique_ptr, so correct memory free after the exception is thrown.
        CompilerError error(compile_error);

        if (error->expression < 0) { // error has nothing to do with the patterns themselves
            throw doris::Exception(Status::InternalError("Compile regexp expression failed. got {}",
                                                         error->message));
        } else {
            throw doris::Exception(Status::InvalidArgument(
                    "Compile regexp expression failed. got {}. some expressions may be illegal",
                    error->message));
        }
    }

    /// We allocate the scratch space only once, then copy it across multiple threads with hs_clone_scratch
    /// function which is faster than allocating scratch space each time in each thread.
    hs_scratch_t* scratch = nullptr;
    err = hs_alloc_scratch(db, &scratch);

    if (err != HS_SUCCESS) [[unlikely]] {
        if (err == HS_NOMEM) [[unlikely]] {
            throw doris::Exception(Status::MemoryAllocFailed(
                    "Allocating memory failed on compiling regexp expressions."));
        } else {
            throw doris::Exception(Status::InvalidArgument(
                    "Compile regexp expression failed with unexpected arguments perhaps"));
        }
    }

    return {db, scratch};
}

/// Maps string pattern vectors + edit distance to compiled vectorscan regexps. Uses the same eviction mechanism as the LocalCacheTable for
/// re2 patterns. Because vectorscan regexes are overall more heavy-weight (more expensive compilation, regexes can grow up to multiple
/// MBs, usage of scratch space), 1. GlobalCacheTable is a global singleton and, as a result, needs locking 2. the pattern compilation is
/// done outside GlobalCacheTable's lock, at the cost of another level of locking.
struct GlobalCacheTable {
    constexpr static size_t CACHE_SIZE = 500; /// collision probability

    struct Bucket {
        std::vector<String> patterns;        /// key
        std::optional<UInt32> edit_distance; /// key
        /// The compiled patterns and their state (vectorscan 'database' + scratch space) are wrapped in a shared_ptr. Refcounting guarantees
        /// that eviction of a pattern does not affect parallel threads still using the pattern.
        DeferredConstructedRegexpsPtr regexps; /// value
    };

    std::mutex mutex;
    std::array<Bucket, CACHE_SIZE> known_regexps;

    static size_t getBucketIndexFor(const std::vector<String> patterns,
                                    std::optional<UInt32> edit_distance) {
        size_t hash = 0;
        for (const auto& pattern : patterns) {
            boost::hash_combine(hash, pattern);
        }
        boost::hash_combine(hash, edit_distance);
        return hash % CACHE_SIZE;
    }
};

/// If WithEditDistance is False, edit_distance must be nullopt. Also, we use templates here because each instantiation of function template
/// has its own copy of local static variables which must not be the same for different hyperscan compilations.
template <bool save_indices, bool WithEditDistance>
DeferredConstructedRegexpsPtr getOrSet(const std::vector<StringRef>& patterns,
                                       std::optional<UInt32> edit_distance) {
    static GlobalCacheTable
            pool; /// Different variables for different pattern parameters, thread-safe in C++11

    std::vector<String> str_patterns;
    str_patterns.reserve(patterns.size());
    for (const auto& pattern : patterns) {
        str_patterns.emplace_back(pattern.to_string());
    }

    size_t bucket_idx = GlobalCacheTable::getBucketIndexFor(str_patterns, edit_distance);

    /// Lock cache to find compiled regexp for given pattern vector + edit distance.
    std::lock_guard lock(pool.mutex);

    GlobalCacheTable::Bucket& bucket = pool.known_regexps[bucket_idx];

    /// Pattern compilation is expensive and we don't want to block other threads reading from / inserting into the cache while we hold the
    /// cache lock during pattern compilation. Therefore, when a cache entry is created or replaced, only set the regexp constructor method
    /// and compile outside the cache lock.
    /// Note that the string patterns and the edit distance is passed into the constructor lambda by value, i.e. copied - it is not an
    /// option to reference the corresponding string patterns / edit distance key in the cache table bucket because the cache entry may
    /// already be evicted at the time the compilation starts.

    if (bucket.regexps == nullptr) {
        /// insert new entry
        auto deferred_constructed_regexps =
                std::make_shared<DeferredConstructedRegexps>([str_patterns, edit_distance]() {
                    return constructRegexps<save_indices, WithEditDistance>(str_patterns,
                                                                            edit_distance);
                });
        bucket = {std::move(str_patterns), edit_distance, deferred_constructed_regexps};
    } else if (bucket.patterns != str_patterns || bucket.edit_distance != edit_distance) {
        /// replace existing entry
        auto deferred_constructed_regexps =
                std::make_shared<DeferredConstructedRegexps>([str_patterns, edit_distance]() {
                    return constructRegexps<save_indices, WithEditDistance>(str_patterns,
                                                                            edit_distance);
                });
        bucket = {std::move(str_patterns), edit_distance, deferred_constructed_regexps};
    }

    return bucket.regexps;
}

} // namespace doris::vectorized::multiregexps
