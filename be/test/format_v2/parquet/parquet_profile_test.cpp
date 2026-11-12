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

#include "format_v2/parquet/parquet_profile.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace doris::format::parquet {

TEST(ParquetProfileCacheTest, InitializesLazilyAndKeepsCounterIdentity) {
    RuntimeProfile profile("scan");
    ParquetProfileCache cache(&profile);
    EXPECT_EQ(profile.get_counter("ParquetReader"), nullptr);
    const auto& counters = cache.get();
    ASSERT_NE(counters.total_time, nullptr);
    EXPECT_EQ(counters.total_time, profile.get_counter("ParquetReader"));
    counters.total_time->update(17);
    EXPECT_EQ(&cache.get(), &counters);
    EXPECT_EQ(cache.get().total_time->value(), 17);
}

TEST(ParquetProfileCacheTest, ConcurrentReadersShareOneCounterSet) {
    RuntimeProfile profile("scan");
    ParquetProfileCache cache(&profile);
    std::array<const ParquetProfile*, 16> handles {};
    std::atomic<bool> start = false;
    std::vector<std::thread> readers;
    for (size_t i = 0; i < handles.size(); ++i) {
        readers.emplace_back([&, i] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            handles[i] = &cache.get();
            for (int file = 0; file < 100; ++file) {
                cache.get().open_file_num->update(1);
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& reader : readers) {
        reader.join();
    }
    for (const auto* handle : handles) {
        EXPECT_EQ(handle, handles[0]);
    }
    EXPECT_EQ(cache.get().open_file_num->value(), 1600);
}

TEST(ParquetProfileCacheTest, ChildCachesShareAcrossThreadsButKeepProfilesSeparate) {
    RuntimeProfile root("scan");
    RuntimeProfile* child = root.get_or_create_child("nested");
    RuntimeProfile* other = root.get_or_create_child("other");
    ParquetProfileCache cache(&root);
    std::array<ParquetProfileCache*, 16> children {};
    std::vector<std::thread> readers;
    for (size_t i = 0; i < children.size(); ++i) {
        readers.emplace_back([&, i] { children[i] = cache.get_child_cache(child); });
    }
    for (auto& reader : readers) {
        reader.join();
    }
    for (auto* handle : children) {
        EXPECT_EQ(handle, children[0]);
    }
    EXPECT_NE(cache.get_child_cache(other), children[0]);
    EXPECT_NE(cache.get().total_time, children[0]->get().total_time);
    EXPECT_EQ(child->get_counter("ParquetReader"), children[0]->get().total_time);
}

TEST(ParquetProfileCacheTest, CachedReadersDoNotAcquireCounterRegistrationLock) {
    RuntimeProfile profile("scan");
    ParquetProfileCache cache(&profile);
    const auto* counters = &cache.get();
    // BE tests compile with -fno-access-control, as do the existing RuntimeProfile tests.
    std::unique_lock registration_lock(profile._counter_map_lock);
    std::promise<const ParquetProfile*> result;
    auto future = result.get_future();
    std::thread reader([&] { result.set_value(&cache.get()); });
    const auto status = future.wait_for(std::chrono::seconds(5));
    registration_lock.unlock();
    reader.join();
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_EQ(future.get(), counters);
}

TEST(ParquetProfileCacheTest, ScanInstancesKeepIndependentCounters) {
    RuntimeProfile first("first");
    RuntimeProfile second("second");
    ParquetProfileCache first_cache(&first);
    ParquetProfileCache second_cache(&second);
    first_cache.get().raw_rows_read->update(3);
    EXPECT_NE(first_cache.get().raw_rows_read, second_cache.get().raw_rows_read);
    EXPECT_EQ(second_cache.get().raw_rows_read->value(), 0);
}

TEST(ParquetProfileCacheTest, NullProfileKeepsCountersDisabled) {
    ParquetProfileCache cache(nullptr);
    EXPECT_EQ(cache.get().total_time, nullptr);
    EXPECT_EQ(cache.get().variant_profile().variant_reconstruction_time, nullptr);
    EXPECT_EQ(&cache.get(), &cache.get());
}

TEST(ParquetProfileCacheTest, VariantCountersOutliveScanProfile) {
    ParquetProfile reader_counters;
    {
        RuntimeProfile profile("scan");
        ParquetProfileCache cache(&profile);
        reader_counters = cache.get();
    }
    // Deferred Variant materialization retains shared counters after the scanner is destroyed.
    ASSERT_NE(reader_counters.variant_profile().variant_reconstruction_time, nullptr);
    reader_counters.variant_profile().variant_reconstruction_time->update(23);
    EXPECT_EQ(reader_counters.variant_profile().variant_reconstruction_time->value(), 23);
    reader_counters.variant_profile().variant_direct_leaf_rows->update(7);
    EXPECT_EQ(reader_counters.variant_profile().variant_direct_leaf_rows->value(), 7);
}

TEST(ParquetProfileCacheTest, ColumnProfileCopiesRetainOneOwnerAfterScanDestruction) {
    ParquetColumnReaderProfile reader;
    std::weak_ptr<const ParquetVariantProfile> lifetime;
    {
        RuntimeProfile profile("scan");
        ParquetProfileCache cache(&profile);
        reader = cache.get().column_reader_profile();
        lifetime = reader.variant_counters;
        const auto& counter = reader.variant_profile().variant_unshredded_direct_import_rows;
        const auto references = counter.use_count();
        auto copy = reader;
        EXPECT_EQ(copy.variant_counters, reader.variant_counters);
        EXPECT_EQ(counter.use_count(), references);
        copy.variant_profile().variant_unshredded_direct_import_rows->update(9);
    }
    EXPECT_FALSE(lifetime.expired());
    EXPECT_EQ(reader.variant_profile().variant_unshredded_direct_import_rows->value(), 9);
    reader.variant_profile().variant_unshredded_direct_import_rows->update(3);
    EXPECT_EQ(reader.variant_profile().variant_unshredded_direct_import_rows->value(), 12);
    reader = {};
    EXPECT_TRUE(lifetime.expired());
}

TEST(ParquetProfileCacheTest, DefaultColumnProfileHasDisabledVariantCounters) {
    ParquetColumnReaderProfile reader;
    EXPECT_EQ(reader.variant_profile().variant_unshredded_direct_import_time, nullptr);
    EXPECT_EQ(reader.variant_profile().variant_reconstruction_time, nullptr);
}

} // namespace doris::format::parquet
