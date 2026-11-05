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

#include "format_v2/parquet/reader/decoded_column_cache.h"

#include <cstring>
#include <sstream>
#include <utility>

#include "common/check.h"
#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "io/file_factory.h"
#include "runtime/thread_context.h"
#include "storage/cache/page_cache.h"
#include "util/defer_op.h"
#include "util/sha.h"

namespace doris::format::parquet {
namespace {

size_t value_width(const DataTypePtr& type) {
    switch (remove_nullable(type)->get_primitive_type()) {
    case TYPE_TINYINT:
        return 1;
    case TYPE_SMALLINT:
        return 2;
    case TYPE_INT:
    case TYPE_FLOAT:
        return 4;
    case TYPE_BIGINT:
    case TYPE_DOUBLE:
        return 8;
    default:
        return 0;
    }
}

void append_key_part(std::string* key, std::string_view part) {
    const uint64_t length = part.size();
    key->append(reinterpret_cast<const char*>(&length), sizeof(length));
    key->append(part);
}

void add_counter(RuntimeProfile::Counter* counter, int64_t value = 1) {
    if (counter != nullptr) {
        COUNTER_UPDATE(counter, value);
    }
}

class DecodedCacheColumnReader final : public ParquetColumnReader {
public:
    DecodedCacheColumnReader(DecodedColumnCache* cache, std::string key,
                             const ParquetColumnSchema& schema, DataTypePtr output_type,
                             ParquetColumnReaderProfile profile, DecodedColumnCache::Handle hit,
                             std::unique_ptr<DecodedColumnCache::Entry> fill,
                             std::unique_ptr<ParquetColumnReader> native)
            : ParquetColumnReader(schema, std::move(output_type), std::move(profile)),
              _cache(cache),
              _key(std::move(key)),
              _hit(std::move(hit)),
              _fill(std::move(fill)),
              _native(std::move(native)) {}

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override {
        DORIS_CHECK(rows >= 0);
        if (_hit) {
            DORIS_CHECK(static_cast<size_t>(rows) <= _hit->rows - _cursor);
            {
                SCOPED_TIMER(_profile.decoded_cache_read_time);
                _hit->append_to(*column, _cursor, rows);
            }
            _cursor += rows;
            *rows_read = rows;
            update_reader_read_rows(rows);
            add_counter(_profile.decoded_cache_read_bytes, rows * (_hit->width + _hit->nullable));
            return Status::OK();
        }
        const size_t start = column->size();
        auto status = _native->read(rows, column, rows_read);
        if (!status.ok()) {
            _fill.reset();
            return status;
        }
        if (_fill) {
            DORIS_CHECK(column->size() == start + *rows_read);
            DORIS_CHECK(static_cast<size_t>(*rows_read) <= _fill->rows - _fill->filled_rows);
            {
                SCOPED_TIMER(_profile.decoded_cache_fill_time);
                _fill->append_from(*column, start, *rows_read);
            }
            if (_fill->filled_rows == _fill->rows) {
                add_counter(_profile.decoded_cache_writes);
                _cache->publish(_key, std::move(_fill));
            }
        }
        return Status::OK();
    }

    Status skip(int64_t rows) override {
        DORIS_CHECK(rows >= 0);
        if (_hit) {
            DORIS_CHECK(static_cast<size_t>(rows) <= _hit->rows - _cursor);
            _cursor += rows;
            update_reader_skip_rows(rows);
            return Status::OK();
        }
        // Even a one-row gap means the eventual column is incomplete. For example, a LIMIT or
        // page-index gap must never publish its materialized suffix as an entire row group.
        if (rows != 0) {
            _fill.reset();
        }
        return _native->skip(rows);
    }

    Status select(const SelectionVector& selection, uint16_t selected_rows, int64_t batch_rows,
                  MutableColumnPtr& column) override {
        if (_hit) {
            return ParquetColumnReader::select(selection, selected_rows, batch_rows, column);
        }
        _fill.reset();
        return _native->select(selection, selected_rows, batch_rows, column);
    }

    void flush_profile() override {
        if (_native) {
            _native->flush_profile();
        }
    }
    bool crossed_page_since_last_batch() override {
        return _native && _native->crossed_page_since_last_batch();
    }

private:
    DecodedColumnCache* _cache;
    std::string _key;
    DecodedColumnCache::Handle _hit;
    std::unique_ptr<DecodedColumnCache::Entry> _fill;
    std::unique_ptr<ParquetColumnReader> _native;
    size_t _cursor = 0;
};

} // namespace

DecodedColumnCache* DecodedColumnCache::_instance = nullptr;

DecodedColumnCache::DecodedColumnCache(size_t capacity, size_t max_entry_bytes,
                                       size_t max_file_admission_bytes)
        : LRUCachePolicy(CachePolicy::CacheType::PARQUET_DECODED_COLUMN_CACHE, capacity,
                         LRUCacheType::SIZE, 3600, 1, 0, true, false),
          _capacity_bytes(capacity),
          _max_entry_bytes(max_entry_bytes),
          _max_file_admission_bytes(max_file_admission_bytes) {}

DecodedColumnCache* DecodedColumnCache::instance() {
    return _instance;
}

Status DecodedColumnCache::create_global_cache() {
    DORIS_CHECK(_instance == nullptr);
    if (config::parquet_decoded_column_cache_capacity_bytes < 0 ||
        config::parquet_decoded_column_cache_max_entry_bytes <= 0 ||
        config::parquet_decoded_column_cache_max_file_admission_bytes <= 0) {
        return Status::InvalidArgument("Invalid Parquet decoded-column cache byte limits");
    }
    if (config::parquet_decoded_column_cache_capacity_bytes != 0) {
        _instance = new DecodedColumnCache(
                config::parquet_decoded_column_cache_capacity_bytes,
                config::parquet_decoded_column_cache_max_entry_bytes,
                config::parquet_decoded_column_cache_max_file_admission_bytes);
    }
    return Status::OK();
}

void DecodedColumnCache::destroy_global_cache() {
    delete _instance;
    _instance = nullptr;
}

std::string DecodedColumnCache::file_key(const io::FileSystemProperties& source,
                                         const io::FileDescription& file, size_t file_size) {
    // A seconds-resolution mtime alone cannot distinguish same-size in-place overwrites.
    // Immutable table data files have an explicit no-overwrite contract; generic files do not.
    if (!file.is_immutable) {
        return {};
    }
    std::string key;
    append_key_part(&key, std::to_string(source.system_type));
    append_key_part(&key, file.fs_name);
    append_key_part(&key, file.path);
    append_key_part(&key, std::to_string(file_size));
    append_key_part(&key, std::to_string(file.mtime));
    for (const auto& [name, value] : source.properties) {
        append_key_part(&key, name);
        append_key_part(&key, value);
    }
    std::ostringstream thrift_properties;
    source.hdfs_params.printTo(thrift_properties);
    for (const auto& address : source.broker_addresses) {
        address.printTo(thrift_properties);
    }
    append_key_part(&key, thrift_properties.str());
    SHA256Digest digest;
    digest.reset(key.data(), key.size());
    return std::string(digest.digest());
}

std::string DecodedColumnCache::column_key(std::string_view file_key,
                                           const ParquetColumnSchema& schema, int row_group,
                                           bool strict_mode) {
    if (file_key.empty() || schema.kind != ParquetColumnSchemaKind::PRIMITIVE ||
        schema.max_repetition_level != 0 || value_width(schema.type) == 0) {
        return {};
    }
    std::string key;
    append_key_part(&key, file_key);
    append_key_part(&key, std::to_string(row_group));
    append_key_part(&key, std::to_string(schema.leaf_column_id));
    append_key_part(&key, std::to_string(schema.parquet_field_id));
    append_key_part(&key, schema.type->get_name());
    // ParquetReader::_fill_column_definition exposes every file field as Nullable, including
    // physical REQUIRED fields. The cached representation must include that actual null map.
    append_key_part(&key, make_nullable(schema.type)->get_name());
    key.push_back(strict_mode);
    return key;
}

DecodedColumnCache::Entry::Entry(size_t row_count, size_t value_size, bool has_nulls,
                                 size_t entry_charge, std::shared_ptr<Budget> entry_budget,
                                 std::shared_ptr<MemTrackerLimiter> tracker)
        : rows(row_count),
          width(value_size),
          nullable(has_nulls),
          charge(entry_charge),
          budget(std::move(entry_budget)),
          page(std::make_unique<DataPage>(rows * (width + nullable), std::move(tracker))) {}

DecodedColumnCache::Entry::~Entry() {
    page.reset();
    budget->bytes.fetch_sub(charge, std::memory_order_relaxed);
}

void DecodedColumnCache::Entry::append_from(const IColumn& column, size_t start, size_t count) {
    DORIS_CHECK(count <= rows - filled_rows);
    DORIS_CHECK(start <= column.size() && count <= column.size() - start);
    const size_t destination = filled_rows;
    const IColumn* data = &column;
    if (nullable) {
        const auto& null_column = assert_cast<const ColumnNullable&>(column);
        memcpy(page->data() + rows * width + destination,
               null_column.get_null_map_data().data() + start, count);
        data = &null_column.get_nested_column();
    }
    memcpy(page->data() + destination * width, data->get_raw_data().data + start * width,
           count * width);
    filled_rows += count;
}

void DecodedColumnCache::Entry::append_to(IColumn& column, size_t start, size_t count) const {
    IColumn* data = &column;
    if (nullable) {
        auto& null_column = assert_cast<ColumnNullable&>(column);
        auto& null_map = null_column.get_null_map_data();
        null_map.insert(page->data() + rows * width + start,
                        page->data() + rows * width + start + count);
        data = &null_column.get_nested_column();
    }
    data->insert_many_raw_data(page->data() + start * width, count);
}

DecodedColumnCache::Handle DecodedColumnCache::lookup_column(const std::string& key) {
    auto* handle = lookup(key);
    if (handle == nullptr) {
        return {};
    }
    // Holding the cache itself keeps pinned values valid even if a local cache owner is destroyed.
    return {static_cast<const Entry*>(value(handle)),
            [cache = _cache, handle](const Entry*) { cache->release(handle); }};
}

bool DecodedColumnCache::reserve(size_t charge) {
    std::lock_guard lock(_admission_mutex);
    size_t used = live_bytes();
    if (charge > _capacity_bytes) {
        return false;
    }
    if (used > _capacity_bytes - charge) {
        size_t remaining = used - (_capacity_bytes - charge);
        // Only unpinned LRU entries are visited. Stop once enough old entries have been chosen;
        // active readers and concurrent fills keep their reservations and cannot be evicted here.
        _cache->prune_if(
                [&remaining](const LRUHandle* handle) {
                    if (remaining == 0) {
                        return false;
                    }
                    const auto* entry = static_cast<const Entry*>(handle->value);
                    remaining -= std::min(remaining, entry->charge);
                    return true;
                },
                true);
        used = live_bytes();
        if (used > _capacity_bytes - charge) {
            return false;
        }
    }
    _budget->bytes.fetch_add(charge, std::memory_order_relaxed);
    return true;
}

std::unique_ptr<DecodedColumnCache::Entry> DecodedColumnCache::start_fill(
        const std::string& key, const DataTypePtr& type, size_t rows,
        size_t* file_admission_bytes) {
    const size_t width = value_width(type);
    const bool nullable = type->is_nullable();
    const size_t overhead = sizeof(Entry) + sizeof(DataPage) + sizeof(LRUHandle) + key.size();
    const size_t limit = std::min(_max_entry_bytes, _capacity_bytes);
    if (rows == 0 || width == 0 || overhead >= limit ||
        rows > (limit - overhead) / (width + nullable)) {
        return {};
    }
    const size_t charge = overhead + rows * (width + nullable);
    if (*file_admission_bytes > _max_file_admission_bytes ||
        charge > _max_file_admission_bytes - *file_admission_bytes) {
        return {};
    }
    // Check query headroom without taking ownership of its existing reservation. Cache bytes
    // are reserved and allocated under the same limiter; switching after reserve would hide that
    // reservation from DataPage and count a large payload twice against the process limit.
    auto* tracker = thread_context()->thread_mem_tracker_mgr.get();
    tracker->flush_untracked_mem();
    if (!tracker->limiter_mem_tracker()->check_limit(charge).ok()) {
        return {};
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker());
    if (!tracker->try_reserve(charge).ok()) {
        return {};
    }
    DEFER_RELEASE_RESERVED();
    if (!reserve(charge)) {
        return {};
    }
    size_t release_bytes = charge;
    Defer release_on_error {
            [&] { _budget->bytes.fetch_sub(release_bytes, std::memory_order_relaxed); }};
    auto entry = std::make_unique<Entry>(rows, width, nullable, charge, _budget, mem_tracker());
    release_bytes = 0;
    *file_admission_bytes += charge;
    return entry;
}

void DecodedColumnCache::publish(const std::string& key, std::unique_ptr<Entry> entry) {
    DORIS_CHECK(entry->filled_rows == entry->rows);
    const size_t value_bytes = entry->page->capacity() + sizeof(Entry) + sizeof(DataPage);
    auto* handle = insert(key, entry.get(), value_bytes, sizeof(Entry) + sizeof(DataPage));
    entry.release();
    release(handle);
}

DecodedColumnCache::ReadPlan DecodedColumnCache::prepare_reader(
        std::string key, const ParquetColumnSchema& schema, ParquetColumnReaderProfile profile) {
    if (key.empty()) {
        add_counter(profile.decoded_cache_bypasses);
        return {};
    }
    add_counter(profile.decoded_cache_requests);
    auto hit = lookup_column(key);
    add_counter(hit ? profile.decoded_cache_hits : profile.decoded_cache_misses);
    return {std::move(key), std::move(hit), make_nullable(schema.type)};
}

Status DecodedColumnCache::create_reader(ReadPlan plan, const ParquetColumnSchema& schema,
                                         size_t rows, bool allow_fill,
                                         ParquetColumnReaderProfile profile,
                                         size_t* file_admission_bytes, const ReaderFactory& factory,
                                         std::unique_ptr<ParquetColumnReader>* reader) {
    if (plan.key.empty()) {
        return factory(reader);
    }
    if (plan.hit) {
        *reader = std::make_unique<DecodedCacheColumnReader>(this, std::move(plan.key), schema,
                                                             std::move(plan.output_type), profile,
                                                             std::move(plan.hit), nullptr, nullptr);
        return Status::OK();
    }
    RETURN_IF_ERROR(factory(reader));
    auto fill = allow_fill ? start_fill(plan.key, plan.output_type, rows, file_admission_bytes)
                           : nullptr;
    if (!fill) {
        add_counter(profile.decoded_cache_bypasses);
        return Status::OK();
    }
    *reader = std::make_unique<DecodedCacheColumnReader>(
            this, std::move(plan.key), schema, std::move(plan.output_type), profile, nullptr,
            std::move(fill), std::move(*reader));
    return Status::OK();
}

} // namespace doris::format::parquet
