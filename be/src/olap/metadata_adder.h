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

#include <bvar/bvar.h>
#include <stdint.h>

#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/runtime_profile.h"

namespace doris {

inline bvar::Adder<int64_t> g_rowset_meta_mem_bytes("doris_rowset_meta_mem_bytes");
inline bvar::Adder<int64_t> g_rowset_meta_num("doris_rowset_meta_num");

inline bvar::Adder<int64_t> g_rowset_mem_bytes("doris_rowset_mem_bytes");
inline bvar::Adder<int64_t> g_rowset_num("doris_rowset_num");

inline bvar::Adder<int64_t> g_tablet_meta_mem_bytes("doris_tablet_meta_mem_bytes");
inline bvar::Adder<int64_t> g_tablet_meta_num("doris_tablet_meta_num");

inline bvar::Adder<int64_t> g_tablet_column_mem_bytes("doris_tablet_column_mem_bytes");
inline bvar::Adder<int64_t> g_tablet_column_num("doris_tablet_column_num");

inline bvar::Adder<int64_t> g_tablet_index_mem_bytes("doris_tablet_index_mem_bytes");
inline bvar::Adder<int64_t> g_tablet_index_num("doris_tablet_index_num");

inline bvar::Adder<int64_t> g_tablet_schema_mem_bytes("doris_tablet_schema_mem_bytes");
inline bvar::Adder<int64_t> g_tablet_schema_num("doris_tablet_schema_num");

inline bvar::Adder<int64_t> g_segment_mem_bytes("doris_segment_mem_bytes");
inline bvar::Adder<int64_t> g_segment_num("doris_segment_num");
inline bvar::Adder<int64_t> g_segment_estimate_mem_bytes("doris_segment_estimate_mem_bytes");

inline bvar::Adder<int64_t> g_column_reader_mem_bytes("doris_column_reader_mem_bytes");
inline bvar::Adder<int64_t> g_column_reader_num("doris_column_reader_num");

inline bvar::Adder<int64_t> g_bitmap_index_reader_mem_bytes("doris_bitmap_index_reader_mem_bytes");
inline bvar::Adder<int64_t> g_bitmap_index_reader_num("doris_bitmap_index_reader_num");

inline bvar::Adder<int64_t> g_bloom_filter_index_reader_mem_bytes(
        "doris_bloom_filter_index_reader_mem_bytes");
inline bvar::Adder<int64_t> g_bloom_filter_index_reader_num("doris_bloom_filter_index_reader_num");

inline bvar::Adder<int64_t> g_index_page_reader_mem_bytes("doris_index_page_reader_mem_bytes");
inline bvar::Adder<int64_t> g_index_page_reader_num("doris_index_page_reader_num");

inline bvar::Adder<int64_t> g_indexed_column_reader_mem_bytes(
        "doris_indexed_column_reader_mem_bytes");
inline bvar::Adder<int64_t> g_indexed_column_reader_num("doris_indexed_column_reader_num");

inline bvar::Adder<int64_t> g_inverted_index_reader_mem_bytes(
        "doris_inverted_index_reader_mem_bytes");
inline bvar::Adder<int64_t> g_inverted_index_reader_num("doris_inverted_index_reader_num");

inline bvar::Adder<int64_t> g_ordinal_index_reader_mem_bytes(
        "doris_ordinal_index_reader_mem_bytes");
inline bvar::Adder<int64_t> g_ordinal_index_reader_num("doris_ordinal_index_reader_num");

inline bvar::Adder<int64_t> g_zone_map_index_reader_mem_bytes(
        "doris_zone_map_index_reader_mem_bytes");
inline bvar::Adder<int64_t> g_zone_map_index_reader_num("doris_zone_map_index_reader_num");

class RowsetMeta;
class TabletMeta;
class TabletColumn;
class TabletIndex;
class TabletSchema;

namespace segment_v2 {
class Segment;
class ColumnReader;
class BitmapIndexReader;
class BloomFilterIndexReader;
class IndexPageReader;
class IndexedColumnReader;
class InvertedIndexReader;
class OrdinalIndexReader;
class ZoneMapIndexReader;
}; // namespace segment_v2

/*
    When a derived Class extends MetadataAdder, then the Class's number and fixed length field's memory can be counted automatically.
    But if the Class has variable length field, then you should overwrite get_metadata_size and call update_metadata_size when the Class's memory changes.

    get_metadata_size is only the memory of the metadata object itself, not include child objects,
    for example, TabletMeta::get_metadata_size does not include the memory of TabletSchema.
    Note, the memory allocated by Doris Allocator is not included.

    There are some special situations that need to be noted:
    1. when the derived Class override copy constructor, you'd better update memory size(call update_metadata_size) if derived class's 
    memory changed in its copy constructor or you not call MetadataAdder's copy constructor.
    2. when the derived Class override operator=, you'd better update memory size(call update_metadata_size) if the derived Class has variable length field;

    Anyway, you should update mem size whenever derived Class's memory changes.
*/

template <typename T>
class MetadataAdder {
public:
    MetadataAdder();

    static void dump_metadata_object(RuntimeProfile* object_heap_dump_snapshot);

    static int64_t get_all_tablets_size() {
        return g_tablet_meta_mem_bytes.get_value() + g_tablet_column_mem_bytes.get_value() +
               g_tablet_index_mem_bytes.get_value() + g_tablet_schema_mem_bytes.get_value();
    }

    static int64_t get_all_rowsets_size() {
        return g_rowset_meta_mem_bytes.get_value() + g_rowset_mem_bytes.get_value();
    }

    static int64_t get_all_segments_size() {
        return g_segment_mem_bytes.get_value() + g_column_reader_mem_bytes.get_value() +
               g_bitmap_index_reader_mem_bytes.get_value() +
               g_bloom_filter_index_reader_mem_bytes.get_value() +
               g_index_page_reader_mem_bytes.get_value() +
               g_indexed_column_reader_mem_bytes.get_value() +
               g_inverted_index_reader_mem_bytes.get_value() +
               g_ordinal_index_reader_mem_bytes.get_value() +
               g_zone_map_index_reader_mem_bytes.get_value();
    }

    // Doris currently uses the estimated segments memory as the basis, maybe it is more realistic.
    static int64_t get_all_segments_estimate_size() {
        return g_segment_estimate_mem_bytes.get_value();
    }

protected:
    MetadataAdder(const MetadataAdder& other);

    virtual ~MetadataAdder();

    virtual int64_t get_metadata_size() const { return sizeof(T); }

    void update_metadata_size();

    MetadataAdder<T>& operator=(const MetadataAdder<T>& other) = default;

    int64_t _current_meta_size {0};

    void add_mem_size(int64_t val);

    void add_num(int64_t val);
};

template <typename T>
MetadataAdder<T>::MetadataAdder(const MetadataAdder<T>& other) {
    this->_current_meta_size = other._current_meta_size;
    add_num(1);
    add_mem_size(this->_current_meta_size);
}

template <typename T>
MetadataAdder<T>::MetadataAdder() {
    this->_current_meta_size = sizeof(T);
    add_mem_size(this->_current_meta_size);
    add_num(1);
}

template <typename T>
MetadataAdder<T>::~MetadataAdder() {
    add_mem_size(-_current_meta_size);
    add_num(-1);
}

template <typename T>
void MetadataAdder<T>::update_metadata_size() {
    int64_t old_size = _current_meta_size;
    _current_meta_size = get_metadata_size();
    int64_t size_diff = _current_meta_size - old_size;

    add_mem_size(size_diff);
}

template <typename T>
void MetadataAdder<T>::add_mem_size(int64_t val) {
    if (val == 0) {
        return;
    }
    if constexpr (std::is_same_v<T, RowsetMeta>) {
        g_rowset_meta_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, Rowset>) {
        g_rowset_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, TabletMeta>) {
        g_tablet_meta_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, TabletColumn>) {
        g_tablet_column_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, TabletIndex>) {
        g_tablet_index_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, TabletSchema>) {
        g_tablet_schema_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, segment_v2::Segment>) {
        g_segment_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, segment_v2::ColumnReader>) {
        g_column_reader_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, segment_v2::BitmapIndexReader>) {
        g_bitmap_index_reader_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, segment_v2::BloomFilterIndexReader>) {
        g_bloom_filter_index_reader_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, segment_v2::IndexPageReader>) {
        g_index_page_reader_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, segment_v2::IndexedColumnReader>) {
        g_indexed_column_reader_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, segment_v2::InvertedIndexReader>) {
        g_inverted_index_reader_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, segment_v2::OrdinalIndexReader>) {
        g_ordinal_index_reader_mem_bytes << val;
    } else if constexpr (std::is_same_v<T, segment_v2::ZoneMapIndexReader>) {
        g_zone_map_index_reader_mem_bytes << val;
    } else {
        LOG(FATAL) << "add_mem_size not match class type: " << typeid(T).name() << ", " << val;
        __builtin_unreachable();
    }
}

template <typename T>
void MetadataAdder<T>::add_num(int64_t val) {
    if (val == 0) {
        return;
    }
    if constexpr (std::is_same_v<T, RowsetMeta>) {
        g_rowset_meta_num << val;
    } else if constexpr (std::is_same_v<T, Rowset>) {
        g_rowset_num << val;
    } else if constexpr (std::is_same_v<T, TabletMeta>) {
        g_tablet_meta_num << val;
    } else if constexpr (std::is_same_v<T, TabletColumn>) {
        g_tablet_column_num << val;
    } else if constexpr (std::is_same_v<T, TabletIndex>) {
        g_tablet_index_num << val;
    } else if constexpr (std::is_same_v<T, TabletSchema>) {
        g_tablet_schema_num << val;
    } else if constexpr (std::is_same_v<T, segment_v2::Segment>) {
        g_segment_num << val;
    } else if constexpr (std::is_same_v<T, segment_v2::ColumnReader>) {
        g_column_reader_num << val;
    } else if constexpr (std::is_same_v<T, segment_v2::BitmapIndexReader>) {
        g_bitmap_index_reader_num << val;
    } else if constexpr (std::is_same_v<T, segment_v2::BloomFilterIndexReader>) {
        g_bloom_filter_index_reader_num << val;
    } else if constexpr (std::is_same_v<T, segment_v2::IndexPageReader>) {
        g_index_page_reader_num << val;
    } else if constexpr (std::is_same_v<T, segment_v2::IndexedColumnReader>) {
        g_indexed_column_reader_num << val;
    } else if constexpr (std::is_same_v<T, segment_v2::InvertedIndexReader>) {
        g_inverted_index_reader_num << val;
    } else if constexpr (std::is_same_v<T, segment_v2::OrdinalIndexReader>) {
        g_ordinal_index_reader_num << val;
    } else if constexpr (std::is_same_v<T, segment_v2::ZoneMapIndexReader>) {
        g_zone_map_index_reader_num << val;
    } else {
        LOG(FATAL) << "add_num not match class type: " << typeid(T).name() << ", " << val;
        __builtin_unreachable();
    }
}

template <typename T>
void MetadataAdder<T>::dump_metadata_object(RuntimeProfile* object_heap_dump_snapshot) {
    RuntimeProfile::Counter* rowset_meta_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "RowsetMetaMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* rowset_meta_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "RowsetMetaNum", TUnit::UNIT);
    COUNTER_SET(rowset_meta_mem_bytes_counter, g_rowset_meta_mem_bytes.get_value());
    COUNTER_SET(rowset_meta_num_counter, g_rowset_meta_num.get_value());

    RuntimeProfile::Counter* rowset_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "RowsetMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* rowset_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "RowsetNum", TUnit::UNIT);
    COUNTER_SET(rowset_mem_bytes_counter, g_rowset_mem_bytes.get_value());
    COUNTER_SET(rowset_num_counter, g_rowset_num.get_value());

    RuntimeProfile::Counter* tablet_meta_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "TabletMetaMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* tablet_meta_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "TabletMetaNum", TUnit::UNIT);
    COUNTER_SET(tablet_meta_mem_bytes_counter, g_tablet_meta_mem_bytes.get_value());
    COUNTER_SET(tablet_meta_num_counter, g_tablet_meta_num.get_value());

    RuntimeProfile::Counter* tablet_column_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "TabletColumnMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* tablet_column_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "TabletColumnNum", TUnit::UNIT);
    COUNTER_SET(tablet_column_mem_bytes_counter, g_tablet_column_mem_bytes.get_value());
    COUNTER_SET(tablet_column_num_counter, g_tablet_column_num.get_value());

    RuntimeProfile::Counter* tablet_index_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "TabletIndexMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* tablet_index_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "TabletIndexNum", TUnit::UNIT);
    COUNTER_SET(tablet_index_mem_bytes_counter, g_tablet_index_mem_bytes.get_value());
    COUNTER_SET(tablet_index_num_counter, g_tablet_index_num.get_value());

    RuntimeProfile::Counter* tablet_schema_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "TabletSchemaMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* tablet_schema_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "TabletSchemaNum", TUnit::UNIT);
    COUNTER_SET(tablet_schema_mem_bytes_counter, g_tablet_schema_mem_bytes.get_value());
    COUNTER_SET(tablet_schema_num_counter, g_tablet_schema_num.get_value());

    RuntimeProfile::Counter* segment_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "SegmentMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* segment_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "SegmentNum", TUnit::UNIT);
    COUNTER_SET(segment_mem_bytes_counter, g_segment_mem_bytes.get_value());
    COUNTER_SET(segment_num_counter, g_segment_num.get_value());

    RuntimeProfile::Counter* column_reader_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "ColumnReaderMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* column_reader_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "ColumnReaderNum", TUnit::UNIT);
    COUNTER_SET(column_reader_mem_bytes_counter, g_column_reader_mem_bytes.get_value());
    COUNTER_SET(column_reader_num_counter, g_column_reader_num.get_value());

    RuntimeProfile::Counter* bitmap_index_reader_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "BitmapIndexReaderMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* bitmap_index_reader_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "BitmapIndexReaderNum", TUnit::UNIT);
    COUNTER_SET(bitmap_index_reader_mem_bytes_counter, g_bitmap_index_reader_mem_bytes.get_value());
    COUNTER_SET(bitmap_index_reader_num_counter, g_bitmap_index_reader_num.get_value());

    RuntimeProfile::Counter* bloom_filter_index_reader_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "BloomFilterIndexReaderMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* filter_index_reader_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "BloomFilterIndexReaderNum", TUnit::UNIT);
    COUNTER_SET(bloom_filter_index_reader_mem_bytes_counter,
                g_bloom_filter_index_reader_mem_bytes.get_value());
    COUNTER_SET(filter_index_reader_num_counter, g_bloom_filter_index_reader_num.get_value());

    RuntimeProfile::Counter* index_page_reader_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "IndexPageReaderMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* index_page_reader_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "IndexPageReaderNum", TUnit::UNIT);
    COUNTER_SET(index_page_reader_mem_bytes_counter, g_index_page_reader_mem_bytes.get_value());
    COUNTER_SET(index_page_reader_num_counter, g_index_page_reader_num.get_value());

    RuntimeProfile::Counter* indexed_column_reader_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "IndexedColumnReaderMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* indexed_column_reader_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "IndexedColumnReaderNum", TUnit::UNIT);
    COUNTER_SET(indexed_column_reader_mem_bytes_counter,
                g_indexed_column_reader_mem_bytes.get_value());
    COUNTER_SET(indexed_column_reader_num_counter, g_indexed_column_reader_num.get_value());

    RuntimeProfile::Counter* inverted_index_reader_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "InvertedIndexReaderMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* inverted_index_reader_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "InvertedIndexReaderNum", TUnit::UNIT);
    COUNTER_SET(inverted_index_reader_mem_bytes_counter,
                g_inverted_index_reader_mem_bytes.get_value());
    COUNTER_SET(inverted_index_reader_num_counter, g_inverted_index_reader_num.get_value());

    RuntimeProfile::Counter* ordinal_index_reader_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "OrdinalIndexReaderMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* ordinal_index_reader_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "OrdinalIndexReaderNum", TUnit::UNIT);
    COUNTER_SET(ordinal_index_reader_mem_bytes_counter,
                g_ordinal_index_reader_mem_bytes.get_value());
    COUNTER_SET(ordinal_index_reader_num_counter, g_ordinal_index_reader_num.get_value());

    RuntimeProfile::Counter* zone_map_index_reader_mem_bytes_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "ZoneMapIndexReaderMemBytes", TUnit::BYTES);
    RuntimeProfile::Counter* zone_map_index_reader_num_counter =
            ADD_COUNTER(object_heap_dump_snapshot, "ZoneMapIndexReaderNum", TUnit::UNIT);
    COUNTER_SET(zone_map_index_reader_mem_bytes_counter,
                g_zone_map_index_reader_mem_bytes.get_value());
    COUNTER_SET(zone_map_index_reader_num_counter, g_zone_map_index_reader_num.get_value());
}

}; // namespace doris
