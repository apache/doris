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

namespace doris {

inline bvar::Adder<int64_t> g_rowset_meta_mem_bytes("doris_rowset_meta_mem_bytes");
inline bvar::Adder<int64_t> g_rowset_meta_num("doris_rowset_meta_num");

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

protected:
    MetadataAdder(const MetadataAdder& other);

    virtual ~MetadataAdder();

    virtual int64_t get_metadata_size() const { return sizeof(T); }

    void update_metadata_size();

    MetadataAdder<T>& operator=(const MetadataAdder<T>& other) = default;

private:
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
    }
}

template <typename T>
void MetadataAdder<T>::add_num(int64_t val) {
    if (val == 0) {
        return;
    }
    if constexpr (std::is_same_v<T, RowsetMeta>) {
        g_rowset_meta_num << val;
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
    }
}

}; // namespace doris