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

#include "olap/rowset/segment_v2/bitmap_index_writer.h"

#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <map>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "vec/common/arena.h"

namespace doris {
namespace segment_v2 {

namespace {

template <typename CppType>
struct BitmapIndexTraits {
    using MemoryIndexType = std::map<CppType, roaring::Roaring>;
};

template <>
struct BitmapIndexTraits<Slice> {
    using MemoryIndexType = std::map<Slice, roaring::Roaring, Slice::Comparator>;
};

// Builder for bitmap index. Bitmap index is comprised of two parts
// - an "ordered dictionary" which contains all distinct values of a column and maps each value to an id.
//   the smallest value mapped to 0, second value mapped to 1, ..
// - a posting list which stores one bitmap for each value in the dictionary. each bitmap is used to represent
//   the list of rowid where a particular value exists.
//
// E.g, if the column contains 10 rows ['x', 'x', 'x', 'b', 'b', 'b', 'x', 'b', 'b', 'b'],
// then the ordered dictionary would be ['b', 'x'] which maps 'b' to 0 and 'x' to 1,
// and the posting list would contain two bitmaps
//   bitmap for ID 0 : [0 0 0 1 1 1 0 1 1 1]
//   bitmap for ID 1 : [1 1 1 0 0 0 1 0 0 0]
//   the n-th bit is set to 1 if the n-th row equals to the corresponding value.
//
template <FieldType field_type>
class BitmapIndexWriterImpl : public BitmapIndexWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using MemoryIndexType = typename BitmapIndexTraits<CppType>::MemoryIndexType;

    explicit BitmapIndexWriterImpl(const TypeInfo* type_info)
            : _type_info(type_info), _reverted_index_size(0) {}

    ~BitmapIndexWriterImpl() override = default;

    void add_values(const void* values, size_t count) override {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            add_value(*p);
            p++;
        }
    }

    void add_value(const CppType& value) {
        auto it = _mem_index.find(value);
        uint64_t old_size = 0;
        if (it != _mem_index.end()) {
            // exiting value, update bitmap
            old_size = it->second.getSizeInBytes(false);
            it->second.add(_rid);
        } else {
            // new value, copy value and insert new key->bitmap pair
            CppType new_value;
            _type_info->deep_copy(&new_value, &value, &_arena);
            _mem_index.insert({new_value, roaring::Roaring::bitmapOf(1, _rid)});
            it = _mem_index.find(new_value);
        }
        _reverted_index_size += it->second.getSizeInBytes(false) - old_size;
        _rid++;
    }

    void add_nulls(uint32_t count) override {
        _null_bitmap.addRange(_rid, _rid + count);
        _rid += count;
    }

    Status finish(io::FileWriter* file_writer, ColumnIndexMetaPB* index_meta) override {
        index_meta->set_type(BITMAP_INDEX);
        BitmapIndexPB* meta = index_meta->mutable_bitmap_index();

        meta->set_bitmap_type(BitmapIndexPB::ROARING_BITMAP);
        meta->set_has_null(!_null_bitmap.isEmpty());

        { // write dictionary
            IndexedColumnWriterOptions options;
            options.write_ordinal_index = false;
            options.write_value_index = true;
            options.encoding = EncodingInfo::get_default_encoding(_type_info, true);
            options.compression = LZ4F;

            IndexedColumnWriter dict_column_writer(options, _type_info, file_writer);
            RETURN_IF_ERROR(dict_column_writer.init());
            for (auto const& it : _mem_index) {
                RETURN_IF_ERROR(dict_column_writer.add(&(it.first)));
            }
            RETURN_IF_ERROR(dict_column_writer.finish(meta->mutable_dict_column()));
        }
        { // write bitmaps
            std::vector<roaring::Roaring*> bitmaps;
            for (auto& it : _mem_index) {
                bitmaps.push_back(&(it.second));
            }
            if (!_null_bitmap.isEmpty()) {
                bitmaps.push_back(&_null_bitmap);
            }

            uint32_t max_bitmap_size = 0;
            std::vector<uint32_t> bitmap_sizes;
            for (auto& bitmap : bitmaps) {
                bitmap->runOptimize();
                uint32_t bitmap_size = bitmap->getSizeInBytes(false);
                if (max_bitmap_size < bitmap_size) {
                    max_bitmap_size = bitmap_size;
                }
                bitmap_sizes.push_back(bitmap_size);
            }

            const auto* bitmap_type_info =
                    get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_OBJECT>();
            IndexedColumnWriterOptions options;
            options.write_ordinal_index = true;
            options.write_value_index = false;
            options.encoding = EncodingInfo::get_default_encoding(bitmap_type_info, false);
            // we already store compressed bitmap, use NO_COMPRESSION to save some cpu
            options.compression = NO_COMPRESSION;

            IndexedColumnWriter bitmap_column_writer(options, bitmap_type_info, file_writer);
            RETURN_IF_ERROR(bitmap_column_writer.init());

            faststring buf;
            buf.reserve(max_bitmap_size);
            for (size_t i = 0; i < bitmaps.size(); ++i) {
                buf.resize(bitmap_sizes[i]); // so that buf[0..size) can be read and written
                bitmaps[i]->write(reinterpret_cast<char*>(buf.data()), false);
                Slice buf_slice(buf);
                RETURN_IF_ERROR(bitmap_column_writer.add(&buf_slice));
            }
            RETURN_IF_ERROR(bitmap_column_writer.finish(meta->mutable_bitmap_column()));
        }
        return Status::OK();
    }

    uint64_t size() const override {
        uint64_t size = 0;
        size += _null_bitmap.getSizeInBytes(false);
        size += _reverted_index_size;
        size += _mem_index.size() * sizeof(CppType);
        size += _arena.used_size();
        return size;
    }

private:
    const TypeInfo* _type_info;
    uint64_t _reverted_index_size;
    rowid_t _rid = 0;
    // row id list for null value
    roaring::Roaring _null_bitmap;
    // unique value to its row id list
    MemoryIndexType _mem_index;
    vectorized::Arena _arena;
};

} // namespace

Status BitmapIndexWriter::create(const TypeInfo* type_info,
                                 std::unique_ptr<BitmapIndexWriter>* res) {
    FieldType type = type_info->type();
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_TINYINT>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_SMALLINT>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_INT:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_INT>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_BIGINT>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_CHAR:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_CHAR>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_VARCHAR>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_STRING:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_STRING>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_DATE:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_DATE>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_DATETIME>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_DATEV2>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_LARGEINT>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_DECIMAL>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_DECIMAL32>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_DECIMAL64>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_DECIMAL256>(type_info));
        break;
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        res->reset(new BitmapIndexWriterImpl<FieldType::OLAP_FIELD_TYPE_BOOL>(type_info));
        break;
    default:
        return Status::NotSupported("unsupported type for bitmap index: {}",
                                    std::to_string(int(type)));
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
