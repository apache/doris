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

#include <map>
#include <roaring/roaring.hh>

#include "env/env.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

namespace {

template<typename CppType>
struct BitmapIndexTraits {
    using MemoryIndexType = std::map<CppType, Roaring>;
};

template<>
struct BitmapIndexTraits<Slice> {
    using MemoryIndexType = std::map<Slice, Roaring, Slice::Comparator>;
};

template <FieldType field_type>
class BitmapIndexWriterImpl : public BitmapIndexWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using MemoryIndexType = typename BitmapIndexTraits<CppType>::MemoryIndexType;

    explicit BitmapIndexWriterImpl(const TypeInfo* typeinfo)
        : _typeinfo(typeinfo), _tracker(), _pool(&_tracker) {}

    ~BitmapIndexWriterImpl() = default;

    void add_values(const void* values, size_t count) override {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            add_value(*p);
            p++;
        }
    }

    void add_value(const CppType& value) {
        auto it = _mem_index.find(value);
        if (it != _mem_index.end()) {
            // exiting value, update bitmap
            it->second.add(_rid);
        } else {
            // new value, copy value and insert new key->bitmap pair
            CppType new_value;
            _typeinfo->deep_copy(&new_value, &value, &_pool);
            _mem_index.insert({new_value, Roaring::bitmapOf(1, _rid)});
        }
        _rid++;
    }

    void add_nulls(uint32_t count) override {
        _null_bitmap.addRange(_rid, _rid + count);
        _rid += count;
    }

    Status finish(WritableFile* file, BitmapIndexColumnPB* meta) override {
        meta->set_bitmap_type(BitmapIndexColumnPB::ROARING_BITMAP);
        meta->set_has_null(!_null_bitmap.isEmpty());

        {   // write dictionary
            IndexedColumnWriterOptions options;
            options.write_ordinal_index = false;
            options.write_value_index = true;
            options.encoding = EncodingInfo::get_default_encoding(_typeinfo, true);
            options.compression = LZ4F;

            IndexedColumnWriter dict_column_writer(options, _typeinfo, file);
            RETURN_IF_ERROR(dict_column_writer.init());
            for (auto const& it : _mem_index) {
                RETURN_IF_ERROR(dict_column_writer.add(&(it.first)));
            }
            RETURN_IF_ERROR(dict_column_writer.finish(meta->mutable_dict_column()));
        }
        {   // write bitmaps
            std::vector<Roaring*> bitmaps;
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

            // TODO change to OLAP_FIELD_TYPE_BITMAP when it's added
            const TypeInfo* bitmap_typeinfo = get_type_info(OLAP_FIELD_TYPE_VARCHAR);

            IndexedColumnWriterOptions options;
            options.write_ordinal_index = true;
            options.write_value_index = false;
            // TODO change to EncodingInfo::get_default_encoding(bitmap_typeinfo, false) when
            // we support OLAP_FIELD_TYPE_BITMAP
            options.encoding = PLAIN_ENCODING; // hard code to avoid pick up DICT_ENCODING
            // we already store compressed bitmap, use NO_COMPRESSION to save some cpu
            options.compression = NO_COMPRESSION;

            IndexedColumnWriter bitmap_column_writer(options, bitmap_typeinfo, file);
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

private:
    const TypeInfo* _typeinfo;
    rowid_t _rid = 0;
    // row id list for null value
    Roaring _null_bitmap;
    // unique value to its row id list
    MemoryIndexType _mem_index;
    MemTracker _tracker;
    MemPool _pool;
};

} // namespace

Status BitmapIndexWriter::create(const TypeInfo* typeinfo, std::unique_ptr<BitmapIndexWriter>* res) {
    FieldType type = typeinfo->type();
    switch (type) {
        case OLAP_FIELD_TYPE_TINYINT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_TINYINT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_SMALLINT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_SMALLINT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_INT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_INT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_UNSIGNED_INT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_BIGINT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_BIGINT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_FLOAT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_FLOAT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_DOUBLE:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_DOUBLE>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_CHAR:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_CHAR>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_VARCHAR:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_VARCHAR>(typeinfo));
            break;
        default:
            return Status::NotSupported("unsupported type for bitmap index");
    }
    return Status::OK();
}

} // segment_v2
} // namespace doris
