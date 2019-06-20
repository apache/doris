#pragma once

#include "util/coding.h"
#include "util/faststring.h"
#include "olap/olap_common.h"
#include "olap/types.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/options.h"

namespace doris {

namespace segment_v2 {

static const size_t plainPageHeaderSize = sizeof(uint32_t) * 2;

template<FieldType Type>
class PlainPageBuilder : public PageBuilder {
public:
    PlainPageBuilder(const PageBuilderOptions *options) :
            _options(options) {
        // Reserve enough space for the block, plus a bit of slop since
        // we often overrun the block by a few values.
        _buffer.reserve(plainPageHeaderSize + _options->data_page_size + 1024);
        reset();
    }

    bool is_page_full() override {
        return _buffer.size() > _options->data_page_size;
    }

    Status add(const uint8_t *vals, size_t *count) override {
        size_t old_size = _buffer.size();
        _buffer.resize(old_size + *count * SIZE_OF_TYPE);
        memcpy(&_buffer[old_size], vals, *count * SIZE_OF_TYPE);
        _count += *count;
        return Status::OK();
    }

    Status get_dictionary_page(Slice *dictionary_page) override {
        return Status::NotSupported("get_dictionary_page not supported in plain page builder");
    }

    Slice finish(const rowid_t page_first_rowid) override {
        encode_fixed32_le((uint8_t *) &_buffer[0], _count);
        encode_fixed32_le((uint8_t *) &_buffer[4], page_first_rowid);
        return Slice(_buffer.data(),  plainPageHeaderSize + _count * SIZE_OF_TYPE);
    }

    void reset() override {
        _count = 0;
        _buffer.clear();
        _buffer.resize(plainPageHeaderSize);
    }

    size_t count() const {
        return _count;
    }

    // this api will release the memory ownership of encoded data
    // Note:
    //     release() should be called after finish
    //     reset() should be called after this function before reuse the builder
    void release() override {
        uint8_t *ret = _buffer.release();
        (void) ret;
    }

private:
    faststring _buffer;
    const PageBuilderOptions *_options;
    size_t _count;
    typedef typename TypeTraits<Type>::CppType CppType;
    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };
};


template<FieldType Type>
class PlainPageDecoder : public PageDecoder {
public:
    PlainPageDecoder(Slice data) : _data(data),
              _parsed(false),
              _num_elems(0),
              _page_first_ordinal(0),
              _cur_idx(0) { }

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < plainPageHeaderSize) {
            std::stringstream ss;
            ss << "file corrupton: not enough bytes for header in PlainBlockDecoder ."
                  "invalid data size:" << _data.size << ", header size:" << plainPageHeaderSize;
            return Status::InternalError(ss.str());
        }

        _num_elems = decode_fixed32_le((const uint8_t *) &_data[0]);
        _page_first_ordinal = decode_fixed32_le((const uint8_t *) &_data[4]);

        if (_data.size != plainPageHeaderSize + _num_elems * SIZE_OF_TYPE) {
            std::stringstream ss;
            ss << "file corrupton: unexpected data size.";
            return Status::InternalError(ss.str());
        }

        _parsed = true;

        seek_to_position_in_page(0);
        return Status::OK();
    }

    Status seek_to_position_in_page(size_t pos) override {
        CHECK(_parsed) << "Must call init()";

        if (PREDICT_FALSE(_num_elems == 0)) {
            DCHECK_EQ(0, pos);
            return Status::InternalError("invalid pos");
        }

        DCHECK_LE(pos, _num_elems);

        _cur_idx = pos;
        return Status::OK();
    }

    Status next_batch(size_t *n, ColumnVectorView *dst) override {
        DCHECK(_parsed);

        if (PREDICT_FALSE(*n == 0 || _cur_idx >= _num_elems)) {
            *n = 0;
            return Status::OK();
        }

        size_t max_fetch = std::min(*n, static_cast<size_t>(_num_elems - _cur_idx));
        memcpy(dst->column_vector()->col_data(),
               &_data[plainPageHeaderSize + _cur_idx * SIZE_OF_TYPE],
               max_fetch * SIZE_OF_TYPE);
        _cur_idx += max_fetch;
        *n = max_fetch;
        return Status::OK();
    }

    size_t count() const override {
        return _num_elems;
    }

    size_t current_index() const override {
        return _cur_idx;
    }

    rowid_t get_first_rowid() const override {
        return _page_first_ordinal;
    }

private:
    Slice _data;
    bool _parsed;
    uint32_t _num_elems;
    rowid_t _page_first_ordinal;
    uint32_t _cur_idx;
    typedef typename TypeTraits<Type>::CppType CppType;
    enum {
        SIZE_OF_TYPE = TypeTraits<Type>::size
    };
};

}
}
