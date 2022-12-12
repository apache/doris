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

#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"

namespace doris {

class TabletSchema;
class TabletColumn;
class MemTracker;
class Status;

namespace vectorized {

class Block;
class ColumnArray;
class DataTypeArray;

class IOlapColumnDataAccessor {
public:
    virtual const UInt8* get_nullmap() const = 0;
    virtual const void* get_data() const = 0;
    virtual const void* get_data_at(size_t offset) const = 0;
    virtual ~IOlapColumnDataAccessor() = default;
};

class OlapBlockDataConvertor {
public:
    OlapBlockDataConvertor(const TabletSchema* tablet_schema);
    OlapBlockDataConvertor(const TabletSchema* tablet_schema, const std::vector<uint32_t>& col_ids);
    void set_source_content(const vectorized::Block* block, size_t row_pos, size_t num_rows);
    void clear_source_content();
    std::pair<Status, IOlapColumnDataAccessor*> convert_column_data(size_t cid);

private:
    class OlapColumnDataConvertorBase;
    using OlapColumnDataConvertorBaseUPtr = std::unique_ptr<OlapColumnDataConvertorBase>;
    using OlapColumnDataConvertorBaseSPtr = std::shared_ptr<OlapColumnDataConvertorBase>;

    OlapColumnDataConvertorBaseUPtr create_olap_column_data_convertor(const TabletColumn& column);

    // accessors for different data types;
    class OlapColumnDataConvertorBase : public IOlapColumnDataAccessor {
    public:
        OlapColumnDataConvertorBase() = default;
        ~OlapColumnDataConvertorBase() override = default;
        OlapColumnDataConvertorBase(const OlapColumnDataConvertorBase&) = delete;
        OlapColumnDataConvertorBase& operator=(const OlapColumnDataConvertorBase&) = delete;
        OlapColumnDataConvertorBase(OlapColumnDataConvertorBase&&) = delete;
        OlapColumnDataConvertorBase& operator=(OlapColumnDataConvertorBase&&) = delete;

        virtual void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                                       size_t num_rows);
        void clear_source_column();
        const UInt8* get_nullmap() const override;
        virtual Status convert_to_olap() = 0;

    protected:
        ColumnWithTypeAndName _typed_column;
        size_t _row_pos = 0;
        size_t _num_rows = 0;
        const UInt8* _nullmap = nullptr;
    };

    class OlapColumnDataConvertorObject : public OlapColumnDataConvertorBase {
    public:
        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        const void* get_data_at(size_t offset) const override;

    protected:
        PaddedPODArray<Slice> _slice;
        PaddedPODArray<char> _raw_data;
    };

    class OlapColumnDataConvertorHLL final : public OlapColumnDataConvertorObject {
    public:
        Status convert_to_olap() override;
    };

    class OlapColumnDataConvertorBitMap final : public OlapColumnDataConvertorObject {
    public:
        Status convert_to_olap() override;
    };

    class OlapColumnDataConvertorChar : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorChar(size_t length);
        ~OlapColumnDataConvertorChar() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        const void* get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        static bool should_padding(const ColumnString* column, size_t padding_length) {
            // Check sum of data length, including terminating zero.
            return column->size() * padding_length != column->chars.size();
        }

        static ColumnPtr clone_and_padding(const ColumnString* input, size_t padding_length) {
            auto column = vectorized::ColumnString::create();
            auto padded_column =
                    assert_cast<vectorized::ColumnString*>(column->assume_mutable().get());

            column->offsets.resize(input->size());
            column->chars.resize(input->size() * padding_length);
            memset(padded_column->chars.data(), 0, input->size() * padding_length);

            for (size_t i = 0; i < input->size(); i++) {
                column->offsets[i] = (i + 1) * padding_length;

                auto str = input->get_data_at(i);

                DCHECK(str.size <= padding_length)
                        << "char type data length over limit, padding_length=" << padding_length
                        << ", real=" << str.size;

                if (str.size) {
                    memcpy(padded_column->chars.data() + i * padding_length, str.data, str.size);
                }
            }

            return column;
        }

        size_t _length;
        PaddedPODArray<Slice> _slice;
        ColumnPtr _column = nullptr;
    };

    class OlapColumnDataConvertorVarChar : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorVarChar(bool check_length);
        ~OlapColumnDataConvertorVarChar() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        const void* get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        bool _check_length;
        PaddedPODArray<Slice> _slice;
    };

    template <typename T>
    class OlapColumnDataConvertorPaddedPODArray : public OlapColumnDataConvertorBase {
    public:
        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override {
            OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos, num_rows);
            _values.resize(num_rows);
        }
        const void* get_data() const override { return _values.data(); }
        const void* get_data_at(size_t offset) const override {
            UInt8 null_flag = 0;
            if (_nullmap) {
                null_flag = _nullmap[offset];
            }
            return null_flag ? nullptr : _values.data() + offset;
        }

    protected:
        PaddedPODArray<T> _values;
    };

    class OlapColumnDataConvertorDate : public OlapColumnDataConvertorPaddedPODArray<uint24_t> {
    public:
        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        Status convert_to_olap() override;
    };

    class OlapColumnDataConvertorDateTime : public OlapColumnDataConvertorPaddedPODArray<uint64_t> {
    public:
        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        Status convert_to_olap() override;
    };

    class OlapColumnDataConvertorDecimal
            : public OlapColumnDataConvertorPaddedPODArray<decimal12_t> {
    public:
        Status convert_to_olap() override;
    };

    // class OlapColumnDataConvertorSimple for simple types, which don't need to do any convert, like int, float, double, etc...
    template <typename T>
    class OlapColumnDataConvertorSimple : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorSimple() = default;
        ~OlapColumnDataConvertorSimple() override = default;

        const void* get_data() const override { return _values; }

        const void* get_data_at(size_t offset) const override {
            assert(offset < _num_rows);
            UInt8 null_flag = 0;
            if (_nullmap) {
                null_flag = _nullmap[offset];
            }
            return null_flag ? nullptr : _values + offset;
        }

        Status convert_to_olap() override {
            const vectorized::ColumnVector<T>* column_data = nullptr;
            if (_nullmap) {
                auto nullable_column =
                        assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
                column_data = assert_cast<const vectorized::ColumnVector<T>*>(
                        nullable_column->get_nested_column_ptr().get());
            } else {
                column_data =
                        assert_cast<const vectorized::ColumnVector<T>*>(_typed_column.column.get());
            }

            assert(column_data);
            _values = (const T*)(column_data->get_data().data()) + _row_pos;
            return Status::OK();
        }

    protected:
        const T* _values = nullptr;
    };

    class OlapColumnDataConvertorDateV2 : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorDateV2() = default;
        ~OlapColumnDataConvertorDateV2() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override {
            OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos, num_rows);
        }

        const void* get_data() const override { return values_; }

        const void* get_data_at(size_t offset) const override {
            assert(offset < _num_rows);
            UInt8 null_flag = 0;
            if (_nullmap) {
                null_flag = _nullmap[offset];
            }
            return null_flag ? nullptr : values_ + offset;
        }

        Status convert_to_olap() override {
            const vectorized::ColumnVector<uint32>* column_data = nullptr;
            if (_nullmap) {
                auto nullable_column =
                        assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
                column_data = assert_cast<const vectorized::ColumnVector<uint32>*>(
                        nullable_column->get_nested_column_ptr().get());
            } else {
                column_data = assert_cast<const vectorized::ColumnVector<uint32>*>(
                        _typed_column.column.get());
            }

            assert(column_data);
            values_ = (const uint32*)(column_data->get_data().data()) + _row_pos;
            return Status::OK();
        }

    private:
        const uint32_t* values_ = nullptr;
    };

    class OlapColumnDataConvertorDateTimeV2 : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorDateTimeV2() = default;
        ~OlapColumnDataConvertorDateTimeV2() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override {
            OlapColumnDataConvertorBase::set_source_column(typed_column, row_pos, num_rows);
        }

        const void* get_data() const override { return values_; }

        const void* get_data_at(size_t offset) const override {
            assert(offset < _num_rows);
            UInt8 null_flag = 0;
            if (_nullmap) {
                null_flag = _nullmap[offset];
            }
            return null_flag ? nullptr : values_ + offset;
        }

        Status convert_to_olap() override {
            const vectorized::ColumnVector<uint64_t>* column_data = nullptr;
            if (_nullmap) {
                auto nullable_column =
                        assert_cast<const vectorized::ColumnNullable*>(_typed_column.column.get());
                column_data = assert_cast<const vectorized::ColumnVector<uint64_t>*>(
                        nullable_column->get_nested_column_ptr().get());
            } else {
                column_data = assert_cast<const vectorized::ColumnVector<uint64_t>*>(
                        _typed_column.column.get());
            }

            assert(column_data);
            values_ = (const uint64_t*)(column_data->get_data().data()) + _row_pos;
            return Status::OK();
        }

    private:
        const uint64_t* values_ = nullptr;
    };

    // decimalv3 don't need to do any convert
    template <typename T>
    class OlapColumnDataConvertorDecimalV3
            : public OlapColumnDataConvertorSimple<typename T::NativeType> {
    public:
        using FieldType = typename T::NativeType;
        OlapColumnDataConvertorDecimalV3() = default;
        ~OlapColumnDataConvertorDecimalV3() override = default;

        Status convert_to_olap() override {
            const vectorized::ColumnDecimal<T>* column_data = nullptr;
            if (this->_nullmap) {
                auto nullable_column = assert_cast<const vectorized::ColumnNullable*>(
                        this->_typed_column.column.get());
                column_data = assert_cast<const vectorized::ColumnDecimal<T>*>(
                        nullable_column->get_nested_column_ptr().get());
            } else {
                column_data = assert_cast<const vectorized::ColumnDecimal<T>*>(
                        this->_typed_column.column.get());
            }

            assert(column_data);
            this->_values = (const FieldType*)(column_data->get_data().data()) + this->_row_pos;
            return Status::OK();
        }
    };

    class OlapColumnDataConvertorArray
            : public OlapColumnDataConvertorPaddedPODArray<CollectionValue> {
    public:
        OlapColumnDataConvertorArray(OlapColumnDataConvertorBaseUPtr item_convertor)
                : _item_convertor(std::move(item_convertor)) {}

        Status convert_to_olap() override;

    private:
        Status convert_to_olap(const UInt8* null_map, const ColumnArray* column_array,
                               const DataTypeArray* data_type_array);
        OlapColumnDataConvertorBaseUPtr _item_convertor;
    };

private:
    std::vector<OlapColumnDataConvertorBaseUPtr> _convertors;
};

} // namespace vectorized
} // namespace doris
