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
#include "olap/tablet_schema.h"
#include "vec/core/block.h"

namespace doris::vectorized {

class IOlapColumnDataAccessor {
public:
    virtual const UInt8* get_nullmap() const = 0;
    virtual const void* get_data() const = 0;
    virtual const void* get_data_at(size_t offset) const = 0;
    virtual ~IOlapColumnDataAccessor() {}
};
using IOlapColumnDataAccessorSPtr = std::shared_ptr<IOlapColumnDataAccessor>;

class OlapBlockDataConvertor {
public:
    OlapBlockDataConvertor(const TabletSchema* tablet_schema);
    void set_source_content(const vectorized::Block* block, size_t row_pos, size_t num_rows);
    void clear_source_content();
    std::pair<Status, IOlapColumnDataAccessorSPtr> convert_column_data(size_t cid);

private:
    // accessors for different data types;
    class OlapColumnDataConvertorBase : public IOlapColumnDataAccessor {
    public:
        OlapColumnDataConvertorBase() = default;
        virtual ~OlapColumnDataConvertorBase() = default;
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
    using OlapColumnDataConvertorBaseSPtr = std::shared_ptr<OlapColumnDataConvertorBase>;

    class OlapColumnDataConvertorObject : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorObject() = default;
        ~OlapColumnDataConvertorObject() override = default;

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
        size_t _length;
        PaddedPODArray<Slice> _slice;
        PaddedPODArray<char> _raw_data;
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

    class OlapColumnDataConvertorDate : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorDate() = default;
        ~OlapColumnDataConvertorDate() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        const void* get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        PaddedPODArray<uint24_t> _values;
    };

    class OlapColumnDataConvertorDateTime : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorDateTime() = default;
        ~OlapColumnDataConvertorDateTime() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        const void* get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        PaddedPODArray<uint64_t> _values;
    };

    class OlapColumnDataConvertorDecimal : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorDecimal() = default;
        ~OlapColumnDataConvertorDecimal() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        const void* get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        PaddedPODArray<decimal12_t> _values;
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

    private:
        const T* _values = nullptr;
    };

private:
    std::vector<OlapColumnDataConvertorBaseSPtr> _convertors;
};

} // namespace doris::vectorized
