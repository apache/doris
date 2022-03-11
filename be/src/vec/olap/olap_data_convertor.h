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

struct OlapFieldData {
    UInt8 null_flag;
    const void* value;
};

class IOlapColumnDataAccessor {
public:
    virtual const UInt8* get_nullmap() const = 0;
    virtual const void* get_data() const = 0;
    virtual OlapFieldData get_data_at(size_t offset) const = 0;
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
        ColumnWithTypeAndName m_typed_column;
        size_t m_row_pos;
        size_t m_num_rows;
    };
    using OlapColumnDataConvertorBaseSPtr = std::shared_ptr<OlapColumnDataConvertorBase>;

    class OlapColumnDataConvertorObject : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorObject() = default;
        ~OlapColumnDataConvertorObject() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        OlapFieldData get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        PaddedPODArray<Slice> m_slice;
        PaddedPODArray<char> m_raw_data;
    };

    class OlapColumnDataConvertorHLL : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorHLL() = default;
        ~OlapColumnDataConvertorHLL() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        OlapFieldData get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        PaddedPODArray<Slice> m_slice;
        PaddedPODArray<char> m_raw_data;
    };

    class OlapColumnDataConvertorChar : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorChar(size_t length);
        ~OlapColumnDataConvertorChar() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        OlapFieldData get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        size_t m_length;
        PaddedPODArray<Slice> m_slice;
        PaddedPODArray<char> m_raw_data;
    };

    class OlapColumnDataConvertorVarChar : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorVarChar(bool check_length);
        ~OlapColumnDataConvertorVarChar() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        OlapFieldData get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        bool m_check_length;
        PaddedPODArray<Slice> m_slice;
    };

    class OlapColumnDataConvertorDate : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorDate() = default;
        ~OlapColumnDataConvertorDate() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        OlapFieldData get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        PaddedPODArray<uint24_t> m_values;
    };

    class OlapColumnDataConvertorDateTime : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorDateTime() = default;
        ~OlapColumnDataConvertorDateTime() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        OlapFieldData get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        PaddedPODArray<uint64_t> m_values;
    };

    class OlapColumnDataConvertorDecimal : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorDecimal() = default;
        ~OlapColumnDataConvertorDecimal() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override;
        const void* get_data() const override;
        OlapFieldData get_data_at(size_t offset) const override;
        Status convert_to_olap() override;

    private:
        PaddedPODArray<decimal12_t> m_values;
    };

    // class OlapColumnDataConvertorSimple for simple types, which don't need to do any convert, like int, float, double, etc...
    template <typename T>
    class OlapColumnDataConvertorSimple : public OlapColumnDataConvertorBase {
    public:
        OlapColumnDataConvertorSimple() = default;
        ~OlapColumnDataConvertorSimple() override = default;

        void set_source_column(const ColumnWithTypeAndName& typed_column, size_t row_pos,
                               size_t num_rows) override {
            OlapBlockDataConvertor::OlapColumnDataConvertorBase::set_source_column(
                    typed_column, row_pos, num_rows);
            m_values.resize(num_rows);
        }

        const void* get_data() const override { return m_values.data(); }

        OlapFieldData get_data_at(size_t offset) const override {
            assert(offset < m_num_rows && m_num_rows == m_values.size());
            UInt8 null_flag = 0;
            auto null_map = get_nullmap();
            if (null_map) {
                null_flag = null_map[offset];
            }
            return {null_flag, m_values.data() + offset};
        }

        Status convert_to_olap() override {
            const vectorized::ColumnVector<T>* column_data = nullptr;
            const UInt8* nullmap = get_nullmap();
            if (nullmap) {
                auto nullable_column =
                        assert_cast<const vectorized::ColumnNullable*>(m_typed_column.column.get());
                column_data = assert_cast<const vectorized::ColumnVector<T>*>(
                        nullable_column->get_nested_column_ptr().get());
            } else {
                column_data = assert_cast<const vectorized::ColumnVector<T>*>(
                        m_typed_column.column.get());
            }

            assert(column_data);

            const T* data_cur = (const T*)(column_data->get_data().data()) + m_row_pos;
            const T* data_end = data_cur + m_num_rows;
            T* value = m_values.data();
            if (nullmap) {
                const UInt8* nullmap_cur = nullmap + m_row_pos;
                while (data_cur != data_end) {
                    if (!*nullmap_cur) {
                        *value = *data_cur;
                    } else {
                        // do nothing
                    }
                    ++value;
                    ++data_cur;
                    ++nullmap_cur;
                }
                assert(nullmap_cur == nullmap + m_row_pos + m_num_rows &&
                       value == m_values.get_end_ptr());
            } else {
                while (data_cur != data_end) {
                    *value = *data_cur;
                    ++value;
                    ++data_cur;
                }
                assert(value == m_values.get_end_ptr());
            }
            return Status::OK();
        }

    private:
        PaddedPODArray<T> m_values;
    };

private:
    std::vector<OlapColumnDataConvertorBaseSPtr> m_convertors;
};

} // namespace doris::vectorized
