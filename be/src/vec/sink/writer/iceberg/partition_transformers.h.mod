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

#include "runtime/types.h"
#include "util/bit_util.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/functions/function_string.h"

namespace doris {

namespace iceberg {
class Type;
class PartitionField;
}; // namespace iceberg

namespace vectorized {

class IColumn;
class PartitionColumnTransform;

class PartitionColumnTransforms {
private:
    PartitionColumnTransforms();

public:
    static std::unique_ptr<PartitionColumnTransform> create(
            const doris::iceberg::PartitionField& field, const TypeDescriptor& source_type);
};

class PartitionColumnTransform {
public:
    PartitionColumnTransform() = default;

    virtual ~PartitionColumnTransform() = default;

    virtual bool preserves_non_null() const { return false; }

    virtual bool monotonic() const { return true; }

    virtual bool temporal() const { return false; }

    virtual const TypeDescriptor& get_result_type() const = 0;

    virtual bool is_void() const { return false; }

    virtual ColumnWithTypeAndName apply(Block& block, int idx) = 0;

    virtual std::string to_human_string(const TypeDescriptor& type, const std::any& value) const;
};

class IdentityPartitionColumnTransform : public PartitionColumnTransform {
public:
    IdentityPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type) {}

    virtual const TypeDescriptor& get_result_type() const { return _source_type; }

    virtual ColumnWithTypeAndName apply(Block& block, int idx) {
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(idx);
        fprintf(stderr, "column_with_type_and_name.name: %s\n",
                column_with_type_and_name.name.c_str());
        return {column_with_type_and_name.column, column_with_type_and_name.type,
                column_with_type_and_name.name};
    }

private:
    TypeDescriptor _source_type;
};

class StringTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    StringTruncatePartitionColumnTransform(const TypeDescriptor& source_type, int width)
            : _source_type(source_type), _width(width) {}

    virtual const TypeDescriptor& get_result_type() const { return _source_type; }

    virtual ColumnWithTypeAndName apply(Block& block, int idx) {
        auto int_type = std::make_shared<DataTypeInt32>();
        size_t num_columns_without_result = block.columns();
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(idx);

        ColumnPtr string_column_ptr;
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column)) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            string_column_ptr = nullable_column->get_nested_column_ptr();
            is_nullable = true;
        } else {
            string_column_ptr = column_with_type_and_name.column;
            is_nullable = false;
        }
        block.replace_by_position(idx, std::move(string_column_ptr));
        block.insert({int_type->create_column_const(block.rows(), to_field(1)), int_type,
                      "const 1"}); // pos is 1
        block.insert({int_type->create_column_const(block.rows(), to_field(_width)), int_type,
                      fmt::format("const {}", _width)});                       // width
        block.insert({nullptr, std::make_shared<DataTypeString>(), "result"}); // result column
        ColumnNumbers temp_arguments(3);
        temp_arguments[0] = idx;                            // str column
        temp_arguments[1] = num_columns_without_result;     // pos
        temp_arguments[2] = num_columns_without_result + 1; // width
        size_t result_column_id = num_columns_without_result + 2;

        SubstringUtil::substring_execute(block, temp_arguments, result_column_id, block.rows());
        if (is_nullable) {
            auto res_column = ColumnNullable::create(block.get_by_position(result_column_id).column,
                                                     null_map_column_ptr);
            Block::erase_useless_column(&block, num_columns_without_result);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            auto res_column = block.get_by_position(result_column_id).column;
            Block::erase_useless_column(&block, num_columns_without_result);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

private:
    TypeDescriptor _source_type;
    int _width;
};

class IntegerTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    IntegerTruncatePartitionColumnTransform(const TypeDescriptor& source_type, int width)
            : _source_type(source_type), _width(width) {}

    virtual const TypeDescriptor& get_result_type() const { return _source_type; }

    virtual ColumnWithTypeAndName apply(Block& block, int idx) {
        //        auto int_type = std::make_shared<DataTypeInt32>();
        //        size_t num_columns_without_result = block.columns();
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(idx);

        ColumnPtr column_ptr;
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column)) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
            is_nullable = true;
        } else {
            column_ptr = column_with_type_and_name.column;
            is_nullable = false;
        }
        if (const ColumnInt32* col_integer = check_and_get_column<ColumnInt32>(column_ptr)) {
            auto col_res = ColumnInt32::create();
            //
            ColumnInt32::Container& out_data = col_res->get_data();
            out_data.resize(col_integer->get_data().size());
            //            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_vector(col_left->get_data(),
            //                                                                 col_right->get_data(), vec_res);
            //
            //            block.replace_by_position(result, std::move(col_res));
            const ColumnInt32::Container& in_data = col_integer->get_data();
            const int* end_in = in_data.data() + in_data.size();

            const Int32* __restrict p_in = in_data.data();
            Int32* __restrict p_out = out_data.data();

            while (p_in < end_in) {
                *p_out = *p_in - ((*p_in % _width) + _width) % _width;
                ++p_in;
                ++p_out;
            }
            if (is_nullable) {
                auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
                return {std::move(res_column),
                        DataTypeFactory::instance().create_data_type(get_result_type(), true),
                        column_with_type_and_name.name};
            } else {
                return {std::move(col_res),
                        DataTypeFactory::instance().create_data_type(get_result_type(), false),
                        column_with_type_and_name.name};
            }
        } else if (auto col_right_const = check_and_get_column_const<ColumnInt32>(column_ptr)) {
            assert(0);
        } else {
            assert(0);
        }

        return {};
    }

private:
    TypeDescriptor _source_type;
    int _width;
};

template <typename T>
class DecimalTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    DecimalTruncatePartitionColumnTransform(const TypeDescriptor& source_type, int width)
            : _source_type(source_type), _width(width) {}

    virtual const TypeDescriptor& get_result_type() const { return _source_type; }

    virtual ColumnWithTypeAndName apply(Block& block, int idx) {
        //        auto int_type = std::make_shared<DataTypeInt32>();
        //        size_t num_columns_without_result = block.columns();
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(idx);

        ColumnPtr column_ptr;
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column)) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
            is_nullable = true;
        } else {
            column_ptr = column_with_type_and_name.column;
            is_nullable = false;
        }

        const auto* const decimal_col = check_and_get_column<ColumnDecimal<T>>(column_ptr);
        const auto& vec_src = decimal_col->get_data();

        auto col_res = ColumnDecimal<T>::create(vec_src.size(), decimal_col->get_scale());
        auto& vec_res = col_res->get_data();

        const typename T::NativeType* __restrict p_in =
                reinterpret_cast<const T::NativeType*>(vec_src.data());
        const typename T::NativeType* end_in =
                reinterpret_cast<const T::NativeType*>(vec_src.data()) + vec_src.size();
        typename T::NativeType* __restrict p_out = reinterpret_cast<T::NativeType*>(vec_res.data());

        while (p_in < end_in) {
            typename T::NativeType remainder = ((*p_in % _width) + _width) % _width;
            *p_out = *p_in - remainder;
            ++p_in;
            ++p_out;
        }

        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

private:
    TypeDescriptor _source_type;
    int _width;
};

class IntBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    IntBucketPartitionColumnTransform(const TypeDescriptor& source_type, int bucket_num)
            : _source_type(source_type), _bucket_num(bucket_num), _target_type(TYPE_INT) {}

    virtual const TypeDescriptor& get_result_type() const { return _target_type; }

    virtual ColumnWithTypeAndName apply(Block& block, int idx) {
        //        auto int_type = std::make_shared<DataTypeInt32>();
        //        size_t num_columns_without_result = block.columns();
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(idx);

        ColumnPtr column_ptr;
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column)) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
            is_nullable = true;
        } else {
            column_ptr = column_with_type_and_name.column;
            is_nullable = false;
        }
        if (const ColumnInt32* col_integer = check_and_get_column<ColumnInt32>(column_ptr)) {
            auto col_res = ColumnInt32::create();
            //
            ColumnInt32::Container& out_data = col_res->get_data();
            out_data.resize(col_integer->get_data().size());
            //            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_vector(col_left->get_data(),
            //                                                                 col_right->get_data(), vec_res);
            //
            //            block.replace_by_position(result, std::move(col_res));

            const ColumnInt32::Container& in_data = col_integer->get_data();
            const int* end_in = in_data.data() + in_data.size();

            const Int32* __restrict p_in = in_data.data();
            Int32* __restrict p_out = out_data.data();

            //            Int64 test = 100;
            //            uint32_t hash_value2 = HashUtil::murmur_hash3_32(&test, 8, 0);
            //            fprintf(stderr, "test: hash_value: %d, _bucket_num: %d\n", hash_value2, _bucket_num);

            while (p_in < end_in) {
                Int64 long_value = static_cast<Int64>(*p_in);
                uint32_t hash_value = HashUtil::murmur_hash3_32(&long_value, sizeof(long_value), 0);
                fprintf(stderr, "*p_in: %d, hash_value: %d, _bucket_num: %d\n", *p_in, hash_value,
                        _bucket_num);

                *p_out = (hash_value & INT32_MAX) % _bucket_num;
                ++p_in;
                ++p_out;
            }
            if (is_nullable) {
                auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
                return {std::move(res_column),
                        DataTypeFactory::instance().create_data_type(get_result_type(), true),
                        column_with_type_and_name.name};
            } else {
                return {std::move(col_res),
                        DataTypeFactory::instance().create_data_type(get_result_type(), false),
                        column_with_type_and_name.name};
            }
        } else {
            assert(0);
            return {};
        }
    }

private:
    TypeDescriptor _source_type;
    int _bucket_num;
    TypeDescriptor _target_type;
};

template <typename T>
class DecimalBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    DecimalBucketPartitionColumnTransform(const TypeDescriptor& source_type, int bucket_num)
            : _source_type(source_type), _bucket_num(bucket_num), _target_type(TYPE_INT) {}

    virtual const TypeDescriptor& get_result_type() const { return _target_type; }

    virtual ColumnWithTypeAndName apply(Block& block, int idx) {
        //        auto int_type = std::make_shared<DataTypeInt32>();
        //        size_t num_columns_without_result = block.columns();
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(idx);

        ColumnPtr column_ptr;
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column)) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
            is_nullable = true;
        } else {
            column_ptr = column_with_type_and_name.column;
            is_nullable = false;
        }
        if (const ColumnDecimal<T>* col_decimal =
                    check_and_get_column<ColumnDecimal<T>>(column_ptr)) {
            auto col_res = ColumnInt32::create();
            //
            ColumnInt32::Container& out_data = col_res->get_data();
            out_data.resize(col_decimal->get_data().size());
            //            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_vector(col_left->get_data(),
            //                                                                 col_right->get_data(), vec_res);
            //
            //            block.replace_by_position(result, std::move(col_res));
            //            const auto* const decimal_col = check_and_get_column<ColumnDecimal<T>>(column_ptr);
            const auto& vec_src = col_decimal->get_data();

            auto& vec_res = col_res->get_data();

            const typename T::NativeType* __restrict p_in =
                    reinterpret_cast<const T::NativeType*>(vec_src.data());
            const typename T::NativeType* end_in =
                    reinterpret_cast<const T::NativeType*>(vec_src.data()) + vec_src.size();
            typename T::NativeType* __restrict p_out =
                    reinterpret_cast<T::NativeType*>(vec_res.data());

            //            Int64 test = 100;
            //            uint32_t hash_value2 = HashUtil::murmur_hash3_32(&test, 8, 0);
            //            fprintf(stderr, "test: hash_value: %d, _bucket_num: %d\n", hash_value2, _bucket_num);

            while (p_in < end_in) {
                fprintf(stderr, "sizeof(*p_in): %zu\n", sizeof(*p_in));
                //                uint8_t bytes[sizeof(*p_in)];
                //                for (int i = 0; i < sizeof(*p_in); ++i) {
                //                    bytes[sizeof(*p_in) - 1 - i] = static_cast<uint8_t>(*p_in >> (i * sizeof(*p_in)));
                //                }
                //                for (size_t i = 0; i < sizeof(*p_in); ++i) {
                //                    printf("%02X ", bytes[i]);
                //                }
                //                printf("\n");
                std::string buffer = BitUtil::IntToByteBuffer(*p_in);

                uint32_t hash_value = HashUtil::murmur_hash3_32(buffer.data(), buffer.size(), 0);
                fprintf(stderr, "hash_value:%d\n", hash_value);

                *p_out = (hash_value & INT32_MAX) % _bucket_num;
                ++p_in;
                ++p_out;
            }
            if (is_nullable) {
                auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
                return {std::move(res_column),
                        DataTypeFactory::instance().create_data_type(get_result_type(), true),
                        column_with_type_and_name.name};
            } else {
                return {std::move(col_res),
                        DataTypeFactory::instance().create_data_type(get_result_type(), false),
                        column_with_type_and_name.name};
            }
        } else {
            assert(0);
        }
        return {};
    }

private:
    TypeDescriptor _source_type;
    int _bucket_num;
    TypeDescriptor _target_type;
};

class DateBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    DateBucketPartitionColumnTransform(const TypeDescriptor& source_type, int bucket_num)
            : _source_type(source_type), _bucket_num(bucket_num), _target_type(TYPE_INT) {}

    virtual const TypeDescriptor& get_result_type() const { return _target_type; }

    virtual ColumnWithTypeAndName apply(Block& block, int idx) {
        //        auto int_type = std::make_shared<DataTypeInt32>();
        //        size_t num_columns_without_result = block.columns();
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(idx);

        ColumnPtr column_ptr;
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column)) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
            is_nullable = true;
        } else {
            column_ptr = column_with_type_and_name.column;
            is_nullable = false;
        }
        if (const ColumnDateV2* col = check_and_get_column<ColumnDateV2>(column_ptr)) {
            auto col_res = ColumnInt32::create();
            //
            ColumnInt32::Container& out_data = col_res->get_data();
            out_data.resize(col->get_data().size());
            //            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_vector(col_left->get_data(),
            //                                                                 col_right->get_data(), vec_res);
            //
            //            block.replace_by_position(result, std::move(col_res));

            const ColumnDateV2::Container& in_data = col->get_data();
            const auto* end_in = in_data.data() + in_data.size();

            const auto* __restrict p_in = in_data.data();
            auto* __restrict p_out = out_data.data();

            //            Int64 test = 100;
            //            uint32_t hash_value2 = HashUtil::murmur_hash3_32(&test, 8, 0);
            //            fprintf(stderr, "test: hash_value: %d, _bucket_num: %d\n", hash_value2, _bucket_num);

            while (p_in < end_in) {
                DateV2Value<DateV2ValueType> value =
                        binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(*(UInt32*)p_in);

                int32_t days_from_unix_epoch = value.daynr() - 719528;
                Int64 long_value = static_cast<Int64>(days_from_unix_epoch);
                uint32_t hash_value = HashUtil::murmur_hash3_32(&long_value, sizeof(long_value), 0);
                fprintf(stderr,
                        "value.daynr(): %d, days_from_unix_epoch: %d, *p_in: %d, hash_value: %d, "
                        "_bucket_num: %d\n",
                        value.daynr(), days_from_unix_epoch, *p_in, hash_value, _bucket_num);

                *p_out = (hash_value & INT32_MAX) % _bucket_num;
                ++p_in;
                ++p_out;
            }
            if (is_nullable) {
                auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
                return {std::move(res_column),
                        DataTypeFactory::instance().create_data_type(get_result_type(), true),
                        column_with_type_and_name.name};
            } else {
                return {std::move(col_res),
                        DataTypeFactory::instance().create_data_type(get_result_type(), false),
                        column_with_type_and_name.name};
            }
        } else {
            assert(0);
            return {};
        }
    }

private:
    TypeDescriptor _source_type;
    int _bucket_num;
    TypeDescriptor _target_type;
};

} // namespace vectorized
} // namespace doris
