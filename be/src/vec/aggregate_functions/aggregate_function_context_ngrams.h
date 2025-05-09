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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

struct AggregateFunctionContextNgramsData {
    using NGramMap = std::map<std::vector<StringRef>, double>;

    Int64 n = 0;
    Int64 k = 0;
    Int64 pf = 0;
    std::vector<StringRef> context;
    NGramMap ngrams;

    void reset() {
        LOG(INFO) << "In AggregateFunctionContextNgramsData::reset(), data is " << this;
        n = 0;
        k = 0;
        pf = 0;
        context.clear();
        ngrams.clear();
    }

    void add(const std::vector<StringRef>& sequence) {
        LOG(INFO) << "In AggregateFunctionContextNgramsData::add(), data is " << this;
        LOG(INFO) << "In AggregateFunctionContextNgramsData::add(), sequence contains "
                  << sequence.size() << " strings";
        for (size_t i = 0; i < sequence.size(); ++i) {
            LOG(INFO) << sequence[i].to_string();
        }
        // find Ngrams in sequence
        for (size_t i = 0; i + context.size() <= sequence.size(); ++i) {
            bool context_matches = true;
            std::vector<StringRef> ngram;
            ngram.reserve(n);

            // check context
            for (size_t j = 0; j < context.size(); ++j) {
                if (context[j].empty()) {
                    ngram.push_back(sequence[i + j]);
                } else if (context[j] != sequence[i + j]) {
                    context_matches = false;
                    break;
                }
            }

            // if match, update n-gram frequency
            if (context_matches) {
                auto it = ngrams.find(ngram);
                if (it != ngrams.end()) {
                    it->second += 1.0;
                } else {
                    ngrams[ngram] = 1.0;
                }
            }
        }

        // if exceed buffer size, trim
        if (ngrams.size() > k * pf * 2) {
            trim(false);
        }
    }

    void merge(const AggregateFunctionContextNgramsData& other) {
        LOG(INFO) << "In AggregateFunctionContextNgramsData::merge(), data is " << this;
        // check parameters
        if (pf == 0) {
            n = other.n;
            k = other.k;
            pf = other.pf;
        }
        LOG(INFO) << "In AggregateFunctionContextNgramsData::merge(), this: n = " << n
                  << " k = " << k << " pf = " << pf << " other: n = " << other.n
                  << " k = " << other.k << " pf = " << other.pf;

        // merge n-gram frequency
        LOG(INFO) << "In AggregateFunctionContextNgramsData::merge(), this contains "
                  << ngrams.size() << " ngrams";
        for (const auto& pair : ngrams) {
            std::string s = "";
            for (const auto& str : pair.first) {
                s += str.to_string() + " ";
            }
            LOG(INFO) << s << "occurs " << pair.second << " times";
        }
        LOG(INFO) << "In AggregateFunctionContextNgramsData::merge(), other is " << &other;
        LOG(INFO) << "In AggregateFunctionContextNgramsData::merge(), other contains "
                  << other.ngrams.size() << " ngrams";
        for (const auto& pair : other.ngrams) {
            std::string s = "";
            for (const auto& str : pair.first) {
                s += str.to_string() + " ";
            }
            LOG(INFO) << s << "occurs " << pair.second << " times";
            auto it = ngrams.find(pair.first);
            if (it != ngrams.end()) {
                it->second += pair.second;
            } else {
                ngrams[pair.first] = pair.second;
            }
        }
        LOG(INFO) << "In AggregateFunctionContextNgramsData::merge(), after merge, this contains "
                  << ngrams.size() << " ngrams";
        for (const auto& pair : ngrams) {
            std::string s = "";
            for (const auto& str : pair.first) {
                s += str.to_string() + " ";
            }
            LOG(INFO) << s << "occurs " << pair.second << " times";
        }

        // if exceed buffer size, trim
        if (ngrams.size() > k * pf * 2) {
            trim(false);
            LOG(INFO)
                    << "In AggregateFunctionContextNgramsData::merge(), after trim, this contains "
                    << ngrams.size() << " ngrams";
            for (const auto& pair : ngrams) {
                std::string s = "";
                for (const auto& str : pair.first) {
                    s += str.to_string() + " ";
                }
                LOG(INFO) << s << "occurs " << pair.second << " times";
            }
        }
    }

    void trim(bool final_trim) {
        LOG(INFO) << "In AggregateFunctionContextNgramsData::trim(), data is " << this;
        // sort by frequency
        std::vector<std::pair<std::vector<StringRef>, double>> sorted_ngrams(ngrams.begin(),
                                                                             ngrams.end());
        std::sort(sorted_ngrams.begin(), sorted_ngrams.end(),
                  [](const auto& a, const auto& b) { return a.second < b.second; });

        // keep top-k or top-(k*pf) n-grams
        size_t keep_size = final_trim ? k : k * pf;
        if (sorted_ngrams.size() > keep_size) {
            ngrams.clear();
            for (size_t i = sorted_ngrams.size() - keep_size; i < sorted_ngrams.size(); ++i) {
                ngrams[sorted_ngrams[i].first] = sorted_ngrams[i].second;
            }
        }
    }

    void serialize(BufferWritable& buf) const {
        LOG(INFO) << "In AggregateFunctionContextNgramsData::serialize(), data is " << this;
        LOG(INFO) << "In AggregateFunctionContextNgramsData::serialize(), k = " << k
                  << " pf = " << pf << " n = " << n;
        write_binary(k, buf);
        write_binary(pf, buf);
        write_binary(n, buf);

        // serialize context
        LOG(INFO) << "In AggregateFunctionContextNgramsData::serialize(), context contains "
                  << context.size() << " strings";
        write_binary(context.size(), buf);
        for (const auto& str : context) {
            LOG(INFO) << str.to_string();
            write_string_binary(str, buf);
        }

        // serialize n-grams
        LOG(INFO) << "In AggregateFunctionContextNgramsData::serialize(), contains "
                  << ngrams.size() << " ngrams";
        write_binary(ngrams.size(), buf);
        for (const auto& pair : ngrams) {
            std::string s = "";
            for (const auto& str : pair.first) {
                write_string_binary(str, buf);
                s += str.to_string() + " ";
            }
            LOG(INFO) << s << "occur " << pair.second << " times";
            write_binary(pair.second, buf);
        }
    }

    void deserialize(BufferReadable& buf) {
        LOG(INFO) << "In AggregateFunctionContextNgramsData::deserialize(), data is " << this;
        read_binary(k, buf);
        read_binary(pf, buf);
        read_binary(n, buf);
        LOG(INFO) << "In AggregateFunctionContextNgramsData::deserialize(), k = " << k
                  << " pf = " << pf << " n = " << n;

        // deserialize context
        size_t context_size;
        read_binary(context_size, buf);
        LOG(INFO) << "In AggregateFunctionContextNgramsData::deserialize(), context contains "
                  << context_size << " strings";
        context.clear();
        context.reserve(context_size);
        for (size_t i = 0; i < context_size; ++i) {
            StringRef str;
            read_string_binary(str, buf);
            LOG(INFO) << str.to_string();
            context.push_back(str);
        }

        // deserialize n-grams
        size_t ngrams_size;
        read_binary(ngrams_size, buf);
        LOG(INFO) << "In AggregateFunctionContextNgramsData::deserialize(), contains "
                  << ngrams_size << " ngrams";
        ngrams.clear();
        for (size_t i = 0; i < ngrams_size; ++i) {
            std::vector<StringRef> ngram;
            ngram.reserve(n);
            std::string s = "";
            for (size_t j = 0; j < n; ++j) {
                StringRef str;
                read_string_binary(str, buf);
                s += str.to_string() + " ";
                ngram.push_back(str);
            }
            double freq;
            read_binary(freq, buf);
            LOG(INFO) << s << "occur " << freq << " times";
            ngrams[ngram] = freq;
        }
    }
};

class AggregateFunctionContextNgrams
        : public IAggregateFunctionDataHelper<AggregateFunctionContextNgramsData,
                                              AggregateFunctionContextNgrams> {
public:
    AggregateFunctionContextNgrams(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_) {
        LOG(INFO) << "In AggregateFunctionContextNgrams::AggregateFunctionContextNgrams()";
        // check argument number
        if (argument_types_.size() != 3 && argument_types_.size() != 4) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Function {} requires 3 or 4 arguments",
                            get_name());
        }

        // check first argument is array<array<string>>
        WhichDataType which1(remove_nullable(argument_types_[0]));
        if (which1.idx != TypeIndex::Array) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "First argument of function {} must be array<array<string>>",
                            get_name());
        }
        const auto* array_type =
                assert_cast<const DataTypeArray*>(remove_nullable(argument_types_[0]).get());

        WhichDataType which2(remove_nullable(array_type->get_nested_type()));
        if (which2.idx != TypeIndex::Array) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "First argument of function {} must be array<array<string>>",
                            get_name());
        }

        const auto* inner_array_type = assert_cast<const DataTypeArray*>(
                remove_nullable(array_type->get_nested_type()).get());
        WhichDataType which3(remove_nullable(inner_array_type->get_nested_type()));
        if (which3.idx != TypeIndex::String && which3.idx != TypeIndex::FixedString) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "First argument of function {} must be array<array<string>>",
                            get_name());
        }

        // check second argument is array<string> or int
        WhichDataType which4(remove_nullable(argument_types_[1]));
        if (which4.idx == TypeIndex::Array) {
            const auto* context_array_type =
                    assert_cast<const DataTypeArray*>(remove_nullable(argument_types_[1]).get());
            WhichDataType which5(remove_nullable(context_array_type->get_nested_type()));
            if (which5.idx != TypeIndex::String && which5.idx != TypeIndex::FixedString) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Second argument must be array<string> or int");
            }
        } else if (!which4.is_int_or_uint()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Second argument must be array<string> or int");
        }

        // check remaining arguments must be integer
        for (size_t i = 2; i < argument_types_.size(); ++i) {
            WhichDataType which(remove_nullable(argument_types_[i]));
            if (!which.is_int_or_uint()) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Argument {} of function {} must be integer", i + 1, get_name());
            }
        }
    }

    String get_name() const override {
        LOG(INFO) << "In AggregateFunctionContextNgrams::get_name()";
        return "context_ngrams";
    }

    DataTypePtr get_return_type() const override {
        LOG(INFO) << "In AggregateFunctionContextNgrams::get_return_type()";
        auto string_type = std::make_shared<DataTypeString>();
        auto inner_array_type = std::make_shared<DataTypeArray>(make_nullable(string_type));
        auto outer_array_type = std::make_shared<DataTypeArray>(make_nullable(inner_array_type));
        return make_nullable(outer_array_type);
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        auto& data = this->data(place);
        LOG(INFO) << "In AggregateFunctionContextNgrams::add(), data is " << &data;
        LOG(INFO) << "In AggregateFunctionContextNgrams::add(), row_num is " << row_num;

        // get input sequence
        const IColumn* col = columns[0];
        if (const auto* nullable_col = typeid_cast<const ColumnNullable*>(col)) {
            if (nullable_col->is_null_at(row_num)) {
                return;
            }
            col = nullable_col->get_nested_column_ptr().get();
        }
        const ColumnArray* array = assert_cast<const ColumnArray*>(col);
        LOG(INFO) << "In AggregateFunctionContextNgrams::add(), array<array<string>>'s size is "
                  << array->size();
        const IColumn* inner_col = array->get_data_ptr().get();
        if (const auto* nullable_inner = typeid_cast<const ColumnNullable*>(inner_col)) {
            inner_col = nullable_inner->get_nested_column_ptr().get();
        }
        const ColumnArray* inner_array = assert_cast<const ColumnArray*>(inner_col);
        const IColumn* string_col = inner_array->get_data_ptr().get();
        if (const auto* nullable_string = typeid_cast<const ColumnNullable*>(string_col)) {
            string_col = nullable_string->get_nested_column_ptr().get();
        }
        const ColumnString* strings = assert_cast<const ColumnString*>(string_col);
        LOG(INFO) << "In AggregateFunctionContextNgrams::add(), sequence contains "
                  << strings->size() << " strings: ";
        for (size_t i = 0; i < strings->size(); ++i) {
            LOG(INFO) << strings->get_data_at(i);
        }

        // get second argument(context pattern or n)
        WhichDataType which(remove_nullable(argument_types[1]));
        if (which.idx == TypeIndex::Array) {
            // context_ngrams(array<array<string>>, array<string>, int k [, int pf])
            const IColumn* context_col = columns[1];

            // Nullable Array
            if (const auto* nullable_array = typeid_cast<const ColumnNullable*>(context_col)) {
                context_col = nullable_array->get_nested_column_ptr().get();
            }
            const ColumnArray* context_array = assert_cast<const ColumnArray*>(context_col);

            // Nullable String
            const IColumn* context_string_col = context_array->get_data_ptr().get();
            if (const auto* nullable_string =
                        typeid_cast<const ColumnNullable*>(context_string_col)) {
                context_string_col = nullable_string->get_nested_column_ptr().get();
            }
            const ColumnString* context_strings =
                    assert_cast<const ColumnString*>(context_string_col);

            // initialize context pattern
            if (data.context.empty()) {
                const auto& offsets = context_array->get_offsets();
                size_t start = row_num == 0 ? 0 : offsets[row_num - 1];
                size_t end = offsets[row_num];

                data.n = 0;
                for (size_t i = start; i < end; ++i) {
                    StringRef str;

                    // Nullable Column
                    if (const auto* nullable_array =
                                typeid_cast<const ColumnNullable*>(context_col)) {
                        if (nullable_array->is_null_at(i)) {
                            str = StringRef(); // null
                        } else {
                            str = context_strings->get_data_at(i);
                        }
                    } else {
                        str = context_strings->get_data_at(i);
                    }

                    data.context.push_back(str);
                    if (str.empty()) {
                        data.n++;
                    }
                }

                LOG(INFO) << "In AggregateFunctionContextNgrams::add(), n = " << data.n;

                if (data.context.empty()) {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "context_ngrams requires non-empty context pattern");
                }
                if (data.n == 0) {
                    throw Exception(ErrorCode::INVALID_ARGUMENT,
                                    "context_ngrams requires at least one null in context pattern");
                }
            }
        } else {
            // context_ngrams(array<array<string>>, int n, int k [, int pf])
            // get n
            data.n = get_int_value(columns[1], row_num);
            if (data.n <= 0) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "context_ngrams requires n parameter must greater than zero");
            }
            LOG(INFO) << "In AggregateFunctionContextNgrams::add(), n = " << data.n;
            // create full null context for n-gram length
            if (data.context.empty()) {
                data.context.resize(data.n);
                for (int i = 0; i < data.n; ++i) {
                    data.context[i] = StringRef();
                }
            }
        }

        // get k and pf
        data.k = get_int_value(columns[2], row_num);
        if (data.k <= 0) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "context_ngrams requires the third parameter must greater than zero");
        }
        if (argument_types.size() > 3) {
            data.pf = get_int_value(columns[3], row_num);
        } else {
            data.pf = 1;
        }
        LOG(INFO) << "In AggregateFunctionContextNgrams::add(), k = " << data.k
                  << " pf = " << data.pf;
        if (data.pf <= 0) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "context_ngrams requires the fourth parameter must greater than zero");
        }

        // collect and process input sequence
        const auto& offsets = array->get_offsets();
        LOG(INFO) << "In AggregateFunctionContextNgrams::add(), check offsets:";
        for (size_t i = 0; i < offsets.size(); ++i) {
            LOG(INFO) << offsets[i];
        }
        size_t start = row_num == 0 ? 0 : offsets[row_num - 1];
        size_t end = offsets[row_num];

        const auto& inner_offsets = inner_array->get_offsets();
        LOG(INFO) << "In AggregateFunctionContextNgrams::add(), check inner offsets:";
        for (size_t i = 0; i < inner_offsets.size(); ++i) {
            LOG(INFO) << inner_offsets[i];
        }
        size_t inner_start = start == 0 ? 0 : inner_offsets[start - 1];
        size_t inner_end = inner_offsets[end - 1];

        std::vector<StringRef> sequence;
        sequence.reserve(inner_end - inner_start);
        for (size_t i = inner_start; i < inner_end; ++i) {
            sequence.push_back(strings->get_data_at(i));
        }

        data.add(sequence);
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).deserialize(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& data = this->data(place);
        LOG(INFO) << "In AggregateFunctionContextNgrams::insert_result_into(), data is " << &data;

        // final trim, only keep top-k results
        LOG(INFO) << "In AggregateFunctionContextNgrams::insert_result_into(), before final trim, "
                     "data contains "
                  << data.ngrams.size() << " ngrams";
        for (const auto& pair : data.ngrams) {
            std::string s = "";
            for (const auto& str : pair.first) {
                s += str.to_string() + " ";
            }
            LOG(INFO) << s << "occurs " << pair.second << " times";
        }
        const_cast<AggregateFunctionContextNgramsData&>(data).trim(true);
        LOG(INFO) << "In AggregateFunctionContextNgrams::insert_result_into(), after final trim, "
                     "data contains "
                  << data.ngrams.size() << " ngrams";
        for (const auto& pair : data.ngrams) {
            std::string s = "";
            for (const auto& str : pair.first) {
                s += str.to_string() + " ";
            }
            LOG(INFO) << s << "occurs " << pair.second << " times";
        }

        // insert result
        // outer array
        ColumnArray* outer_array = nullptr;
        if (auto* nullable = typeid_cast<ColumnNullable*>(&to)) {
            auto& null_map = nullable->get_null_map_data();
            if (data.ngrams.empty()) {
                // if no results, return null
                null_map.push_back(1);
                auto& nested_column = nullable->get_nested_column();
                assert_cast<ColumnArray&>(nested_column).get_offsets().push_back(0);
                return;
            }
            null_map.push_back(0);
            outer_array = assert_cast<ColumnArray*>(&nullable->get_nested_column());
        } else {
            outer_array = assert_cast<ColumnArray*>(&to);
        }

        // inner array
        auto& inner_nullable = assert_cast<ColumnNullable&>(outer_array->get_data());
        auto& inner_array = assert_cast<ColumnArray&>(inner_nullable.get_nested_column());
        auto& inner_offsets = inner_array.get_offsets();

        // string
        auto& string_nullable = assert_cast<ColumnNullable&>(inner_array.get_data());
        auto& string_data = assert_cast<ColumnString&>(string_nullable.get_nested_column());
        auto& string_null_map = string_nullable.get_null_map_data();
        auto& inner_null_map = inner_nullable.get_null_map_data();

        // sort results by frequency in descending order
        std::vector<std::pair<std::vector<StringRef>, double>> sorted_ngrams(data.ngrams.begin(),
                                                                             data.ngrams.end());
        std::sort(sorted_ngrams.begin(), sorted_ngrams.end(), [](const auto& a, const auto& b) {
            if (a.second != b.second) {
                return a.second > b.second;
            }
            return a.first < b.first;
        });

        for (const auto& pair : sorted_ngrams) {
            LOG(INFO) << "Inserting n-gram with " << pair.first.size() << " elements";
            if (pair.first.empty()) {
                inner_null_map.push_back(1);
                inner_offsets.push_back(string_data.size());
            } else {
                inner_null_map.push_back(0);
                for (const auto& str : pair.first) {
                    if (str.size == 0) {
                        string_null_map.push_back(1);
                    } else {
                        string_null_map.push_back(0);
                        string_data.insert_data(str.data, str.size);
                    }
                }
                inner_offsets.push_back(string_data.size());
            }
        }
        outer_array->get_offsets().push_back(inner_offsets.size());
    }

    static Int64 get_int_value(const IColumn* col, size_t row_num) {
        if (const auto* const_col = typeid_cast<const ColumnConst*>(col)) {
            return const_col->get_int(0);
        }

        if (const auto* vec_int64 = typeid_cast<const ColumnVector<Int64>*>(col)) {
            return vec_int64->get_data()[row_num];
        }
        if (const auto* vec_int32 = typeid_cast<const ColumnVector<Int32>*>(col)) {
            return vec_int32->get_data()[row_num];
        }
        if (const auto* vec_int16 = typeid_cast<const ColumnVector<Int16>*>(col)) {
            return vec_int16->get_data()[row_num];
        }
        if (const auto* vec_int8 = typeid_cast<const ColumnVector<Int8>*>(col)) {
            return vec_int8->get_data()[row_num];
        }

        throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid integer column type");
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"