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
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
  #include "common/compile_check_begin.h"

struct AggregateFunctionContextNgramsData {
    using NGramMap = std::map<std::vector<StringRef>, double>;
    
    Int64 n;
    Int64 k;
    Int64 pf;
    std::vector<StringRef> context;
    NGramMap ngrams;
    
    void add(const std::vector<StringRef>& sequence) {
        // find Ngrams in sequence
        for (size_t i = 0; i <= sequence.size() - context.size(); ++i) {
            bool context_matches = true;
            std::vector<StringRef> ngram;
            ngram.reserve(n);
            
            // check context
            for (size_t j = 0; j < context.size(); ++j) {
                if (context[j].empty()) { // null in context
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
                
                // if exceed buffer size, trim
                if (ngrams.size() > k * pf * 2) {
                    trim(false);
                }
            }
        }
    }

    void merge(const AggregateFunctionContextNgramsData& other) {
        // check parameters
        if (k != other.k || n != other.n || pf != other.pf) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Mismatch in context_ngrams parameters");
        }
        
        // merge n-gram frequency
        for (const auto& pair : other.ngrams) {
            auto it = ngrams.find(pair.first);
            if (it != ngrams.end()) {
                it->second += pair.second;
            } else {
                ngrams[pair.first] = pair.second;
            }
        }
        
        // if exceed buffer size, trim
        if (ngrams.size() > k * pf * 2) {
            trim(false);
        }
    }
    
    void trim(bool final_trim) {
        // sort by frequency
        std::vector<std::pair<std::vector<StringRef>, double>> sorted_ngrams(
            ngrams.begin(), ngrams.end());
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
        write_binary(k, buf);
        write_binary(pf, buf);
        write_binary(n, buf);
        
        // serialize context
        write_binary(context.size(), buf);
        for (const auto& str : context) {
            write_string_binary(str, buf);
        }
        
        // serialize n-grams
        write_binary(ngrams.size(), buf);
        for (const auto& pair : ngrams) {
            for (const auto& str : pair.first) {
                write_string_binary(str, buf);
            }
            write_binary(pair.second, buf);
        }
    }

    void deserialize(BufferReadable& buf) {
        read_binary(k, buf);
        read_binary(pf, buf);
        read_binary(n, buf);
        
        // deserialize context
        size_t context_size;
        read_binary(context_size, buf);
        context.clear();
        context.reserve(context_size);
        for (size_t i = 0; i < context_size; ++i) {
            StringRef str;
            read_string_binary(str, buf);
            context.push_back(str);
        }
        
        // deserialize n-grams
        size_t ngrams_size;
        read_binary(ngrams_size, buf);
        ngrams.clear();
        for (size_t i = 0; i < ngrams_size; ++i) {
            std::vector<StringRef> ngram;
            ngram.reserve(n);
            for (size_t j = 0; j < n; ++j) {
                StringRef str;
                read_string_binary(str, buf);
                ngram.push_back(str);
            }
            double freq;
            read_binary(freq, buf);
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
        // check argument number
        if (argument_types_.size() != 3 && argument_types_.size() != 4) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                "Function {} requires 3 or 4 arguments", get_name());
        }

        // check first argument is array<array<string>>
        WhichDataType which1(remove_nullable(argument_types_[0]));
        if (which1.idx != TypeIndex::Array) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                "First argument of function {} must be array<array<string>>", get_name());
        }
        const auto* array_type = assert_cast<const DataTypeArray*>(argument_types_[0].get());
        
        WhichDataType which2(array_type->get_nested_type());
        if (which2.idx != TypeIndex::Array) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                "First argument of function {} must be array<array<string>>", get_name());
        }
        const auto* inner_array_type = assert_cast<const DataTypeArray*>(array_type->get_nested_type().get());
        
        WhichDataType which3(remove_nullable(inner_array_type->get_nested_type()));
        if (which3.idx != TypeIndex::String && which3.idx != TypeIndex::FixedString) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                "First argument of function {} must be array<array<string>>", get_name());
        }

        // check second argument is array<string> or int
        WhichDataType which4(remove_nullable(argument_types_[1]));
        if (which4.idx == TypeIndex::Array) {
            const auto* context_array_type = assert_cast<const DataTypeArray*>(argument_types_[1].get());
            WhichDataType which5(remove_nullable(context_array_type->get_nested_type()));
            if (which5.idx != TypeIndex::String && which5.idx != TypeIndex::FixedString) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                    "Second argument must be array<string> or int");
            }
        } else if (!(which4.idx == TypeIndex::Int8 || which4.idx == TypeIndex::Int16 ||
                    which4.idx == TypeIndex::Int32 || which4.idx == TypeIndex::Int64 ||
                    which4.idx == TypeIndex::UInt8 || which4.idx == TypeIndex::UInt16 ||
                    which4.idx == TypeIndex::UInt32 || which4.idx == TypeIndex::UInt64)) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                "Second argument must be array<string> or int");
        }

        // check remaining arguments must be integer
        for (size_t i = 2; i < argument_types.size(); ++i) {
            WhichDataType which(remove_nullable(argument_types[i]));
            if (!(which.idx == TypeIndex::Int8 || which.idx == TypeIndex::Int16 ||
                which.idx == TypeIndex::Int32 || which.idx == TypeIndex::Int64 ||
                which.idx == TypeIndex::UInt8 || which.idx == TypeIndex::UInt16 ||
                which.idx == TypeIndex::UInt32 || which.idx == TypeIndex::UInt64)) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                    "Argument {} of function {} must be integer", i + 1, get_name());
            }
        }
    }

    String get_name() const override { return "context_ngrams"; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    void create(AggregateDataPtr __restrict place) const override {
        new (place) AggregateFunctionContextNgramsData;
        this->data(place).k = 0;
        this->data(place).n = 0;
        this->data(place).pf = 0;
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        this->data(place).~AggregateFunctionContextNgramsData();
    }

    bool allocates_memory_in_arena() { return false; }

    void add(AggregateDataPtr __restrict place,
             const IColumn** columns,
             ssize_t row_num,
             Arena*) const override {
        auto& data = this->data(place);
    
        // get input sequence
        const ColumnArray* array = assert_cast<const ColumnArray*>(columns[0]);
        const ColumnString* strings = assert_cast<const ColumnString*>(array->get_data_ptr().get());
        
        // get second argument(context pattern or n)
        WhichDataType which(remove_nullable(argument_types[1]));
        bool is_context_pattern = (which.idx == TypeIndex::Array);

        // initialize parameters
        if (is_context_pattern) {
            // context_ngrams(array<array<string>>, array<string>, int k [, int pf])
            const ColumnArray* context_array = assert_cast<const ColumnArray*>(columns[1]);
            const ColumnString* context_strings = assert_cast<const ColumnString*>(context_array->get_data_ptr().get());
            data.k = assert_cast<const ColumnConst*>(columns[2])->get_int(0);
            if (argument_types.size() > 3) {
                data.pf = assert_cast<const ColumnConst*>(columns[3])->get_int(0);
            } else {
                data.pf = 1;
            }
            
            // initialize context pattern
            if (data.context.empty()) {
                const auto& offsets = context_array->get_offsets();
                size_t start = row_num == 0 ? 0 : offsets[row_num - 1];
                size_t end = offsets[row_num];
                
                data.n = 0;
                for (size_t i = start; i < end; ++i) {
                    StringRef str = context_strings->get_data_at(i);
                    data.context.push_back(str);
                    if (str.empty()) {
                        data.n++;
                    }
                }
                
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
            data.n = assert_cast<const ColumnConst*>(columns[1])->get_int(0);
            data.k = assert_cast<const ColumnConst*>(columns[2])->get_int(0);
            if (argument_types.size() > 3) {
                data.pf = assert_cast<const ColumnConst*>(columns[3])->get_int(0);
            } else {
                data.pf = 1;
            }
            
            // create full null context for n-gram length
            if (data.context.empty()) {
                data.context.resize(data.n);
                for (int i = 0; i < data.n; ++i) {
                    data.context[i] = StringRef();
                }
            }
        }

        // collect and process input sequence
        const auto& offsets = array->get_offsets();
        size_t start = row_num == 0 ? 0 : offsets[row_num - 1];
        size_t end = offsets[row_num];
        
        std::vector<StringRef> sequence;
        sequence.reserve(end - start);
        for (size_t i = start; i < end; ++i) {
            sequence.push_back(strings->get_data_at(i));
        }
        
        data.add(sequence);
    }

    void merge(AggregateDataPtr __restrict place,
               ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place,
                  BufferWritable& buf) const override {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place,
                    BufferReadable& buf,
                    Arena*) const override {
        this->data(place).deserialize(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place,
                           IColumn& to) const override {
        auto& data = this->data(place);
    
        // final trim, only keep top-k results
        const_cast<AggregateFunctionContextNgramsData&>(data).trim(true);
        
        // if no results, return null
        if (data.ngrams.empty()) {
            return;
        }
        
        // sort results by frequency in descending order
        std::vector<std::pair<std::vector<StringRef>, double>> sorted_ngrams(
            data.ngrams.begin(), data.ngrams.end());
        std::sort(sorted_ngrams.begin(), sorted_ngrams.end(),
                [](const auto& a, const auto& b) {
                    if (a.second != b.second) {
                        return a.second > b.second;
                    }
                    // sort by lexicographical order when frequencies are the same
                    return a.first < b.first;
                });
        
        // insert results
        auto& arr_to = assert_cast<ColumnArray&>(to);
        auto& offsets_to = arr_to.get_offsets();
        auto& data_to = assert_cast<ColumnString&>(arr_to.get_data());
        
        for (const auto& pair : sorted_ngrams) {
            for (const auto& str : pair.first) {
                data_to.insert_data(str.data, str.size);
            }
            offsets_to.push_back(data_to.size());
        }
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"