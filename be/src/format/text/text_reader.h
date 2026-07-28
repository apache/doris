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

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/internal_service.pb.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "format/csv/csv_reader.h"
#include "io/file_factory.h"

namespace doris {

class HiveTextFieldSplitter : public BaseCsvTextFieldSplitter<HiveTextFieldSplitter> {
public:
    explicit HiveTextFieldSplitter(bool trim_tailing_space, bool trim_ends, std::string value_sep,
                                   size_t value_sep_len = 1, char trimming_char = 0,
                                   char escape_char = 0)
            : BaseCsvTextFieldSplitter(trim_tailing_space, trim_ends, value_sep_len, trimming_char),
              _value_sep(std::move(value_sep)),
              _escape_char(escape_char) {
        if (_value_sep_len > 1) {
            _kmp_next.resize(_value_sep_len);
            _kmp_next[0] = -1;
            for (int i = 1, j = -1; i < (int)_value_sep_len; i++) {
                while (j >= 0 && _value_sep[i] != _value_sep[j + 1]) {
                    j = _kmp_next[j];
                }
                if (_value_sep[i] == _value_sep[j + 1]) {
                    j++;
                }
                _kmp_next[i] = j;
            }
        }
    }

    void do_split(const Slice& line, std::vector<Slice>* splitted_values);

private:
    void _split_field_single_char(const Slice& line, std::vector<Slice>* splitted_values);
    void _split_field_multi_char(const Slice& line, std::vector<Slice>* splitted_values);

    std::string _value_sep;
    char _escape_char;
    // Precomputed KMP prefix table for multi-char separator, built once in ctor.
    std::vector<int> _kmp_next;
};

class TextReader : public CsvReader {
    ENABLE_FACTORY_CREATOR(TextReader);

public:
    TextReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
               const TFileScanRangeParams& params, const TFileRangeDesc& range,
               const std::vector<SlotDescriptor*>& file_slot_descs, size_t batch_size,
               io::IOContext* io_ctx, std::shared_ptr<io::IOContext> io_ctx_holder = nullptr);

    ~TextReader() override = default;

private:
    Status _init_options() override;
    Status _create_line_reader() override;
    Status _deserialize_one_cell(DataTypeSerDeSPtr serde, IColumn& column, Slice& slice) override;
    Status _validate_line(const Slice& line, bool* success) override;
    Status _deserialize_nullable_string(IColumn& column, Slice& slice) override;
    bool _empty_line_as_record() const override;
};

} // namespace doris
