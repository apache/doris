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
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "io/file_factory.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/csv/csv_reader.h"
#include "vec/exec/format/generic_reader.h"

namespace doris {
#include "common/compile_check_begin.h"

class LineReader;
class Decompressor;
class SlotDescriptor;
class RuntimeProfile;
class RuntimeState;

namespace io {
struct IOContext;
} // namespace io
struct TypeDescriptor;

namespace vectorized {
struct ScannerCounter;
class Block;

class HiveCsvTextFieldSplitter : public BaseCsvTextFieldSplitter<HiveCsvTextFieldSplitter> {
public:
    explicit HiveCsvTextFieldSplitter(bool trim_tailing_space, bool trim_ends,
                                      std::string value_sep, size_t value_sep_len = 1,
                                      char trimming_char = 0, char escape_char = 0)
            : BaseCsvTextFieldSplitter(trim_tailing_space, trim_ends, value_sep_len, trimming_char),
              _value_sep(std::move(value_sep)),
              _escape_char(escape_char) {}

    void do_split(const Slice& line, std::vector<Slice>* splitted_values);

private:
    std::string _value_sep;
    char _escape_char;
};

class TextReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(TextReader);

public:
    TextReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
               const TFileScanRangeParams& params, const TFileRangeDesc& range,
               const std::vector<SlotDescriptor*>& file_slot_descs, io::IOContext* io_ctx);

    ~TextReader() override;

    Status init_reader(bool is_load);
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<DataTypePtr>* col_types) override;

    Status close() override;

private:
    // init options for type serde
    Status _init_options();
    Status _create_decompressor();
    Status _create_file_reader();
    Status _create_line_reader();
    Status _fill_dest_columns(const Slice& line, Block* block,
                              std::vector<MutableColumnPtr>& columns, size_t* rows);
    Status _line_split_to_values(const Slice& line, bool* success);
    void _split_line(const Slice& line);
    void _init_system_properties();
    void _init_file_description();

    // If the TEXT file is an UTF8 encoding with BOM,
    // then remove the first 3 bytes at the beginning of this file
    // and set size = size - 3.
    const uint8_t* _remove_bom(const uint8_t* ptr, size_t& size);

    // used for tvf
    Status _parse_col_nums(size_t* col_nums);

    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    ScannerCounter* _counter = nullptr;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    io::FileSystemProperties _system_properties;
    io::FileDescription _file_description;
    const std::vector<SlotDescriptor*>& _file_slot_descs;
    // Only for query task, save the file slot to columns in block map.
    // eg, there are 3 cols in "_file_slot_descs" named: k1, k2, k3
    // and this 3 columns in block are k2, k3, k1,
    // the _file_slot_idx_map will save: 2, 0, 1
    std::vector<int> _file_slot_idx_map;
    // Only for query task, save the columns' index which need to be read.
    // eg, there are 3 cols in "_file_slot_descs" named: k1, k2, k3
    // and the corresponding position in file is 0, 3, 5.
    // So the _col_idx will be: <0, 3, 5>
    std::vector<int> _col_idxs;

    io::FileReaderSPtr _file_reader = nullptr;
    std::unique_ptr<LineReader> _line_reader = nullptr;
    bool _line_reader_eof = false;
    std::unique_ptr<Decompressor> _decompressor = nullptr;

    TFileFormatType::type _file_format_type;
    TFileCompressType::type _file_compress_type;
    int64_t _size;

    // When we fetch range doesn't start from 0 will always skip the first line
    int _skip_lines = 0;

    int64_t _start_offset = 0;

    std::string _value_separator;
    std::string _line_delimiter;

    char _escape = 0;

    vectorized::DataTypeSerDeSPtrs _serdes;
    vectorized::DataTypeSerDe::FormatOptions _options;

    // TODO: hive only support single char separator and delimiter
    size_t _value_separator_length;
    size_t _line_delimiter_length;

    io::IOContext* _io_ctx = nullptr;

    bool _is_load = false;

    // save source text which have been splitted.
    std::vector<Slice> _split_values;
    std::unique_ptr<LineFieldSplitterIf> _fields_splitter;
};
} // namespace vectorized
#include "common/compile_check_end.h"
} // namespace doris
