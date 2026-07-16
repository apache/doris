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

#include <memory>
#include <optional>

#include "format_v2/delimited_text/delimited_text_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "util/slice.h"

namespace doris {
class SlotDescriptor;
} // namespace doris

namespace doris::format::csv {

class EncloseCsvLineReaderV2Ctx;

// FileScannerV2 CSV reader.
//
// CSV files do not carry a physical schema. FE provides the table slot descriptors plus
// TFileScanRangeParams::column_idxs, where each file slot maps to a CSV field ordinal. This reader
// exposes that information as a v2 file-local schema and implements CSV parsing directly in the v2
// FileReader contract.
class CsvReader final : public ::doris::format::DelimitedTextReader {
public:
    // `file_slot_descs` must contain only columns physically readable from the CSV payload.
    // Partition/default/virtual columns are materialized by TableReader after this reader returns
    // a file-local block. Keeping that boundary is important because CSV has no embedded schema
    // from which those non-file columns could be derived.
    CsvReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
              std::unique_ptr<io::FileDescription>& file_description,
              std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
              const TFileScanRangeParams* scan_params,
              const std::vector<SlotDescriptor*>& file_slot_descs,
              TFileCompressType::type range_compress_type = TFileCompressType::UNKNOWN,
              std::optional<TUniqueId> stream_load_id = std::nullopt);
    ~CsvReader() override;

private:
    Status _init_format_state() override;
    Status _create_decompressor() override;
    Status _create_line_reader() override;
    Status _validate_line(const Slice& line) override;
    void _split_line(const Slice& line) override;
    Status _deserialize_one_cell(const RequestedColumn& column, IColumn* output,
                                 Slice value) override;
    Slice _normalize_value(Slice value) const override;
    bool _can_split() const override;
    void _on_bom_removed(size_t bom_size) override;

    TFileFormatType::type _file_format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    char _enclose = 0;
    bool _trim_double_quotes = false;
    bool _trim_tailing_spaces = false;
    bool _empty_field_as_null = false;
    bool _keep_cr = false;
    std::shared_ptr<EncloseCsvLineReaderV2Ctx> _enclose_reader_ctx;
};

} // namespace doris::format::csv
