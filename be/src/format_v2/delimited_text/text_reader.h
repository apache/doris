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

namespace doris::format::text {

// FileScannerV2 Hive text reader.
//
// Text files do not have embedded schema, so FE-provided file slots and column_idxs are converted
// into a file-local schema in the same way as CSV v2. The row parser is intentionally different
// from CSV: field splitting follows Hive text escaping rules and cells are deserialized through
// deserialize_one_cell_from_hive_text().
class TextReader final : public ::doris::format::DelimitedTextReader {
public:
    TextReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
               std::unique_ptr<io::FileDescription>& file_description,
               std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
               const TFileScanRangeParams* scan_params,
               const std::vector<SlotDescriptor*>& file_slot_descs,
               TFileCompressType::type range_compress_type = TFileCompressType::UNKNOWN,
               std::optional<TUniqueId> stream_load_id = std::nullopt);
    ~TextReader() override;

private:
    Status _init_format_state() override;
    Status _create_decompressor() override;
    Status _create_line_reader() override;
    void _split_line(const Slice& line) override;
    void _split_line_single_char(const Slice& line);
    void _split_line_multi_char(const Slice& line);
    Status _deserialize_one_cell(const RequestedColumn& column, IColumn* output,
                                 Slice value) override;
};

} // namespace doris::format::text
