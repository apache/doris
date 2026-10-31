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
#include <span>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/variant_v2/column_variant_v2.h"

namespace doris {
class TabletIndex;
namespace segment_v2 {
class IndexFileWriter;
class SniiIndexColumnWriter;

// One docs-only SNII document per final assembled Variant row.
class VariantValuesIndexWriter {
public:
    VariantValuesIndexWriter(IndexFileWriter* index_file_writer, const TabletIndex* index_meta,
                             bool is_direct_load);
    ~VariantValuesIndexWriter();
    Status init();
    Status append(const ColumnVariantV2::ReadView& view, size_t begin, size_t length,
                  std::span<const uint8_t> outer_nulls);
    Status finish() const;
    void close_on_error();

private:
    IndexFileWriter* _index_file_writer;
    const TabletIndex* _index_meta;
    bool _is_direct_load;
    std::unique_ptr<SniiIndexColumnWriter> _writer;
    std::vector<std::string> _texts;
};
} // namespace segment_v2
} // namespace doris
