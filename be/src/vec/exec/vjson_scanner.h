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

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exec/base_scanner.h"
#include "exec/json_scanner.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace doris {
class ExprContext;

namespace vectorized {
class VJsonReader;

class VJsonScanner : public JsonScanner {
public:
    VJsonScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                 const std::vector<TBrokerRangeDesc>& ranges,
                 const std::vector<TNetworkAddress>& broker_addresses,
                 const TExpr& vpre_filter_texpr, ScannerCounter* counter);

    Status get_next(doris::Tuple* tuple, MemPool* tuple_pool, bool* eof,
                    bool* fill_tuple) override {
        return Status::NotSupported("Not Implemented get tuple");
    }
    Status get_next(vectorized::Block* output_block, bool* eof) override;

private:
    Status open_vjson_reader();
    Status open_next_reader();

private:
    std::unique_ptr<VJsonReader> _cur_vjson_reader;
};

class VJsonReader : public JsonReader {
public:
    VJsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                bool strip_outer_array, bool num_as_string, bool fuzzy_parse, bool* scanner_eof,
                FileReader* file_reader = nullptr, LineReader* line_reader = nullptr);

    ~VJsonReader();

    Status init(const std::string& jsonpath, const std::string& json_root);

    Status read_json_column(std::vector<MutableColumnPtr>& columns,
                            const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                            bool* eof);

private:
    Status (VJsonReader::*_vhandle_json_callback)(
            std::vector<vectorized::MutableColumnPtr>& columns,
            const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row, bool* eof);

    Status _vhandle_simple_json(std::vector<MutableColumnPtr>& columns,
                                const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                                bool* eof);

    Status _vhandle_flat_array_complex_json(std::vector<MutableColumnPtr>& columns,
                                            const std::vector<SlotDescriptor*>& slot_descs,
                                            bool* is_empty_row, bool* eof);

    Status _vhandle_nested_complex_json(std::vector<MutableColumnPtr>& columns,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof);

    Status _write_columns_by_jsonpath(rapidjson::Value& objectValue,
                                      const std::vector<SlotDescriptor*>& slot_descs,
                                      std::vector<MutableColumnPtr>& columns, bool* valid);

    Status _set_column_value(rapidjson::Value& objectValue, std::vector<MutableColumnPtr>& columns,
                             const std::vector<SlotDescriptor*>& slot_descs, bool* valid);

    Status _write_data_to_column(rapidjson::Value::ConstValueIterator value,
                                 SlotDescriptor* slot_desc, vectorized::IColumn* column_ptr,
                                 bool* valid);

    Status _parse_json(bool* is_empty_row, bool* eof);

    Status _append_error_msg(const rapidjson::Value& objectValue, std::string error_msg,
                             std::string col_name, bool* valid);
};

} // namespace vectorized
} // namespace doris
