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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "format/generic_reader.h"
#include "runtime/runtime_profile.h"

namespace doris {

class ESScanReader;
class ScrollParser;
class RuntimeState;
class SlotDescriptor;
class TupleDescriptor;
class TFileRangeDesc;
class TFileScanRangeParams;

class EsHttpReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(EsHttpReader);

public:
    EsHttpReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                 RuntimeProfile* profile, const TFileRangeDesc& range,
                 const TFileScanRangeParams& params, const TupleDescriptor* tuple_desc);

    ~EsHttpReader() override;

    Status init_reader();

    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status close() override;

    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override;

private:
    struct EsProfile {
        RuntimeProfile::Counter* read_time = nullptr;
        RuntimeProfile::Counter* materialize_time = nullptr;
        RuntimeProfile::Counter* batches_read = nullptr;
        RuntimeProfile::Counter* rows_read = nullptr;
    };

    static std::string _extract_hostname(std::string host);
    void _init_profile();
    void _collect_profile_before_close() override;
    Status _scroll_and_parse();
    std::string _select_host(const std::map<std::string, std::string>& properties) const;

    static const std::string KEY_ES_HOSTS;

    RuntimeState* _state;
    RuntimeProfile* _profile;
    const TupleDescriptor* _tuple_desc;
    const TFileRangeDesc& _range;
    const TFileScanRangeParams& _params;
    const std::vector<SlotDescriptor*>& _file_slot_descs;
    EsProfile _es_profile;

    int64_t _read_timer_ns = 0;
    int64_t _materialize_timer_ns = 0;
    int64_t _batches_read = 0;
    int64_t _rows_read = 0;

    std::map<std::string, std::string> _docvalue_context;
    std::map<std::string, std::string> _fields_context;
    bool _doc_value_mode = false;

    std::unique_ptr<ESScanReader> _es_reader;
    std::unique_ptr<ScrollParser> _es_scroll_parser;

    bool _es_eof = false;
    bool _line_eof = true;
    bool _batch_eof = false;
};

} // namespace doris
