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

#include <stdint.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/factory_creator.h"
#include "common/global_types.h"
#include "common/status.h"
#include "exec/es/es_scan_reader.h"
#include "exec/es/es_scroll_parser.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class TupleDescriptor;

namespace vectorized {
class Block;
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class NewEsScanNode;

class NewEsScanner : public VScanner {
    ENABLE_FACTORY_CREATOR(NewEsScanner);

public:
    NewEsScanner(RuntimeState* state, pipeline::ScanLocalStateBase* local_state, int64_t limit,
                 TupleId tuple_id, const std::map<std::string, std::string>& properties,
                 const std::map<std::string, std::string>& docvalue_context, bool doc_value_mode,
                 RuntimeProfile* profile);

    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

public:
    Status prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts);

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eof) override;

private:
    Status _get_next(std::vector<vectorized::MutableColumnPtr>& columns);

private:
    bool _es_eof;

    const std::map<std::string, std::string>& _properties;

    bool _line_eof;
    bool _batch_eof;

    TupleId _tuple_id;
    const TupleDescriptor* _tuple_desc = nullptr;

    std::unique_ptr<ESScanReader> _es_reader;
    std::unique_ptr<ScrollParser> _es_scroll_parser;

    const std::map<std::string, std::string>& _docvalue_context;
    bool _doc_value_mode;
};
} // namespace doris::vectorized
