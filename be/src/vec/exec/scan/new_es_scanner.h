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

#include "exec/es/es_scan_reader.h"
#include "exec/es/es_scroll_parser.h"
#include "runtime/runtime_state.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {

class NewEsScanNode;

class NewEsScanner : public VScanner {
public:
    NewEsScanner(RuntimeState* state, NewEsScanNode* parent, int64_t limit, TupleId tuple_id,
                 const std::map<std::string, std::string>& properties,
                 const std::map<std::string, std::string>& docvalue_context, bool doc_value_mode);

    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

public:
    Status prepare(RuntimeState* state, VExprContext** vconjunct_ctx_ptr);

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eof) override;

private:
    Status _get_next(std::vector<vectorized::MutableColumnPtr>& columns);

private:
    bool _is_init;
    bool _es_eof;

    const std::map<std::string, std::string>& _properties;

    bool _line_eof;
    bool _batch_eof;

    TupleId _tuple_id;
    const TupleDescriptor* _tuple_desc;

    std::unique_ptr<MemPool> _mem_pool;

    std::unique_ptr<ESScanReader> _es_reader;
    std::unique_ptr<ScrollParser> _es_scroll_parser;

    const std::map<std::string, std::string>& _docvalue_context;
    bool _doc_value_mode;
};
} // namespace doris::vectorized
