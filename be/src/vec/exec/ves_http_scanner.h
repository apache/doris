
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

#include <exec/es_http_scanner.h>

namespace doris::vectorized {

class VEsHttpScanner : public EsHttpScanner {
public:
    VEsHttpScanner(RuntimeState* state, RuntimeProfile* profile, TupleId tuple_id,
                   const std::map<std::string, std::string>& properties,
                   const std::vector<ExprContext*>& conjunct_ctxs, EsScanCounter* counter,
                   bool doc_value_mode)
            : EsHttpScanner(state, profile, tuple_id, properties, conjunct_ctxs, counter,
                            doc_value_mode) {};
    ~VEsHttpScanner();

    Status get_next(std::vector<vectorized::MutableColumnPtr>& columns, MemPool* tuple_pool,
                    bool* eof, const std::map<std::string, std::string>& docvalue_context);
};

} // namespace doris::vectorized
