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

#include "vec/exec/ves_http_scanner.h"

namespace doris::vectorized {

VEsHttpScanner::~VEsHttpScanner() {
    close();
}

Status VEsHttpScanner::get_next(std::vector<vectorized::MutableColumnPtr>& columns,
                                MemPool* tuple_pool, bool* eof,
                                const std::map<std::string, std::string>& docvalue_context) {
    SCOPED_TIMER(_read_timer);
    if (_line_eof && _batch_eof) {
        *eof = true;
        return Status::OK();
    }

    while (!_batch_eof) {
        if (_line_eof || _es_scroll_parser == nullptr) {
            RETURN_IF_ERROR(_es_reader->get_next(&_batch_eof, _es_scroll_parser));
            if (_batch_eof) {
                *eof = true;
                return Status::OK();
            }
        }

        COUNTER_UPDATE(_rows_read_counter, 1);
        SCOPED_TIMER(_materialize_timer);
        RETURN_IF_ERROR(_es_scroll_parser->fill_columns(_tuple_desc, columns, tuple_pool,
                                                        &_line_eof, docvalue_context));
        if (!_line_eof) {
            break;
        }
    }

    return Status::OK();
}

} // namespace doris::vectorized
