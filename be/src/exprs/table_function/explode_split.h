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

#include "exprs/table_function/table_function.h"
#include "gutil/strings/stringpiece.h"
#include "runtime/string_value.h"

namespace doris {

class ExplodeSplitTableFunction : public TableFunction {
public:
    ExplodeSplitTableFunction();
    virtual ~ExplodeSplitTableFunction();

    virtual Status open() override;
    virtual Status process(TupleRow* tuple_row) override;
    virtual Status reset() override;
    virtual Status get_value(void** output) override;

protected:
    // The string value splitted from source, and will be referenced by
    // table function scan node.
    // the `_backup` saved the real string entity.
    std::vector<StringValue> _data;
    std::vector<std::string> _backup;

    // indicate whether the delimiter is constant.
    // if true, the constant delimiter will be saved in `_const_delimter`
    bool _is_delimiter_constant = false;
    StringPiece _const_delimter;
};

} // namespace doris
