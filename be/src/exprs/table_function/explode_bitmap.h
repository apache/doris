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
#include "util/bitmap_value.h"

namespace doris {

class ExplodeBitmapTableFunction : public TableFunction {
public:
    ExplodeBitmapTableFunction();
    virtual ~ExplodeBitmapTableFunction();

    virtual Status process(TupleRow* tuple_row) override;
    virtual Status reset() override;
    virtual Status get_value(void** output) override;

    virtual Status forward(bool* eos) override;

private:
    void _reset_iterator();

private:
    // Read from tuple row.
    // if _cur_bitmap_owned is true, need to delete it when deconstruction
    BitmapValue* _cur_bitmap = nullptr;
    bool _cur_bitmap_owned = false;
    // iterator of _cur_bitmap
    BitmapValueIterator* _cur_iter = nullptr;
    // current value read from bitmap, it will be referenced by
    // table function scan node.
    uint64_t _cur_value = 0;
};

} // namespace doris
