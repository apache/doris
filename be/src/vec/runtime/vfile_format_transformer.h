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
#include <vector>

#include "common/status.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris::vectorized {

class VFileFormatTransformer {
public:
    VFileFormatTransformer(const VExprContextSPtrs& output_vexpr_ctxs, bool output_object_data)
            : _output_vexpr_ctxs(output_vexpr_ctxs),
              _cur_written_rows(0),
              _output_object_data(output_object_data) {
        DataTypes data_types;
        for (int i = 0; i < output_vexpr_ctxs.size(); ++i) {
            data_types.push_back(output_vexpr_ctxs[i]->root()->data_type());
        }
        _options._output_object_data = output_object_data;
        _serdes = vectorized::create_data_type_serdes(data_types);
    }

    virtual ~VFileFormatTransformer() = default;

    virtual Status open() = 0;
    virtual Status write(const Block& block) = 0;
    virtual Status close() = 0;
    virtual int64_t written_len() = 0;

protected:
    const VExprContextSPtrs& _output_vexpr_ctxs;
    int64_t _cur_written_rows;
    bool _output_object_data;
    vectorized::DataTypeSerDeSPtrs _serdes;
    vectorized::DataTypeSerDe::FormatOptions _options;
};
} // namespace doris::vectorized
