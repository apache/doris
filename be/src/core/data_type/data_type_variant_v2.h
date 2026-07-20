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

#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_variant.h"

namespace doris {

class DataTypeVariantV2 final : public DataTypeVariant {
public:
    DataTypeVariantV2() = default;
    explicit DataTypeVariantV2(int32_t max_subcolumns_count)
            : DataTypeVariant(max_subcolumns_count) {}
    DataTypeVariantV2(int32_t max_subcolumns_count, bool enable_doc_mode)
            : DataTypeVariant(max_subcolumns_count, enable_doc_mode) {}

    MutableColumnPtr create_column() const override { return ColumnVariantV2::create(); }
    bool is_variant_v2() const override { return true; }
};

} // namespace doris
