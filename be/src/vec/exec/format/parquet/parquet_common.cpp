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

#include "parquet_common.h"

namespace doris::vectorized {

Status Decoder::getDecoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                           std::unique_ptr<Decoder>& decoder) {
    switch (encoding) {
    case tparquet::Encoding::PLAIN:
        switch (type) {
        case tparquet::Type::INT32:
            decoder.reset(new PlainDecoder<Int32>());
            break;
        case tparquet::Type::INT64:
            decoder.reset(new PlainDecoder<Int64>());
            break;
        case tparquet::Type::FLOAT:
            decoder.reset(new PlainDecoder<Float32>());
            break;
        case tparquet::Type::DOUBLE:
            decoder.reset(new PlainDecoder<Float64>());
            break;
        default:
            return Status::InternalError("Unsupported plain type {} in parquet decoder",
                                         tparquet::to_string(type));
        }
    case tparquet::Encoding::RLE_DICTIONARY:
        break;
    default:
        return Status::InternalError("Unsupported encoding {} in parquet decoder",
                                     tparquet::to_string(encoding));
    }
    return Status::OK();
}

MutableColumnPtr Decoder::getMutableColumnPtr(ColumnPtr& doris_column) {
    // src column always be nullable for simple converting
    CHECK(doris_column->is_nullable());
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
            (*std::move(doris_column)).mutate().get());
    return nullable_column->get_nested_column_ptr();
}

} // namespace doris::vectorized
