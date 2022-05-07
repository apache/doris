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

#include <iostream>
#include <memory>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include "common/status.h"
#include "runtime/primitive_type.h"
#include "vec/core/column_with_type_and_name.h"

// This files contains some utilities to convert Doris internal
// data format from Apache Arrow format. 
namespace doris::vectorized {

const PrimitiveType arrow_type_to_primitive_type(::arrow::Type::type type);

Status arrow_column_to_doris_column(const arrow::Array* arrow_column,
                                    size_t arrow_batch_cur_idx,
                                    ColumnWithTypeAndName& doirs_column,
                                    size_t num_elements,
                                    const std::string& timezone);
} // namespace doris
