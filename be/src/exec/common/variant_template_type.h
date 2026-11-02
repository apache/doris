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

#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"

namespace doris::variant_util {

// Single decision point for "is a VARIANT subcolumn stored as its template type?".
//
// A segment written before a template ADD may store a template path as another type: an old
// dynamic leaf (e.g. BIGINT under an INT template) or a JSONB carrier. Readers, compaction
// planning and the fixed partial update writer must agree on when the stored type can stand in
// for the template type. They use two notions on purpose:
//
// * Type level (this function): the stored type equals the template type ignoring nullability.
//   Compaction keeps a path typed over old leaves, and readers trust zone maps, bloom filters,
//   dictionaries and indexes, only when this holds. Type metadata alone cannot prove that every
//   historical value survives a cast, so anything weaker would cast old values or apply a typed
//   predicate to statistics of another type.
// * Value level (variant_copied_value_fits_template): a fixed partial update has the actual
//   copied values in hand, so it keeps the template type when every value casts to it and
//   round-trips unchanged (BIGINT 5 into INT keeps the template; "001" into INT does not).
//   A path that fails is written untyped and later compaction reconciles it with the type-level
//   rule above, so both notions converge on the same stored layout.
inline bool stored_type_equals_template(const DataTypePtr& stored_type,
                                        const DataTypePtr& template_type) {
    return stored_type != nullptr && template_type != nullptr &&
           remove_nullable(stored_type)->equals(*remove_nullable(template_type));
}

} // namespace doris::variant_util
