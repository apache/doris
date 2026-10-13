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

#include <cstddef>
#include <cstdint>

#include "core/custom_allocator.h"
#include "core/value/variant/variant_value.h"

namespace doris {

// Reusable form of validate_variant_payload() (variant_field.h), which is the one-shot call for a
// single value. Checking an object needs the byte spans of its field values, and this validator
// keeps them in one memory-tracked buffer that every later call reuses. A caller that validates
// many rows holds one validator for all of them, so a batch of 4096 object rows grows the buffer a
// few times instead of allocating once per object. The class is defined in variant_field.cpp next
// to the checks it shares with validate_variant_payload(); it is declared apart from
// variant_field.h so that core/field.h, which includes that header, does not also pull in
// core/custom_allocator.h.
class VariantPayloadValidator {
public:
    // Validate exactly one recursive Variant payload, with the same checks and errors as
    // validate_variant_payload(). The referenced metadata must already have passed
    // validate_variant_metadata().
    void validate(VariantRef value);

#ifdef BE_TEST
    size_t object_span_capacity_for_test() const noexcept { return _object_spans.capacity(); }
#endif

private:
    // The bytes one field value takes inside its object's value section.
    struct ObjectValueSpan {
        size_t offset;
        size_t size;
    };

    void require_exact_value(VariantRef value, uint32_t depth);
    void require_valid_object(VariantRef value, uint32_t depth);
    void require_valid_array(VariantRef value, uint32_t depth);

    // A stack shared by the objects on the path from the root to the value being checked. Each
    // object pushes the span of a field after checking that field, so a nested object pushes its
    // own spans above those of its ancestors and pops them before it returns. For
    // {"a":1,"b":{"c":2,"d":3},"e":4}, the stack holds [a] when the root starts checking "b";
    // "b" grows it to [a, c, d], checks its own two entries and pops back to [a], and the root
    // then pushes "b" and "e" and checks [a, b, e].
    DorisVector<ObjectValueSpan> _object_spans;
};

} // namespace doris
