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

#ifndef DORIS_BE_SRC_TESTUTIL_DESC_TBL_BUILDER_H
#define DORIS_BE_SRC_TESTUTIL_DESC_TBL_BUILDER_H

#include <gen_cpp/Descriptors_types.h>

#include <tuple>
#include <vector>

#include "common/object_pool.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace doris {

class TupleDescBuilder;

// Aids in the construction of a DescriptorTbl by declaring tuples and slots
// associated with those tuples.
// TupleIds are monotonically increasing from 0 for each declare_tuple, and
// SlotIds increase similarly, but are always greater than all TupleIds.
// Unlike FE, slots are not reordered based on size, and padding is not added.
//
// Example usage:
// DescriptorTblBuilder builder;
// builder.declare_tuple() << TYPE_TINYINT << TYPE_TIMESTAMP; // gets TupleId 0
// builder.declare_tuple() << TYPE_FLOAT; // gets TupleId 1
// builder.declare_tuple() << std::make_tuple(TYPE_INT, "col1") << std::make_tuple(TYPE_STRING, "col2"); // gets Tuple with type and name
// DescriptorTbl desc_tbl = builder.build();
class DescriptorTblBuilder {
public:
    DescriptorTblBuilder(ObjectPool* object_pool);
    // a null dtor to pass codestyle check
    ~DescriptorTblBuilder() {}

    TupleDescBuilder& declare_tuple();
    DescriptorTbl* build();

private:
    // Owned by caller.
    ObjectPool* _obj_pool = nullptr;

    std::vector<TupleDescBuilder*> _tuples_descs;

    TTupleDescriptor build_tuple(const std::vector<TypeDescriptor>& slot_types,
                                 const std::vector<std::string>& slot_names,
                                 TDescriptorTable* thrift_desc_tbl, int* tuple_id, int* slot_id);
};

class TupleDescBuilder {
public:
    using SlotType = std::tuple<TypeDescriptor, std::string>;
    TupleDescBuilder& operator<<(const SlotType& slot) {
        _slot_types.push_back(std::get<0>(slot));
        _slot_names.push_back(std::get<1>(slot));
        return *this;
    }

    TupleDescBuilder& operator<<(const TypeDescriptor& slot_type) {
        _slot_types.push_back(slot_type);
        _slot_names.emplace_back("");
        return *this;
    }

    std::vector<TypeDescriptor> slot_types() const { return _slot_types; }
    std::vector<std::string> slot_names() const { return _slot_names; }

private:
    std::vector<TypeDescriptor> _slot_types;
    std::vector<std::string> _slot_names;
};

} // end namespace doris

#endif // DORIS_BE_SRC_TESTUTIL_DESC_TBL_BUILDER_H
