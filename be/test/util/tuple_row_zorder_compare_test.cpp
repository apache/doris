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

#include "util/tuple_row_zorder_compare.h"

#include <gtest/gtest.h>

#include "common/logging.h"
#include "exec/sort_exec_exprs.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "olap/memtable.h"
#include "olap/row.h"
#include "olap/schema.h"
#include "runtime/descriptors.h"
#include "runtime/large_int_value.h"
#include "runtime/raw_value.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/json_util.h"

namespace doris {
class TupleRowZOrderCompareTest : public testing::Test {
public:
    ObjectPool _agg_buffer_pool;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _buffer_mem_pool;

    TupleRowZOrderCompareTest() {
        _mem_tracker.reset(new MemTracker(-1));
        _buffer_mem_pool.reset(new MemPool(_mem_tracker.get()));
    }

    ~TupleRowZOrderCompareTest() = default;

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareInt8Test(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::TINYINT);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(int8_t) + 1 * i;

            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(int8_t) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;
            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_TINYINT, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_TINYINT, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int8_t),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1, lval2);
        uint8_t* lhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int8_t));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int8_t),
                                         _buffer_mem_pool.get());
        FillMem(rhs_tuple, 1, rval1, rval2);
        uint8_t* rhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int8_t));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareInt16Test(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::SMALLINT);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(int16_t) + 1 * i;

            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(int16_t) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;

            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_SMALLINT, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_SMALLINT, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int16_t),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1, lval2);
        uint8_t* lhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int16_t));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int16_t),
                                         _buffer_mem_pool.get());
        FillMem(rhs_tuple, 1, rval1, rval2);
        uint8_t* rhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int16_t));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareIntTest(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::INT);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(int32_t) + 1 * i;
            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(int32_t) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;
            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_INT, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_INT, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int32_t),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1, lval2);
        uint8_t* lhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int32_t));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int32_t),
                                         _buffer_mem_pool.get());
        FillMem(rhs_tuple, 1, rval1, rval2);
        uint8_t* rhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int32_t));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareInt64Test(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::BIGINT);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(int64_t) + 1 * i;
            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(int64_t) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;
            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_BIGINT, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_BIGINT, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int64_t),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1, lval2);
        uint8_t* lhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int64_t));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int64_t),
                                         _buffer_mem_pool.get());
        FillMem(rhs_tuple, 1, rval1, rval2);
        uint8_t* rhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int64_t));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareInt128Test(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::LARGEINT);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(int128_t) + 1 * i;
            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(int128_t) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;
            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_LARGEINT, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_LARGEINT, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int128_t),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1, lval2);
        uint8_t* lhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int128_t));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int128_t),
                                         _buffer_mem_pool.get());
        FillMem(rhs_tuple, 1, rval1, rval2);
        uint8_t* rhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int128_t));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareFloatTest(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::FLOAT);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(float) + 1 * i;
            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(float) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;
            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_FLOAT, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_FLOAT, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(float),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1, lval2);
        uint8_t* lhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(float));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(float),
                                         _buffer_mem_pool.get());
        FillMem(rhs_tuple, 1, rval1, rval2);
        uint8_t* rhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(float));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareDoubleTest(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::DOUBLE);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(double) + 1 * i;
            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(double) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;

            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DOUBLE, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DOUBLE, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(double),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1, lval2);
        uint8_t* lhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(double));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(double),
                                         _buffer_mem_pool.get());
        FillMem(rhs_tuple, 1, rval1, rval2);
        uint8_t* rhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(double));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareBoolTest(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::BOOLEAN);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(bool) + 1 * i;
            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(bool) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;
            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_BOOL, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_BOOL, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(bool),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1, lval2);
        uint8_t* lhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(bool));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(bool),
                                         _buffer_mem_pool.get());
        FillMem(rhs_tuple, 1, rval1, rval2);
        uint8_t* rhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(bool));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareCharTest(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::VARCHAR);
                scalar_type.__set_len(65535);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(StringValue) + 1 * i;
            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(StringValue) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;
            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(StringValue),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1, lval2);
        uint8_t* lhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(StringValue));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(StringValue),
                                         _buffer_mem_pool.get());
        FillMem(rhs_tuple, 1, rval1, rval2);
        uint8_t* rhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(StringValue));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareDateTest(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::DATETIME);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(DateTimeValue) + 1 * i;
            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(DateTimeValue) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;
            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DATETIME, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DATETIME, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(DateTimeValue),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1, lval2);
        uint8_t* lhs_tuple_buf = _buffer_mem_pool->allocate(col_num * sizeof(char) +
                                                            col_num * sizeof(DateTimeValue));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(DateTimeValue),
                                         _buffer_mem_pool.get());
        FillMem(rhs_tuple, 1, rval1, rval2);
        uint8_t* rhs_tuple_buf = _buffer_mem_pool->allocate(col_num * sizeof(char) +
                                                            col_num * sizeof(DateTimeValue));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    template <typename T, bool IS_FIRST_SLOT_NULL = false>
    int CompareDecimalTest(T lval1, T lval2, T rval1, T rval2) {
        int col_num = 2;
        std::vector<SlotDescriptor> slot_descs;
        for (int i = 1; i <= col_num; i++) {
            TSlotDescriptor slot_desc;
            slot_desc.id = i;
            slot_desc.parent = 0;
            TTypeDesc type;
            {
                TTypeNode node;
                node.__set_type(TTypeNodeType::SCALAR);
                TScalarType scalar_type;
                scalar_type.__set_type(TPrimitiveType::type::DECIMALV2);
                scalar_type.__isset.precision = true;
                scalar_type.__isset.scale = true;
                scalar_type.__set_precision(-1);
                scalar_type.__set_scale(-1);
                node.__set_scalar_type(scalar_type);
                type.types.push_back(node);
            }
            slot_desc.slotType = type;
            slot_desc.columnPos = i;
            slot_desc.byteOffset = (i - 1) * sizeof(int128_t) + 1 * i;
            slot_desc.nullIndicatorByte = 0 + (i - 1) * sizeof(int128_t) + 1 * (i - 1);
            slot_desc.nullIndicatorBit = 1;
            std::ostringstream ss;
            ss << "col_" << i;
            slot_desc.colName = ss.str();
            slot_desc.slotIdx = i;
            slot_desc.isMaterialized = true;
            slot_descs.emplace_back(slot_desc);
        }

        std::vector<TabletColumn> col_schemas;
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DECIMAL, true);
        col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DECIMAL, true);

        Schema schema = Schema(col_schemas, col_num);
        TupleRowZOrderComparator comparator(&schema, 2);
        Tuple* lhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int128_t),
                                         _buffer_mem_pool.get());
        if (IS_FIRST_SLOT_NULL) {
            lhs_tuple->set_null(NullIndicatorOffset(0, 1));
        }
        FillMem(lhs_tuple, 1, lval1.value(), lval2.value());
        uint8_t* lhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int128_t));
        ContiguousRow lhs_row(&schema, lhs_tuple_buf);
        tuple_to_row(schema, slot_descs, lhs_tuple, &lhs_row, _buffer_mem_pool.get());

        Tuple* rhs_tuple = Tuple::create(col_num * sizeof(char) + col_num * sizeof(int128_t),
                                         _buffer_mem_pool.get());

        FillMem(rhs_tuple, 1, rval1.value(), rval2.value());
        uint8_t* rhs_tuple_buf =
                _buffer_mem_pool->allocate(col_num * sizeof(char) + col_num * sizeof(int128_t));
        ContiguousRow rhs_row(&schema, rhs_tuple_buf);
        tuple_to_row(schema, slot_descs, rhs_tuple, &rhs_row, _buffer_mem_pool.get());
        int result = comparator.compare(reinterpret_cast<const char*>(lhs_row.row_ptr()),
                                        reinterpret_cast<const char*>(rhs_row.row_ptr()));
        return result;
    }

    void tuple_to_row(const Schema& schema, const vector<SlotDescriptor>& slot_descs,
                      const Tuple* tuple, ContiguousRow* row, MemPool* mem_pool) {
        for (size_t i = 0; i < slot_descs.size(); ++i) {
            auto cell = row->cell(i);
            const SlotDescriptor& slot = slot_descs[i];
            bool is_null = tuple->is_null(slot.null_indicator_offset());
            const void* value = tuple->get_slot(slot.tuple_offset());
            schema.column(i)->consume(&cell, (const char*)value, is_null, mem_pool,
                                      &_agg_buffer_pool);
        }
    }

    template <typename T>
    void FillMem(Tuple* tuple_mem, int idx, T val) {
        memcpy(tuple_mem->get_slot(idx), &val, sizeof(T));
    }

    template <typename T, typename... Args>
    void FillMem(Tuple* tuple_mem, int idx, T val, Args... args) {
        // Use memcpy to avoid gcc generating unaligned instructions like movaps
        // for int128_t. They will raise SegmentFault when addresses are not
        // aligned to 16 bytes.
        memcpy(tuple_mem->get_slot(idx), &val, sizeof(T));
        FillMem(tuple_mem, idx + sizeof(T) + 1, args...);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int Int8Int8Test(int8_t lval1, int8_t lval2, int8_t rval1, int8_t rval2) {
        return CompareInt8Test<int8_t, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int Int16Int16Test(int16_t lval1, int16_t lval2, int16_t rval1, int16_t rval2) {
        return CompareInt16Test<int16_t, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int IntIntTest(int32_t lval1, int32_t lval2, int32_t rval1, int32_t rval2) {
        return CompareIntTest<int32_t, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int Int64Int64Test(int64_t lval1, int64_t lval2, int64_t rval1, int64_t rval2) {
        return CompareInt64Test<int64_t, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int Int128Int128Test(int128_t lval1, int128_t lval2, int128_t rval1, int128_t rval2) {
        return CompareInt128Test<int128_t, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int FloatFloatTest(float lval1, float lval2, float rval1, float rval2) {
        return CompareFloatTest<float, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int DoubleDoubleTest(double lval1, double lval2, double rval1, double rval2) {
        return CompareDoubleTest<double, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int BoolBoolTest(bool lval1, bool lval2, bool rval1, bool rval2) {
        return CompareBoolTest<bool, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int CharCharTest(StringValue lval1, StringValue lval2, StringValue rval1, StringValue rval2) {
        return CompareCharTest<StringValue, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int DateDateTest(DateTimeValue lval1, DateTimeValue lval2, DateTimeValue rval1,
                     DateTimeValue rval2) {
        return CompareDateTest<DateTimeValue, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }

    template <bool IS_FIRST_SLOT_NULL = false>
    int DecimalDecimalTest(DecimalV2Value lval1, DecimalV2Value lval2, DecimalV2Value rval1,
                           DecimalV2Value rval2) {
        return CompareDecimalTest<DecimalV2Value, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
    }
};

TEST_F(TupleRowZOrderCompareTest, DecimalTest) {
    std::string str1 = "1.00";
    std::string str2 = "1.00";
    std::string str3 = "1.00";
    std::string str4 = "1.00";
    DecimalV2Value val1(str1);
    DecimalV2Value val2(str2);
    DecimalV2Value val3(str3);
    DecimalV2Value val4(str4);
    EXPECT_EQ(DecimalDecimalTest(val1, val2, val3, val4), 0);
    str1 = "-5.0";
    str2 = "3.0";
    str3 = "-5.0";
    str4 = "3.0";
    DecimalV2Value val5(str1);
    DecimalV2Value val6(str2);
    DecimalV2Value val7(str3);
    DecimalV2Value val8(str4);
    EXPECT_EQ(DecimalDecimalTest(val5, val6, val7, val8), 0);
    str1 = "1.0";
    str2 = "0.0";
    str3 = "0.0";
    str4 = "1.0";
    DecimalV2Value val9(str1);
    DecimalV2Value val10(str2);
    DecimalV2Value val11(str3);
    DecimalV2Value val12(str4);
    EXPECT_EQ(DecimalDecimalTest(val9, val10, val11, val12), 1);
    str1 = "0";
    str2 = "1";
    str3 = "1";
    str4 = "0";
    DecimalV2Value val13(str1);
    DecimalV2Value val14(str2);
    DecimalV2Value val15(str3);
    DecimalV2Value val16(str4);
    EXPECT_EQ(DecimalDecimalTest(val13, val14, val15, val16), -1);
    str1 = "256";
    str2 = "10";
    str3 = "255";
    str4 = "100";
    DecimalV2Value val17(str1);
    DecimalV2Value val18(str2);
    DecimalV2Value val19(str3);
    DecimalV2Value val20(str4);
    EXPECT_EQ(DecimalDecimalTest(val17, val18, val19, val20), -1);
    str1 = "3";
    str2 = "1024";
    str3 = "128";
    str4 = "1023";
    DecimalV2Value val21(str1);
    DecimalV2Value val22(str2);
    DecimalV2Value val23(str3);
    DecimalV2Value val24(str4);
    EXPECT_EQ(DecimalDecimalTest(val21, val22, val23, val24), -1);
    str1 = "1024";
    str2 = "511";
    str3 = "1023";
    str4 = "0";
    DecimalV2Value val25(str1);
    DecimalV2Value val26(str2);
    DecimalV2Value val27(str3);
    DecimalV2Value val28(str4);
    EXPECT_EQ(DecimalDecimalTest(val25, val26, val27, val28), 1);
    str1 = "5550";
    str2 = "0";
    str3 = "5000";
    str4 = "4097";
    DecimalV2Value val29(str1);
    DecimalV2Value val30(str2);
    DecimalV2Value val31(str3);
    DecimalV2Value val32(str4);
    EXPECT_EQ(DecimalDecimalTest(val29, val30, val31, val32), -1);
}

TEST_F(TupleRowZOrderCompareTest, DateDateTest) {
    DateTimeValue val1;
    DateTimeValue val2;
    DateTimeValue val3;
    DateTimeValue val4;
    std::string str1 = "2015-04-09 14:07:46";
    std::string str2 = "2015-04-09 14:07:46";
    std::string str3 = "2015-04-09 14:07:46";
    std::string str4 = "2015-04-09 14:07:46";
    val1.from_date_str(str1.c_str(), str1.size());
    val2.from_date_str(str2.c_str(), str2.size());
    val3.from_date_str(str3.c_str(), str3.size());
    val4.from_date_str(str4.c_str(), str4.size());
    EXPECT_EQ(DateDateTest(val1, val2, val3, val4), 0);
    str1 = "1415-12-09 10:07:44";
    str2 = "2015-04-09 14:07:46";
    str3 = "1415-12-09 10:07:44";
    str4 = "2015-04-09 14:07:46";
    val1.from_date_str(str1.c_str(), str1.size());
    val2.from_date_str(str2.c_str(), str2.size());
    val3.from_date_str(str3.c_str(), str3.size());
    val4.from_date_str(str4.c_str(), str4.size());
    EXPECT_EQ(DateDateTest(val1, val2, val3, val4), 0);
    str1 = "1400-01-01 00:00:00";
    str2 = "9999-12-31 14:07:46";
    str3 = "8000-12-09 10:07:44";
    str4 = "2015-04-09 14:07:46";
    val1.from_date_str(str1.c_str(), str1.size());
    val2.from_date_str(str2.c_str(), str2.size());
    val3.from_date_str(str3.c_str(), str3.size());
    val4.from_date_str(str4.c_str(), str4.size());
    EXPECT_EQ(DateDateTest(val1, val2, val3, val4), -1);
    str1 = "1400-01-01 00:00:01";
    str2 = "1400-01-01 00:00:00";
    str3 = "1400-01-01 00:00:00";
    str4 = "1400-01-01 00:00:01";
    val1.from_date_str(str1.c_str(), str1.size());
    val2.from_date_str(str2.c_str(), str2.size());
    val3.from_date_str(str3.c_str(), str3.size());
    val4.from_date_str(str4.c_str(), str4.size());
    EXPECT_EQ(DateDateTest(val1, val2, val3, val4), 1);
    str1 = "1400-01-01 00:00:03";
    str2 = "1400-01-01 00:00:07";
    str3 = "1400-01-01 00:00:04";
    str4 = "1400-01-01 00:00:00";
    val1.from_date_str(str1.c_str(), str1.size());
    val2.from_date_str(str2.c_str(), str2.size());
    val3.from_date_str(str3.c_str(), str3.size());
    val4.from_date_str(str4.c_str(), str4.size());
    EXPECT_EQ(DateDateTest(val1, val2, val3, val4), -1);
    str1 = "1400-01-01 00:00:06";
    str2 = "1400-01-01 00:00:04";
    str3 = "1400-01-01 00:00:05";
    str4 = "1400-01-01 00:00:07";
    val1.from_date_str(str1.c_str(), str1.size());
    val2.from_date_str(str2.c_str(), str2.size());
    val3.from_date_str(str3.c_str(), str3.size());
    val4.from_date_str(str4.c_str(), str4.size());
    EXPECT_EQ(DateDateTest(val1, val2, val3, val4), 1);
    str1 = "1400-01-01 00:00:05";
    str2 = "1400-01-01 00:00:05";
    str3 = "1400-01-01 00:00:06";
    str4 = "1400-01-01 00:00:04";
    val1.from_date_str(str1.c_str(), str1.size());
    val2.from_date_str(str2.c_str(), str2.size());
    val3.from_date_str(str3.c_str(), str3.size());
    val4.from_date_str(str4.c_str(), str4.size());
    EXPECT_EQ(DateDateTest(val1, val2, val3, val4), -1);
    str1 = "1400-01-01 23:59:59";
    str2 = "1400-01-01 00:00:00";
    str3 = "1400-01-02 00:00:00";
    str4 = "1400-01-01 00:00:00";
    val1.from_date_str(str1.c_str(), str1.size());
    val2.from_date_str(str2.c_str(), str2.size());
    val3.from_date_str(str3.c_str(), str3.size());
    val4.from_date_str(str4.c_str(), str4.size());
    EXPECT_EQ(DateDateTest(val1, val2, val3, val4), -1);
    str1 = "3541-11-03 23:59:59";
    str2 = "3541-11-03 00:00:00";
    str3 = "3541-11-04 00:00:00";
    str4 = "3541-11-03 00:00:00";
    val1.from_date_str(str1.c_str(), str1.size());
    val2.from_date_str(str2.c_str(), str2.size());
    val3.from_date_str(str3.c_str(), str3.size());
    val4.from_date_str(str4.c_str(), str4.size());
    EXPECT_EQ(DateDateTest(val1, val2, val3, val4), -1);
}
TEST_F(TupleRowZOrderCompareTest, CharTest) {
    EXPECT_EQ(CharCharTest(StringValue("a"), StringValue("b"), StringValue("a"), StringValue("b")),
              0);
    EXPECT_EQ(CharCharTest(StringValue("a"), StringValue("b"), StringValue("a"), StringValue("b")),
              0);
    EXPECT_EQ(CharCharTest(StringValue("h"), StringValue("0"), StringValue("h"), StringValue("0")),
              0);
    EXPECT_EQ(CharCharTest(StringValue("h"), StringValue("z"), StringValue("z"), StringValue("h")),
              -1);
    EXPECT_EQ(CharCharTest(StringValue("a"), StringValue("0"), StringValue("h"), StringValue("0")),
              -1);
    EXPECT_EQ(CharCharTest(StringValue("!"), StringValue("{"), StringValue("0"), StringValue("K")),
              1);
    EXPECT_EQ(CharCharTest(StringValue("A"), StringValue("~"), StringValue("B"), StringValue("Z")),
              1);
    EXPECT_EQ(CharCharTest(StringValue("aaa"), StringValue("bbb"), StringValue("aaa"),
                           StringValue("bbb")),
              0);
    EXPECT_EQ(CharCharTest(StringValue("abc"), StringValue("bbc"), StringValue("abc"),
                           StringValue("bbc")),
              0);
    EXPECT_EQ(CharCharTest(StringValue("aah"), StringValue("aa0"), StringValue("aah"),
                           StringValue("aa0")),
              0);
    EXPECT_EQ(CharCharTest(StringValue("aaa"), StringValue("aa0"), StringValue("aah"),
                           StringValue("aa0")),
              -1);
    EXPECT_EQ(CharCharTest(StringValue("aah"), StringValue("aaz"), StringValue("aaz"),
                           StringValue("aah")),
              -1);
    EXPECT_EQ(CharCharTest(StringValue("aa!"), StringValue("aa{"), StringValue("aa0"),
                           StringValue("aaK")),
              1);
    EXPECT_EQ(CharCharTest(StringValue("aaA"), StringValue("aa~"), StringValue("aaB"),
                           StringValue("aaZ")),
              1);
}
TEST_F(TupleRowZOrderCompareTest, BoolTest) {
    EXPECT_EQ(BoolBoolTest(true, false, true, false), 0);
    EXPECT_EQ(BoolBoolTest(false, true, false, true), 0);
    EXPECT_EQ(BoolBoolTest(true, true, true, false), 1);
    EXPECT_EQ(BoolBoolTest(false, true, true, true), -1);
    EXPECT_EQ(BoolBoolTest(false, true, false, false), 1);
    EXPECT_EQ(BoolBoolTest(false, false, false, true), -1);
    EXPECT_EQ(BoolBoolTest(true, false, false, false), 1);
}
TEST_F(TupleRowZOrderCompareTest, DoubleTest) {
    EXPECT_EQ(DoubleDoubleTest(1.0, 0.0, 0.0, 1.0f), 1);
    EXPECT_EQ(DoubleDoubleTest(0.0, 1.0, 1.0, 0.0f), -1);
    EXPECT_EQ(DoubleDoubleTest(4.0, 3.0, 3.0, 4.0), 1);
    EXPECT_EQ(DoubleDoubleTest(5.0, 7.0, 4.0, 10.0), -1);
    EXPECT_EQ(DoubleDoubleTest(6.0, 10.0, 7.0, 3.0), 1);
    EXPECT_EQ(DoubleDoubleTest(9.0, 7.0, 8.0, 10.0), -1);
    EXPECT_EQ(DoubleDoubleTest(8.0, 8.0, 9.0, 7.0), 1);
    EXPECT_EQ(DoubleDoubleTest(9.0, 4.0, 6.0, 10.0), 1);
    EXPECT_EQ(DoubleDoubleTest(-4.0, -3.0, -3.0, -4.0), -1);
    EXPECT_EQ(DoubleDoubleTest(-5.0, -7.0, -4.0, -10.0), 1);
    EXPECT_EQ(DoubleDoubleTest(-6.0, -10.0, -7.0, -3.0), -1);
    EXPECT_EQ(DoubleDoubleTest(-9.0, -7.0, -8.0, -10.0), 1);
    EXPECT_EQ(DoubleDoubleTest(-8.0, -8.0, -9.0, -7.0), -1);
    EXPECT_EQ(DoubleDoubleTest(-9.0, -4.0, -6.0, -10.0), -1);
    EXPECT_EQ(DoubleDoubleTest(DBL_MAX / 2.0 + 2.0, 1.0, 1.0, DBL_MAX), 1);
}
TEST_F(TupleRowZOrderCompareTest, FloatTest) {
    EXPECT_EQ(FloatFloatTest(1.0f, 0.0f, 0.0f, 1.0f), 1);
    EXPECT_EQ(FloatFloatTest(0.0f, 1.0f, 1.0f, 0.0f), -1);
    EXPECT_EQ(FloatFloatTest(4.0f, 3.0f, 3.0f, 4.0f), 1);
    EXPECT_EQ(FloatFloatTest(5.0f, 7.0f, 4.0f, 10.0f), -1);
    EXPECT_EQ(FloatFloatTest(6.0f, 10.0f, 7.0f, 3.0f), 1);
    EXPECT_EQ(FloatFloatTest(9.0f, 7.0f, 8.0f, 10.0f), -1);
    EXPECT_EQ(FloatFloatTest(8.0f, 8.0f, 9.0f, 7.0f), 1);
    EXPECT_EQ(FloatFloatTest(9.0f, 4.0f, 6.0f, 10.0f), 1);
    EXPECT_EQ(FloatFloatTest(-4.0f, -3.0f, -3.0f, -4.0f), -1);
    EXPECT_EQ(FloatFloatTest(-5.0f, -7.0f, -4.0f, -10.0f), 1);
    EXPECT_EQ(FloatFloatTest(-6.0f, -10.0f, -7.0f, -3.0f), -1);
    EXPECT_EQ(FloatFloatTest(-9.0f, -7.0f, -8.0f, -10.0f), 1);
    EXPECT_EQ(FloatFloatTest(-8.0f, -8.0f, -9.0f, -7.0f), -1);
    EXPECT_EQ(FloatFloatTest(-9.0f, -4.0f, -6.0f, -10.0f), -1);
    EXPECT_EQ(FloatFloatTest(FLT_MAX / 2.0f + 2.0f, 1.0f, 1.0f, FLT_MAX), 1);
}
TEST_F(TupleRowZOrderCompareTest, Int8Test) {
    EXPECT_EQ(Int8Int8Test(0, 0, 0, 0), 0);
    EXPECT_EQ(Int8Int8Test(-5, 3, -5, 3), 0);
    EXPECT_EQ(Int8Int8Test(1, 0, 0, 1), 1);
    EXPECT_EQ(Int8Int8Test(0, 1, 1, 0), -1);
    EXPECT_EQ(Int8Int8Test(1, 0, 0, 1), 1);
    EXPECT_EQ(Int8Int8Test(2, 4, 1, 7), 1);
    EXPECT_EQ(Int8Int8Test(3, 7, 4, 0), -1);
    EXPECT_EQ(Int8Int8Test(6, 4, 5, 7), 1);
    EXPECT_EQ(Int8Int8Test(5, 5, 6, 4), -1);
    EXPECT_EQ(Int8Int8Test(6, 1, 3, 7), 1);
    EXPECT_EQ(Int8Int8Test(INT8_MAX / 2 + 2, 1, 1, INT8_MAX), 1);
    EXPECT_EQ(Int8Int8Test(INT8_MAX / 2, 1, 1, INT8_MAX), -1);
}
TEST_F(TupleRowZOrderCompareTest, Int16Test) {
    EXPECT_EQ(Int16Int16Test(0, 0, 0, 0), 0);
    EXPECT_EQ(Int16Int16Test(-5, 3, -5, 3), 0);
    EXPECT_EQ(Int16Int16Test(1, 0, 0, 1), 1);
    EXPECT_EQ(Int16Int16Test(0, 1, 1, 0), -1);
    EXPECT_EQ(Int16Int16Test(1, 0, 0, 1), 1);
    EXPECT_EQ(Int16Int16Test(2, 4, 1, 7), 1);
    EXPECT_EQ(Int16Int16Test(3, 7, 4, 0), -1);
    EXPECT_EQ(Int16Int16Test(6, 4, 5, 7), 1);
    EXPECT_EQ(Int16Int16Test(5, 5, 6, 4), -1);
    EXPECT_EQ(Int16Int16Test(6, 1, 3, 7), 1);
    EXPECT_EQ(Int16Int16Test(INT16_MAX / 2 + 2, 1, 1, INT16_MAX), 1);
    EXPECT_EQ(Int16Int16Test(INT16_MAX / 2, 1, 1, INT16_MAX), -1);
}

TEST_F(TupleRowZOrderCompareTest, Int32Test) {
    EXPECT_EQ(IntIntTest(0, 2, 1, 1), 1);
    EXPECT_EQ(IntIntTest(-5, 3, -5, 3), 0);

    EXPECT_EQ(IntIntTest(0, 4, 1, 2), 1);
    EXPECT_EQ(IntIntTest(0, 1, 1, 0), -1);

    EXPECT_EQ(IntIntTest(1, 0, 0, 1), 1);
    EXPECT_EQ(IntIntTest(2, 4, 1, 7), 1);
    EXPECT_EQ(IntIntTest(3, 7, 4, 0), -1);
    EXPECT_EQ(IntIntTest(6, 4, 5, 7), 1);
    EXPECT_EQ(IntIntTest(5, 5, 6, 4), -1);
    EXPECT_EQ(IntIntTest(6, 1, 3, 7), 1);

    EXPECT_EQ(IntIntTest(INT32_MAX / 2 + 2, 1, 1, INT32_MAX), 1);
    EXPECT_EQ(IntIntTest(INT32_MAX / 2, 1, 1, INT32_MAX), -1);

    EXPECT_EQ(IntIntTest<true>(1, 1, 1, 1), -1);
    EXPECT_EQ(IntIntTest<true>(4242, 1, 1, 1), -1);
    EXPECT_EQ(IntIntTest<true>(1, 0, 0, 1), -1);
    EXPECT_EQ(IntIntTest<true>(1, 0, INT32_MIN, 0), 0);
}

TEST_F(TupleRowZOrderCompareTest, Int64Test) {
    EXPECT_EQ(Int64Int64Test(0, 0, 0, 0), 0);
    EXPECT_EQ(Int64Int64Test(-5, 3, -5, 3), 0);
    EXPECT_EQ(Int64Int64Test(1, 0, 0, 1), 1);
    EXPECT_EQ(Int64Int64Test(0, 1, 1, 0), -1);
    EXPECT_EQ(Int64Int64Test(1, 0, 0, 1), 1);
    EXPECT_EQ(Int64Int64Test(2, 4, 1, 7), 1);
    EXPECT_EQ(Int64Int64Test(3, 7, 4, 0), -1);
    EXPECT_EQ(Int64Int64Test(6, 4, 5, 7), 1);
    EXPECT_EQ(Int64Int64Test(5, 5, 6, 4), -1);
    EXPECT_EQ(Int64Int64Test(6, 1, 3, 7), 1);
    EXPECT_EQ(Int64Int64Test(INT64_MAX / 2 + 2, 1, 1, INT64_MAX), 1);
    EXPECT_EQ(Int64Int64Test(INT64_MAX / 2, 1, 1, INT64_MAX), -1);
}
TEST_F(TupleRowZOrderCompareTest, LargeIntTest) {
    EXPECT_EQ(Int128Int128Test(0, 0, 0, 0), 0);
    EXPECT_EQ(Int128Int128Test(-5, 3, -5, 3), 0);
    EXPECT_EQ(Int128Int128Test(1, 0, 0, 1), 1);
    EXPECT_EQ(Int128Int128Test(0, 1, 1, 0), -1);
    EXPECT_EQ(Int128Int128Test(1, 0, 0, 1), 1);
    EXPECT_EQ(Int128Int128Test(2, 4, 1, 7), 1);
    EXPECT_EQ(Int128Int128Test(3, 7, 4, 0), -1);
    EXPECT_EQ(Int128Int128Test(6, 4, 5, 7), 1);
    EXPECT_EQ(Int128Int128Test(5, 5, 6, 4), -1);
    EXPECT_EQ(Int128Int128Test(6, 1, 3, 7), 1);
    EXPECT_EQ(Int128Int128Test(MAX_INT128 / 2 + 2, 1, 1, MAX_INT128), 1);
    Int128Int128Test(MAX_INT128 / 2, 1, 1, MAX_INT128);
}

} // namespace doris
