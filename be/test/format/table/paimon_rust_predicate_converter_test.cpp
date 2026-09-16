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

#include "format/table/paimon_rust_predicate_converter.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "core/value/vdatetime_value.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"

namespace doris {
namespace {

struct table_deleter {
    void operator()(paimon_table* table) const {
        if (table) {
            paimon_table_free(table);
        }
    }
};
using table_ptr = std::unique_ptr<paimon_table, table_deleter>;

struct predicate_deleter {
    void operator()(paimon_predicate* predicate) const {
        if (predicate) {
            paimon_predicate_free(predicate);
        }
    }
};
using predicate_ptr = std::unique_ptr<paimon_predicate, predicate_deleter>;

// A binary predicate node with a hand-set opcode. The converter only inspects
// op() and the children, never executes the expr, so execute_column_impl is a
// stub that is never reached.
class TestBinaryPredicate final : public VExpr {
public:
    TestBinaryPredicate(TExprOpcode::type opcode, VExprSPtr left, VExprSPtr right)
            : VExpr(std::make_shared<DataTypeUInt8>(), false) {
        _node_type = opcode == TExprOpcode::EQ_FOR_NULL ? TExprNodeType::NULL_AWARE_BINARY_PRED
                                                        : TExprNodeType::BINARY_PRED;
        _opcode = opcode;
        add_child(std::move(left));
        add_child(std::move(right));
    }

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* /*context*/, const Block* /*block*/,
                               const Selector* /*selector*/, size_t /*count*/,
                               ColumnPtr& result_column) const override {
        result_column = ColumnUInt8::create();
        return Status::OK();
    }

private:
    const std::string _name = "test_binary_predicate";
};

// A slot ref resolved by column name, like FileScannerV2's rewritten conjuncts.
VExprSPtr slot_ref(const std::string& column_name, const DataTypePtr& type) {
    return std::make_shared<VSlotRef>(0, 0, -1, type, column_name);
}

VExprSPtr slot_ref(const std::string& column_name) {
    return slot_ref(column_name, make_nullable(std::make_shared<DataTypeInt32>()));
}

VExprSPtr int_literal(int32_t value) {
    return VLiteral::create_shared(std::make_shared<DataTypeInt32>(),
                                   Field::create_field<TYPE_INT>(value));
}

// 2024-01-01 00:00:00.<microsecond> as a DATETIMEV2(6) literal.
const DataTypePtr& datetimev2_type() {
    static const auto type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    return type;
}

VExprSPtr datetimev2_literal(uint32_t microsecond) {
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2024, 1, 1, 0, 0, 0, microsecond);
    return VLiteral::create_shared(std::make_shared<DataTypeDateTimeV2>(6),
                                   Field::create_field<TYPE_DATETIMEV2>(value));
}

} // namespace

// Pins the EQ_FOR_NULL (<=>) decision matrix of the converter. Semantics are
// aligned with the FE PaimonPredicateConverter, which only converts a binary
// conjunct when the RHS is a convertible literal: a column-to-column
// `a <=> b` must NOT be pushed down — the old code pushed `a IS NULL` and
// wrongly discarded rows like (1, 1), which the residual conjunct cannot
// recover.
class PaimonRustPredicateConverterTest : public testing::Test {
protected:
    void SetUp() override {
        // The table handle is built in memory from the schema JSON (the same
        // format as an on-disk schema/schema-N file): building predicates only
        // resolves fields against the schema, so the path never needs to exist.
        // NOTE: the custom `json` delimiter matters — the schema contains
        // `TIMESTAMP(6)"`, whose `)"` would terminate a plain R"(...)" early.
        static constexpr const char* kSchemaJson =
                R"json({"version":3,"id":0,"fields":[{"id":0,"name":"a","type":"INT"},)json"
                R"json({"id":1,"name":"b","type":"INT"},{"id":2,"name":"ts",)json"
                R"json("type":"TIMESTAMP(6)"}],"highestFieldId":2,)json"
                R"json("partitionKeys":[],"primaryKeys":[],"options":{},"timeMillis":0})json";
        auto result = paimon_table_from_schema_json("/tmp/paimon_rust_predicate_converter_test",
                                                    kSchemaJson, "db", "t", "main", nullptr, 0);
        if (result.error != nullptr) {
            std::string message;
            if (result.error->message.data != nullptr && result.error->message.len > 0) {
                message.assign(reinterpret_cast<const char*>(result.error->message.data),
                               result.error->message.len);
            }
            const int32_t code = result.error->code;
            paimon_error_free(result.error);
            FAIL() << "paimon_table_from_schema_json failed: code=" << code << ", msg=" << message;
        }
        _table.reset(result.table);

        _column_names = {"a", "b", "ts"};
        _column_types = {make_nullable(std::make_shared<DataTypeInt32>()),
                         make_nullable(std::make_shared<DataTypeInt32>()), datetimev2_type()};
    }

    // Runs one conjunct through a fresh converter; a null return means the
    // conjunct was rejected (left to the Doris residual).
    predicate_ptr push(TExprOpcode::type opcode, VExprSPtr left, VExprSPtr right) {
        PaimonRustPredicateConverter converter(_column_names, _column_types, _table.get());
        VExprSPtr root = std::make_shared<TestBinaryPredicate>(opcode, std::move(left),
                                                               std::move(right));
        VExprContextSPtrs conjuncts {VExprContext::create_shared(std::move(root))};
        return predicate_ptr(converter.build(conjuncts));
    }

    // Converts a literal expr against a column type so tests can inspect the
    // resulting paimon_datum (tag / int_val / int_val2).
    auto convert_literal(const VExprSPtr& expr, const DataTypePtr& column_type) {
        PaimonRustPredicateConverter converter(_column_names, _column_types, _table.get());
        return converter.TEST_convert_literal(expr, column_type);
    }

    // Base epoch millis of 2024-01-01 00:00:00 through the same conversion;
    // the absolute value depends on the converter's timezone resolution.
    int64_t converted_base_millis() {
        auto holder = convert_literal(datetimev2_literal(0), datetimev2_type());
        EXPECT_TRUE(holder.has_value());
        return holder->datum.int_val;
    }

    table_ptr _table;
    std::vector<std::string> _column_names;
    std::vector<DataTypePtr> _column_types;
};

TEST_F(PaimonRustPredicateConverterTest, EqForNullColumnToColumnIsNotPushed) {
    // The P1 case: `a <=> b` must stay in the residual. Pushing `a IS NULL`
    // would keep only the (NULL, NULL) row and drop (1, 1) forever.
    // (The literal forms `a <=> NULL` / `a <=> 1` are not covered: FE's
    // NullSafeEqualToEqual rewrite turns them into IS NULL / plain equality
    // before they ever reach a BE converter, so EQ_FOR_NULL arriving here is
    // always column-to-column.)
    EXPECT_EQ(push(TExprOpcode::EQ_FOR_NULL, slot_ref("a"), slot_ref("b")).get(), nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, EqLiteralIsPushed) {
    // Sanity: plain equality still converts.
    auto predicate = push(TExprOpcode::EQ, slot_ref("a"), int_literal(1));
    EXPECT_NE(predicate.get(), nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, UnknownColumnIsNotPushed) {
    // A slot outside the reader's projected columns cannot be resolved.
    EXPECT_EQ(push(TExprOpcode::EQ, slot_ref("nope"), int_literal(1)).get(), nullptr);
}

// The sub-millisecond remainder must land in int_val2 (nanos) instead of being
// truncated — the rust datum holds (millis = int_val, nanos = int_val2) and
// timestamp comparison includes both. A truncated conversion would turn
// `ts = '2024-01-01 00:00:00.123456'` into `.123000` and wrongly drop the
// matching rows — the runtime-filter IN scenario from the review. (The base
// epoch millis depend on the converter's timezone resolution, so expectations
// are expressed relative to a whole-second literal converted the same way.)
TEST_F(PaimonRustPredicateConverterTest, TimestampV2FractionalMicrosArePreserved) {
    const int64_t base = converted_base_millis();
    auto holder = convert_literal(datetimev2_literal(123456), datetimev2_type());
    ASSERT_TRUE(holder.has_value());
    // paimon_datum tag 10 = Timestamp (see paimon.h).
    EXPECT_EQ(holder->datum.tag, 10);
    EXPECT_EQ(holder->datum.int_val, base + 123);
    EXPECT_EQ(holder->datum.int_val2, 456000);
}

TEST_F(PaimonRustPredicateConverterTest, TimestampV2WholeMillisLeaveNanosZero) {
    const int64_t base = converted_base_millis();
    auto holder = convert_literal(datetimev2_literal(123000), datetimev2_type());
    ASSERT_TRUE(holder.has_value());
    EXPECT_EQ(holder->datum.int_val, base + 123);
    EXPECT_EQ(holder->datum.int_val2, 0);
}

TEST_F(PaimonRustPredicateConverterTest, TimestampV2MaxFractionIsPreserved) {
    const int64_t base = converted_base_millis();
    auto holder = convert_literal(datetimev2_literal(999999), datetimev2_type());
    ASSERT_TRUE(holder.has_value());
    EXPECT_EQ(holder->datum.int_val, base + 999);
    EXPECT_EQ(holder->datum.int_val2, 999000);
}

TEST_F(PaimonRustPredicateConverterTest, TimestampV2FractionalEqualityIsPushed) {
    // The full build path accepts a fractional timestamp literal; the value it
    // pushes now matches the conjunct exactly (see the datum tests above).
    auto predicate = push(TExprOpcode::EQ, slot_ref("ts", datetimev2_type()),
                           datetimev2_literal(123456));
    EXPECT_NE(predicate.get(), nullptr);
}

} // namespace doris
