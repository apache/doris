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

#include "format_v2/table/paimon_rust_predicate_converter.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vin_predicate.h"
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

// A cast node wrapping an operand, like the BE tree of
// `CAST(amount AS DECIMAL(10,1)) = 1.2`. The converter only inspects
// node_type() and the children, never executes the expr.
class TestCastExpr final : public VExpr {
public:
    TestCastExpr(const DataTypePtr& target_type, VExprSPtr child) : VExpr(target_type, false) {
        _node_type = TExprNodeType::CAST_EXPR;
        add_child(std::move(child));
    }

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* /*context*/, const Block* /*block*/,
                               const Selector* /*selector*/, size_t /*count*/,
                               ColumnPtr& result_column) const override {
        result_column = ColumnUInt8::create();
        return Status::OK();
    }

private:
    const std::string _name = "test_cast";
};

// An error-preserving conjunct like assert_true(...): executing it only on
// rows selected by other predicates could discard the error it must raise, so
// VExpr::is_safe_to_execute_on_selected_rows reports it unsafe (a failing
// VCastExpr behaves the same way).
class UnsafeExpr final : public VExpr {
public:
    explicit UnsafeExpr(VExprSPtr child)
            : VExpr(make_nullable(std::make_shared<DataTypeInt32>()), false) {
        _node_type = TExprNodeType::BINARY_PRED;
        add_child(std::move(child));
    }

    bool is_safe_to_execute_on_selected_rows() const override { return false; }

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* /*context*/, const Block* /*block*/,
                               const Selector* /*selector*/, size_t /*count*/,
                               ColumnPtr& result_column) const override {
        result_column = ColumnUInt8::create();
        return Status::OK();
    }

private:
    const std::string _name = "unsafe_expr";
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

// A STRING column type.
const DataTypePtr& string_type() {
    static const auto type = make_nullable(std::make_shared<DataTypeString>());
    return type;
}

VExprSPtr string_literal(std::string value) {
    return VLiteral::create_shared(std::make_shared<DataTypeString>(),
                                   Field::create_field<TYPE_STRING>(std::move(value)));
}

// A like(col, pattern[, escape]) function call. The converter only inspects
// function_name() and the children, never executes the expr.
VExprSPtr like_call(std::vector<VExprSPtr> children) {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_fn(TFunction());
    node.fn.__set_name(TFunctionName());
    node.fn.name.function_name = "like";
    node.__set_is_nullable(false);
    auto call = VectorizedFnCall::create_shared(node);
    for (auto& child : children) {
        call->add_child(std::move(child));
    }
    return call;
}

// An IN / NOT IN predicate over the given children (built through the
// TExprNode path like VDirectInPredicate::get_slot_in_expr does, so
// is_not_in() is initialized). The converter only inspects node_type(),
// op() and the children.
VExprSPtr in_predicate(bool is_not, std::vector<VExprSPtr> children) {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::IN_PRED);
    node.in_predicate.__set_is_not_in(is_not);
    node.__set_opcode(is_not ? TExprOpcode::FILTER_NOT_IN : TExprOpcode::FILTER_IN);
    node.__set_is_nullable(false);
    auto predicate = VInPredicate::create_shared(node);
    for (auto& child : children) {
        predicate->add_child(std::move(child));
    }
    return predicate;
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

// A DECIMAL(10,2) column type.
const DataTypePtr& decimal_type() {
    static const auto type = make_nullable(std::make_shared<DataTypeDecimal64>(10, 2));
    return type;
}

// A DECIMAL(10,2) literal, e.g. decimal_literal(1, 24) is 1.24.
VExprSPtr decimal_literal(int64_t integer, int64_t fraction) {
    return VLiteral::create_shared(
            std::make_shared<DataTypeDecimal64>(10, 2),
            Field::create_field<TYPE_DECIMAL64>(Decimal64::from_int_frac(integer, fraction, 2)));
}

// A DOUBLE column type.
const DataTypePtr& double_type() {
    static const auto type = make_nullable(std::make_shared<DataTypeFloat64>());
    return type;
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
                R"json("type":"TIMESTAMP(6)"},{"id":3,"name":"amount",)json"
                R"json("type":"DECIMAL(10, 2)"},{"id":4,"name":"d",)json"
                R"json("type":"DOUBLE"},{"id":5,"name":"s","type":"STRING"}],)json"
                R"json("highestFieldId":5,"partitionKeys":[],"primaryKeys":[],)json"
                R"json("options":{},"timeMillis":0})json";
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

        _column_names = {"a", "b", "ts", "amount", "d", "s"};
        _column_types = {make_nullable(std::make_shared<DataTypeInt32>()),
                         make_nullable(std::make_shared<DataTypeInt32>()),
                         datetimev2_type(),
                         decimal_type(),
                         double_type(),
                         string_type()};
    }

    // Runs one conjunct through a fresh converter; a null return means the
    // conjunct was rejected (left to the Doris residual).
    predicate_ptr push(TExprOpcode::type opcode, VExprSPtr left, VExprSPtr right) {
        PaimonRustPredicateConverter converter(_column_names, _column_types, _table.get());
        VExprSPtr root =
                std::make_shared<TestBinaryPredicate>(opcode, std::move(left), std::move(right));
        VExprContextSPtrs conjuncts {VExprContext::create_shared(std::move(root))};
        return predicate_ptr(converter.build(conjuncts));
    }

    // Runs one pre-built expr (e.g. an IN predicate) through a fresh
    // converter; a null return means the conjunct was rejected.
    predicate_ptr push_expr(VExprSPtr expr) {
        PaimonRustPredicateConverter converter(_column_names, _column_types, _table.get());
        VExprContextSPtrs conjuncts {VExprContext::create_shared(std::move(expr))};
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

// ---- pushdown stops at unsafe conjuncts (safe-prefix rule) ----

TEST_F(PaimonRustPredicateConverterTest, UnsafeConjunctStopsLaterPredicatesFromBeingPushed) {
    // The review's P1 case: an error-preserving conjunct (assert_true(...),
    // a failing cast) that precedes a convertible predicate — in practice an
    // arrived IN runtime filter, modeled here by a plain equality — must stop
    // the pushdown. Pushing the later predicate would let rust prune the row
    // on which the unsafe conjunct must still raise.
    PaimonRustPredicateConverter converter(_column_names, _column_types, _table.get());
    VExprContextSPtrs conjuncts;
    conjuncts.push_back(VExprContext::create_shared(std::make_shared<UnsafeExpr>(slot_ref("a"))));
    conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<TestBinaryPredicate>(TExprOpcode::EQ, slot_ref("b"), int_literal(1))));

    predicate_ptr predicate(converter.build(conjuncts));
    EXPECT_EQ(predicate.get(), nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, SafePrefixBeforeUnsafeConjunctIsStillPushed) {
    // The prefix itself stays pushable: the convertible conjunct before the
    // unsafe one is pushed, and the unsafe one only blocks everything after
    // it (the later equality must not be pushed).
    PaimonRustPredicateConverter converter(_column_names, _column_types, _table.get());
    VExprContextSPtrs conjuncts;
    conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<TestBinaryPredicate>(TExprOpcode::EQ, slot_ref("a"), int_literal(1))));
    conjuncts.push_back(VExprContext::create_shared(std::make_shared<UnsafeExpr>(slot_ref("b"))));
    conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<TestBinaryPredicate>(TExprOpcode::EQ, slot_ref("b"), int_literal(2))));

    predicate_ptr predicate(converter.build(conjuncts));
    EXPECT_NE(predicate.get(), nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, DoubleColumnPredicatesAreNotPushed) {
    // Doris defines NaN as equal to itself and greater than every finite
    // value, but the pinned rust evaluator compares doubles with
    // f64::partial_cmp (IEEE partial ordering: NaN unordered, NaN != NaN), so
    // a pushed `d > 1.0` would drop a stored NaN row Doris retains and a
    // pushed IN (NaN) would reject it — rows pruned by the rust filter cannot
    // be recovered by the residual. DOUBLE pushdown is skipped entirely
    // (paimon-java's JNI path is unaffected: its CompareUtils goes through
    // Double.compareTo, which is Doris's total ordering).
    EXPECT_EQ(push(TExprOpcode::EQ, slot_ref("d", double_type()), int_literal(1)).get(), nullptr);
    EXPECT_EQ(push(TExprOpcode::GT, slot_ref("d", double_type()), int_literal(1)).get(), nullptr);
}

// ---- IN list values must be bare literals (FE doInPredicate parity) ----

TEST_F(PaimonRustPredicateConverterTest, InListIsPushed) {
    // Sanity: an IN over bare literals still converts, so the rejections
    // below come from the casted children, not from IN handling.
    auto predicate =
            push_expr(in_predicate(false, {slot_ref("a"), int_literal(1), int_literal(2)}));
    EXPECT_NE(predicate.get(), nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, CastedInListValueIsNotPushed) {
    // The review's P1 case (IN form): with debug_skip_fold_constant=true, a
    // cast in the IN list reaches the BE un-folded. Unwrapping it (the old
    // one-level cast rule from _convert_literal) would push the pre-cast
    // value, but Doris compares against the cast result — `amount IN
    // (CAST(1.24 AS DECIMAL(10,1)))` must compare against 1.2, and the
    // unwrapped 1.24 push would permanently remove the 1.2 rows. _convert_literal
    // rejects the casted value, which rejects the whole predicate; the
    // residual applies the cast correctly.
    auto casted = std::make_shared<TestCastExpr>(make_nullable(std::make_shared<DataTypeInt64>()),
                                                 int_literal(1));
    EXPECT_EQ(push_expr(in_predicate(false, {slot_ref("a"), int_literal(1), std::move(casted)}))
                      .get(),
              nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, CastedNotInListValueIsNotPushed) {
    // Same rule for NOT IN: rejecting the push keeps both readers on the
    // Doris residual, where the cast is evaluated correctly.
    auto casted = std::make_shared<TestCastExpr>(make_nullable(std::make_shared<DataTypeInt64>()),
                                                 int_literal(1));
    EXPECT_EQ(push_expr(in_predicate(true, {slot_ref("a"), std::move(casted)})).get(), nullptr);
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
    auto predicate =
            push(TExprOpcode::EQ, slot_ref("ts", datetimev2_type()), datetimev2_literal(123456));
    EXPECT_NE(predicate.get(), nullptr);
}

// ---- casted operands are rejected, mirroring the FE converter ----

TEST_F(PaimonRustPredicateConverterTest, CastedColumnEqIsNotPushed) {
    // `CAST(a AS BIGINT) = 1` must stay in the residual: a cast on the column
    // is not equivalence-preserving in general, so like the FE converter's
    // convertDorisExprToSlotRef the cast is not unwrapped. (The old code
    // stripped it via expr_without_cast and pushed against the raw column.)
    auto casted = std::make_shared<TestCastExpr>(make_nullable(std::make_shared<DataTypeInt64>()),
                                                 slot_ref("a"));
    EXPECT_EQ(push(TExprOpcode::EQ, std::move(casted), int_literal(1)).get(), nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, DecimalScaleCastColumnIsNotPushed) {
    // The review's P1 case: for a DECIMAL(10,2) column holding 1.24,
    // `CAST(amount AS DECIMAL(10,1)) = 1.2` must retain the row, but pushing
    // the unwrapped `amount = 1.2` against the original column prunes it. The
    // rust filter does not save us either: it accepts decimal literals with
    // different scales and compares their mathematical values (1.24 != 1.2),
    // so the unsafe predicate is not rejected as a type mismatch. The conjunct
    // must stay in the Doris residual.
    auto casted = std::make_shared<TestCastExpr>(
            make_nullable(std::make_shared<DataTypeDecimal64>(10, 1)),
            slot_ref("amount", decimal_type()));
    EXPECT_EQ(push(TExprOpcode::EQ, std::move(casted), decimal_literal(1, 2)).get(), nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, DecimalLiteralEqIsPushed) {
    // Positive control: the same decimal literal converts against the uncast
    // column, so the rejection above comes from the cast, not decimal support.
    auto predicate =
            push(TExprOpcode::EQ, slot_ref("amount", decimal_type()), decimal_literal(1, 24));
    EXPECT_NE(predicate.get(), nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, SingleCastLiteralRhsIsNotPushed) {
    // The review's P1 case (binary form): a single cast wrapping a literal on
    // the RHS must stay in the residual. Unwrapping it (the old one-level cast
    // rule from _convert_literal) pushed the pre-cast value, but Doris compares
    // against the cast result, so with debug_skip_fold_constant=true the rust
    // filter and the residual disagree on lossy casts. The cast is not
    // evaluated here, so the predicate cannot be built from the raw literal.
    auto casted = std::make_shared<TestCastExpr>(make_nullable(std::make_shared<DataTypeInt64>()),
                                                 int_literal(1));
    EXPECT_EQ(push(TExprOpcode::EQ, slot_ref("a"), std::move(casted)).get(), nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, DecimalScaleCastLiteralRhsIsNotPushed) {
    // The differential case: `amount = CAST(1.24 AS DECIMAL(10,1))` on a
    // DECIMAL(10,2) column. The cast evaluates to 1.2, so Doris's residual
    // keeps a stored 1.20 row; the unwrapped literal 1.24 pushed against the
    // raw column would prune it before the residual runs, and pruned rows
    // cannot be recovered. The rust filter does not reject this as a type
    // mismatch either — it accepts decimals of different scales and compares
    // their mathematical values (1.24 != 1.20) — so the only safe conversion
    // is none: the conjunct stays in the residual.
    auto casted = std::make_shared<TestCastExpr>(
            make_nullable(std::make_shared<DataTypeDecimal64>(10, 1)), decimal_literal(1, 24));
    EXPECT_EQ(push(TExprOpcode::EQ, slot_ref("amount", decimal_type()), std::move(casted)).get(),
              nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, NestedCastLiteralIsNotPushed) {
    // `CAST(CAST(1 AS BIGINT) AS DOUBLE)`: cast trees are rejected at the
    // outer level now — the old code stripped all cast levels, and the
    // interim code unwrapped exactly one.
    auto inner = std::make_shared<TestCastExpr>(make_nullable(std::make_shared<DataTypeInt64>()),
                                                int_literal(1));
    auto outer = std::make_shared<TestCastExpr>(make_nullable(std::make_shared<DataTypeFloat64>()),
                                                std::move(inner));
    EXPECT_EQ(push(TExprOpcode::EQ, slot_ref("a"), std::move(outer)).get(), nullptr);
}

// ---- LIKE patterns must be bare literals ----

TEST_F(PaimonRustPredicateConverterTest, LikeIsPushed) {
    // Sanity: a plain `s LIKE 'abc%'` converts, so the rejections below come
    // from the pattern / escape handling, not from LIKE support itself.
    auto predicate = push_expr(like_call({slot_ref("s", string_type()), string_literal("abc%")}));
    EXPECT_NE(predicate.get(), nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, CastedLikePatternIsNotPushed) {
    // A cast wrapping the pattern must stay in the residual: _extract_string_
    // literal does not execute the cast, so the pushed pattern would be the
    // pre-cast value while the Doris residual matches the cast result (a
    // truncating cast like CAST('abc%' AS CHAR(2)) changes the prefix). Same
    // rule as _convert_literal for the binary RHS and IN-list values.
    auto casted = std::make_shared<TestCastExpr>(make_nullable(std::make_shared<DataTypeString>()),
                                                 string_literal("abc%"));
    EXPECT_EQ(push_expr(like_call({slot_ref("s", string_type()), std::move(casted)})).get(),
              nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, LikeWithDivergentEscapeIsNotPushed) {
    // A backslash before an ordinary character diverges between Doris and the
    // pinned rust like (Doris keeps both characters, rust consumes the
    // backslash), so such patterns stay in the residual. Escapes before %, _
    // and \\ (and a trailing backslash) have identical semantics on both
    // sides and still convert.
    EXPECT_EQ(push_expr(like_call({slot_ref("s", string_type()), string_literal("a\\qb%")})).get(),
              nullptr);
    EXPECT_NE(push_expr(like_call({slot_ref("s", string_type()), string_literal("a\\%b")})).get(),
              nullptr);
    EXPECT_NE(push_expr(like_call({slot_ref("s", string_type()), string_literal("a\\\\b_")})).get(),
              nullptr);
}

TEST_F(PaimonRustPredicateConverterTest, LikeWithNonDefaultEscapeIsNotPushed) {
    // The 3-arg like(col, pattern, escape) form only converts for the Doris
    // default escape '\'; the rust predicate builder rejects anything else.
    EXPECT_NE(push_expr(like_call({slot_ref("s", string_type()), string_literal("abc%"),
                                   string_literal("\\")}))
                      .get(),
              nullptr);
    EXPECT_EQ(push_expr(like_call({slot_ref("s", string_type()), string_literal("a!b"),
                                   string_literal("!")}))
                      .get(),
              nullptr);
    // A casted escape char is rejected like a casted pattern.
    auto casted_escape = std::make_shared<TestCastExpr>(
            make_nullable(std::make_shared<DataTypeString>()), string_literal("\\"));
    EXPECT_EQ(push_expr(like_call({slot_ref("s", string_type()), string_literal("abc%"),
                                   std::move(casted_escape)}))
                      .get(),
              nullptr);
}

} // namespace doris
