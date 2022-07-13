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

#include <arrow/type_fwd.h>

#include <unordered_set>
#include "common/status.h"

#include <exprs/expr.h>
#include <exprs/expr_context.h>
#include <exprs/in_predicate.h>

namespace doris {
template <typename ArrowType>
class ArrowRange {
    ArrowType _min;
    ArrowType _max;

public:
    ArrowRange(ArrowType min, ArrowType max): _min(min), _max(max) {}

    bool determine_filter_row_group(const std::vector<ExprContext*>& conjuncts) {
        bool need_filter = false;
        for (int i = 0; i < conjuncts.size(); i++) {
            Expr* conjunct = conjuncts[i]->root();
            if (TExprNodeType::BINARY_PRED == conjunct->node_type()) {
                _eval_binary_predicate(conjuncts[i], need_filter);
            } else if (TExprNodeType::IN_PRED == conjunct->node_type()) {
                _eval_in_predicate(conjuncts[i], need_filter);
            }
        }
        return need_filter;
    }

private:
    void _eval_binary_predicate(ExprContext* ctx, bool& need_filter) {
        Expr* conjunct = ctx->root();
        Expr* expr = conjunct->get_child(1);
        if (expr == nullptr) {
            return;
        }
        // supported conjunct example: slot_ref < 123, slot_ref > func(123), ..
        auto conjunct_type = expr->type().type;
        void* conjunct_value = ctx->get_value(expr, nullptr);
        ArrowType value = convertToArrowType(conjunct_type, conjunct_value);
        // use is_match var to help understand the compare logic
        bool is_match = false;
        switch (conjunct->op()) {
            case TExprOpcode::EQ:
                //  _min      value   _max    
                //  --|---------^-------|----
                if (largeEqual(value, _min) && largeEqual(_max, value)) {
                    is_match = true;
                }
                break;
            case TExprOpcode::NE:
                //   value _min         _max  value(or)
                //  ---^-----|------------|-----^------
                if (large(_min, value) || large(value, _max)) {
                    is_match = true;
                }
                break;
            case TExprOpcode::GT:
                //    _min   value     
                //  ----|------^------
                if (large(value, _min)) {
                    is_match = true;
                }
                break;
            case TExprOpcode::GE:
                //    _min   value     
                //  ----|------^------
                if (largeEqual(value, _min)) {
                    is_match = true;
                }
                break;
            case TExprOpcode::LT:
                //    value  _max  
                //  ----^------|------
                if (large(_max, value)) {
                    is_match = true;
                }
                break;
            case TExprOpcode::LE:
                //    value  _max  
                //  ----^------|------
                if (largeEqual(_max, value)) {
                    is_match = true;
                }
                break;
            default:
                // Treat nonsupport predict as match.
                is_match = true;
                break;
        }
        need_filter = !is_match;
    }

    void _eval_in_predicate(ExprContext* ctx, bool& need_filter) {
        Expr* conjunct = ctx->root();
        std::vector<ArrowType> in_pred_values;
        const InPredicate* pred = static_cast<const InPredicate*>(conjunct);
        HybridSetBase::IteratorBase* iter = pred->hybrid_set()->begin();
        auto conjunct_type = conjunct->get_child(1)->type().type;
        // TODO: process expr: in(func(123),123)
        while (iter->has_next()) {
            if (nullptr == iter->get_value()) {
                return;
            }
            in_pred_values.emplace_back(convertToArrowType(conjunct_type, const_cast<void*>(iter->get_value())));
            iter->next();
        }
        std::sort(in_pred_values.begin(), in_pred_values.end());
        ArrowType in_min = in_pred_values.front();                                    \
        ArrowType in_max = in_pred_values.back();
        // use is_match var to help understand the compare logic
        bool is_match = false;
        switch (conjunct->op()) {
            case TExprOpcode::FILTER_IN:
                //  _min  in_min    in_max _max
                //  --|-----^---------^------|---
                if (largeEqual(in_min, _min) && largeEqual(_max, in_max)) {
                    is_match = true;
                }
                break;
            case TExprOpcode::FILTER_NOT_IN:
                //   in_max _min      _max   in_min(or)
                //  ---^------|---------|------^-------
                if (large(_min, in_max) || large(in_min, _max)) {
                    is_match = true;
                }
                break;
            default:
                is_match = true;
        }
        need_filter = !is_match;
    }

protected:
    virtual ArrowType convertToArrowType(PrimitiveType conjunct_type, void* conjunct_value) = 0;

    virtual bool largeEqual(ArrowType one, ArrowType another) = 0;

    virtual bool large(ArrowType one, ArrowType another) = 0;
};

struct IntegerArrowRange: public ArrowRange<int64_t> {
public:
    IntegerArrowRange(int64_t min, int64_t max): ArrowRange(min, max) {}

    int64_t convertToArrowType(PrimitiveType conjunct_type, void* conjunct_value) override {
        int64_t out_value = 0;

        switch (conjunct_type) {
            case TYPE_TINYINT: {
                out_value = (int64_t)(*((int8_t*)conjunct_value));
                break;
            }
            case TYPE_SMALLINT: {
                out_value = (int64_t)(*((int16_t*)conjunct_value));
                break;
            }
            case TYPE_INT: {
                out_value = (int64_t)(*((int32_t*)conjunct_value));
                break;
            }
            case TYPE_BIGINT: {
                out_value = (int64_t)(*((int64_t*)conjunct_value));
                break;
            }
            default:
                // never go into here.
                VLOG_CRITICAL << conjunct_type << "go to DoubleArrowRange forbid area.";
                DCHECK(0);
                break;
        }
        return out_value;
    }

    bool largeEqual(int64_t one, int64_t another) override {
        return one >= another;
    }

    bool large(int64_t one, int64_t another) override {
        return one > another;
    }
};

struct DoubleArrowRange: public ArrowRange<double> {
public:
    DoubleArrowRange(double min, double max): ArrowRange(min, max) {}

    double convertToArrowType(PrimitiveType conjunct_type, void* conjunct_value) override {
        double out_value = 0.0;

        switch (conjunct_type) {
            case TYPE_FLOAT: {
                out_value = (int64_t)(*((float*)conjunct_value));
                break;
            }
            case TYPE_DOUBLE: {
                out_value = (int64_t)(*((double*)conjunct_value));
                break;
            }
            default:
                // never go into here.
                VLOG_CRITICAL << conjunct_type << "go to DoubleArrowRange forbid area.";
                DCHECK(0);
                break;
        }
        return out_value;
    }

    bool largeEqual(double one, double another) override {
        return one >= another;
    }

    bool large(double one, double another) override {
        return one > another;
    }
};

struct StringArrowRange: public ArrowRange<std::string> {
public:
    StringArrowRange(std::string min, std::string max): ArrowRange(min, max) {}

    std::string convertToArrowType(PrimitiveType conjunct_type, void* conjunct_value) override {
        std::string out_value = "";

        switch (conjunct_type) {
            case TYPE_VARCHAR:
            case TYPE_STRING: {
                out_value = ((std::string*)conjunct_value)->c_str();
                break;
            }
            default:
                // never go into here.
                VLOG_CRITICAL << conjunct_type << "go to StringArrowRange forbid area.";
                DCHECK(0);
                break;
        }
        return out_value;
    }

    bool largeEqual(std::string one, std::string another) override {
        return strcmp(one.c_str(), another.c_str()) >= 0;
    }

    bool large(std::string one, std::string another) override {
        return strcmp(one.c_str(), another.c_str()) > 0;
    }
};

} // namespace doris
