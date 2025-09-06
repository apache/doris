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
#include <stack>
#include <string>

#include "util/bitmap_intersect.h"

namespace doris {

// Compute the intersection union difference set of two or more bitmaps
// Usage: orthogonal_bitmap_parse_calculate(bitmap_column, filter_column, input_string)
// Example: orthogonal_bitmap_expr_calculate(user_id, event, '(A|B)&(C-D)'), meaning find the intersection union difference set of user_id in all A/B/C/D 4 bitmaps
// Operation symbol:
// the operator '|' stands for union, the operator '&' stands for intersection, the operator '-' indicates the difference set, the operator '^' stands for xor
class BitmapExprCalculation : public BitmapIntersect<std::string> {
public:
    BitmapExprCalculation() = default;

    explicit BitmapExprCalculation(const char* src) { deserialize(src); }

    void bitmap_calculation_init(std::string& input_str) {
        _polish = reverse_polish(input_str);
        std::string bitmap_key;
        for (int i = 0; i < _polish.length(); i++) {
            char c = _polish.at(i);
            if (c != '&' && c != '|' && c != '^' && c != '-' && c != ' ' && c != '\\') {
                bitmap_key += c;
            } else if (i != 0 && _polish.at(i - 1) == '\\') {
                bitmap_key += c;
            } else if (c == '\\') {
                continue;
            } else {
                if (bitmap_key.length() > 0) {
                    add_key(bitmap_key);
                    bitmap_key.clear();
                }
            }
        }
        if (bitmap_key.length() > 0) {
            add_key(bitmap_key);
            bitmap_key.clear();
        }
    }

    BitmapValue bitmap_calculate() const {
        // to use a non-const function and no modify, use const_cast is acceptable
        return const_cast<BitmapExprCalculation*>(this)->bitmap_calculate();
    }

    BitmapValue bitmap_calculate() {
        std::stack<BitmapValue> values;
        std::string bitmap_key;
        for (int i = 0; i < _polish.length(); i++) {
            char c = _polish.at(i);
            if (c == ' ') {
                if (bitmap_key.length() > 0) {
                    values.push(_bitmaps[bitmap_key]);
                    bitmap_key.clear();
                }
            } else if (c != '&' && c != '|' && c != '^' && c != '-' && c != '\\') {
                bitmap_key += c;
            } else if (i != 0 && _polish.at(i - 1) == '\\') {
                bitmap_key += c;
            } else if (c == '\\') {
                continue;
            } else {
                if (bitmap_key.length() > 0) {
                    values.push(_bitmaps[bitmap_key]);
                    bitmap_key.clear();
                }
                if (values.size() >= 2) {
                    BitmapValue op_a = values.top();
                    values.pop();
                    BitmapValue op_b = values.top();
                    values.pop();
                    BitmapValue cal_result;
                    bitmap_calculate(op_a, op_b, c, cal_result);
                    values.push(cal_result);
                }
            }
        }
        BitmapValue result;
        if (bitmap_key.length() > 0) {
            result |= _bitmaps[bitmap_key];
        } else if (!values.empty()) {
            result |= values.top();
        }
        return result;
    }

    // calculate the bitmap value by expr bitmap calculate
    int64_t bitmap_calculate_count() const {
        if (_bitmaps.empty()) {
            return 0;
        }
        return bitmap_calculate().cardinality();
    }

private:
    constexpr int priority(char c) {
        switch (c) {
        case '&':
            return 1;
        case '|':
            return 1;
        case '^':
            return 1;
        case '-':
            return 1;
        default:
            return 0;
        }
    }

    template <class T>
    std::string print_stack(std::stack<T>& stack) {
        std::string result;
        while (!stack.empty()) {
            result = stack.top() + result;
            stack.pop();
        }
        return result;
    }

    std::string reverse_polish(const std::string& input_str) {
        std::stack<char> polish;
        std::stack<char> op_stack;
        bool last_is_char = false;
        for (int i = 0; i < input_str.length(); i++) {
            char cur_char = input_str.at(i);
            if (cur_char != '&' && cur_char != '|' && cur_char != '^' && cur_char != '-' &&
                cur_char != '(' && cur_char != ')' && cur_char != ' ' && cur_char != '\t') {
                if (!last_is_char) {
                    polish.push(' ');
                }
                polish.push(cur_char);
                last_is_char = true;
                continue;
            } else if (i != 0 && input_str.at(i - 1) == '\\') {
                polish.push(cur_char);
                last_is_char = true;
                continue;
            } else if (cur_char == ' ' || cur_char == '\t') {
                last_is_char = false;
                continue;
            } else if (cur_char == '(') {
                op_stack.push(cur_char);
            } else if (!op_stack.empty() && cur_char == ')') {
                while (!op_stack.empty() && op_stack.top() != '(') {
                    polish.push(op_stack.top());
                    op_stack.pop();
                }
                op_stack.pop();
            } else {
                if (!op_stack.empty() && op_stack.top() == '(') {
                    op_stack.push(cur_char);
                } else {
                    if (!op_stack.empty() && priority(cur_char) > priority(op_stack.top())) {
                        op_stack.push(cur_char);
                    } else {
                        while (!op_stack.empty()) {
                            if (op_stack.top() == '(') {
                                break;
                            }
                            if (priority(cur_char) <= priority(op_stack.top())) {
                                polish.push(op_stack.top());
                                op_stack.pop();
                            } else {
                                break;
                            }
                        }
                        op_stack.push(cur_char);
                    }
                }
            }
            last_is_char = false;
        }

        while (!op_stack.empty()) {
            polish.push(op_stack.top());
            op_stack.pop();
        }
        return print_stack(polish);
    }

    void bitmap_calculate(BitmapValue& op_a, BitmapValue& op_b, char op, BitmapValue& result) {
        result |= op_b;
        switch (op) {
        case '&':
            result &= op_a;
            break;
        case '|':
            result |= op_a;
            break;
        case '-':
            result -= op_a;
            break;
        case '^':
            result ^= op_a;
            break;
        }
    }

    std::string _polish;
};
} // namespace doris
