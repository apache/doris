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
#include <string>
#include <stack>

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

    void bitmap_calculation_init(std::string& inputStr) {
        _polish = reversePolish(inputStr);
        std::string bitmapKey = "";
        for (int i = 0; i < _polish.length(); i++) {
            char c = _polish.at(i);
            if (c != '&' && c != '|' && c != '^' && c != '-' && c != ' ' && c != '\\') {
                bitmapKey += c;
            } else if (i != 0 && _polish.at(i - 1) == '\\') {
                bitmapKey += c;
            } else if (c == '\\') {
                continue;
            } else {
                if (bitmapKey.length() > 0) {
                    add_key(bitmapKey);
                    bitmapKey.clear();
                }
            }
        }
        if (bitmapKey.length() > 0) {
            add_key(bitmapKey);
            bitmapKey.clear();
        }
    }

    // 计算表达式的值
    BitmapValue bitmap_calculate() {
        std::stack<BitmapValue> values;
        std::string bitmapKey = "";
        for (int i = 0; i < _polish.length(); i++) {
            char c = _polish.at(i);
            if (c == ' ') {
                if (bitmapKey.length() > 0) {
                    values.push(_bitmaps[bitmapKey]);
                    bitmapKey.clear();
                }
            } else if (c != '&' && c != '|' && c != '^' && c != '-' && c != '\\') {
                bitmapKey += c;
            } else if (i != 0 && _polish.at(i - 1) == '\\') {
                bitmapKey += c;
            } else if (c == '\\') {
                continue;
            } else {
                if (bitmapKey.length() > 0) {
                    values.push(_bitmaps[bitmapKey]);
                    bitmapKey.clear();
                }
                if (values.size() >= 2) {
                    BitmapValue opA = values.top();
                    values.pop();
                    BitmapValue opB = values.top();
                    values.pop();
                    BitmapValue calResult;
                    bitmapCalculate(opA, opB, c, calResult);
                    values.push(calResult);
                }
            }
        }
        BitmapValue result;
        if (bitmapKey.length() > 0) {
            result |= _bitmaps[bitmapKey];
        } else if (!values.empty()) {
            result |= values.top();
        }
        return result;
    }

    // calculate the bitmap value by expr bitmap calculate
    int64_t bitmap_calculate_count() {
        if (_bitmaps.empty()) {
            return 0;
        }
        return bitmap_calculate().cardinality();
    }

private:
    int priority(char c) {
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

    template<class T> 
    std::string printStack(std::stack<T> stack) {
        std::string result = "";
        while (!stack.empty()) {
            result = stack.top() + result;
            stack.pop();
        }
        return result;
    }

    std::string reversePolish(std::string inputStr) {
        std::stack<char> polish;
        std::stack<char> opStack;
        bool lastIsChar = false;
        for (int i = 0; i < inputStr.length(); i++) {
            char curChar = inputStr.at(i);
            // 如果是字符串
            if (curChar != '&' && curChar != '|' && curChar != '^' && curChar != '-' &&
                curChar != '(' && curChar != ')' && curChar != ' ' && curChar != '\t') {
                if (!lastIsChar) {
                    polish.push(' ');
                }
                polish.push(curChar);
                lastIsChar = true;
                continue;
            }
            // 转义字符
            else if (i != 0 && inputStr.at(i - 1) == '\\') {
                polish.push(curChar);
                lastIsChar = true;
                continue;
            }
            // 为空格
            else if (curChar == ' ' || curChar == '\t') {
                lastIsChar = false;
                continue;
            }
            // 否则为操作符
            else if (curChar == '(') {
                opStack.push(curChar);
            } else if (!opStack.empty() && curChar == ')') {
                while (!opStack.empty() && opStack.top() != '(') {
                    polish.push(opStack.top());
                    opStack.pop();
                }
                opStack.pop();
            } else {
                if (!opStack.empty() && opStack.top() == '(') {
                    opStack.push(curChar);
                } else {
                    if (!opStack.empty() && priority(curChar) > priority(opStack.top())) {
                        opStack.push(curChar);
                    } else {
                        while (!opStack.empty()) {
                            if (opStack.top() == '(') {
                                break;
                            }
                            if (priority(curChar) <= priority(opStack.top())) {
                                polish.push(opStack.top());
                                opStack.pop();
                            } else {
                                break;
                            }
                        }
                        opStack.push(curChar);
                    }
                }
            }
            lastIsChar = false;
        }

        while (!opStack.empty()) {
            polish.push(opStack.top());
            opStack.pop();
        }
        return printStack(polish);
    }

    // bitmap交并差运算 因为数据是放在堆栈中 所以前一个操作数是opB 后一个操作数是opA
    void bitmapCalculate(BitmapValue& opA, BitmapValue& opB, char op, BitmapValue& result) {
        result |= opB;
        switch (op) {
        // 交集计算
        case '&':
            result &= opA;
            break;
        // 并集计算
        case '|':
            result |= opA;
            break;
        // 差集计算
        case '-':
            result -= opA;
            break;
        // 异或计算
        case '^':
            result ^= opA;
            break;
        }
    }

    std::string _polish;
};

} // namespace doris
