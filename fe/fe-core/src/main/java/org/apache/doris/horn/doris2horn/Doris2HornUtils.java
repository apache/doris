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

package org.apache.doris.horn.doris2horn;


import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;

/**
 * doris2horn 正向翻译路径下的纯静态辅助方法集中点（对称 horn2doris 侧的
 * {@code Horn2DorisUtils}）。跟具体 visitor 实例状态无关的小工具放这里复用。
 */
public final class Doris2HornUtils {

    private Doris2HornUtils() {
    }

    /**
     * 剥掉一层 {@link Alias} 取内部表达式（Alias 在 horn wire 上透明，exprId/name 由输出列
     * SlotReference 携带）；非 Alias 原样返回。
     */
    public static Expression unwrapExpr(Expression expr) {
        return (expr instanceof Alias) ? ((Alias) expr).child() : expr;
    }
}
