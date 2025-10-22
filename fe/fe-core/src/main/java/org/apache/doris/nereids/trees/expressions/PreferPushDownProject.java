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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;

/**
 * some expressions prefer push down project under the plan,
 *
 * e.g. the element_at function prefer push down to a project in under the plan:
 *    Project(element_at(left.column, 'id') as a)
 *               |
 *              Join
 *           /       |
 *         left      right
 *
 * we will push down project through the Join, so we can prune the column which has complex type:
 *
 *                                Join
 *      /                                                     \
 *  Project(element_at(left.column, 'id') as a, ...)         right
 *       |
 *    left
 */
public interface PreferPushDownProject extends ExpressionTrait {
}
