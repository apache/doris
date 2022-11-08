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

package org.apache.doris.nereids.trees.expressions.functions;

/**
 * you can implement this interface to simply compute nullable property.
 * this interface migrated from NullableMode enum.
 *
 * nullable mode:
 * 1. PropagateNullable: nullable = true if any children is nullable
 * 2. AlwaysNullable: nullable = true
 * 3. AlwaysNotNullable: nullable = false
 * 4. Custom(No interface): if you want to custom compute nullable, you can not inherit the above three interfaces
 *                          and override nullable function. e.g. `cast(string as bigint)` is AlwaysNullable and
 *                          `cast(int as bigint)` is PropagateNullable. if you want to find the custom nullable
 *                          functions, you can find the override nullable functions which is not the above three
 *                          interfaces.
 */
public interface ComputeNullable extends ExpressionTrait {
}
