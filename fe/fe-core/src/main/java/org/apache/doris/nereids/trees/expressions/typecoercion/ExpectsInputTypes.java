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

package org.apache.doris.nereids.trees.expressions.typecoercion;

import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.types.DataType;

import java.util.List;

/**
 * An interface to define the expected input types of an expression.
 *
 * This interface is typically used by operator expressions
 * (e.g. {@link org.apache.doris.nereids.trees.expressions.Add})
 * to define expected input types without any implicit casting.
 *
 * Most function expressions (e.g. {@link Substring}
 * should extend {@link ImplicitCastInputTypes}) instead.
 */
public interface ExpectsInputTypes {

    List<DataType> expectedInputTypes();
}
