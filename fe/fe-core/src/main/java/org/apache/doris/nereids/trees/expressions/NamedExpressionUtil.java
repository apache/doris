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

import org.apache.doris.common.IdGenerator;

import com.google.common.annotations.VisibleForTesting;

import java.lang.reflect.Field;

/**
 * The util of named expression.
 */
public class NamedExpressionUtil {
    /**
     * Tool class for generate next ExprId.
     */
    private static final IdGenerator<ExprId> ID_GENERATOR = ExprId.createGenerator();

    public static ExprId newExprId() {
        return ID_GENERATOR.getNextId();
    }

    /**
     *  Reset Id Generator
     */
    @VisibleForTesting
    public static void clear() throws Exception {
        Field nextId = ID_GENERATOR.getClass().getSuperclass().getDeclaredField("nextId");
        nextId.setAccessible(true);
        nextId.setInt(ID_GENERATOR, 0);
    }
}
