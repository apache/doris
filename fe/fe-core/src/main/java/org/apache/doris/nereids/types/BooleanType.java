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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.coercion.PrimitiveType;

/**
 * Boolean type in Nereids.
 */
public class BooleanType extends PrimitiveType {
    public static final BooleanType INSTANCE = new BooleanType();

    private static int WIDTH = 1;

    private BooleanType() {
    }

    @Override
    public Type toCatalogDataType() {
        return Type.BOOLEAN;
    }

    @Override
    public String simpleString() {
        return "boolean";
    }

    @Override
    public int width() {
        return WIDTH;
    }
}
