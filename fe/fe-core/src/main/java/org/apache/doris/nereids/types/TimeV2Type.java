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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.coercion.PrimitiveType;
import org.apache.doris.nereids.types.coercion.RangeScalable;

/**
 * Datetime type in Nereids.
 */
public class TimeV2Type extends PrimitiveType implements RangeScalable {

    public static final TimeV2Type INSTANCE = new TimeV2Type();

    private static final int WIDTH = 8;
    private final int scale;

    private TimeV2Type(int scale) {
        this.scale = scale;
    }

    private TimeV2Type() {
        scale = 0;
    }

    @Override
    public Type toCatalogDataType() {
        return ScalarType.createTimeV2Type(scale);
    }

    /**
     * create TimeV2Type from scale
     */
    public static TimeV2Type of(int scale) {
        return new TimeV2Type(scale);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TimeV2Type;
    }

    @Override
    public int width() {
        return WIDTH;
    }

    public int getScale() {
        return scale;
    }
}
