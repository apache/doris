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
import org.apache.doris.nereids.types.coercion.DateLikeType;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Datetime type in Nereids.
 */
public class DateTimeV2Type extends DateLikeType {
    public static final int MAX_SCALE = 6;
    public static final DateTimeV2Type INSTANCE = new DateTimeV2Type(0);

    private static final int WIDTH = 8;

    private int scale;

    private DateTimeV2Type(int scale) {
        Preconditions.checkArgument(0 <= scale && scale <= MAX_SCALE);
        this.scale = scale;
    }

    public static DateTimeV2Type of(int scale) {
        if (scale == INSTANCE.scale) {
            return INSTANCE;
        } else {
            return new DateTimeV2Type(scale);
        }
    }

    @Override
    public Type toCatalogDataType() {
        return ScalarType.createDatetimeV2Type(scale);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DateTimeV2Type that = (DateTimeV2Type) o;
        return Objects.equals(scale, that.scale);
    }

    @Override
    public int width() {
        return WIDTH;
    }

    public int getScale() {
        return scale;
    }
}
