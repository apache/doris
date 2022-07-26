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

/**
 * Decimal type in Nereids.
 */
public class DecimalType extends FractionalType {

    private int precision;
    private int scale;

    public DecimalType(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    public static DecimalType createDecimalType(int precision, int scale) {
        return new DecimalType(precision, scale);
    }

    @Override
    public Type toCatalogDataType() {
        return Type.DECIMALV2;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }
}

