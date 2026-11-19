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

package org.apache.doris.nereids.types.coercion;

import org.apache.doris.nereids.types.DataType;

/**
 * A temporal type whose scale participates in function precision promotion.
 * TIMESTAMP_NS also has a scale, but its fixed nanosecond representation must not be promoted
 * or narrowed along with DATETIMEV2, TIMEV2, and TIMESTAMPTZ arguments.
 */
public interface AdjustableScaleTimeType extends ScaleTimeType {
    DataType withScale(int scale);
}
