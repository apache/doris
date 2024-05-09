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

/**
 * numeric type/ date related type are range scalable
 * RangeScalable Column can be estimated by filter like "A < 10" more accurate.
 * For example, for a given relation R, which contains 10 rows. R.A in (1, 100),
 * the selectivity of filter "A<10" is "(10-1) / (100 -1)"
 * But for string column A, the filter selectivity of "A<'abc'" can not be estimated by range, although we could
 * have an order reserved mapping from string value to double.
 *
 */
public interface RangeScalable {
}
