/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ORC_TRUTHVALUE_HH
#define ORC_TRUTHVALUE_HH

namespace orc {

  /**
   * The potential result sets of logical operations.
   */
  enum class TruthValue {
    YES,         // all rows satisfy the predicate
    NO,          // all rows dissatisfy the predicate
    IS_NULL,     // all rows are null value
    YES_NULL,    // null values exist, not-null rows satisfy the predicate
    NO_NULL,     // null values exist, not-null rows dissatisfy the predicate
    YES_NO,      // some rows satisfy the predicate and the others not
    YES_NO_NULL  // null values exist, some rows satisfy predicate and some not
  };

  // Compute logical or between the two values.
  TruthValue operator||(TruthValue left, TruthValue right);

  // Compute logical AND between the two values.
  TruthValue operator&&(TruthValue left, TruthValue right);

  // Compute logical NOT for one value.
  TruthValue operator!(TruthValue val);

  // Do we need to read the data based on the TruthValue?
  bool isNeeded(TruthValue val);

}  // namespace orc

#endif  // ORC_TRUTHVALUE_HH
