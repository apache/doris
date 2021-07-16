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

#pragma once

#include <vec/aggregate_functions/aggregate_function.h>
#include <vec/data_types/data_type.h>

#include <memory>

namespace doris::vectorized {

/** Aggregate function combinator allows to take one aggregate function
  *  and transform it to another aggregate function.
  *
  * In SQL language they are used as suffixes for existing aggregate functions.
  *
  * Example: -If combinator takes an aggregate function and transforms it
  *  to aggregate function with additional argument at end (condition),
  *  that will pass values to original aggregate function when the condition is true.
  *
  * More examples:
  *
  * sum(x) - calculate sum of x
  * sumIf(x, cond) - calculate sum of x for rows where condition is true.
  * sumArray(arr) - calculate sum of all elements of arrays.
  *
  * PS. Please don't mess it with so called "combiner" - totally unrelated notion from Hadoop world.
  * "combining" - merging the states of aggregate functions - is supported naturally in ClickHouse.
  */

class IAggregateFunctionCombinator {
public:
    virtual String get_name() const = 0;

    virtual bool is_for_internal_usage_only() const { return false; }

    /** From the arguments for combined function (ex: UInt64, UInt8 for sumIf),
      *  get the arguments for nested function (ex: UInt64 for sum).
      * If arguments are not suitable for combined function, throw an exception.
      */
    virtual DataTypes transform_arguments(const DataTypes& arguments) const { return arguments; }

    /** From the parameters for combined function,
      *  get the parameters for nested function.
      * If arguments are not suitable for combined function, throw an exception.
      */
    virtual Array transform_parameters(const Array& parameters) const { return parameters; }

    /** Create combined aggregate function (ex: sumIf)
      *  from nested function (ex: sum)
      *  and arguments for combined agggregate function (ex: UInt64, UInt8 for sumIf).
      * It's assumed that function transform_arguments was called before this function and 'arguments' are validated.
      */
    virtual AggregateFunctionPtr transform_aggregate_function(
            const AggregateFunctionPtr& nested_function, const DataTypes& arguments,
            const Array& params, const bool result_is_nullable) const = 0;

    virtual ~IAggregateFunctionCombinator() {}
};

using AggregateFunctionCombinatorPtr = std::shared_ptr<const IAggregateFunctionCombinator>;

} // namespace doris::vectorized
