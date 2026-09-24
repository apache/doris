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

suite("test_pow_square") {
    qt_square_aliases """
        select number, pow(x, 2.0), power(x, 2.0), dpow(x, 2.0), fpow(x, 2.0), x * x
        from (select number, cast(number - 4 as double) / 2 as x
              from numbers("number" = "9")) t
        order by number
    """

    qt_nullable_square """
        select number, pow(x, 2.0), pow(x, cast(null as double)), x * x
        from (select number, if(number = 0, null, cast(number - 4 as double)) as x
              from numbers("number" = "9")) t
        order by number
    """

    qt_column_shapes """
        select number, pow(2.0, x), pow(x, x), pow(-2.0, 2.0)
        from (select number, cast(number as double) as x
              from numbers("number" = "5")) t
        order by number
    """

    // The first two rows have an exponent of 2, but y remains a vector because later rows
    // have an exponent of 3. These bases expose a one-ULP difference between pow(x, 2) and x * x.
    // Derive x from number to keep alias evaluation in BE rather than FE constant folding.
    // Do not guard the equality with y = 2: predicate inference can simplify it away.
    qt_square_shape_equality """
        select number,
               pow(x, 2.0) = pow(x, y),
               power(x, 2.0) = power(x, y),
               dpow(x, 2.0) = dpow(x, y),
               fpow(x, 2.0) = fpow(x, y)
        from (
            select number,
                   cast(2 * number - 1 as double)
                       * cast('1.1500729535343723e-17' as double) as x,
                   if(number < 2, 2.0, 3.0) as y
            from numbers("number" = "4")
        ) t order by number
    """

    qt_exact_integer_square """
        select number, pow(x, 2.0), power(x, 2.0), dpow(x, 2.0), fpow(x, 2.0), x * x
        from (
            select number,
                   cast(number * 10000000 as double) * if(number % 2 = 0, 1, -1) as x
            from numbers("number" = "7")
        ) t order by number
    """

    qt_integer_square_boundaries """
        select number, pow(x, 2.0), power(x, 2.0), dpow(x, 2.0), fpow(x, 2.0),
               pow(x, 2.0) = pow(x, y)
        from (
            select number,
                   cast(number + 67108860 as double) * if(number % 2 = 0, 1, -1) as x,
                   if(number < 8, 2.0, 3.0) as y
            from numbers("number" = "10")
        ) t order by number
    """

    // Integer bases alone are not sufficient: these squares are not exactly representable.
    qt_out_of_range_square_shapes """
        select number,
               pow(x, 2.0) = pow(x, y), power(x, 2.0) = power(x, y),
               dpow(x, 2.0) = dpow(x, y), fpow(x, 2.0) = fpow(x, y)
        from (
            select number, cast(2 * number - 1 as double) * 94906297.0 as x,
                   if(number < 2, 2.0, 3.0) as y
            from numbers("number" = "4")
        ) t order by number
    """

    qt_other_exponents """
        select number, pow(x, 0.0), pow(x, 1.0), pow(x, -2.0), pow(x, 3.0), pow(x, 0.5)
        from (select number, cast(number - 2 as double) as x
              from numbers("number" = "5")) t
        order by number
    """

    qt_square_boundaries """
        select number, pow(x, 2.0), x * x
        from (
            select number, case number
                when 0 then cast('nan' as double)
                when 1 then cast('inf' as double)
                when 2 then cast('-inf' as double)
                when 3 then cast('-0.0' as double)
                when 4 then cast('1e308' as double)
                when 5 then cast('1e-308' as double)
                when 6 then cast('1e154' as double)
                when 7 then cast('1e-154' as double)
                end as x
            from numbers("number" = "8")
        ) t order by number
    """
}
