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
