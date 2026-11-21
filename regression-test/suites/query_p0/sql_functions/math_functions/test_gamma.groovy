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

suite("test_gamma") {
    sql " drop table if exists test_gamma"
    sql """
        create table test_gamma (
            k0 int,
            a double not null,
            b double null
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select gamma(b) from test_gamma"
    order_qt_empty_not_nullable "select gamma(a) from test_gamma"

    sql "insert into test_gamma values (1, 1, null), (1, 1, null), (1, 1, null)"
    order_qt_all_null "select gamma(b) from test_gamma"

    sql "truncate table test_gamma"
    sql """ insert into test_gamma values
        (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 10, 10),
        (7, 0, 0), (8, cast('-0.0' as double), cast('-0.0' as double)), (9, -1, -1), (10, -2, -2), (11, -3, -3),
        (12, 0.5, 0.5), (13, -0.5, -0.5), (14, 1.5, 1.5), (15, -1.5, -1.5), (16, -2.5, -2.5),
        (17, 171, 171), (18, 172, 172), (19, 1e308, 1e308),
        (20, cast('nan' as double), cast('nan' as double)),
        (21, cast('inf' as double), cast('inf' as double)),
        (22, cast('-inf' as double), cast('-inf' as double)),
        (23, 123, null),
        (24, 1e-300, 1e-300), (25, 5e-324, 5e-324), (26, 2.2250738585072014e-308, 2.2250738585072014e-308),
        (27, 171.5, 171.5), (28, 171.8, 171.8), (29, -1000.5, -1000.5), (30, -171.5, -171.5);
    """

    // k0 = 8 must really hold a negative zero. Spelled as the plain integer "-0" it is stored
    // as +0.0 and silently duplicates k0 = 7, so the negative-zero input would never be
    // covered while the generated .out file still looks plausible. This assertion guards the
    // test input rather than a query result, which is why it is not a qt_sql block.
    def negativeZero = sql "select signbit(a), signbit(b) from test_gamma where k0 = 8"
    assertTrue(negativeZero.size() == 1 && negativeZero[0][0] && negativeZero[0][1],
            "k0 = 8 must store -0.0 in both a and b, but got ${negativeZero}")

    // The near-zero rows hold whatever libm's tgamma returns rather than the correctly rounded
    // value: gamma(1e-300) is 9.999999999999763e299 instead of 1e300 and gamma(2^-1022) is
    // 4.4942328371556665e307 instead of 2^1022, about 1e-14 relative away. That is amplified by
    // |ln gamma(x)| ~ 700 and is still far inside the 1e-8 relative tolerance the framework
    // applies to DOUBLE cells, so those rows do not pin one libm version. Two properties are not
    // visible to that tolerance and are asserted here instead: the sign of a result that
    // underflows to zero (0.0 and -0.0 compare equal) and a subnormal result that must not
    // collapse to zero (which the framework's decimal-place fallback would accept).
    def underflowSign = sql "select signbit(gamma(a)) from test_gamma where k0 = 29"
    assertTrue(underflowSign.size() == 1 && underflowSign[0][0],
            "gamma(-1000.5) must underflow to a signed zero, but got ${underflowSign}")
    def subnormalResult = sql "select gamma(a) > 0, gamma(a) < 2.2250738585072014e-308 from test_gamma where k0 = 30"
    assertTrue(subnormalResult.size() == 1 && subnormalResult[0][0] && subnormalResult[0][1],
            "gamma(-171.5) must stay a positive subnormal, but got ${subnormalResult}")

    order_qt_nullable "select gamma(b) from test_gamma"
    order_qt_not_nullable "select gamma(a) from test_gamma"
    order_qt_nullable_no_null "select gamma(nullable(a)) from test_gamma"
    order_qt_const_nullable "select gamma(NULL) from test_gamma"
    order_qt_const_not_nullable "select gamma(0.5) from test_gamma"
    order_qt_const_nullable_no_null "select gamma(nullable(0.5))"

    // Both settings evaluate gamma in the BE now that the FE folding is gone, so this passes by
    // construction; it guards the folding that used to live here. A boundary classified differently
    // (NULL, NaN or an infinity against a finite value, as gamma(-1000.5) would be) always fails
    // here, but two numeric divergences slip through the tolerance checkCell applies to DOUBLE
    // cells: it divides by the magnitude of the real cell, so -0.0 against 0.0 reports no error,
    // and its decimal-place fallback accepts a subnormal result rounded to 0.0. The last two
    // columns are therefore BOOLEAN, which checkCell compares exactly: signbit(gamma(-1000.5))
    // catches a flipped sign, and gamma(-171.5) > 0 a result that underflowed to zero. A last-place
    // difference in the value itself is tolerated on purpose, the same reason the folding was
    // dropped.
    testFoldConst """ select gamma(0), gamma(-1), gamma(-2), gamma(0.5), gamma(5), gamma(-2.5), gamma(171), gamma(172), gamma(cast('nan' as double)), gamma(cast('inf' as double)), gamma(cast('-inf' as double)), gamma(171.5), gamma(171.8), gamma(1e-300), gamma(5e-324), gamma(2.2250738585072014e-308), gamma(-1000.5), gamma(-171.5), signbit(gamma(-1000.5)), gamma(-171.5) > 0 """
}
