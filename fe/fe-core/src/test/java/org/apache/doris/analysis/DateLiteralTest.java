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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.junit.Assert;
import org.junit.Test;

public class DateLiteralTest {

    @Test
    public void TwoDigitYear() {
        boolean hasException = false;
        try {
            DateLiteral literal = new DateLiteral("1997-10-07", Type.DATE);
            Assert.assertEquals(1997, literal.getYear());

            DateLiteral literal2 = new DateLiteral("97-10-07", Type.DATE);
            Assert.assertEquals(1997, literal2.getYear());

            DateLiteral literal3 = new DateLiteral("0097-10-07", Type.DATE);
            Assert.assertEquals(97, literal3.getYear());

            DateLiteral literal4 = new DateLiteral("99-10-07", Type.DATE);
            Assert.assertEquals(1999, literal4.getYear());

            DateLiteral literal5 = new DateLiteral("70-10-07", Type.DATE);
            Assert.assertEquals(1970, literal5.getYear());

            DateLiteral literal6 = new DateLiteral("69-10-07", Type.DATE);
            Assert.assertEquals(2069, literal6.getYear());

            DateLiteral literal7 = new DateLiteral("00-10-07", Type.DATE);
            Assert.assertEquals(2000, literal7.getYear());

        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }

    @Test
    public void uncheckedCastTo() {
        boolean hasException = false;
        try {
            DateLiteral literal = new DateLiteral("1997-10-07", Type.DATE);
            Expr castToExpr = literal.uncheckedCastTo(Type.DATETIME);
            Assert.assertTrue(castToExpr instanceof DateLiteral);
            Assert.assertEquals(castToExpr.type, Type.DATETIME);

            DateLiteral literal2 = new DateLiteral("1997-10-07 12:23:23", Type.DATETIME);
            Expr castToExpr2 = literal2.uncheckedCastTo(Type.DATETIME);
            Assert.assertTrue(castToExpr2 instanceof DateLiteral);
        } catch (AnalysisException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertFalse(hasException);
    }
}
