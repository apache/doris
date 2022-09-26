package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.junit.Assert;
import org.junit.Test;

public class CastExprTest {
    @Test
    public void testCastTo() {
        ScalarType charType = ScalarType.createType(PrimitiveType.CHAR, 2, 0, 0);
        ScalarType varcharType = ScalarType.createType(PrimitiveType.VARCHAR, 2, 0, 0);
        ScalarType stringType = ScalarType.createType(PrimitiveType.STRING, 2, 0, 0);

        StringLiteral value = new StringLiteral("20210926");
        if (charType.getLength() >= 0  && value.getStringValue() != null
                && charType.getLength() < value.getStringValue().length()) {
            String subStringValue = value.getStringValue().substring(0, charType.getLength());
            Assert.assertEquals(subStringValue, "20");
        }
        if (varcharType.getLength() >= 0  && value.getStringValue() != null
            && varcharType.getLength() < value.getStringValue().length()) {
            String subStringValue = value.getStringValue().substring(0, varcharType.getLength());
            Assert.assertEquals(subStringValue, "20");
        }
        if (stringType.getLength() >= 0  && value.getStringValue() != null
            && stringType.getLength() < value.getStringValue().length()) {
            String subStringValue = value.getStringValue().substring(0, stringType.getLength());
            Assert.assertEquals(subStringValue, "20");
        }

    }
}

