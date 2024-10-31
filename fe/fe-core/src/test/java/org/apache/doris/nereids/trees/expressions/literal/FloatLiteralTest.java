package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.nereids.trees.expressions.literal.format.FloatChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.function.Function;

public class FloatLiteralTest {
    @Test
    public void testChecker() {
        assertValid(
                "123.45",
                "-34.56",
                "0",
                "0.01",
                "10000",
                "+1",
                "-1",
                "+1",
                "1.0",
                "-1.0",
                "-1.0e3",
                ".1",
                "1.",
                "1e3"
        );

        assertInValid(
                "e3",
                "abc",
                "12.34.56",
                "1,234.56"
        );

        Assertions.assertThrows(
                Throwable.class,
                () -> check("e3", s -> new FloatLiteral(new BigDecimal(s).floatValue()))
        );
    }

    private void assertValid(String...validString) {
        for (String str : validString) {
            check(str, s -> new FloatLiteral(new BigDecimal(s).floatValue()));
        }
    }

    private void assertInValid(String...validString) {
        for (String str : validString) {
            Assertions.assertThrows(
                    Throwable.class,
                    () -> check(str, s -> new FloatLiteral(new BigDecimal(s).floatValue()))
            );
        }
    }

    private <T extends FractionalLiteral> T check(String str, Function<String, T> literalBuilder) {
        Assertions.assertTrue(FloatChecker.isValidFloat(str), "Invalid fractional: " + str);
        return literalBuilder.apply(str);
    }
}
