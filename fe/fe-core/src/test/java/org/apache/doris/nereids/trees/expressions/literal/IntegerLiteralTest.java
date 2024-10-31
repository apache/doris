package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.nereids.trees.expressions.literal.format.IntegerChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.function.Function;

public class IntegerLiteralTest {
    @Test
    public void testChecker() {
        assertValid(
                "1",
                "+1",
                "-1",
                "456"
        );

        assertInValid(
                "1.0",
                "1e3",
                "abc"
        );
    }


    private void assertValid(String...validString) {
        for (String str : validString) {
            check(str, s -> new IntegerLiteral(new BigInteger(s).intValueExact()));
        }
    }

    private void assertInValid(String...validString) {
        for (String str : validString) {
            Assertions.assertThrows(
                    Throwable.class,
                    () -> check(str, s -> new IntegerLiteral(new BigInteger(s).intValueExact()))
            );
        }
    }

    private <T extends IntegerLikeLiteral> T check(String str, Function<String, T> literalBuilder) {
        Assertions.assertTrue(IntegerChecker.isValidInteger(str), "Invalid integer: " + str);
        return literalBuilder.apply(str);
    }
}
