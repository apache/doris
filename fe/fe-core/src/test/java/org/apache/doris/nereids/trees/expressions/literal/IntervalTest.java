package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.nereids.trees.expressions.literal.Interval.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IntervalTest {
    @Test
    public void testIntervalEquals() {
        Interval i1 = new Interval(new IntegerLiteral(1), TimeUnit.DAY);
        Interval i2 = new Interval(new IntegerLiteral(1), TimeUnit.SECOND);
        Assertions.assertNotEquals(i1, i2);
    }
}
