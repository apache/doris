package org.apache.doris.common;

import org.junit.Assert;
import org.junit.Test;

public class PairTest {

    @Test
    public void testToString() {
        Pair<String, Object> pairFirstNull = Pair.of(null, "world");
        Assert.assertEquals(":world", pairFirstNull.toString());

        Pair<String, Object> pairSecondNull = Pair.of("hello", null);
        Assert.assertEquals("hello:", pairSecondNull.toString());
    }

    @Test
    public void testEquals() {
        Pair<String, Object> firstPair = Pair.of(null, "world");
        Pair<String, Object> secondPair = null;

        Assert.assertTrue(firstPair.equals(firstPair));
        Assert.assertFalse(firstPair.equals(secondPair));

        secondPair = Pair.of(null, "world");
        Assert.assertTrue(firstPair.equals(secondPair));

        secondPair = Pair.of("hello", null);
        Assert.assertFalse(firstPair.equals(secondPair));

        firstPair = Pair.of("hello", "world");
        secondPair = Pair.of("hello", "world");
        Assert.assertTrue(firstPair.equals(secondPair));

        secondPair = Pair.of("world", "hello");
        Assert.assertFalse(firstPair.equals(secondPair));
    }
}
