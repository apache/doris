package org.apache.doris.common.util;

import org.junit.Test;

import org.junit.Assert;

public class S3URITest {
    @Test
    public void testLocationParsing() {
        String p1 = "s3://bucket/path/to/file";
        S3URI uri1 = new S3URI(p1);

        Assert.assertEquals("bucket", uri1.getBucket());
        Assert.assertEquals("path/to/file", uri1.getKey());
        Assert.assertEquals(p1, uri1.toString());
    }
    @Test
    public void testPathLocationParsing() {
        String p1 = "s3://bucket/path/";
        S3URI uri1 = new S3URI(p1);

        Assert.assertEquals("bucket", uri1.getBucket());
        Assert.assertEquals("path/", uri1.getKey());
        Assert.assertEquals(p1, uri1.toString());
    }

    @Test
    public void testEncodedString() {
        String p1 = "s3://bucket/path%20to%20file";
        S3URI uri1 = new S3URI(p1);

        Assert.assertEquals("bucket", uri1.getBucket());
        Assert.assertEquals("path%20to%20file", uri1.getKey());
        Assert.assertEquals(p1, uri1.toString());
    }

    @Test(expected = IllegalStateException.class)
    public void missingKey() {
        new S3URI("https://bucket/");
    }

    @Test(expected = IllegalStateException.class)
    public void relativePathing() {
        new S3URI("/path/to/file");
    }

    @Test(expected = IllegalStateException.class)
    public void invalidScheme() {
        new S3URI("ftp://bucket/");
    }

    @Test
    public void testQueryAndFragment() {
        String p1 = "s3://bucket/path/to/file?query=foo#bar";
        S3URI uri1 = new S3URI(p1);

        Assert.assertEquals("bucket", uri1.getBucket());
        Assert.assertEquals("path/to/file", uri1.getKey());
        Assert.assertEquals(p1, uri1.toString());
    }
}