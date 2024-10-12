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

package org.apache.doris.datasource.jdbc;

import org.apache.doris.datasource.jdbc.client.JdbcClientException;

import  org.junit.Assert;
import org.junit.Test;

public class JdbcClientExceptionTest {

    @Test
    public void testExceptionWithoutArgs() {
        String message = "An error occurred.";
        JdbcClientException exception = new JdbcClientException(message);

        Assert.assertEquals(message, exception.getMessage());
        Assert.assertNull(exception.getCause());
    }

    @Test
    public void testExceptionWithFormattingArgs() {
        String format = "Error code: %d, message: %s";
        int errorCode = 404;
        String errorMsg = "Not Found";
        JdbcClientException exception = new JdbcClientException(format, errorCode, errorMsg);

        String expectedMessage = String.format(format, errorCode, errorMsg);
        Assert.assertEquals(expectedMessage, exception.getMessage());
        Assert.assertNull(exception.getCause());
    }

    @Test
    public void testExceptionWithPercentInFormatString() {
        String format = "Usage is at 80%%, threshold is %d%%";
        int threshold = 75;
        JdbcClientException exception = new JdbcClientException(format, threshold);

        String expectedMessage = String.format(format, threshold);
        Assert.assertEquals(expectedMessage, exception.getMessage());
        Assert.assertNull(exception.getCause());
    }

    @Test
    public void testExceptionWithPercentInArgs() {
        String format = "Invalid input: %s";
        String input = "50% discount";
        JdbcClientException exception = new JdbcClientException(format, input);

        String expectedMessage = String.format(format, input.replace("%", "%%"));
        Assert.assertEquals(expectedMessage, exception.getMessage());
        Assert.assertNull(exception.getCause());
    }

    @Test
    public void testExceptionWithCause() {
        String message = "Database connection failed.";
        Exception cause = new Exception("Timeout occurred");
        JdbcClientException exception = new JdbcClientException(message, cause);

        Assert.assertEquals(message, exception.getMessage());
        Assert.assertEquals(cause, exception.getCause());
    }

    @Test
    public void testExceptionWithFormattingArgsAndCause() {
        String format = "Failed to execute query: %s";
        String query = "SELECT * FROM users";
        Exception cause = new Exception("Syntax error");
        JdbcClientException exception = new JdbcClientException(format, cause, query);

        String expectedMessage = String.format(format, query);
        Assert.assertEquals(expectedMessage, exception.getMessage());
        Assert.assertEquals(cause, exception.getCause());
    }

    @Test
    public void testExceptionWithPercentInArgsAndCause() {
        String format = "File path: %s";
        String filePath = "C:\\Program Files\\App%20Data";
        Exception cause = new Exception("File not found");
        JdbcClientException exception = new JdbcClientException(format, cause, filePath);

        String expectedMessage = String.format(format, filePath.replace("%", "%%"));
        Assert.assertEquals(expectedMessage, exception.getMessage());
        Assert.assertEquals(cause, exception.getCause());
    }

    @Test
    public void testExceptionWithNoFormattingNeeded() {
        String message = "Simple error message.";
        JdbcClientException exception = new JdbcClientException(message, (Object[]) null);

        Assert.assertEquals(message, exception.getMessage());
        Assert.assertNull(exception.getCause());
    }

    @Test
    public void testExceptionWithNullArgs() {
        String format = "Error occurred: %s";
        JdbcClientException exception = new JdbcClientException(format, (Object[]) null);

        // Since args are null, message should remain unformatted
        Assert.assertEquals(format, exception.getMessage());
        Assert.assertNull(exception.getCause());
    }

    @Test
    public void testExceptionWithEmptyArgs() {
        String format = "Error occurred: %s";
        JdbcClientException exception = new JdbcClientException(format, new Object[]{});

        // Since args are empty, message should remain unformatted
        Assert.assertEquals(format, exception.getMessage());
        Assert.assertNull(exception.getCause());
    }
}
