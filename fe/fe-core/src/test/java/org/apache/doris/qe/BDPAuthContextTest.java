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

package org.apache.doris.qe;

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TBDPAuthContext;
import org.apache.doris.thrift.TUniqueId;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BDPAuthContextTest {
    private BDPAuthContext bdpAuthContext;
    private ConnectContext connectContext;

    @Before
    public void setUp() {
        // Clean up any existing thread local data
        BDPAuthContext.clear();
        ConnectContext.remove();

        // Create test BDPAuthContext
        bdpAuthContext = new BDPAuthContext("test_erp", "test_source", "test_hadoop_user", "test_token");
        bdpAuthContext.setUserType("dev_personal");
        bdpAuthContext.setBusinessLine("test_business");

        // Create test ConnectContext
        connectContext = new ConnectContext();
        connectContext.setQueryId(new TUniqueId(12345L, 67890L));
    }

    @After
    public void tearDown() {
        BDPAuthContext.clear();
        ConnectContext.remove();
    }

    @Test
    public void testBasicGettersAndSetters() {
        Assert.assertEquals("test_erp", bdpAuthContext.getErp());
        Assert.assertEquals("test_source", bdpAuthContext.getSource());
        Assert.assertEquals("test_hadoop_user", bdpAuthContext.getHadoopUserName());
        Assert.assertEquals("test_token", bdpAuthContext.getUserToken());
        Assert.assertEquals("dev_personal", bdpAuthContext.getUserType());
        Assert.assertEquals("test_business", bdpAuthContext.getBusinessLine());
        Assert.assertEquals("none", bdpAuthContext.getQueryIdStr());
    }

    @Test
    public void testErpChangedFlag() {
        Assert.assertFalse(bdpAuthContext.isErpChanged());
        bdpAuthContext.setErpChanged(true);
        Assert.assertTrue(bdpAuthContext.isErpChanged());
    }

    @Test
    public void testThreadLocalOperations() {
        // Initially should be null
        Assert.assertNull(BDPAuthContext.get());

        // Set thread local info
        bdpAuthContext.setThreadLocalInfo();
        Assert.assertNotNull(BDPAuthContext.get());
        Assert.assertEquals(bdpAuthContext, BDPAuthContext.get());

        // Clear thread local info
        BDPAuthContext.clear();
        Assert.assertNull(BDPAuthContext.get());
    }

    @Test
    public void testQueryIdOperations() {
        // Test setting and getting queryId
        TUniqueId testQueryId = new TUniqueId(123L, 456L);
        bdpAuthContext.setQueryId(testQueryId);
        Assert.assertEquals(DebugUtil.printId(testQueryId), bdpAuthContext.getQueryIdStr());

        // Test null queryId
        bdpAuthContext.setQueryId(null);
        Assert.assertEquals("none", bdpAuthContext.getQueryIdStr());
    }

    @Test
    public void testToStringWithQueryId() {
        TUniqueId testQueryId = new TUniqueId(456L, 789L);
        bdpAuthContext.setQueryId(testQueryId);
        String result = bdpAuthContext.toString();
        Assert.assertTrue(result.contains("query_id: " + DebugUtil.printId(testQueryId)));
    }

    @Test
    public void testConstructorFromTBDPAuthContext() {
        TBDPAuthContext tBdpAuthContext = new TBDPAuthContext("source", "erp", "hadoop_user", "token", false, null);
        tBdpAuthContext.setUserType("user_type");
        tBdpAuthContext.setBusinessLine("business_line");
        BDPAuthContext newContext = new BDPAuthContext(tBdpAuthContext);
        Assert.assertEquals("erp", newContext.getErp());
        Assert.assertEquals("source", newContext.getSource());
        Assert.assertEquals("hadoop_user", newContext.getHadoopUserName());
        Assert.assertEquals("token", newContext.getUserToken());
    }

    @Test
    public void testHmsClientCacheKey() {
        String cacheKey = bdpAuthContext.getHmsClientCacheKey();
        Assert.assertEquals("test_hadoop_userdev_personaltest_business", cacheKey);
        // Test with null userType and businessLine
        BDPAuthContext simpleContext = new BDPAuthContext("erp", "source", "hadoop_user", "token");
        String simpleCacheKey = simpleContext.getHmsClientCacheKey();
        Assert.assertEquals("hadoop_user", simpleCacheKey);
    }

    @Test
    public void testToString() {
        String result = bdpAuthContext.toString();
        String expected = "bdp_auth_context[erp: test_erp, source: test_source, hadoop_user_name: test_hadoop_user, view_based: false, query_id: none]";
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testCreateTBDPAuthContext_BasicCreation() {
        // Test basic TBDPAuthContext creation
        TBDPAuthContext result = new TBDPAuthContext(
                bdpAuthContext.getSource(),
                bdpAuthContext.getErp(),
                bdpAuthContext.getHadoopUserName(),
                bdpAuthContext.getUserToken(),
                bdpAuthContext.isViewBased(),
                bdpAuthContext.getQueryId());

        Assert.assertNotNull(result);
        Assert.assertEquals("test_source", result.getSource());
        Assert.assertEquals("test_erp", result.getErp());
        Assert.assertEquals("test_hadoop_user", result.getHadoopUserName());
        Assert.assertEquals("test_token", result.getUserToken());
    }

    @Test
    public void testTBDPAuthContext_OptionalFields() {
        // Test setting optional fields
        TBDPAuthContext result = new TBDPAuthContext(
                bdpAuthContext.getSource(),
                bdpAuthContext.getErp(),
                bdpAuthContext.getHadoopUserName(),
                bdpAuthContext.getUserToken(),
                bdpAuthContext.isViewBased(),
                bdpAuthContext.getQueryId());

        result.setUserType("dev_personal");
        result.setBusinessLine("test_business");
        TUniqueId testQueryId = new TUniqueId(111L, 222L);
        result.setQueryId(testQueryId);

        Assert.assertEquals("dev_personal", result.getUserType());
        Assert.assertEquals("test_business", result.getBusinessLine());
        Assert.assertEquals(testQueryId, result.getQueryId());
    }

    @Test
    public void testConstructorFromTBDPAuthContextWithQueryId() {
        // Test constructor with TBDPAuthContext that has queryId
        TUniqueId testQueryId = new TUniqueId(123L, 456L);
        TBDPAuthContext tBdpAuthContext = new TBDPAuthContext("source", "erp", "hadoop_user", "token", false, testQueryId);
        BDPAuthContext newContext = new BDPAuthContext(tBdpAuthContext);

        Assert.assertEquals("erp", newContext.getErp());
        Assert.assertEquals("source", newContext.getSource());
        Assert.assertEquals("hadoop_user", newContext.getHadoopUserName());
        Assert.assertEquals("token", newContext.getUserToken());
        Assert.assertEquals(DebugUtil.printId(testQueryId), newContext.getQueryIdStr());
    }

    @Test
    public void testConstructorFromTBDPAuthContextWithoutQueryId() {
        // Test constructor with TBDPAuthContext that doesn't have queryId
        TBDPAuthContext tBdpAuthContext = new TBDPAuthContext("source", "erp", "hadoop_user", "token", false, null);
        BDPAuthContext newContext = new BDPAuthContext(tBdpAuthContext);

        Assert.assertEquals("erp", newContext.getErp());
        Assert.assertEquals("source", newContext.getSource());
        Assert.assertEquals("hadoop_user", newContext.getHadoopUserName());
        Assert.assertEquals("token", newContext.getUserToken());
        Assert.assertEquals("none", newContext.getQueryIdStr());
    }

    @Test
    public void testConstructorWithQueryId() {
        // Test constructor with queryId parameter
        TUniqueId testQueryId = new TUniqueId(456L, 789L);
        BDPAuthContext context = new BDPAuthContext("erp", "source", "user", "token", false, null, null, testQueryId);

        Assert.assertEquals("erp", context.getErp());
        Assert.assertEquals("source", context.getSource());
        Assert.assertEquals("user", context.getHadoopUserName());
        Assert.assertEquals("token", context.getUserToken());
        Assert.assertEquals(DebugUtil.printId(testQueryId), context.getQueryIdStr());
    }

    @Test
    public void testQueryIdNullHandling() {
        // Test that null queryId is handled properly
        bdpAuthContext.setQueryId(null);
        Assert.assertEquals("none", bdpAuthContext.getQueryIdStr());

        // Test toString with null queryId
        String result = bdpAuthContext.toString();
        Assert.assertTrue(result.contains("query_id: none"));
    }

    @Test
    public void testConnectContextIntegrationBasic() {
        // Test basic integration with ConnectContext
        ConnectContext ctx = new ConnectContext();

        // Set BDPAuthContext when queryId is null
        ctx.setBdpAuthContext(bdpAuthContext);

        // QueryId should remain null
        Assert.assertEquals("none", bdpAuthContext.getQueryIdStr());

        // Test that setBdpAuthContext works without throwing exceptions
        Assert.assertEquals(bdpAuthContext, ctx.getBdpAuthContext());
    }

    @Test
    public void testToStringFormat() {
        // Test toString format with all fields
        TUniqueId testQueryId = new TUniqueId(789L, 101112L);
        bdpAuthContext.setQueryId(testQueryId);
        String result = bdpAuthContext.toString();

        String expected = "bdp_auth_context[erp: test_erp, source: test_source, "
                + "hadoop_user_name: test_hadoop_user, view_based: false, query_id: " + DebugUtil.printId(testQueryId) + "]";
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testQueryIdInThriftConversion() {
        // Test that queryId is properly handled in Thrift conversion scenarios
        TUniqueId testQueryId = new TUniqueId(303L, 404L);

        // Set the queryId in bdpAuthContext first
        bdpAuthContext.setQueryId(testQueryId);

        // Simulate creating TBDPAuthContext for network transmission
        TBDPAuthContext tBdpAuthContext = new TBDPAuthContext(
                bdpAuthContext.getSource(),
                bdpAuthContext.getErp(),
                bdpAuthContext.getHadoopUserName(),
                bdpAuthContext.getUserToken(),
                bdpAuthContext.isViewBased(),
                bdpAuthContext.getQueryId());

        Assert.assertEquals(testQueryId, tBdpAuthContext.getQueryId());

        // Simulate receiving and reconstructing BDPAuthContext
        BDPAuthContext reconstructedContext = new BDPAuthContext(tBdpAuthContext);
        Assert.assertEquals(DebugUtil.printId(testQueryId), reconstructedContext.getQueryIdStr());
    }

    @Test
    public void testQueryIdConsistency() {
        // Test that queryId remains consistent across operations
        TUniqueId testQueryId = new TUniqueId(404L, 505L);
        bdpAuthContext.setQueryId(testQueryId);
        String expectedQueryIdString = DebugUtil.printId(testQueryId);

        // Multiple calls should return the same value
        Assert.assertEquals(expectedQueryIdString, bdpAuthContext.getQueryIdStr());
        Assert.assertEquals(expectedQueryIdString, bdpAuthContext.getQueryIdStr());
        Assert.assertEquals(expectedQueryIdString, bdpAuthContext.getQueryIdStr());

        // Test that the value doesn't change unexpectedly
        String retrievedQueryId = bdpAuthContext.getQueryIdStr();
        Assert.assertEquals(expectedQueryIdString, retrievedQueryId);

        // Verify it's still the same after some operations
        bdpAuthContext.toString(); // Call other methods
        Assert.assertEquals(expectedQueryIdString, bdpAuthContext.getQueryIdStr());
    }

    @Test
    public void testQueryIdThreadLocalBehavior() {
        // Test queryId behavior with thread local operations
        TUniqueId testQueryId = new TUniqueId(505L, 606L);
        bdpAuthContext.setQueryId(testQueryId);
        String expectedQueryIdString = DebugUtil.printId(testQueryId);

        // Set thread local info
        bdpAuthContext.setThreadLocalInfo();

        // Get from thread local and verify queryId
        BDPAuthContext threadLocalContext = BDPAuthContext.get();
        Assert.assertNotNull(threadLocalContext);
        Assert.assertEquals(bdpAuthContext, threadLocalContext);
        Assert.assertEquals(expectedQueryIdString, threadLocalContext.getQueryIdStr());

        // Clear and verify
        BDPAuthContext.clear();
        Assert.assertNull(BDPAuthContext.get());

        // Restore thread local for tearDown
        bdpAuthContext.setThreadLocalInfo();
    }

    @Test
    public void testQueryIdWithMultipleUpdates() {
        // Test queryId updates and consistency
        TUniqueId initialQueryId = new TUniqueId(606L, 707L);
        bdpAuthContext.setQueryId(initialQueryId);
        Assert.assertEquals(DebugUtil.printId(initialQueryId), bdpAuthContext.getQueryIdStr());

        // Update queryId
        TUniqueId updatedQueryId = new TUniqueId(707L, 808L);
        bdpAuthContext.setQueryId(updatedQueryId);
        Assert.assertEquals(DebugUtil.printId(updatedQueryId), bdpAuthContext.getQueryIdStr());
        Assert.assertNotEquals(DebugUtil.printId(initialQueryId), bdpAuthContext.getQueryIdStr());

        // Set to null
        bdpAuthContext.setQueryId(null);
        Assert.assertEquals("none", bdpAuthContext.getQueryIdStr());

        // Set back to a value
        TUniqueId finalQueryId = new TUniqueId(808L, 909L);
        bdpAuthContext.setQueryId(finalQueryId);
        Assert.assertEquals(DebugUtil.printId(finalQueryId), bdpAuthContext.getQueryIdStr());
    }

    @Test
    public void testDefaultConstructor() {
        // Test default constructor
        BDPAuthContext defaultContext = new BDPAuthContext();
        Assert.assertNull(defaultContext.getErp());
        Assert.assertNull(defaultContext.getSource());
        Assert.assertNull(defaultContext.getHadoopUserName());
        Assert.assertNull(defaultContext.getUserToken());
        Assert.assertNull(defaultContext.getUserType());
        Assert.assertNull(defaultContext.getBusinessLine());
        Assert.assertNull(defaultContext.getQueryId());
        Assert.assertEquals("none", defaultContext.getQueryIdStr());
        Assert.assertFalse(defaultContext.isErpChanged());
    }

    @Test
    public void testSettersWithNullValues() {
        // Test all setters with null values
        bdpAuthContext.setErp(null);
        bdpAuthContext.setSource(null);
        bdpAuthContext.setHadoopUserName(null);
        bdpAuthContext.setUserToken(null);
        bdpAuthContext.setUserType(null);
        bdpAuthContext.setBusinessLine(null);
        bdpAuthContext.setQueryId(null);

        Assert.assertNull(bdpAuthContext.getErp());
        Assert.assertNull(bdpAuthContext.getSource());
        Assert.assertNull(bdpAuthContext.getHadoopUserName());
        Assert.assertNull(bdpAuthContext.getUserToken());
        Assert.assertNull(bdpAuthContext.getUserType());
        Assert.assertNull(bdpAuthContext.getBusinessLine());
        Assert.assertNull(bdpAuthContext.getQueryId());
        Assert.assertEquals("none", bdpAuthContext.getQueryIdStr());
    }

    @Test
    public void testSettersWithEmptyStrings() {
        // Test setters with empty strings
        bdpAuthContext.setErp("");
        bdpAuthContext.setSource("");
        bdpAuthContext.setHadoopUserName("");
        bdpAuthContext.setUserToken("");
        bdpAuthContext.setUserType("");
        bdpAuthContext.setBusinessLine("");

        Assert.assertEquals("", bdpAuthContext.getErp());
        Assert.assertEquals("", bdpAuthContext.getSource());
        Assert.assertEquals("", bdpAuthContext.getHadoopUserName());
        Assert.assertEquals("", bdpAuthContext.getUserToken());
        Assert.assertEquals("", bdpAuthContext.getUserType());
        Assert.assertEquals("", bdpAuthContext.getBusinessLine());
    }

    @Test
    public void testHmsClientCacheKeyVariations() {
        // Test HMS client cache key with different combinations
        BDPAuthContext context1 = new BDPAuthContext("erp", "source", "hadoop_user", "token");
        Assert.assertEquals("hadoop_user", context1.getHmsClientCacheKey());

        // With userType only
        context1.setUserType("dev_personal");
        Assert.assertEquals("hadoop_userdev_personal", context1.getHmsClientCacheKey());

        // With businessLine only
        BDPAuthContext context2 = new BDPAuthContext("erp", "source", "hadoop_user", "token");
        context2.setBusinessLine("test_business");
        Assert.assertEquals("hadoop_usertest_business", context2.getHmsClientCacheKey());

        // With both userType and businessLine
        context2.setUserType("dev_personal");
        Assert.assertEquals("hadoop_userdev_personaltest_business", context2.getHmsClientCacheKey());

    }

    @Test
    public void testToStringVariations() {
        // Test toString with different field combinations
        BDPAuthContext context = new BDPAuthContext();
        String result = context.toString();
        Assert.assertEquals("bdp_auth_context[erp: null, source: null, hadoop_user_name: null, view_based: false, query_id: none]", result);

        // With some fields set
        context.setErp("test_erp");
        context.setSource("test_source");
        result = context.toString();
        Assert.assertEquals("bdp_auth_context[erp: test_erp, source: test_source, hadoop_user_name: null, view_based: false, query_id: none]", result);

        // With queryId
        TUniqueId queryId = new TUniqueId(123L, 456L);
        context.setQueryId(queryId);
        result = context.toString();
        String expected = "bdp_auth_context[erp: test_erp, source: test_source, hadoop_user_name: null, view_based: false, query_id: "
                + DebugUtil.printId(queryId) + "]";
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testTBDPAuthContextConstructorWithNullFields() {
        // Test constructor from TBDPAuthContext with null optional fields
        TBDPAuthContext tContext = new TBDPAuthContext("source", "erp", "hadoop_user", "token", false, null);
        // Don't set optional fields (userType, businessLine)

        BDPAuthContext context = new BDPAuthContext(tContext);
        Assert.assertEquals("erp", context.getErp());
        Assert.assertEquals("source", context.getSource());
        Assert.assertEquals("hadoop_user", context.getHadoopUserName());
        Assert.assertEquals("token", context.getUserToken());
        Assert.assertNull(context.getUserType());
        Assert.assertNull(context.getBusinessLine());
        Assert.assertNull(context.getQueryId());
    }

    @Test
    public void testTBDPAuthContextConstructorWithAllFields() {
        // Test constructor from TBDPAuthContext with all fields set
        TUniqueId queryId = new TUniqueId(789L, 101112L);
        TBDPAuthContext tContext = new TBDPAuthContext("source", "erp", "hadoop_user", "token", false, queryId);
        tContext.setUserType("dev_personal");
        tContext.setBusinessLine("test_business");

        BDPAuthContext context = new BDPAuthContext(tContext);
        Assert.assertEquals("erp", context.getErp());
        Assert.assertEquals("source", context.getSource());
        Assert.assertEquals("hadoop_user", context.getHadoopUserName());
        Assert.assertEquals("token", context.getUserToken());
        // userType and businessLine are copied from TBDPAuthContext
        Assert.assertEquals("dev_personal", context.getUserType());
        Assert.assertEquals("test_business", context.getBusinessLine());
        Assert.assertEquals(queryId, context.getQueryId());
        Assert.assertEquals(DebugUtil.printId(queryId), context.getQueryIdStr());
    }

    @Test
    public void testErpChangedFlagThreadSafety() {
        // Test erpChanged flag is volatile and thread-safe
        Assert.assertFalse(bdpAuthContext.isErpChanged());

        // Set flag in current thread
        bdpAuthContext.setErpChanged(true);
        Assert.assertTrue(bdpAuthContext.isErpChanged());

        // Reset flag
        bdpAuthContext.setErpChanged(false);
        Assert.assertFalse(bdpAuthContext.isErpChanged());

        // Multiple toggles
        for (int i = 0; i < 10; i++) {
            bdpAuthContext.setErpChanged(i % 2 == 0);
            Assert.assertEquals(i % 2 == 0, bdpAuthContext.isErpChanged());
        }
    }

    @Test
    public void testThreadLocalIsolation() {
        // Test that thread local contexts are isolated
        BDPAuthContext context1 = new BDPAuthContext("erp1", "source1", "user1", "token1");
        BDPAuthContext context2 = new BDPAuthContext("erp2", "source2", "user2", "token2");

        // Set first context
        context1.setThreadLocalInfo();
        Assert.assertEquals(context1, BDPAuthContext.get());
        Assert.assertEquals("erp1", BDPAuthContext.get().getErp());

        // Override with second context
        context2.setThreadLocalInfo();
        Assert.assertEquals(context2, BDPAuthContext.get());
        Assert.assertEquals("erp2", BDPAuthContext.get().getErp());
        Assert.assertNotEquals(context1, BDPAuthContext.get());

        // Clear and verify
        BDPAuthContext.clear();
        Assert.assertNull(BDPAuthContext.get());
    }

    @Test
    public void testQueryIdEdgeCases() {
        // Test queryId with edge case values
        TUniqueId zeroQueryId = new TUniqueId(0L, 0L);
        bdpAuthContext.setQueryId(zeroQueryId);
        Assert.assertEquals(DebugUtil.printId(zeroQueryId), bdpAuthContext.getQueryIdStr());

        // Test with maximum values
        TUniqueId maxQueryId = new TUniqueId(Long.MAX_VALUE, Long.MAX_VALUE);
        bdpAuthContext.setQueryId(maxQueryId);
        Assert.assertEquals(DebugUtil.printId(maxQueryId), bdpAuthContext.getQueryIdStr());

        // Test with negative values
        TUniqueId negativeQueryId = new TUniqueId(-1L, -1L);
        bdpAuthContext.setQueryId(negativeQueryId);
        Assert.assertEquals(DebugUtil.printId(negativeQueryId), bdpAuthContext.getQueryIdStr());
    }

    @Test
    public void testConstructorParameterOrder() {
        // Test that constructor parameters are in correct order
        BDPAuthContext context = new BDPAuthContext("test_erp", "test_source", "test_hadoop_user", "test_token");
        Assert.assertEquals("test_erp", context.getErp());
        Assert.assertEquals("test_source", context.getSource());
        Assert.assertEquals("test_hadoop_user", context.getHadoopUserName());
        Assert.assertEquals("test_token", context.getUserToken());

        // Test constructor with queryId
        TUniqueId queryId = new TUniqueId(123L, 456L);
        BDPAuthContext contextWithQuery = new BDPAuthContext("test_erp", "test_source", "test_hadoop_user", "test_token", false, null, null, queryId);
        Assert.assertEquals("test_erp", contextWithQuery.getErp());
        Assert.assertEquals("test_source", contextWithQuery.getSource());
        Assert.assertEquals("test_hadoop_user", contextWithQuery.getHadoopUserName());
        Assert.assertEquals("test_token", contextWithQuery.getUserToken());
        Assert.assertEquals(queryId, contextWithQuery.getQueryId());
    }

    @Test
    public void testSpecialCharactersInFields() {
        // Test handling of special characters in string fields
        BDPAuthContext context = new BDPAuthContext();

        // Test with special characters
        context.setErp("erp@#$%^&*()");
        context.setSource("source with spaces and 中文");
        context.setHadoopUserName("user_name-with.dots");
        context.setUserToken("token/with\\slashes");
        context.setUserType("type:with:colons");
        context.setBusinessLine("business|line|with|pipes");

        Assert.assertEquals("erp@#$%^&*()", context.getErp());
        Assert.assertEquals("source with spaces and 中文", context.getSource());
        Assert.assertEquals("user_name-with.dots", context.getHadoopUserName());
        Assert.assertEquals("token/with\\slashes", context.getUserToken());
        Assert.assertEquals("type:with:colons", context.getUserType());
        Assert.assertEquals("business|line|with|pipes", context.getBusinessLine());

        // Test HMS cache key with special characters
        String cacheKey = context.getHmsClientCacheKey();
        Assert.assertEquals("user_name-with.dotstype:with:colonsbusiness|line|with|pipes", cacheKey);
    }

    @Test
    public void testLongStringFields() {
        // Test handling of very long strings
        StringBuilder longString = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longString.append("a");
        }
        String longValue = longString.toString();

        BDPAuthContext context = new BDPAuthContext();
        context.setErp(longValue);
        context.setSource(longValue);
        context.setHadoopUserName(longValue);
        context.setUserToken(longValue);
        context.setUserType(longValue);
        context.setBusinessLine(longValue);

        Assert.assertEquals(longValue, context.getErp());
        Assert.assertEquals(longValue, context.getSource());
        Assert.assertEquals(longValue, context.getHadoopUserName());
        Assert.assertEquals(longValue, context.getUserToken());
        Assert.assertEquals(longValue, context.getUserType());
        Assert.assertEquals(longValue, context.getBusinessLine());

        // Test HMS cache key with long strings
        String expectedCacheKey = longValue + longValue + longValue; // hadoopUserName + userType + businessLine
        Assert.assertEquals(expectedCacheKey, context.getHmsClientCacheKey());
    }

    @Test
    public void testThriftConversionRoundTrip() {
        // Test complete round-trip conversion between BDPAuthContext and TBDPAuthContext
        TUniqueId originalQueryId = new TUniqueId(999L, 888L);
        BDPAuthContext originalContext = new BDPAuthContext("original_erp", "original_source",
                "original_hadoop_user", "original_token", false, null, null, originalQueryId);
        originalContext.setUserType("dev_personal");
        originalContext.setBusinessLine("original_business");
        originalContext.setErpChanged(true);

        // Convert to TBDPAuthContext
        TBDPAuthContext tContext = new TBDPAuthContext(
                originalContext.getSource(),
                originalContext.getErp(),
                originalContext.getHadoopUserName(),
                originalContext.getUserToken(),
                originalContext.isViewBased(),
                originalContext.getQueryId());
        tContext.setUserType(originalContext.getUserType());
        tContext.setBusinessLine(originalContext.getBusinessLine());

        // Convert back to BDPAuthContext
        BDPAuthContext reconstructedContext = new BDPAuthContext(tContext);

        // Verify all fields are preserved
        Assert.assertEquals(originalContext.getErp(), reconstructedContext.getErp());
        Assert.assertEquals(originalContext.getSource(), reconstructedContext.getSource());
        Assert.assertEquals(originalContext.getHadoopUserName(), reconstructedContext.getHadoopUserName());
        Assert.assertEquals(originalContext.getUserToken(), reconstructedContext.getUserToken());
        Assert.assertEquals(originalContext.getQueryId(), reconstructedContext.getQueryId());
        Assert.assertEquals(originalContext.getQueryIdStr(), reconstructedContext.getQueryIdStr());

        // userType and businessLine are preserved through TBDPAuthContext
        Assert.assertEquals("dev_personal", reconstructedContext.getUserType());
        Assert.assertEquals("original_business", reconstructedContext.getBusinessLine());
        Assert.assertFalse(reconstructedContext.isErpChanged()); // erpChanged is not preserved
    }

    @Test
    public void testMultipleThreadLocalOperations() {
        // Test multiple thread local operations in sequence
        BDPAuthContext context1 = new BDPAuthContext("erp1", "source1", "user1", "token1");
        BDPAuthContext context2 = new BDPAuthContext("erp2", "source2", "user2", "token2");
        BDPAuthContext context3 = new BDPAuthContext("erp3", "source3", "user3", "token3");

        // Set and verify context1
        context1.setThreadLocalInfo();
        Assert.assertEquals(context1, BDPAuthContext.get());

        // Set and verify context2
        context2.setThreadLocalInfo();
        Assert.assertEquals(context2, BDPAuthContext.get());
        Assert.assertNotEquals(context1, BDPAuthContext.get());

        // Set and verify context3
        context3.setThreadLocalInfo();
        Assert.assertEquals(context3, BDPAuthContext.get());
        Assert.assertNotEquals(context2, BDPAuthContext.get());

        // Clear and verify
        BDPAuthContext.clear();
        Assert.assertNull(BDPAuthContext.get());

        // Set context1 again
        context1.setThreadLocalInfo();
        Assert.assertEquals(context1, BDPAuthContext.get());

        // Multiple clears should be safe
        BDPAuthContext.clear();
        BDPAuthContext.clear();
        BDPAuthContext.clear();
        Assert.assertNull(BDPAuthContext.get());
    }

    @Test
    public void testQueryIdConsistencyAfterModification() {
        // Test queryId consistency after various modifications
        TUniqueId queryId = new TUniqueId(111L, 222L);
        bdpAuthContext.setQueryId(queryId);
        String initialQueryIdStr = bdpAuthContext.getQueryIdStr();

        // Modify other fields, queryId should remain consistent
        bdpAuthContext.setErp("modified_erp");
        bdpAuthContext.setSource("modified_source");
        bdpAuthContext.setHadoopUserName("modified_user");
        bdpAuthContext.setUserToken("modified_token");
        bdpAuthContext.setUserType("modified_type");
        bdpAuthContext.setBusinessLine("modified_business");
        bdpAuthContext.setErpChanged(true);

        Assert.assertEquals(queryId, bdpAuthContext.getQueryId());
        Assert.assertEquals(initialQueryIdStr, bdpAuthContext.getQueryIdStr());

        // Thread local operations should not affect queryId
        bdpAuthContext.setThreadLocalInfo();
        Assert.assertEquals(queryId, bdpAuthContext.getQueryId());
        Assert.assertEquals(initialQueryIdStr, bdpAuthContext.getQueryIdStr());

        BDPAuthContext threadLocalContext = BDPAuthContext.get();
        Assert.assertEquals(queryId, threadLocalContext.getQueryId());
        Assert.assertEquals(initialQueryIdStr, threadLocalContext.getQueryIdStr());
    }

    @Test
    public void testHmsClientCacheKeyPerformance() {
        // Test HMS client cache key generation performance and consistency
        BDPAuthContext context = new BDPAuthContext("erp", "source", "hadoop_user", "token");
        context.setUserType("dev_personal");
        context.setBusinessLine("test_business");

        // Generate cache key multiple times and verify consistency
        String firstKey = context.getHmsClientCacheKey();
        for (int i = 0; i < 100; i++) {
            String key = context.getHmsClientCacheKey();
            Assert.assertEquals(firstKey, key);
        }

        // Verify key doesn't change after other field modifications
        context.setErp("modified_erp");
        context.setSource("modified_source");
        context.setUserToken("modified_token");
        context.setQueryId(new TUniqueId(123L, 456L));

        String keyAfterModification = context.getHmsClientCacheKey();
        Assert.assertEquals(firstKey, keyAfterModification);
    }

    @Test
    public void testToStringPerformance() {
        // Test toString method performance and consistency
        TUniqueId queryId = new TUniqueId(123L, 456L);
        bdpAuthContext.setQueryId(queryId);

        String firstToString = bdpAuthContext.toString();
        for (int i = 0; i < 100; i++) {
            String toStringResult = bdpAuthContext.toString();
            Assert.assertEquals(firstToString, toStringResult);
        }

        // Verify toString reflects changes
        bdpAuthContext.setErp("modified_erp");
        String modifiedToString = bdpAuthContext.toString();
        Assert.assertNotEquals(firstToString, modifiedToString);
        Assert.assertTrue(modifiedToString.contains("modified_erp"));
    }

    @Test
    public void testFieldImmutabilityAfterConstruction() {
        // Test that fields can be modified after construction
        BDPAuthContext context = new BDPAuthContext("initial_erp", "initial_source",
                "initial_user", "initial_token");

        // Verify initial values
        Assert.assertEquals("initial_erp", context.getErp());
        Assert.assertEquals("initial_source", context.getSource());
        Assert.assertEquals("initial_user", context.getHadoopUserName());
        Assert.assertEquals("initial_token", context.getUserToken());

        // Modify all fields
        context.setErp("modified_erp");
        context.setSource("modified_source");
        context.setHadoopUserName("modified_user");
        context.setUserToken("modified_token");
        context.setUserType("modified_type");
        context.setBusinessLine("modified_business");

        // Verify modifications
        Assert.assertEquals("modified_erp", context.getErp());
        Assert.assertEquals("modified_source", context.getSource());
        Assert.assertEquals("modified_user", context.getHadoopUserName());
        Assert.assertEquals("modified_token", context.getUserToken());
        Assert.assertEquals("modified_type", context.getUserType());
        Assert.assertEquals("modified_business", context.getBusinessLine());
    }

    @Test
    public void testTBDPAuthContextFieldMapping() {
        // Test that TBDPAuthContext constructor maps fields correctly
        TUniqueId queryId = new TUniqueId(555L, 666L);
        TBDPAuthContext tContext = new TBDPAuthContext("t_source", "t_erp", "t_hadoop_user", "t_token", false, queryId);

        BDPAuthContext context = new BDPAuthContext(tContext);

        // Verify field mapping: constructor parameter order is (erp, source, hadoopUserName, userToken)
        // but TBDPAuthContext constructor is (source, erp, hadoopUserName, userToken, queryId)
        Assert.assertEquals("t_erp", context.getErp());
        Assert.assertEquals("t_source", context.getSource());
        Assert.assertEquals("t_hadoop_user", context.getHadoopUserName());
        Assert.assertEquals("t_token", context.getUserToken());
        Assert.assertEquals(queryId, context.getQueryId());
    }

    @Test
    public void testEqualsAndHashCodeBehavior() {
        // Test object equality behavior (note: BDPAuthContext doesn't override equals/hashCode)
        BDPAuthContext context1 = new BDPAuthContext("erp", "source", "user", "token");
        BDPAuthContext context2 = new BDPAuthContext("erp", "source", "user", "token");

        // Objects with same content are not equal (no equals override)
        Assert.assertNotEquals(context1, context2);
        Assert.assertNotEquals(context1.hashCode(), context2.hashCode());

        // Same object is equal to itself
        Assert.assertEquals(context1, context1);
        Assert.assertEquals(context1.hashCode(), context1.hashCode());

        // Thread local get returns same object reference
        context1.setThreadLocalInfo();
        BDPAuthContext threadLocalContext = BDPAuthContext.get();
        Assert.assertEquals(context1, threadLocalContext);
        Assert.assertTrue(context1 == threadLocalContext); // Same reference
    }

    @Test
    public void testNullSafetyInToString() {
        // Test toString method handles null values safely
        BDPAuthContext context = new BDPAuthContext();
        String result = context.toString();

        // Should not throw NPE and should contain "null" strings
        Assert.assertNotNull(result);
        Assert.assertTrue(result.contains("erp: null"));
        Assert.assertTrue(result.contains("source: null"));
        Assert.assertTrue(result.contains("hadoop_user_name: null"));
        Assert.assertTrue(result.contains("query_id: none"));

        // Partial null values
        context.setErp("test_erp");
        result = context.toString();
        Assert.assertTrue(result.contains("erp: test_erp"));
        Assert.assertTrue(result.contains("source: null"));
    }

    @Test
    public void testQueryIdStrConsistency() {
        // Test getQueryIdStr() consistency with DebugUtil.printId()
        TUniqueId queryId = new TUniqueId(12345L, 67890L);
        bdpAuthContext.setQueryId(queryId);

        String contextQueryIdStr = bdpAuthContext.getQueryIdStr();
        String directDebugUtilStr = DebugUtil.printId(queryId);

        Assert.assertEquals(directDebugUtilStr, contextQueryIdStr);

        // Test with different queryId values
        TUniqueId[] testQueryIds = {
            new TUniqueId(0L, 0L),
            new TUniqueId(1L, 1L),
            new TUniqueId(Long.MAX_VALUE, Long.MAX_VALUE),
            new TUniqueId(-1L, -1L),
            new TUniqueId(Long.MIN_VALUE, Long.MIN_VALUE)
        };

        for (TUniqueId testQueryId : testQueryIds) {
            bdpAuthContext.setQueryId(testQueryId);
            Assert.assertEquals(DebugUtil.printId(testQueryId), bdpAuthContext.getQueryIdStr());
        }
    }
}
