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

package org.apache.doris.connector.hms;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;

/**
 * Tests {@link ThriftHmsClient#withRootCause}: the client-creation failure message must preserve the deepest
 * cause so a SASL/kerberos/thrift transport error stays visible to the user.
 *
 * <p>WHY: Hive buries the real connection failure — e.g. {@code TTransportException: GSS initiate failed} on a
 * kerberos misconfiguration — inside a generic {@code RuntimeException("Unable to instantiate
 * ...HiveMetaStoreClient")} whose own message drops that cause, and FE surfaces only the top exception's
 * message. The legacy {@code ThriftHMSCachedClient} appended {@code Util.getRootCauseMessage(cause)}, so
 * {@code external_table_p0/kerberos/test_single_hive_kerberos} asserts the surfaced error contains the thrift
 * transport reason. These assertions pin that the connector restores the same behavior — and does not
 * duplicate the reason when the pool re-wraps a fresh-client failure.
 */
public class ThriftHmsClientRootCauseMessageTest {

    // The message Hive builds via StringUtils.stringifyException when it cannot open the metastore transport.
    private static final String GSS_REASON =
            "Could not connect to meta store using any of the URIs provided. Most recent failure: "
            + "shade.doris.hive.org.apache.thrift.transport.TTransportException: GSS initiate failed";

    /** Rebuilds the exact nesting the plugin-driven HMS client produces for a kerberos SASL failure. */
    private static Throwable unableToInstantiate() {
        MetaException root = new MetaException(GSS_REASON);
        InvocationTargetException reflective = new InvocationTargetException(root);
        return new RuntimeException(
                "Unable to instantiate org.apache.hadoop.hive.metastore.HiveMetaStoreClient", reflective);
    }

    @Test
    public void freshClientMessageSurfacesThriftRootCause() {
        Throwable cause = unableToInstantiate();
        String message = ThriftHmsClient.withRootCause("Failed to create HMS client: " + cause.getMessage(), cause);
        // err1 assertion in test_single_hive_kerberos.groovy.
        Assertions.assertTrue(message.contains("thrift.transport.TTransportException"), message);
        // err2 assertion in the same suite expects the full "could not connect ... GSS initiate failed" reason.
        Assertions.assertTrue(message.contains(GSS_REASON), message);
    }

    @Test
    public void poolReWrapDoesNotDuplicateReason() {
        Throwable cause = unableToInstantiate();
        // First wrap: what createFreshClient() throws.
        String createMessage =
                ThriftHmsClient.withRootCause("Failed to create HMS client: " + cause.getMessage(), cause);
        HmsClientException createFailure = new HmsClientException(createMessage, cause);
        // Second wrap: what borrowClient() throws around the pool factory failure.
        String borrowMessage = ThriftHmsClient.withRootCause(
                "Failed to borrow HMS client from pool: " + createFailure.getMessage(), createFailure);

        Assertions.assertTrue(borrowMessage.contains("thrift.transport.TTransportException"), borrowMessage);
        Assertions.assertTrue(borrowMessage.contains(GSS_REASON), borrowMessage);
        // The reason must be appended exactly once even though the exception is wrapped twice.
        Assertions.assertEquals(1, countOccurrences(borrowMessage, ". reason: "), borrowMessage);
    }

    @Test
    public void nullCauseLeavesMessageUnchanged() {
        Assertions.assertEquals("plain", ThriftHmsClient.withRootCause("plain", null));
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            count++;
        }
        return count;
    }
}
