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

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link HmsClientConfig#removedMetastoreTypeError(Map)} and the client-class dispatch that depends on it.
 *
 * <p>WHY: {@code hive.metastore.type=glue} (AWS Glue as an HMS-thrift metastore) was removed. The dispatch in
 * {@link ThriftHmsClient#getMetastoreClientClassName(HiveConf)} is an if/else whose final arm returns the plain
 * {@code HiveMetaStoreClient}, so deleting the glue arm alone makes {@code glue} degrade SILENTLY: Doris would
 * connect to whatever plain HMS is reachable while the user believes they configured Glue. Nothing else in the
 * codebase covers this dispatch, so without these tests the rejection could be deleted and no test would go red.
 *
 * <p>The two tests pin the two halves of the contract: {@code glue} must be REJECTED with a message that names
 * the removed type (not merely absent from the dispatch), and every surviving type must still route as before —
 * a rejection that also broke {@code hms}/{@code dlf} would be a worse regression than the silent fallthrough.
 */
public class HmsClientConfigRemovedTypeTest {

    private static Map<String, String> propsWithType(String type) {
        Map<String, String> props = new HashMap<>();
        if (type != null) {
            props.put(HmsClientConfig.METASTORE_TYPE_KEY, type);
        }
        return props;
    }

    @Test
    public void removedGlueTypeIsRejectedAndNamesTheRemovedType() {
        // WHY: the message is surfaced verbatim to the user (checkProperties unwraps it into DdlException with no
        // prefix), so it must be self-contained. MUTATION: dropping the rejection, or emitting a message that
        // never mentions glue, leaves the user with an error that cannot be acted on -> red.
        String error = HmsClientConfig.removedMetastoreTypeError(propsWithType("glue"));
        Assertions.assertNotNull(error, "hive.metastore.type=glue must be rejected, never silently ignored");
        Assertions.assertTrue(error.contains(HmsClientConfig.METASTORE_TYPE_KEY),
                "message must name the offending property, got: " + error);
        Assertions.assertTrue(error.contains("glue"), "message must name the removed type, got: " + error);
        Assertions.assertTrue(error.contains("no longer supported"),
                "message must say the type was removed rather than merely invalid, got: " + error);

        // Case-insensitive: the dispatch it replaces used equalsIgnoreCase, so "GLUE" must not slip through.
        Assertions.assertNotNull(HmsClientConfig.removedMetastoreTypeError(propsWithType("GLUE")),
                "rejection must be case-insensitive, matching the dispatch it replaces");
    }

    @Test
    public void survivingTypesAreNotRejectedAndStillRouteToTheirClients() {
        // WHY: guards the blast radius of the rejection. MUTATION: rejecting a type that still works, or breaking
        // the dlf/plain-hms routing while removing the glue arm -> red.
        Assertions.assertNull(HmsClientConfig.removedMetastoreTypeError(propsWithType(null)),
                "absent type defaults to plain hms and must be accepted");
        Assertions.assertNull(HmsClientConfig.removedMetastoreTypeError(
                propsWithType(HmsClientConfig.METASTORE_TYPE_HMS)));
        Assertions.assertNull(HmsClientConfig.removedMetastoreTypeError(
                propsWithType(HmsClientConfig.METASTORE_TYPE_DLF)));

        HiveConf dlfConf = new HiveConf();
        dlfConf.set(HmsClientConfig.METASTORE_TYPE_KEY, HmsClientConfig.METASTORE_TYPE_DLF);
        Assertions.assertEquals("com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient",
                ThriftHmsClient.getMetastoreClientClassName(dlfConf));

        HiveConf hmsConf = new HiveConf();
        hmsConf.set(HmsClientConfig.METASTORE_TYPE_KEY, HmsClientConfig.METASTORE_TYPE_HMS);
        Assertions.assertEquals(org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class.getName(),
                ThriftHmsClient.getMetastoreClientClassName(hmsConf));
    }
}
