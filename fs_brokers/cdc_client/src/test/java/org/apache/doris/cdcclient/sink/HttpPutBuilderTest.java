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

package org.apache.doris.cdcclient.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.doris.cdcclient.common.Constants;

import org.apache.http.client.methods.HttpPut;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

/** Unit tests for {@link HttpPutBuilder}. */
class HttpPutBuilderTest {

    @Test
    void build_requiresUrlAndEntity() {
        assertThrows(NullPointerException.class, () -> new HttpPutBuilder().build());
        assertThrows(
                NullPointerException.class,
                () -> new HttpPutBuilder().setUrl("http://x").build());
    }

    @Test
    void build_setsUrlEntityAndHeaders() {
        HttpPut put =
                new HttpPutBuilder()
                        .setUrl("http://be:8040/api/db/t/_stream_load")
                        .setEmptyEntity()
                        .addCommonHeader()
                        .addBodyContentType()
                        .formatJson()
                        .setLabel("lbl-1")
                        .build();

        assertEquals("http://be:8040/api/db/t/_stream_load", put.getURI().toString());
        assertEquals("100-continue", put.getFirstHeader("Expect").getValue());
        assertEquals("json", put.getFirstHeader("format").getValue());
        assertEquals("true", put.getFirstHeader("read_json_by_line").getValue());
        assertEquals("lbl-1", put.getFirstHeader("label").getValue());
    }

    @Test
    void baseAuth_encodesBasicCredentials() {
        HttpPut put =
                new HttpPutBuilder()
                        .setUrl("http://x")
                        .setEmptyEntity()
                        .baseAuth("root", "secret")
                        .build();

        String expected =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString("root:secret".getBytes(StandardCharsets.UTF_8));
        assertEquals(expected, put.getFirstHeader("Authorization").getValue());
    }

    @Test
    void addHiddenColumns_onlyWhenEnabled() {
        HttpPut on =
                new HttpPutBuilder()
                        .setUrl("http://x")
                        .setEmptyEntity()
                        .addHiddenColumns(true)
                        .build();
        assertEquals(Constants.DORIS_DELETE_SIGN, on.getFirstHeader("hidden_columns").getValue());

        HttpPut off =
                new HttpPutBuilder()
                        .setUrl("http://x")
                        .setEmptyEntity()
                        .addHiddenColumns(false)
                        .build();
        assertNull(off.getFirstHeader("hidden_columns"));
    }

    @Test
    void txnHeaders() {
        HttpPut put =
                new HttpPutBuilder()
                        .setUrl("http://x")
                        .setEmptyEntity()
                        .enable2PC()
                        .addTxnId(99L)
                        .commit()
                        .build();
        assertEquals("true", put.getFirstHeader("two_phase_commit").getValue());
        assertEquals("99", put.getFirstHeader("txn_id").getValue());
        assertEquals("commit", put.getFirstHeader("txn_operation").getValue());

        HttpPut abort =
                new HttpPutBuilder().setUrl("http://x").setEmptyEntity().abort().build();
        assertEquals("abort", abort.getFirstHeader("txn_operation").getValue());
    }

    @Test
    void addProperties_and_getLabel() {
        HttpPutBuilder builder =
                new HttpPutBuilder()
                        .setLabel("L")
                        .addProperties(Collections.singletonMap("k", "v"));
        assertEquals("L", builder.getLabel());

        HttpPut put = builder.setUrl("http://x").setEmptyEntity().build();
        assertEquals("v", put.getFirstHeader("k").getValue());
    }

    @Test
    void setLabel_null_isIgnored() {
        assertNull(new HttpPutBuilder().setLabel(null).getLabel());
    }
}
