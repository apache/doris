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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TlsOptionsTest {

    @Test
    public void testAnalyzeRejectsSanEntryWithoutValue() {
        TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(Pair.of("SAN", "DNS:")));

        AnalysisException e = Assert.assertThrows(AnalysisException.class, tlsOptions::analyze);
        Assert.assertTrue(e.getMessage().contains("Invalid SAN entry format"));
    }

    @Test
    public void testAnalyzeRejectsUnsupportedSanType() {
        TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(Pair.of("SAN", "FOO:bar")));

        AnalysisException e = Assert.assertThrows(AnalysisException.class, tlsOptions::analyze);
        Assert.assertTrue(e.getMessage().contains("Unsupported SAN entry type"));
    }

    @Test
    public void testAnalyzeRejectsEmptyEntryInList() {
        TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(Pair.of("SAN", "DNS:example.com, ")));

        AnalysisException e = Assert.assertThrows(AnalysisException.class, tlsOptions::analyze);
        Assert.assertTrue(e.getMessage().contains("empty entry"));
    }

    @Test
    public void testAnalyzeNormalizesValidEntries() throws AnalysisException {
        TlsOptions tlsOptions = TlsOptions.of(Collections.singletonList(
                Pair.of("SAN", "email:alice@example.com, DNS:Example.com., URI:spiffe://Example.com/workload, IP Address:192.168.1.1")
        ));

        tlsOptions.analyze();
        Assert.assertEquals(
                "email:alice@example.com, DNS:Example.com, URI:spiffe://Example.com/workload, IP Address:192.168.1.1",
                tlsOptions.getSan());
    }
}
