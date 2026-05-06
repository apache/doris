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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

public class MediumAllocationModeTest {

    @Test
    public void testFromStringCaseInsensitive() throws AnalysisException {
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString("strict"));
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString("STRICT"));
        Assert.assertEquals(MediumAllocationMode.STRICT, MediumAllocationMode.fromString(" Strict "));
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, MediumAllocationMode.fromString("adaptive"));
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, MediumAllocationMode.fromString("ADAPTIVE"));
    }

    @Test(expected = AnalysisException.class)
    public void testFromStringNullThrows() throws AnalysisException {
        MediumAllocationMode.fromString(null);
    }

    @Test(expected = AnalysisException.class)
    public void testFromStringBlankThrows() throws AnalysisException {
        MediumAllocationMode.fromString("   ");
    }

    @Test(expected = AnalysisException.class)
    public void testFromStringUnknownThrows() throws AnalysisException {
        MediumAllocationMode.fromString("lax");
    }

    @Test
    public void testIsStrictIsAdaptive() {
        Assert.assertTrue(MediumAllocationMode.STRICT.isStrict());
        Assert.assertFalse(MediumAllocationMode.STRICT.isAdaptive());
        Assert.assertTrue(MediumAllocationMode.ADAPTIVE.isAdaptive());
        Assert.assertFalse(MediumAllocationMode.ADAPTIVE.isStrict());
    }
}
