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

package org.apache.doris.httpv2.rest;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

public class LoadActionTest {

    @Test
    public void testGetAllHeadersMasksSensitiveHeaders() throws Exception {
        LoadAction action = new LoadAction();
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeaderNames()).thenReturn(Collections.enumeration(Arrays.asList(
                "Authorization", "Cookie", "Set-Cookie", "token", "label")));
        Mockito.when(request.getHeader("label")).thenReturn("load_label");

        Method method = LoadAction.class.getDeclaredMethod("getAllHeaders", HttpServletRequest.class);
        method.setAccessible(true);
        String headers = (String) method.invoke(action, request);

        Assert.assertTrue(headers.contains("Authorization:***MASKED***"));
        Assert.assertTrue(headers.contains("Cookie:***MASKED***"));
        Assert.assertTrue(headers.contains("Set-Cookie:***MASKED***"));
        Assert.assertTrue(headers.contains("token:***MASKED***"));
        Assert.assertTrue(headers.contains("label:load_label"));
    }
}
