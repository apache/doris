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

package org.apache.doris.common.proc;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class FrontendsProcNodeTest {

    @Test
    public void testFormatIp() throws Exception {
        // Test input and expected output
        String input = "fe80:0:0:0:20c:29ff:fef9:3a18";
        String expected = "fe80::20c:29ff:fef9:3a18";

        // Use reflection to get the formatIp method
        Method formatIpMethod = FrontendsProcNode.class.getDeclaredMethod("formatIp", String.class);
        formatIpMethod.setAccessible(true); // Make the method accessible

        // Invoke the formatIp method to format the IP
        String actual = (String) formatIpMethod.invoke(null, input);

        // Assert the result
        Assert.assertEquals(expected, actual);
    }
}
