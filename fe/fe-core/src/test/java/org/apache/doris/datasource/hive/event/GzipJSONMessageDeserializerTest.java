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

package org.apache.doris.datasource.hive.event;

import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.junit.Assert;
import org.junit.Test;

public class GzipJSONMessageDeserializerTest {

    @Test
    public void testGetAlterPartitionMessage() {
        // copy from an online HDP Hive cluster,
        // eventType: ALTER_PARTITION, messageFormat: gzip(json-2.0)
        String messageBody = "H4sIAAAAAAAAAO1WW2sbRxT+L/usjHa1ullQqOMkbUpSBWwKJVuW0e7IGntvmZlVcIwhT8UlfuhDiimUGk"
                + "NKk5fQQkmD3dI/Y8nuv+iZmZWtXa+VKNSUgh7sHc05cy5z5jvf2TY4YUPCjI4hBoz2RadaTVqW3WwP0SANNlFvI/TRIzqIY"
                + "xQR0Vky27ZRUYeoRx4wGnk0wQEcH9AhqbqfdlfXPr735coKWuneB0W/B6I+jVzsczeOAhoR2BW4FxAQyM3ETd0eGeAhjZnL"
                + "iBcz3wXTLhdYUC6oByo+nRxa20rkwfvLny9/cvuWu7Z8897tiazb2/iMxxGItx3DcowOfLhgsHDm8OQYOxXHqOWP5zPQKnZ"
                + "eReZ/nrnWqGsNaktrVrPVqNVNq2lLUWNaZMqd5qWdlt6BSPUiSyngAhYPM0GlUbmcbZacoCEpTQe+NFovTeP0+cF499uz17"
                + "+M/vxuvP/m7/3fQG2nxMfUrfl4a243xwdnrw/H+z+Nfzi4wgEZkki4HhZkPWZzOzg5enZy/Obk7dPTo59Hu1+f/no8042Ad"
                + "/VhLsDy6MdnVxj3scDuEAdpufEeXaeRmHFBo6d/SMtflZwd+H0OYLWb5gaNWBytB1ueV00BmVUFxceYkUEMv6v5x4v8XnVe"
                + "NBRii9k6wgn2BgQNsB/HCZIe0aMA0RjFzENd5t2NklTciVmIRQ4M89jopqJgJION6Jei5oZVBpva/I5XCfPzKA9xokGnzFQ"
                + "mX0vWHK6c4oA+gXuLI9TPwu2oxwCl25Fm2gXs6uOmKuxSKa61zDJn+jcr2/pxWNY7msW0w1pBphaZrJivluX9qYysWq4WpV"
                + "nqTKwSYCQf0DPGe9+MXrwCMI/2js5RsTTzfpZUfRLMNhGHCvM4ZR7hiEP1Q4yiNHyAmViJAz6pV2WGOuwLZCrNbceBzGXPc"
                + "ORvR/lLPSFXFfmvT0ngcyV9qJQjHF4oT/fnyYmiNXkLE1mUBoFkOCUHR0TthkRg2WDUrnLixWEI3ezcTEkzd7JOVQyp0M6v"
                + "Narp3n9VPIXuf53xlFDF7KgmQVxzRBmzXBXLFLeUxhLE/1qtJA1dFUZy3c8lB3tHA18B1WMEnsdkvgtp9G74AtQzBOsGBPq"
                + "C4YhTWdYAc3HLD9bU1CTbwfnEphR7qbdJBGTkwrzModUrpZoOJYtZtanvj07+Ojx9/nK8+/vZ4d7J26PsEl+8Ojt8OTvGrC"
                + "OVtyOdL8p5RzaSfxaqI/OGbTUU40x150nvzA3Murtal8jUahVGDCDGTLc9zbOKZq2s89JmXRMAjOGUr7E0kqDtJnLuxwFX4"
                + "z7cKPSXMFFTcLtu11v1RsV4zKggd304XDE2ydYX8ilzsKlqA8drZs02LduCCAxZPCr5FYb8mwQ4llwa9QtUZ0luySw4RtkE"
                + "9R5D/byDUsnEb7Vbsyf+xXy/mO//s/m+KrH20QVOFtP+Ytp/32m/NTNYW17WTHLVRKDJVcQCB6v0iRYuNTT3AR3eoQHhU/d"
                + "7cYMlLGVepqRp4ljuw7iw4I0Fbyx4Y8EbC974f/NG4zp5Y+cflz5IfxgZAAA=";
        MessageDeserializer messageDeserializer = MetastoreEventsProcessor.getMessageDeserializer("gzip(json-2.0)");
        Assert.assertTrue(messageDeserializer instanceof GzipJSONMessageDeserializer);

        try {
            AlterPartitionMessage alterPartitionMessage = messageDeserializer.getAlterPartitionMessage(messageBody);
            Assert.assertTrue(alterPartitionMessage != null);
            Assert.assertTrue(alterPartitionMessage.getEventType() == EventMessage.EventType.ALTER_PARTITION);
            Assert.assertTrue(alterPartitionMessage.getTableObj() != null);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

}
