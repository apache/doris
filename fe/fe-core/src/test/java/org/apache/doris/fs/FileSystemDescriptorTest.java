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

package org.apache.doris.fs;

import org.apache.doris.foundation.fs.FsStorageType;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class FileSystemDescriptorTest {

    /**
     * A BROKER descriptor carries the raw WITH BROKER properties, which routing does not claim (the
     * broker name is not a key of the map). Binding them by routing threw out of the first BACKUP on a
     * broker repository; they have to be bound the way the repository was created.
     */
    @Test
    public void testBrokerDescriptorBackendPropertiesAreNotRouted() {
        FileSystemDescriptor descriptor = new FileSystemDescriptor(FsStorageType.BROKER, "my_broker",
                ImmutableMap.of("username", "u", "password", "p"));
        Map<String, String> backend = Assertions.assertDoesNotThrow(descriptor::getBackendConfigProperties);
        Assertions.assertEquals("u", backend.get("username"));
        Assertions.assertEquals("p", backend.get("password"));
    }
}
