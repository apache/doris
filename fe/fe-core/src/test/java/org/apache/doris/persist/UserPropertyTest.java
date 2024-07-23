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

package org.apache.doris.persist;

import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.UserProperty;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class UserPropertyTest {
    @Test
    public void testSerialization() throws IOException, UserException {
        // 1. Write objects to file
        final Path path = Files.createTempFile("propertiesInfo", "tmp");
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(path));

        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.of(UserProperty.PROP_MAX_USER_CONNECTIONS, "100"));
        properties.add(Pair.of(UserProperty.PROP_MAX_QUERY_INSTANCES, "2"));
        properties.add(Pair.of(UserProperty.PROP_PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM, "8"));
        properties.add(Pair.of(UserProperty.PROP_SQL_BLOCK_RULES, "r1,r2"));

        UserProperty prop = new UserProperty();
        prop.update(properties);

        prop.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(path));

        UserProperty prop2 = UserProperty.read(in);

        Assert.assertEquals(100, prop2.getMaxConn());
        Assert.assertEquals(2, prop2.getMaxQueryInstances());
        Assert.assertEquals(8, prop2.getParallelFragmentExecInstanceNum());
        Assert.assertEquals(Lists.newArrayList("r1", "r2"),
                Arrays.stream(prop2.getSqlBlockRules()).sorted().collect(Collectors.toList()));

        // 3. delete files
        in.close();
        Files.delete(path);
    }
}
