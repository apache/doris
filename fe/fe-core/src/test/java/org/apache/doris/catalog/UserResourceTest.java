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

import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.privilege.UserResource;
import org.apache.doris.thrift.TResourceType;
import org.apache.doris.thrift.TUserResource;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class UserResourceTest {
    @Test
    public void testNormal() throws IOException, DdlException {
        UserResource resource = new UserResource(123);
        // To thrift
        TUserResource tUserResource = resource.toThrift();
        Assert.assertEquals(3, tUserResource.getShareByGroupSize());
        Assert.assertEquals(123,
                tUserResource.getResource().getResourceByType().get(TResourceType.TRESOURCE_CPU_SHARE).intValue());

        resource.updateResource("cpu_share", 321);
        resource.updateGroupShare("low", 987);
        // To image
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        resource.write(outputStream);
        outputStream.flush();
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
        UserResource newResource = UserResource.readIn(inputStream);
        Assert.assertEquals(321, newResource.getResource().getByDesc("cpu_share"));
        Assert.assertEquals(987, newResource.getShareByGroup().get("low").intValue());
    }

    @Test(expected = DdlException.class)
    public void testNoGroup() throws DdlException {
        UserResource resource = new UserResource(123);
        resource.updateGroupShare("noGroup", 234);
        Assert.fail("No exception throws");
    }

    @Test(expected = DdlException.class)
    public void testNoResource() throws DdlException {
        UserResource resource = new UserResource(123);
        resource.updateResource("noResource", 120);
        Assert.fail("No exception throws");
    }

    @Test
    public void testValidGroup() throws DdlException {
        Assert.assertTrue(UserResource.isValidGroup("low"));
        Assert.assertTrue(UserResource.isValidGroup("lOw"));
        Assert.assertTrue(UserResource.isValidGroup("normal"));
        Assert.assertTrue(UserResource.isValidGroup("high"));
        Assert.assertFalse(UserResource.isValidGroup("high223"));
    }
}