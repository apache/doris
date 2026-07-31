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

import org.apache.doris.catalog.AzureResource;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.common.io.Text;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ResourcePersistTest {
    @Test
    public void test() throws IOException {
        Resource resource = new S3Resource("s3_resource");
        S3Resource resource1 = (S3Resource) readWrittenResource(resource);
        Assert.assertEquals(resource1.toString(), resource.toString());
        resource1.readLock();
        resource1.readUnlock();
    }

    @Test
    public void testAzureResourcePersist() throws IOException {
        Resource resource = new AzureResource("azure_resource");
        Assert.assertTrue(resource.toString().contains("\"clazz\":\"AzureResource\""));

        Resource readResource = readWrittenResource(resource);
        Assert.assertTrue(readResource instanceof AzureResource);
        Assert.assertEquals("azure_resource", readResource.getName());
        Assert.assertEquals(Resource.ResourceType.AZURE, readResource.getType());
        readResource.readLock();
        readResource.readUnlock();
    }

    @Test
    public void testReadLegacyAzureResourceWithoutClazz() throws IOException {
        String json = "{\"name\":\"legacy_azure_resource\",\"type\":\"AZURE\","
                + "\"references\":{},\"id\":123,\"version\":0}";

        Resource readResource = readResourceFromJson(json);
        Assert.assertTrue(readResource instanceof AzureResource);
        Assert.assertEquals("legacy_azure_resource", readResource.getName());
        Assert.assertEquals(Resource.ResourceType.AZURE, readResource.getType());
        readResource.readLock();
        readResource.readUnlock();
    }

    @Test
    public void testReadLegacyAzureResourceMgrWithoutClazz() throws IOException {
        String json = "{\"nameToResource\":{\"legacy_azure_resource\":{\"name\":\"legacy_azure_resource\","
                + "\"type\":\"AZURE\",\"references\":{},\"id\":123,\"version\":0}}}";

        ResourceMgr resourceMgr = readResourceMgrFromJson(json);
        Resource readResource = resourceMgr.getResource("legacy_azure_resource");
        Assert.assertTrue(readResource instanceof AzureResource);
        Assert.assertEquals("legacy_azure_resource", readResource.getName());
        Assert.assertEquals(Resource.ResourceType.AZURE, readResource.getType());
        readResource.readLock();
        readResource.readUnlock();
    }

    private Resource readWrittenResource(Resource resource) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
        resource.write(dos);
        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        Resource readResource = Resource.read(dis);
        dis.close();
        return readResource;
    }

    private Resource readResourceFromJson(String json) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
        Text.writeString(dos, json);
        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        Resource readResource = Resource.read(dis);
        dis.close();
        return readResource;
    }

    private ResourceMgr readResourceMgrFromJson(String json) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
        Text.writeString(dos, json);
        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        ResourceMgr resourceMgr = ResourceMgr.read(dis);
        dis.close();
        return resourceMgr;
    }
}
