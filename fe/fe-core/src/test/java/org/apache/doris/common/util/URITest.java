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

package org.apache.doris.common.util;

import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;

public class URITest {
    private void check(java.net.URI javaURI, URI myURI) {
        Assertions.assertEquals(javaURI.getAuthority(), myURI.getAuthority());
        Assertions.assertEquals(javaURI.getPath(), myURI.getPath());
        Assertions.assertEquals(javaURI.getHost(), myURI.getHost());
        Assertions.assertEquals(javaURI.getPort(), myURI.getPort());
        Assertions.assertEquals(javaURI.getScheme(), myURI.getScheme());
        Assertions.assertEquals(javaURI.getQuery(), myURI.getQuery());
        Assertions.assertEquals(javaURI.getFragment(), myURI.getFragment());
        Assertions.assertEquals(javaURI.getUserInfo(), myURI.getUserInfo());
    }

    @Test
    public void testNormal() throws UserException, URISyntaxException {
        String str1 = "foo://username:password@example.com:8042/over/there/index.dtb?type=animal&name=narwhal#nose";
        java.net.URI javaURI1 = new java.net.URI(str1);
        URI myURI1 = URI.create(str1);
        check(javaURI1, myURI1);
        Assertions.assertEquals(myURI1.getUserName(), "username");
        Assertions.assertEquals(myURI1.getPassWord(), "password");
        Assertions.assertEquals(myURI1.getQueryMap().get("type"), "animal");

        String str2 = "foo://example.com/over/there/index.dtb#nose";
        java.net.URI javaURI2 = new java.net.URI(str2);
        URI myURI2 = URI.create(str2);
        check(javaURI2, myURI2);
        Assertions.assertEquals(myURI2.getFragment(), "nose");

        String str3 = "foo://example.com/over/there/index.dtb?type=animal";
        java.net.URI javaURI3 = new java.net.URI(str3);
        URI myURI3 = URI.create(str3);
        check(javaURI3, myURI3);
        Assertions.assertEquals(myURI3.getQueryMap().get("type"), "animal");

        String str4 = "foo://:password@example.com/over/there/index.dtb?type=animal";
        java.net.URI javaURI4 = new java.net.URI(str4);
        URI myURI4 = URI.create(str4);
        check(javaURI4, myURI4);
        Assertions.assertEquals(myURI4.getQueryMap().get("type"), "animal");

        String str5 = "foo://password@example.com/over/there/index.dtb?type=animal";
        java.net.URI javaURI5 = new java.net.URI(str5);
        URI myURI5 = URI.create(str5);
        check(javaURI5, myURI5);
        Assertions.assertEquals(myURI5.getQueryMap().get("type"), "animal");

        String str6 = "foo://example.com";
        java.net.URI javaURI6 = new java.net.URI(str6);
        URI myURI6 = URI.create(str6);
        check(javaURI6, myURI6);

        String str7 = "example.com";
        java.net.URI javaURI7 = new java.net.URI(str7);
        URI myURI7 = URI.create(str7);
        check(javaURI7, myURI7);

        String str8 = "example.com";
        java.net.URI javaURI8 = new java.net.URI(str8);
        URI myURI8 = URI.create(str8);
        check(javaURI8, myURI8);

        URI myURI9 = URI.create("hdfs://ip:12/test/test/data/{20220131,20220201}/*");
        Assertions.assertEquals(myURI9.getScheme(), "hdfs");
        Assertions.assertEquals(myURI9.getPath(), "/test/test/data/{20220131,20220201}/*");
        Assertions.assertEquals(myURI9.getHost(), "ip");
        Assertions.assertEquals(myURI9.getPort(), 12);
        Assertions.assertEquals(myURI9.getAuthority(), "ip:12");

        URI myURI10 = URI.create("hdfs");
        Assertions.assertEquals(myURI10.getPath(), "hdfs");
    }
}
