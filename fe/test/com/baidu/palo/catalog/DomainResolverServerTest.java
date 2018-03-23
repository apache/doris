// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.catalog;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class DomainResolverServerTest {
    private DomainResolverServer server;
    private String user = "test";
    private List<String> domainNameList;

    @Before
    public void setUp() {
        server = DomainResolverServer.getInstance();
        domainNameList = Lists.newArrayList();
        domainNameList.add("www.baidu.com");
    }

    @Test
    public void registerTest() {
        // param error test
        final List<String> sizeZeroDomainNameList = Lists.newArrayList();
        // empty domain list
        Assert.assertFalse(server.register(user, sizeZeroDomainNameList));
        // null domain list
        Assert.assertFalse(server.register(user, null));
        // empty user
        Assert.assertFalse(server.register("", domainNameList));
        // null user
        Assert.assertFalse(server.register(null, domainNameList));
        
        // normal test
        Assert.assertTrue(server.register(user, domainNameList));
        Assert.assertTrue(server.getRegisteredUserDomain(user).size() == 1);
        
        // domain list contains null
        final List<String> nullDomainNameList = Lists.newArrayList();
        nullDomainNameList.add(null);
        Assert.assertFalse(server.register(null, nullDomainNameList));
        Assert.assertTrue(server.getRegisteredUserDomain(user).size() == 1);
        
        // domains having registered
        Assert.assertTrue(server.register(user, domainNameList));
        Assert.assertTrue(server.getRegisteredUserDomain(user).size() == 1);
        
        // normal test 2
        final List<String> domainNameList2 = Lists.newArrayList();
        domainNameList2.add("www.sina.com.cn");
        Assert.assertTrue(server.register(user, domainNameList2));
        Assert.assertTrue(server.getRegisteredUserDomain(user).size() == 2);
    }

    @Test
    public void getIpsWithDNSTest() {
        
        // no exist user
        Assert.assertEquals(null, server.getUserDomainToIps("user1"));
        // null user
        Assert.assertEquals(null, server.getUserDomainToIps(null));
    
        try {
            // wait for DomainResolverServer
            Thread.currentThread();
            Thread.sleep(500);
            // normal test
            Assert.assertTrue(server.getUserDomainToIps(user).size() == 2);
        } catch (InterruptedException e) {   
        }
    }

    @Test
    public void unregisterTest() {
        // param error test
        // null domain list
        server.unregister(user, null);
        Assert.assertTrue(server.getUserDomainToIps(user).size() == 2);        
        // empty domain list
        final List<String> sizeZeroDomainNameList = Lists.newArrayList();
        server.unregister(user, sizeZeroDomainNameList);
        Assert.assertTrue(server.getUserDomainToIps(user).size() == 2);  
        // null user
        server.unregister(null, domainNameList);
        Assert.assertTrue(server.getUserDomainToIps(user).size() == 2);  
        // no exist user
        server.unregister("test1", domainNameList);
        Assert.assertTrue(server.getUserDomainToIps(user).size() == 2);  
        // normal test
        server.unregister(user, domainNameList);
        Assert.assertTrue(server.getUserDomainToIps(user).size() == 1);   
        final List<String> domainNameList2 = Lists.newArrayList();
        domainNameList2.add("www.sina.com.cn");
        server.unregister(user, domainNameList2);
        Assert.assertEquals(null, server.getUserDomainToIps(user));   
    }
    
    @Test
    public void registerNoExistDomain() {
        // no exist domain
        final List<String> noExistDomainNameList = Lists.newArrayList();
        noExistDomainNameList.add("www.weqwetw.com.cnllll");
        Assert.assertTrue(server.register("test2", noExistDomainNameList));
        try {
            // wait for DomainResolverServer
            Thread.currentThread();
            Thread.sleep(500);
            // normal test
            Assert.assertEquals(null, server.getUserDomainToIps("test2"));
        } catch (InterruptedException e) {   
        }
        server.unregister(user, noExistDomainNameList);
        Assert.assertEquals(null, server.getUserDomainToIps(user));  
    }
    
    @Test
    public void isAvaliableDomainTest() {
        // normal test
        Assert.assertTrue(server.isAvaliableDomain("www.sogo.com.cn"));
        // param error test
        Assert.assertFalse(server.isAvaliableDomain(""));
        Assert.assertFalse(server.isAvaliableDomain(null));
        // no exist domain
        Assert.assertFalse(server.isAvaliableDomain("www.sina.com.cn11sdfqweg"));
    }
}
