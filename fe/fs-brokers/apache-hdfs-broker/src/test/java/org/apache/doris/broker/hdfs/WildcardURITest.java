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

package org.apache.doris.broker.hdfs;

import org.apache.doris.common.WildcardURI;

import org.junit.Assert;
import org.junit.Test;

public class WildcardURITest {

    @Test
    public void test() {
        String path = "hdfs://host/testdata/20180[8-9]*";
        WildcardURI wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/20180[8-9]*", wildcardURI.getPath());

        path = "hdfs://host/testdata/2018+ 0[8-9]*";
        wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/2018+ 0[8-9]*", wildcardURI.getPath());

        path = "hdfs://host/testdata/2018-01-01 00%3A00%3A00";
        wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/2018-01-01 00%3A00%3A00", wildcardURI.getPath());

        path = "hdfs://host/testdata/2018-01-01   00*";
        wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/2018-01-01   00*", wildcardURI.getPath());

        path = "hdfs://host/testdata/2018-01-01#123#";
        wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/2018-01-01#123#", wildcardURI.getPath());

        path = "hdfs://host/testdata/2018-01-01#123 +#*";
        wildcardURI = new WildcardURI(path);
        Assert.assertEquals("/testdata/2018-01-01#123 +#*", wildcardURI.getPath());
    }

}
