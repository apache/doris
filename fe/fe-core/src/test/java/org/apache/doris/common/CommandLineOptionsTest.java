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

package org.apache.doris.common;

import org.apache.doris.journal.bdbje.BDBToolOptions;

import org.junit.Assert;
import org.junit.Test;

public class CommandLineOptionsTest {

    @Test
    public void test() {
        CommandLineOptions options = new CommandLineOptions(true, "", null, "");
        Assert.assertTrue(options.isVersion());
        Assert.assertFalse(options.runBdbTools());
        Assert.assertFalse(options.runImageTool());

        options = new CommandLineOptions(false, "", new BDBToolOptions(true, "", false, "", "", 0), "");
        Assert.assertFalse(options.isVersion());
        Assert.assertTrue(options.runBdbTools());
        Assert.assertFalse(options.runImageTool());

        options = new CommandLineOptions(false, "", null, "image.0");
        Assert.assertFalse(options.isVersion());
        Assert.assertFalse(options.runBdbTools());
        Assert.assertTrue(options.runImageTool());
    }

}
