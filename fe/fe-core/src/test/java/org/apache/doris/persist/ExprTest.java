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

import org.apache.doris.analysis.Expr;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.persist.gson.GsonUtils134;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class ExprTest {
    private static final Logger LOG = LogManager.getLogger(ExprTest.class);

    private static class ExprWrapper {
        @SerializedName("expr")
        private Expr expr;
    }

    @Test
    public void testExprDeserializeCompatibility() {
        LOG.info("run testDeserializeExprWithMetaVersion134 test");

        String serializedString = "{\"expr\":{\"expr\": \"AAAADAAAAAAKZGF0ZV90cnVuYwAAAQAAAAIAAAABAAAAAANkYXkAAAAIAAAABW1vbnRoAAA\\u003d\"}}";

        MetaContext ctx = new MetaContext();
        ctx.setMetaVersion(FeMetaVersion.VERSION_129);
        ctx.setThreadLocalInfo();
        GsonUtils134.GSON.fromJson(serializedString, ExprWrapper.class);
        GsonUtils.GSON.fromJson(serializedString, ExprWrapper.class);
        MetaContext.remove();
    }
}
