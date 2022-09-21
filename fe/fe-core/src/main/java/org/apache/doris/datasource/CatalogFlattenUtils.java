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

package org.apache.doris.datasource;

import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

public class CatalogFlattenUtils {
    public static final String FLATTEN_SEPARATOR = "__";

    /**
     * @param name 1. db
     * 2. catalog.db
     * 3. __catalog__db
     * @return Pair<catalog, db>
     */
    public static Pair<String, String> analyzeFlattenName(String name) {
        String[] nameParts = name.split("\\.");
        if (nameParts.length == 1) {
            if (!name.startsWith(FLATTEN_SEPARATOR)) {
                String ctl = null;
                if (ConnectContext.get() != null) {
                    ctl = ConnectContext.get().getDefaultCatalog();
                }
                ctl = Strings.isNullOrEmpty(ctl) ? InternalCatalog.INTERNAL_CATALOG_NAME : ctl;
                return Pair.of(ctl, name);
            }
            int idx = name.indexOf(FLATTEN_SEPARATOR, FLATTEN_SEPARATOR.length());
            if (idx == -1 || idx == 2) {
                // idx == 2 means "____"
                throw new RuntimeException("invalid flatten name: " + name);
            }
            String ctl = name.substring(FLATTEN_SEPARATOR.length(), idx);
            String db = name.substring(idx + FLATTEN_SEPARATOR.length());
            return Pair.of(ctl, db);
        } else if (nameParts.length == 2) {
            return Pair.of(nameParts[0], nameParts[1]);
        } else {
            throw new RuntimeException("invalid flatten name: " + name);
        }
    }

    public static String flatten(String ctl, String db) {
        return FLATTEN_SEPARATOR + ctl + FLATTEN_SEPARATOR + db;
    }

    public static boolean isFlattenCatalogEnabled() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return false;
        }
        return connectContext.getSessionVariable().flattenCatalog;
    }
}
