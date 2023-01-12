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

package org.apache.doris.datasource.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TException;

import java.lang.reflect.Field;
import java.util.List;

public class MyHMSClient extends HiveMetaStoreClient {
    public MyHMSClient(HiveConf conf) throws MetaException {
        super(conf);
    }

    public MyHMSClient(HiveConf conf, HiveMetaHookLoader hookLoader) throws MetaException {
        super(conf, hookLoader, true);
    }

    public MyHMSClient(HiveConf conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
        super(conf, hookLoader, allowEmbedded);
    }

    @Override
    public Table getTable(String dbname, String name) throws MetaException, TException, NoSuchObjectException {
        ThriftHiveMetastore.Iface client = (ThriftHiveMetastore.Iface) getSuperField(this, "client");
        Table t = client.get_table(dbname, name);
        if (this.fastpath) {
            return t;
        } else {
            MetaStoreFilterHook hook = (MetaStoreFilterHook) getSuperField(this, "filterHook");
            return this.deepCopy(hook.filterTable(t));
        }
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        ThriftHiveMetastore.Iface client = (ThriftHiveMetastore.Iface) getSuperField(this, "client");
        List<FieldSchema> fields = client.get_schema(db, tableName);
        return this.fastpath ? fields : this.deepCopyFieldSchemas(fields);
    }

    public static void setSuperField(Object paramClass, String paramString, Object newClass) throws TException {
        Field field = null;
        try {
            field = paramClass.getClass().getSuperclass().getDeclaredField(paramString);
            field.setAccessible(true);
            field.set(paramClass, newClass);
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }
        return;
    }

    public static Object getSuperField(Object paramClass, String paramString) throws TException {
        Field field = null;
        Object object = null;
        try {
            field = paramClass.getClass().getSuperclass().getDeclaredField(paramString);
            field.setAccessible(true);
            object = field.get(paramClass);
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }
        return object;
    }
}
