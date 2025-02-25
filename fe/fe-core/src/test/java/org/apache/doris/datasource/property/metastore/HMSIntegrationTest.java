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

package org.apache.doris.datasource.property.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import shade.doris.hive.org.apache.thrift.TException;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

@Disabled
public class HMSIntegrationTest {

    // Hive configuration file path
    private static final String HIVE_CONF_PATH = "";
    // krb5 configuration file path
    private static final String KRB5_CONF_PATH = "";
    // Path to the Kerberos keytab file
    private static final String KEYTAB_PATH = "";
    // Principal name for Kerberos authentication
    private static final String PRINCIPAL_NAME = "";

    private static final String QUERY_DB_NAME = "";
    private static final String QUERY_TBL_NAME = "";
    private static final String CREATE_TBL_NAME = "";
    private static final String CREATE_TBL_IN_DB_NAME = "";
    // HDFS URI for the table location
    private static final String HDFS_URI = "";
    private static final boolean ENABLE_EXECUTE_CREATE_TABLE_TEST = false;

    @Test
    public  void testHms() throws IOException {
        // Set up HiveConf and Kerberos authentication
        HiveConf hiveConf = setupHiveConf();
        setupKerberos(hiveConf);

        // Authenticate user using the provided keytab file
        UserGroupInformation ugi = authenticateUser();
        System.out.println("User Credentials: " + ugi.getCredentials());

        // Perform Hive MetaStore client operations
        ugi.doAs((PrivilegedAction<Void>) () -> {
            try {
                HiveMetaStoreClient client = createHiveMetaStoreClient(hiveConf);

                // Get database and table information
                getDatabaseAndTableInfo(client);

                // Create a new table in Hive
                createNewTable(client);

            } catch (TException e) {
                throw new RuntimeException("HiveMetaStoreClient operation failed", e);
            }
            return null;
        });
    }

    /**
     * Sets up the HiveConf object by loading necessary configuration files.
     *
     * @return Configured HiveConf object
     */
    private static HiveConf setupHiveConf() {
        HiveConf hiveConf = new HiveConf();
        // Load the Hive configuration file
        hiveConf.addResource(HIVE_CONF_PATH);
        // Set Hive Metastore URIs and Kerberos principal
        //if not in config-site
        //hiveConf.set("hive.metastore.uris", "");
        //hiveConf.set("hive.metastore.sasl.enabled", "true");
        //hiveConf.set("hive.metastore.kerberos.principal", "");
        return hiveConf;
    }

    /**
     * Sets up Kerberos authentication properties in the HiveConf.
     *
     * @param hiveConf HiveConf object to update with Kerberos settings
     */
    private static void setupKerberos(HiveConf hiveConf) {
        // Set the Kerberos configuration file path
        System.setProperty("java.security.krb5.conf", KRB5_CONF_PATH);
        // Enable Kerberos authentication for Hadoop
        hiveConf.set("hadoop.security.authentication", "kerberos");
        // Set the Hive configuration for Kerberos authentication
        UserGroupInformation.setConfiguration(hiveConf);
    }

    /**
     * Authenticates the user using Kerberos with a provided keytab file.
     *
     * @return Authenticated UserGroupInformation object
     * @throws IOException If there is an error during authentication
     */
    private static UserGroupInformation authenticateUser() throws IOException {
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(PRINCIPAL_NAME, KEYTAB_PATH);
    }

    /**
     * Creates a new HiveMetaStoreClient using the provided HiveConf.
     *
     * @param hiveConf The HiveConf object with configuration settings
     * @return A new instance of HiveMetaStoreClient
     * @throws TException If there is an error creating the client
     */
    private static HiveMetaStoreClient createHiveMetaStoreClient(HiveConf hiveConf) throws TException {
        return new HiveMetaStoreClient(hiveConf);
    }

    /**
     * Retrieves database and table information from the Hive MetaStore.
     *
     * @param client The HiveMetaStoreClient used to interact with the MetaStore
     * @throws TException If there is an error retrieving database or table info
     */
    private static void getDatabaseAndTableInfo(HiveMetaStoreClient client) throws TException {
        // Retrieve and print the list of databases
        System.out.println("Databases: " + client.getAllDatabases());
        Table tbl = client.getTable(QUERY_DB_NAME, QUERY_TBL_NAME);
        System.out.println(tbl);
    }

    /**
     * Creates a new table in Hive with specified metadata.
     *
     * @param client The HiveMetaStoreClient used to create the table
     * @throws TException If there is an error creating the table
     */
    private static void createNewTable(HiveMetaStoreClient client) throws TException {
        if (!ENABLE_EXECUTE_CREATE_TABLE_TEST) {
            return;
        }
        // Create StorageDescriptor for the table
        StorageDescriptor storageDescriptor = createTableStorageDescriptor();

        // Create the table object and set its properties
        Table table = new Table();
        table.setDbName(CREATE_TBL_IN_DB_NAME);
        table.setTableName(CREATE_TBL_NAME);
        table.setPartitionKeys(createPartitionColumns());
        table.setSd(storageDescriptor);

        // Create the table in the Hive MetaStore
        client.createTable(table);
        System.out.println("Table 'exampletable' created successfully.");
    }

    /**
     * Creates the StorageDescriptor for a table, which includes columns and location.
     *
     * @return A StorageDescriptor object containing table metadata
     */
    private static StorageDescriptor createTableStorageDescriptor() {
        // Define the table columns
        List<FieldSchema> columns = new ArrayList<>();
        columns.add(new FieldSchema("id", "int", "ID column"));
        columns.add(new FieldSchema("name", "string", "Name column"));
        columns.add(new FieldSchema("age", "int", "Age column"));

        // Create and configure the StorageDescriptor for the table
        StorageDescriptor storageDescriptor = new StorageDescriptor();
        storageDescriptor.setCols(columns);
        storageDescriptor.setLocation(HDFS_URI);

        // Configure SerDe for the table
        SerDeInfo serDeInfo = createSerDeInfo();
        storageDescriptor.setSerdeInfo(serDeInfo);

        return storageDescriptor;
    }

    /**
     * Creates the SerDeInfo object for the table, which defines how data is serialized and deserialized.
     *
     * @return A SerDeInfo object with the specified serialization settings
     */
    private static SerDeInfo createSerDeInfo() {
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setName("example_serde");
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        return serDeInfo;
    }

    /**
     * Creates the partition columns for the table.
     *
     * @return A list of FieldSchema objects representing partition columns
     */
    private static List<FieldSchema> createPartitionColumns() {
        List<FieldSchema> partitionColumns = new ArrayList<>();
        partitionColumns.add(new FieldSchema("year", "int", "Year partition"));
        partitionColumns.add(new FieldSchema("month", "int", "Month partition"));
        return partitionColumns;
    }
}

