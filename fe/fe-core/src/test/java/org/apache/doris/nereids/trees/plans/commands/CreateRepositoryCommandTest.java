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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.backup.Repository;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CreateRepositoryCommandTest extends TestWithFeService {
    @Test
    public void testValidate() {
        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put("AWS_ENDPOINT", "http://s3.us-east-1.amazonaws.com")
                .put("AWS_ACCESS_KEY", "akk")
                .put("AWS_SECRET_KEY", "skk")
                .put("AWS_REGION", "us-east-1")
                .build();

        StorageBackend storageBackend = new StorageBackend("s3", "s3://s3-repo",
                StorageBackend.StorageType.S3, Maps.newHashMap(properties));
        CreateRepositoryCommand command = new CreateRepositoryCommand(false, "repo", storageBackend);
        Assertions.assertDoesNotThrow(() -> command.validate());
    }

    @Test
    public void testS3RepositoryPropertiesConverter() throws Exception {
        FeConstants.runningUnitTest = true;
        String s3Repo = "CREATE REPOSITORY `s3_repo_command`\n"
                + "WITH S3\n"
                + "ON LOCATION 's3://s3-repo'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "    'AWS_ENDPOINT' = 'http://s3.us-east-1.amazonaws.com',\n"
                + "    'AWS_ACCESS_KEY' = 'akk',\n"
                + "    'AWS_SECRET_KEY'='skk',\n"
                + "    'AWS_REGION' = 'us-east-1'\n"
                + ");";

        NereidsParser nereidsParser = new NereidsParser();

        LogicalPlan logicalPlan = nereidsParser.parseSingle(s3Repo);
        Assertions.assertTrue(logicalPlan instanceof CreateRepositoryCommand);
        Assertions.assertEquals(((CreateRepositoryCommand) logicalPlan).getProperties().size(), 4);
        Repository repository = getRepository((CreateRepositoryCommand) logicalPlan, "s3_repo_command");
        Assertions.assertEquals(4, repository.getRemoteFileSystem().getProperties().size());

        String s3RepoNew = "CREATE REPOSITORY `s3_repo_new_command`\n"
                + "WITH S3\n"
                + "ON LOCATION 's3://s3-repo'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "    's3.endpoint' = 'http://s3.us-east-1.amazonaws.com',\n"
                + "    's3.access_key' = 'akk',\n"
                + "    's3.secret_key' = 'skk'\n"
                + ");";

        LogicalPlan logicalPlan1 = nereidsParser.parseSingle(s3RepoNew);
        Assertions.assertTrue(logicalPlan1 instanceof CreateRepositoryCommand);
        Assertions.assertEquals(((CreateRepositoryCommand) logicalPlan1).getProperties().size(), 3);
        Repository repositoryNew = getRepository((CreateRepositoryCommand) logicalPlan1, "s3_repo_new_command");
        Assertions.assertEquals(3, repositoryNew.getRemoteFileSystem().getProperties().size());
    }

    @Disabled("not support")
    @Test
    public void testBosBrokerRepositoryPropertiesConverter() throws Exception {
        FeConstants.runningUnitTest = true;
        String bosBroker = "CREATE REPOSITORY `bos_broker_repo_command`\n"
                + "WITH BROKER `bos_broker`\n"
                + "ON LOCATION 'bos://backup'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "    'bos_endpoint' = 'http://gz.bcebos.com',\n"
                + "    'bos_accesskey' = 'akk',\n"
                + "    'bos_secret_accesskey'='skk'\n"
                + ");";

        NereidsParser nereidsParser = new NereidsParser();

        LogicalPlan logicalPlan = nereidsParser.parseSingle(bosBroker);
        Assertions.assertTrue(logicalPlan instanceof CreateRepositoryCommand);
        Assertions.assertEquals(((CreateRepositoryCommand) logicalPlan).getProperties().size(), 3);

        List<Pair<String, Integer>> brokers = ImmutableList.of(Pair.of("127.0.0.1", 9999));
        Env.getCurrentEnv().getBrokerMgr().addBrokers("bos_broker1", brokers);

        Repository repositoryNew = getRepository((CreateRepositoryCommand) logicalPlan, "bos_broker_repo_command");
        Assertions.assertEquals(repositoryNew.getRemoteFileSystem().getProperties().size(), 4);
    }

    private static Repository getRepository(CreateRepositoryCommand command, String name) throws DdlException {
        Env.getCurrentEnv().getBackupHandler().createRepository(command);
        return Env.getCurrentEnv().getBackupHandler().getRepoMgr().getRepo(name);
    }
}
