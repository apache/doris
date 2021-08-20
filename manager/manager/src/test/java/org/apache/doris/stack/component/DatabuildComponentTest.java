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

package org.apache.doris.stack.component;

import static org.mockito.Mockito.when;

import org.apache.doris.stack.dao.ManagerDatabaseRepository;
import org.apache.doris.stack.entity.ManagerDatabaseEntity;
import org.apache.doris.stack.exception.NoPermissionException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;

@RunWith(JUnit4.class)
@Slf4j
public class DatabuildComponentTest {

    @InjectMocks
    private DatabuildComponent databuildComponent;

    @Mock
    private ManagerDatabaseRepository databaseRepository;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void checkClusterDatabaseTest() {
        log.debug("check database belong cluster test");
        int dbId = 1;
        int clusterId = 2;

        // Check that the result is correct and return database information
        // Construct database information
        String dbName = "db";
        ManagerDatabaseEntity database = new ManagerDatabaseEntity();
        database.setClusterId(clusterId);
        database.setId(dbId);
        database.setName(dbName);
        when(databaseRepository.findById(dbId)).thenReturn(Optional.of(database));

        try {
            ManagerDatabaseEntity result = databuildComponent.checkClusterDatabase(dbId, clusterId);
            Assert.assertEquals(result.getName(), dbName);
        } catch (Exception e) {
            log.error("check database belong cluster error {}.", e.getMessage());
        }

        // Check the result exception and throw exception information
        database.setClusterId(3);
        when(databaseRepository.findById(dbId)).thenReturn(Optional.of(database));
        try {
            databuildComponent.checkClusterDatabase(dbId, clusterId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), NoPermissionException.MESSAGE);
        }
    }

}
