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


package org.apache.doris.datasource.hive.event;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * MetastoreEvent for CREATE_DATABASE event type
 */
public class CreateDatabaseEvent extends MetastoreEvent {
    private static final Logger LOG = LogManager.getLogger(CreateDatabaseEvent.class);

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private CreateDatabaseEvent(NotificationEvent event,
            String catalogName) {
        super(event, catalogName);
        Preconditions.checkState(getEventType().equals(MetastoreEventType.CREATE_DATABASE));
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
            String catalogName) {
        return Lists.newArrayList(
                new CreateDatabaseEvent(event, catalogName));
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            LOG.info("CreateDatabaseEvent process,catalogName:[{}],dbName:[{}]", catalogName, dbName);
            Env.getCurrentEnv().getCatalogMgr()
                    .createExternalDatabase(dbName, catalogName);
        } catch (DdlException e) {
            LOG.warn("CreateDatabaseEvent failed,dbName:[{}],catalogName:[{}].", dbName, catalogName, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process create database event"));
        }
    }
}
