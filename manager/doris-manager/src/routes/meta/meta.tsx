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

/**
 * @format
 */
import React, { useEffect, useState } from 'react';
import styles from './meta.less';
import { PageSide } from '@src/layout/page-side/index';
import { MetaBaseTree } from '../tree/index';
import { Redirect, Route, Router, Switch } from 'react-router-dom';
import TableContent from '../table-content';
import Database from '../database';

export function Meta(props: any) {
    return (
        <div className={styles['palo-new-main']}>
            <div className={styles['new-main-sider']}>
                <PageSide>
                    <MetaBaseTree></MetaBaseTree>
                </PageSide>
            </div>
            <div
                className={styles['new-main-content']}
                style={{
                    margin: '15px',
                    marginTop: 0,
                    height: 'calc(100vh - 95px)',
                }}
            >
                <Switch>
                    <Route path="/meta/table/:tableId" component={TableContent}/>
                    <Route path="/meta/database" component={Database}/>
                </Switch>
            </div>
        </div>
    );
}
