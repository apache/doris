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
import { renderRoutes } from 'react-router-config';
import CSSModules from 'react-css-modules';
import styles from './content.module.less';
import { PageSide } from '@src/layout/page-side/index';
import { MetaBaseTree } from '../tree/index';
import { Redirect, Route, Router, Switch } from 'react-router-dom';
import TableContent from '../table-content';
import Database from '../database';

function MiddleContent(props: any) {
    return (
        <div styleName="palo-new-main">
            <div styleName="new-main-sider">
                <PageSide>
                    <MetaBaseTree></MetaBaseTree>
                </PageSide>
            </div>
            <div
                styleName="site-layout-background new-main-content"
                style={{
                    margin: '15px',
                    marginTop: 0,
                    height: 'calc(100vh - 95px)',
                    // overflow: 'hidden'
                }}
            >
                <Switch>
                    <Route path="/:nsId/content/table/:tableId" component={TableContent}/>
                    <Route path="/:nsId/content/database" component={Database}/>
                </Switch>
            </div>
        </div>
    );
}

export default CSSModules(styles, { allowMultiple: true })(MiddleContent);
