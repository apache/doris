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

/** @format */

import React, { useState, useCallback, useEffect } from 'react';
import styles from './header.module.less';
import { HeaderProps } from './header.interface';
import { SyncOutlined } from '@ant-design/icons';
import { HeaderAPI } from './header.api';
import CSSModules from 'react-css-modules';
const EventEmitter = require('events').EventEmitter; 
const event = new EventEmitter();

export function Header(props: HeaderProps) {
    const [loading, setLoading] = useState(false);
    // useEffect(() => {
    //     HeaderAPI.refreshData();
    // }, []);
    function refresh() {
        HeaderAPI.refreshData();
        event.emit('refreshData');
        props.callback();
        setTimeout(() => {
            setLoading(false);
        }, 300);
    }
    return (
        <div styleName="common-header">
            <div styleName="common-header-title">
                <span styleName="common-header-icon">{props.icon}</span>
                <span styleName="common-header-name">{props.title}</span>
            </div>
            <div styleName="common-header-refresh">
                <SyncOutlined
                    spin={loading}
                    onClick={() => {
                        refresh();
                        setLoading(true);
                    }}
                />
            </div>
        </div>
    );
}

export const CommonHeader = CSSModules(styles)(Header);