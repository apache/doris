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

import CSSModules from 'react-css-modules';
import React, { useCallback, useState } from 'react';
import styles from './message-item.less';
import { messItemProps } from './message-item.interface';

function MessageItem(props: messItemProps) {
    return (
        <div styleName="mess-item">
            <div styleName="mess-item-title">
                <p styleName="mess-item-icon">{props.icon}</p>
                <p styleName="mess-item-name">{props.title}</p>
            </div>
            <div styleName="mess-item-content">
                <span>{props.des}</span>
            </div>
        </div>
    );
}

export const DashboardItem = CSSModules(styles)(MessageItem);
