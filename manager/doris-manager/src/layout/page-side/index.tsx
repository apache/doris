/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @format
 */

import React, { useState, useEffect, SyntheticEvent } from 'react';
import { Resizable } from 're-resizable';
import styles from './page-side.module.less';

export function PageSide(props: any) {
    const { children } = props;
    const [sideBoxWidth, setSideBoxWidth] = useState(300);
    const directionEnable = {
        top: false,
        right: true,
        bottom: false,
        left: false,
        topRight: false,
        bottomRight: false,
        bottomLeft: false,
        topLeft: false,
    };
    return (
        <Resizable
            size={{ width: sideBoxWidth, height: '100%' }}
            enable={directionEnable}
            onResizeStop={(e, direction, ref, d) => {
                setSideBoxWidth(sideBoxWidth + d.width);
            }}
            className={styles['build-page-side']}
        >
            {props.children}
        </Resizable>
    );
}
