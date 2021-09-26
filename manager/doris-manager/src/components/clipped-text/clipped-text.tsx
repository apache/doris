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

import { Tooltip } from 'antd';
import React, { useRef, useEffect, useState } from 'react';
import CopyText from '../copy-text';

export function ClippedText(props: any) {
    const refClip = useRef<HTMLDivElement>(null);
    const [isShowTip, setShowTip] = useState<boolean>(false);
    let tempWidth = props.width;
    if (props.inTable && typeof props.width === 'number') {
        tempWidth = props.width - 40;
    }
    let width;
    if (props.width) {
        width = typeof props.width === 'number' ? `${tempWidth}px` : props.width;
    } else {
        width = 'calc(100% - 10px)';
    }
    const title = props.text ? props.text : props.children;

    useEffect(() => {
        const elem = refClip.current;
        if (elem) {
            if (elem.scrollWidth > elem.clientWidth) {
                setShowTip(true);
            }
        }
    }, []);

    const content = (
        <div
            className="clipped-text"
            ref={refClip}
            style={{
                // width,
                maxWidth: width,
                overflow: 'hidden',
                whiteSpace: 'nowrap',
                textOverflow: 'ellipsis',
                ...props.style,
            }}
        >
            {props.children}
        </div>
    );

    return isShowTip ? (
        <CopyText text={title}>
            <Tooltip placement="top" title={title} overlayStyle={{ maxHeight: 400, overflow: 'auto', maxWidth: 600 }}>
                {content}
            </Tooltip>
        </CopyText>
    ) : (
        content
    );
}
