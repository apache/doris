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

import React from 'react';
import { CopyOutlined } from '@ant-design/icons';
import { message } from 'antd';
import './index.less';

export default function CopyText(props: any) {
    function handleCopy() {
        const input = document.createElement('input');
        input.style.opacity = '0';
        input.setAttribute('readonly', 'readonly');
        input.setAttribute('value', props.text);
        document.body.appendChild(input);
        input.setSelectionRange(0, 9999);
        input.select();
        if (document.execCommand('copy')) {
            document.execCommand('copy');
            message.success('复制成功');
        }
        document.body.removeChild(input);
    }
    return (
        <div className="copy-wrap">
            {props.children}
            <CopyOutlined className="copy-icon" onClick={handleCopy} />
        </div>
    );
}
