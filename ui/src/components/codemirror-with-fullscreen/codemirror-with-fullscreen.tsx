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
 */
 
import React, {useState} from 'react';
import {IconFont} from 'Components/iconfont';
import {Controlled as CodeMirror} from 'react-codemirror2';
import styles from './codemirror-with-fullscreen.less';
require('codemirror/lib/codemirror.css');
require('codemirror/theme/material.css');
require('./doris.css');
require('codemirror/addon/hint/show-hint.css');
require('codemirror/addon/display/fullscreen.css');
require('codemirror/mode/sql/sql');
require('codemirror/addon/hint/show-hint');
require('codemirror/addon/hint/sql-hint');
require('codemirror/addon/display/fullscreen');
export function CodeMirrorWithFullscreen(props: any) {
    const [options, setOptions] = useState(props.options);
    const [iconType, setIconType] = useState('fullscreen');
    return (
        <div className={styles['codemirror-with-fullscreen']}>
            <IconFont
                onClick={() => {
                    setOptions({
                        ...options,
                        fullScreen: !options.fullScreen,
                    });
                    const iconType = options.fullScreen
                        ? 'fullscreen'
                        : 'fullscreen-exit';
                    setIconType(iconType);
                }}
                type={iconType}
                className={styles[iconType]}
            />
            <CodeMirror
                {...props}
                value={props.value}
                onBeforeChange={props.onBeforeChange}
                className={styles['codemirror']}
                options={{...options,theme:'default'}}
            />
        </div>
    );
}
 
