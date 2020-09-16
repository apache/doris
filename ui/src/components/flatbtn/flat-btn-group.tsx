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
 
import React, { FunctionComponent, useRef } from 'react';
import { DownOutlined } from '@ant-design/icons';
import { Menu, Dropdown, Divider } from 'antd';
import './flat-btn-group.less';

interface FlatItemProps {
  children?: React.ReactNode[];
  showNum?: number;
}

const FlatBtnGroup: FunctionComponent<FlatItemProps> = ({showNum = 3, children = []}) => {
    let childList: React.ReactNode[] = [];
    if (showNum <= 1) {
        showNum = 3;
    }
    if (!Array.isArray(children)) {
        childList.push(children);
    } else {
        childList = children;
    }
    const validChildren = childList.filter(child => !!child).flat(Infinity);
    const newList = validChildren.slice(0, showNum - 1);
    const dropList = validChildren.slice(showNum - 1);

    const menu = (
        <Menu className="flat-menu">
            {dropList.map((item: any, index) => {
                return (
                    <Menu.Item disabled={item.props.disabled} key={index}>
                        {item}
                    </Menu.Item>
                );
            })}
        </Menu>
    )

    const wrap = useRef(null);

    return (
        <div className="flat-btn-group">
            {newList.map((btn, key) => (
                <span key={`flat-btn-${key}`}>
                    {btn}
                    {(key !== showNum - 1 && !(key < showNum && key === newList.length - 1)) ||
                    dropList.length ? <Divider type="vertical" /> : <></>}
                </span>
            ))}
            {dropList.length ? (
                <Dropdown
                    overlay={menu}
                    className="flat-btn-group"
                    getPopupContainer={() => {
                        const dom = wrap.current;
                        if (dom) {
                        return dom;
                        }
                        return document.body;
                    }}>
                        <a className="ant-dropdown-link">
                            更多
                            <DownOutlined />
                        </a>
                </Dropdown>
            ) : <></>}
        </div>
    );
};

export default FlatBtnGroup;
 
