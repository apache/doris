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
import React, { useEffect, useState } from 'react';
import { Input, Spin, Tree } from 'antd';
import { HddOutlined, ReloadOutlined, TableOutlined } from '@ant-design/icons';
import { AdHocAPI } from 'Src/api/api';
import { useTranslation } from 'react-i18next';
import { AdhocContentRouteKeyEnum } from '../adhoc.data';
import './index.css';

const { Search } = Input;

interface DataNode {
    title: string;
    key: string;
    isLeaf?: boolean;
    children?: DataNode[];
}

const initTreeDate: DataNode[] = [];

function updateTreeData(list: DataNode[], key, children) {
    return list.map((node) => {
        if (node.key === key) {
            return {
                ...node,
                children,
            };
        }
        return node;
    });
}

export function AdHocTree(props: any) {
    let { t } = useTranslation();
    const [treeData, setTreeData] = useState(initTreeDate);
    const [realTree, setRealTree] = useState(initTreeDate);
    const [loading, setLoading] = useState(true);
    const [expandedKeys, setExpandedKeys] = useState<any[]>([]);
    const [autoExpandParent, setAutoExpandParent] = useState(true);

    useEffect(() => {
        const ac = new AbortController();
        initTreeData(ac);
        return () => ac.abort();
    }, []);

    function initTreeData(ac?: AbortController) {
        AdHocAPI.getDatabaseList({ signal: ac?.signal })
            .then((res) => {
                if (res.msg === 'success' && Array.isArray(res.data)) {
                    const num = Math.random();
                    const treeData = res.data.map((item, index) => {
                        return {
                            title: item,
                            keys: [item],
                            key: `${num}-1-${index}-${item}`,
                            icon: <HddOutlined />,
                        };
                    });
                    setTreeData(treeData);
                    getRealTree(treeData);
                }
                setLoading(false);
            })
            .catch((err) => {});
    }

    function onLoadData({ key, children }) {
        const [, storey, , db_name] = key.split('-');
        const param = {
            db_name,
            // tbl_name,
        };
        return AdHocAPI.getDatabaseList(param).then((res) => {
            if (res.msg == 'success' && Array.isArray(res.data)) {
                const children = res.data.map((item, index) => {
                    if (storey === '1') {
                        return {
                            title: item,
                            keys: [param.db_name, item],
                            key: `2-${index}-${param.db_name}-${item}`,
                            icon: <TableOutlined />,
                            isLeaf: true,
                        };
                    }
                });
                const trData = updateTreeData(treeData, key, children);
                setTreeData(trData);
                getRealTree(trData);
            }
        });
    }

    function handleTreeSelect(
        keys: React.ReactText[],
        info: any,
        path: AdhocContentRouteKeyEnum = AdhocContentRouteKeyEnum.Result
    ) {
        console.log(info);
        const tablePath = info.node.keys.join('-');
        if (info.node.keys.length > 0) {
            props.history.push(`/Playground/${path}/${tablePath}`);
        }
    }

    function onSearch(e) {
        const { value } = e.target;
        const expandedKeys: any[] = treeData.map((item, index) => {
            if (getParentKey(value, treeData[index].children, index)) {
                return item.key;
            } else {
                return null;
            }
        });
        setExpandedKeys(expandedKeys);
        setAutoExpandParent(true);
        getRealTree(treeData, value);
    }

    function onExpand(expandedKeys) {
        setExpandedKeys(expandedKeys);
        setAutoExpandParent(false);
    }

    const getParentKey = (key, tree, idx) => {
        if (!tree) {
            return false;
        }
        for (let i = 0; i < tree.length; i++) {
            const node = tree[i];
            if (node.title.includes(key)) {
                return true;
            } else {
                treeData[idx].children
                    ? (treeData[idx].children[i].title = node.title)
                    : '';
            }
        }
        return false;
    };

    function getRealTree(treeData, value?) {
        const realTree = inner(treeData);

        function inner(treeData) {
            return treeData.map((item) => {
                const search = value || '';
                const index = item.title.indexOf(search);
                const beforeStr = item.title.substr(0, index);
                const afterStr = item.title.substr(index + search.length);
                const title =
                    index > -1 ? (
                        <span>
                            {beforeStr}
                            <span className="site-tree-search-value">
                                {search}
                            </span>
                            {afterStr}
                        </span>
                    ) : (
                        item.title
                    );
                if (item.children) {
                    return { ...item, title, children: inner(item.children) };
                }
                return {
                    ...item,
                    title,
                };
            });
        }

        debounce(setRealTree(realTree), 300);
    }

    function debounce(fn, wait) {
        let timer = null;
        return function () {
            let context = this;
            let args = arguments;
            if (timer) {
                clearTimeout(timer);
                timer = null;
            }
            timer = setTimeout(function () {
                fn.apply(context, args);
            }, wait);
        };
    }

    return (
        <>
            <Spin spinning={loading} size="small" />
            <div>
                <Search
                    size="small"
                    style={{
                        padding: 5,
                        position: 'fixed',
                        zIndex: '99',
                        width: '300px',
                    }}
                    placeholder={t('search')}
                    enterButton={<ReloadOutlined />}
                    onSearch={initTreeData}
                    onChange={onSearch}
                />
            </div>

            <Tree
                showIcon={true}
                loadData={onLoadData}
                treeData={realTree}
                onExpand={onExpand}
                expandedKeys={expandedKeys}
                autoExpandParent={autoExpandParent}
                style={{
                    width: '100%',
                    height: '86vh',
                    paddingTop: '35px',
                    overflowY: 'scroll',
                }}
                onSelect={(selectedKeys, info) =>
                    handleTreeSelect(
                        selectedKeys,
                        info,
                        AdhocContentRouteKeyEnum.Structure
                    )
                }
            />
        </>
    );
}
