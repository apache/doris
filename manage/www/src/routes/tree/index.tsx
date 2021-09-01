/** @format */

import React, { useState, useEffect } from 'react';
import { Tree, Spin, Input, message } from 'antd';
import { useHistory } from 'react-router-dom';
import { TableOutlined, HddOutlined, HomeOutlined } from '@ant-design/icons';
import { TreeAPI } from './tree.api';
import { DataNode } from './tree.interface';
import { updateTreeData } from './tree.service';
import { ContentRouteKeyEnum } from './tree.data';
import CreateMenu from './create-menu/index';
import styles from './tree.module.less';
import { DEFAULT_NAMESPACE_ID } from '@src/config';
import EventEmitter from '@src/utils/event-emitter';
// import { LoadingWrapper } from '@src/components/loadingwrapper/loadingwrapper';
const initTreeDate: DataNode[] = [];
export function MetaBaseTree(props: any) {
    const [treeData, setTreeData] = useState(initTreeDate);
    const [loading, setLoading] = useState(true);
    const history = useHistory();
    useEffect(() => {
        initTreeData();
        EventEmitter.on('refreshData', initTreeData);
        EventEmitter.on('refreshTreeData', initTreeData);
    }, []);

    function initTreeData() {
        TreeAPI.getDatabaseList({ nsId: '0' }).then(res => {
            if (res.code === 0) {
                const num = Math.random();
                const database = res.data;
                const treeData: Array<DataNode> = [];
                database.forEach((item, index) => {
                    treeData.push({
                        title: `${item.name}`,
                        key: `1¥${num}¥name¥${item.id}¥${item.name}`,
                        icon: <HddOutlined />,
                    });
                });
                setTreeData(treeData);
            } else {
                setTreeData([]);
                message.error(res.msg);
            }
            setLoading(false);
        });
    }

    function onLoadData(node: any) {
        const [storey, id, name, db_id, db_name] = node.key.split('¥');
        return TreeAPI.getTables({ dbId: db_id }).then(res => {
            if (res.code === 0) {
                const tables = res.data;
                const children: Array<any> = [];
                if (tables.length) {
                    tables.forEach((item, index) => {
                        children.push({
                            title: `${item.name}`,
                            key: `2¥${db_id}¥${db_name}¥${item.id}¥${item.name}`,
                            icon: <TableOutlined />,
                            isLeaf: true,
                        });
                    });
                } else {
                    children.push({
                        title: '',
                        key: '',
                        icon: '',
                        isLeaf: true,
                        className: styles['display_none'],
                    });
                }

                const trData = updateTreeData(treeData, node.key, children);
                setTreeData(trData);
            } else {
                message.error(res.msg);
            }
        });
    }

    function handleTreeSelect(keys: any[], info: any) {
        if (keys.length > 0) {
            const [storey, db_id, db_name, id, name] = keys[0].split('¥');
            if (storey === '1') {
                localStorage.setItem('database_id', id);
                localStorage.setItem('database_name', name);
                history.push({
                    pathname: `/${DEFAULT_NAMESPACE_ID}/content/${ContentRouteKeyEnum.Database}/${id}`,
                    state: { id: id, name: name },
                });
            } else {
                localStorage.setItem('database_id', db_id);
                localStorage.setItem('database_name', db_name);
                localStorage.setItem('table_id', id);
                localStorage.setItem('table_name', name);
                history.push({
                    pathname: `/${DEFAULT_NAMESPACE_ID}/content/${ContentRouteKeyEnum.Table}/${id}`,
                    state: { id: id, name: name },
                });
            }
        }
    }

    function goHome() {
        history.push(`/${DEFAULT_NAMESPACE_ID}/content`);
    }
    return (
        <div className={styles['palo-tree-container']}>
            <h2 className={styles['palo-tree-title']}>
                <HomeOutlined onClick={goHome} />
                数据目录树
            </h2>
            {/* <LoadingWrapper loading={loading}> */}
            <div className={styles['palo-tree-wrapper']}>
                <Tree
                    showIcon={true}
                    loadData={onLoadData}
                    treeData={treeData}
                    className={styles['palo-side-tree']}
                    onSelect={(selectedKeys, info) => handleTreeSelect(selectedKeys, info)}
                />
            </div>
            {/* </LoadingWrapper> */}
        </div>
    );
}
