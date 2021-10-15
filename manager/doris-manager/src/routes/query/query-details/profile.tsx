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

/* eslint-disable prettier/prettier */
import React, { FC, useState, useEffect, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Tree, Tag, Space, Card, Tabs } from 'antd';
import { getProfileFragments, getProfileGraph } from '../query.api';
import styles from './query.module.less'

interface IFragment {
    fragment_id: string;
    instance_id: string[];
    time: string;
}
const Profile: FC = () => {
    const [value, setValue] = useState<string>('');
    const [treeData, setTreeData] = useState();
    const params = useParams<{ queryId: string }>();
    const queryId = params.queryId;

    useEffect(() => {
        getProfileFragments({ queryId }).then(res => {
            const temp = res.data.map((item: IFragment) => ({
                
                title: `Fragment${item.fragment_id} -- ${item.time}`,
                key: item.fragment_id,
                disabled: true,
                children: Object.keys(item.instance_id).map(instance => ({
                    title: `${instance} -- ${item.instance_id[instance]}` ,
                    key: instance,
                    isLeaf: true,
                    parent: item.fragment_id,
                  })),
            }));
            setTreeData(temp);
        });
        getProfileGraph({
            queryId,
        }).then(res => {
            setValue(res.data.graph);
        });
    }, []);

    const treeChange = (keys: React.Key[], info: any) => {
        setValue('');
        const { key, parent } = info.node;
        getProfileGraph({
            queryId,
            fragmentId: parent,
            instanceId: key,
        }).then(res => {
            setValue(res.data.graph);
        });
    };
    function overview() {
        getProfileGraph({
            queryId,
        }).then(res => {
            setValue(res.data.graph);
        });
    }

    return (
        <div className={styles.profileBox}>
            <div className={styles.fragment}>
                <a onClick={() => overview()} style = {{margin: '0 0 0 27px'}}>Over View</a>
                <Tree showLine defaultExpandAll treeData={treeData} onSelect={treeChange}></Tree>
            </div>
            <div className={styles.graph}>
                <pre>
                    <code>{value}</code>
                </pre>
            </div>
        </div>
    );
};

export default Profile;
