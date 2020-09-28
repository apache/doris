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
 
import React, {useState,useEffect} from 'react';
import {Tree, Spin, Space} from 'antd';
import {TableOutlined, HddOutlined} from '@ant-design/icons';
import {AdHocAPI} from 'Src/api/api';
import {
    AdhocContentRouteKeyEnum,
} from '../adhoc.data';
interface DataNode {
  title: string;
  key: string;
  isLeaf?: boolean;
  children?: DataNode[];
}

const initTreeDate: DataNode[] = [];
function updateTreeData(list: DataNode[], key: React.Key, children: DataNode[]): DataNode[] {
    return list.map(node => {
        if (node.key === key) {
            return {
                ...node,
                children,
            };
        } else if (node.children) {
            return {
                ...node,
                children: updateTreeData(node.children, key, children),
            };
        }
        return node;
    });
}
export function AdHocTree(props: any) {

    const [treeData, setTreeData] = useState(initTreeDate);
    const [loading, setLoading] = useState(true);
    useEffect(() => {
        AdHocAPI.getDatabaseList().then(res=>{
            if (res.msg === 'success' && Array.isArray(res.data)) {
                const treeData = res.data.map((item,index)=>{
                    return {
                        title: item,
                        key: `1-${index}-${item}`,
                        icon: <HddOutlined/>,
                    };
                });
                setTreeData(treeData);
            }
            setLoading(false);
        });
    }, []);
    function onLoadData({key, children}) {
        const [storey, index, db_name, tbl_name] = key.split('-');
        const param = {
            db_name,
            tbl_name,
        };
        return AdHocAPI.getDatabaseList(param).then(res=>{
            if (res.msg=='success' && Array.isArray(res.data)) {
                const treeData = res.data.map((item,index)=>{
                    if(storey==1){
                        return {
                            title: item,
                            key: `2-${index}-${param.db_name}-${item}`,
                            icon: <TableOutlined />,
                            isLeaf: true,
                        };
                    }

                });
                setTreeData(origin =>
                    updateTreeData(origin, key, treeData),
                );
            }
        });

    }
    function handleTreeSelect(
        keys: React.ReactText[],
        info: any,
        path: AdhocContentRouteKeyEnum = AdhocContentRouteKeyEnum.Result,
    ) {
        if (keys.length > 0) {
            props.history.push(`/Playground/${path}/${keys[0].split(':')[1]}`);
        }
    }
    return (
        <>
            <Spin spinning={loading} size="small"/>
            <Tree
                showIcon={true}
                loadData={onLoadData}
                treeData={treeData}
                style={{'width':'100%', height:'100%' ,paddingTop:'15px'}}
                onSelect={(selectedKeys, info) =>
                    handleTreeSelect(
                        selectedKeys,
                        info,
                        AdhocContentRouteKeyEnum.Structure                                                        )
                }
            />
        </>
    );

}
 
