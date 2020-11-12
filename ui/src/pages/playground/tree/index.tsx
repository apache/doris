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
import {Tree, Spin, Input} from 'antd';
const { Search } = Input;
import {TableOutlined, HddOutlined, ReloadOutlined} from '@ant-design/icons';
import {AdHocAPI} from 'Src/api/api';
import {useTranslation} from 'react-i18next';
import {
    AdhocContentRouteKeyEnum,
} from '../adhoc.data';
interface DataNode {
  title: string;
  key: string;
  isLeaf?: boolean;
  children?: DataNode[];
}
import './index.css';
const initTreeDate: DataNode[] = [];
function updateTreeData(list: DataNode[], key, children) {
    return list.map(node => {
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
    const [expandedKeys, setExpandedKeys] = useState([]);
    const [searchValue, setSearchValue] = useState('');
    const [autoExpandParent, setAutoExpandParent] = useState(true);
    useEffect(() => {
        initTreeData()
    }, []);
    function initTreeData(){
        AdHocAPI.getDatabaseList().then(res=>{
            if (res.msg === 'success' && Array.isArray(res.data)) {
                const num = Math.random()
                const treeData = res.data.map((item,index)=>{
                    return {
                        title: item,
                        key: `${num}-1-${index}-${item}`,
                        icon: <HddOutlined/>,
                    };
                });
                setTreeData(treeData);
                getRealTree(treeData);
            }
            setLoading(false);
        });
    }
    function onLoadData({key, children}) {
        const [random, storey, index, db_name] = key.split('-');
        const param = {
            db_name,
            // tbl_name,
        };
        return AdHocAPI.getDatabaseList(param).then(res=>{
            if (res.msg=='success' && Array.isArray(res.data)) {
                const children = res.data.map((item,index)=>{
                    if (storey === '1'){
                        return {
                            title: item,
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
        path: AdhocContentRouteKeyEnum = AdhocContentRouteKeyEnum.Result,
    ) {
        if (keys.length > 0) {
            props.history.push(`/Playground/${path}/${keys[0].split(':')[1]}`);
        }
    }
    function onSearch(e){
        const { value } = e.target;
        const expandedKeys = treeData
          .map((item, index) => {
              if (getParentKey(value, treeData[index].children, index)) {
                return item.key
              } else {
                  return null;
              }
          })
        setExpandedKeys(expandedKeys);
        setSearchValue(value);
        setAutoExpandParent(true);
        getRealTree(treeData, value);
    };
    function onExpand(expandedKeys) {
        setExpandedKeys(expandedKeys);
        setAutoExpandParent(false);
    };
    const getParentKey = (key, tree, idx) => {
        if (!tree) {
            return false;
        }
        for (let i = 0; i < tree.length; i++) {
          const node = tree[i];
          if (node.title.includes(key)) {
            return true
          } else {
            treeData[idx].children ? treeData[idx].children[i].title = node.title : ''
          }
        }
        return false;
    };
    function getRealTree(treeData, value){
        const realTree  = inner(treeData);
        function inner(treeData){
            return treeData.map(item => {
                const search = value || '';
                const index = item.title.indexOf(search);
                const beforeStr = item.title.substr(0, index);
                const afterStr = item.title.substr(index + search.length);
                const title =
                  index > -1 ? (
                    <span>
                      {beforeStr}
                      <span className="site-tree-search-value">{search}</span>
                      {afterStr}
                    </span>
                  ) : (
                    item.title
                  );
                if (item.children) {
                  return {...item, title, children: inner(item.children)};
                }
                return {
                  ...item,
                  title
                };
            });
        }
        debounce(setRealTree(realTree),300);
    }
    function debounce(fn, wait) {
        var timer = null;
        return function () {
            var context = this
            var args = arguments
            if (timer) {
                clearTimeout(timer);
                timer = null;
            }
            timer = setTimeout(function () {
                fn.apply(context, args)
            }, wait)
        }
    }
    return (
        <>
            <Spin spinning={loading} size="small"/>
            <div>
                <Search 
                    size="small" 
                    style={{ padding: 5, position: 'fixed', zIndex: '99', width: '300px'}} 
                    placeholder={t('search')} 
                    enterButton={<ReloadOutlined />} 
                    onSearch={initTreeData}
                    onChange={onSearch} />
            </div>
            
            <Tree
                showIcon={true}
                loadData={onLoadData}
                treeData={realTree}
                onExpand={onExpand}
                expandedKeys={expandedKeys}
                autoExpandParent={autoExpandParent}
                style={{'width':'100%', height:'86vh' ,paddingTop:'35px',overflowY:'scroll'}}
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
 
