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
 
import React,{useState,SyntheticEvent} from 'react';
import {ResizableBox} from 'react-resizable';
// const ResizableBox = require('react-resizable').ResizableBox;
import styles from './index.less';
require('react-resizable/css/styles.css');
// import VirtualList from 'Components/VirtualList/index';
export function HistoryQuery(props: any) {
    // const { match } = props;
    // const queueInfo = { ...useContext(QueueContext) };
    const [historyBoxWidth,setHistoryBoxWidth] = useState(300);
    // const historyBoxWidth = +(localStorage.getItem('historyBoxWidth')||300);

    const onResize=function(e: SyntheticEvent, data: any) {
        const width = data.size.width || 300;
        setHistoryBoxWidth(width);
        // localStorage.setItem('historyBoxWidth', width);
    };
    const initData = {
        recommendHouse: '名仕公馆',
        recommendReason: '开发商租赁保障，价格低，位置优越',
    };
    const array = [];
    for(let i = 0; i < 999; i++){
        array.push(initData);
    }
    const resource = array;
    const renderTable =(item,index)=>{return (<div style={{height:'150px','boxSizing':'border-box'}} key={index}>
        <span >推荐房源：</span><span className="recommend-reason">{item.recommendHouse }{index}</span>
        <span >推荐理由：</span><span className="recommend-reason">{item.recommendReason}</span>
    </div>);};
    return (
        <ResizableBox
            // axis='x'
            width={historyBoxWidth}
            height={Infinity}
            className={styles['wrap']}
            resizeHandles={['w']}
            onResizeStart={onResize}
            minConstraints={[500, 125]}
            maxConstraints={[1000, 300]}
            axis="x"
        >
            <VirtualList renderItem={renderTable} resource={resource} height="400px" itemHeight="150" estimateHeight="150"></VirtualList>
        </ResizableBox>
    );
}
 
