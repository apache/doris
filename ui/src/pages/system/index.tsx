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
 

import React, {useState, useEffect} from 'react';
import {Typography, Button, Row, Col} from 'antd';
const {Text, Title, Paragraph} = Typography;
import {getSystem} from 'Src/api/api';
import Table from 'Src/components/table';
import {useHistory} from 'react-router-dom';
export default function System(params: any) {
    const [parentUrl, setParentUrl] = useState('');
    const [allTableData, setAllTableData] = useState({});

    const history = useHistory();
    const getSystemData = function(){
        const param = {
            path:location.search,
        };
        getSystem(param).then(res=>{
            if (res && res.msg === 'success') {
                setAllTableData(res.data);
                setParentUrl(res.data.parent_url);
            } else {
                setAllTableData({
                    column_names:[],
                    rows:[],
                });
            }
        })
            .catch(err=>{
                setAllTableData({
                    column_names:[],
                    rows:[],
                });
            });
    };
    useEffect(() => {
        getSystemData();
    }, [location.search]);
    function goPrev(){
        if (parentUrl === '/rest/v1/system') {
            history.push('/System?path=/');
            return;
        }
        if (parentUrl) {
            history.push(parentUrl.split('v1/')[1]);
        }
    }
    return(
        <Typography style={{padding:'30px'}}>
            <Title level={2}>System Info</Title>
            <Text type="strong">This page lists the system info, like /proc in Linux.</Text>
            <Paragraph>

            </Paragraph>
            <Row style={{paddingBottom:'15px'}}>
                <Col span={12} style={{color:'#02a0f9'}}>Current path: {location.search.split('=')[1]}</Col>
                <Col span={12} style={{textAlign:'right'}}>
                    <Button size='small' type="primary" onClick={goPrev}>Parent Dir</Button>
                </Col>
            </Row>
            <Table
                isSort={true}
                isFilter={true}
                isInner={true}
                path = 'System'
                isSystem = {true}
                allTableData={allTableData}
            />
        </Typography>
    );
}
 
