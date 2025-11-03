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


import React, {useEffect, useState} from 'react';
import {Button, Col, Row, Typography} from 'antd';
import {getSystem} from 'Src/api/api';
import Table from 'Src/components/table';
import {useHistory} from 'react-router-dom';
import {Result} from '@src/interfaces/http.interface';

const {Text, Title, Paragraph} = Typography;

export default function System(params: any) {
    const [parentUrl, setParentUrl] = useState<string>('');
    const [allTableData, setAllTableData] = useState<any>({column_names: [], rows: []});

    const history = useHistory();
    const getSystemData = function (ac?: any) {
        const param = {
            path: location.search,
            signal: ac?.signal,
        };
        getSystem(param).then((res: Result<any>) => {
            if (res && res.msg === 'success') {
                setAllTableData(res.data);
                setParentUrl(res.data.parent_url);
            } else {
                setAllTableData({
                    column_names: [],
                    rows: [],
                });
            }
        }).catch(err => {
        });
    };

    useEffect(() => {
        const ac = new AbortController();
        getSystemData(ac);
        return () => ac.abort();
    }, [location.search]);

    function goPrev() {
        if (parentUrl === '/rest/v1/system') {
            history.push('/System?path=/');
            return;
        }
        if (parentUrl) {
            history.push(parentUrl.split('v1/')[1]);
        }
    }

    return (
        <Typography style={{padding: '30px'}}>
            <Title level={2}>System Info</Title>
            <Text strong={true}>This page lists the system info, like /proc in Linux.</Text>
            <Paragraph>

            </Paragraph>
            <Row style={{paddingBottom: '15px'}}>
                <Col span={12} style={{color: '#02a0f9'}}>Current path: {location.search.split('=')[1]}</Col>
                <Col span={12} style={{textAlign: 'right'}}>
                    <Button size='small' type="primary" onClick={goPrev}>Parent Dir</Button>
                </Col>
            </Row>
            <Table
                isSort={true}
                isFilter={true}
                isInner={true}
                scroll={{ x: 'max-content' }}
                path = 'System'
                isSystem = {true}
                allTableData={allTableData}
                rowKey={(record) => record.name}
            />
        </Typography>
    );
}
 
