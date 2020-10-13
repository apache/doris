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
const {Title} = Typography;
import {getConfig} from 'Src/api/api';
import Table from 'Src/components/table';
export default function Configuration(params: any) {
    const [allTableData, setAllTableData] = useState({});
    const getConfigData = function(){
        getConfig({}).then(res=>{
            if (res && res.msg === 'success') {
                setAllTableData(res.data);
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
        getConfigData();
    }, []);

    return(
        <Typography style={{padding:'30px'}}>
            <Title level={2}>Configure Info</Title>
            <Table
                isSort={true}
                isFilter={true}
                // isInner={true}
                allTableData={allTableData}
            />
        </Typography>
    );
}
 
