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
 
import React,{useState, useEffect, useRef} from 'react';
import {Typography, Divider, Row, Col, Input, BackTop} from 'antd';
const {Title, Paragraph, Text} = Typography;
import {getLog} from 'Src/api/api';
const {Search} = Input;
import {Result} from '@src/interfaces/http.interface';
export default function Logs(params: any) {
    const container = useRef();
    const [LogConfiguration, setLogConfiguration] = useState({});
    const [LogContents, setLogContents] = useState({});
    function getLogData(data){
        getLog(data).then(res=>{
            if(res.data && res.msg === 'success'){
                if(res.data.LogConfiguration){
                    setLogConfiguration(res.data.LogConfiguration);
                }
                if(res.data.LogContents){
                    container.current.innerHTML=res.data.LogContents.log;
                    setLogContents(res.data.LogContents);
                }
            }
        });
    }
    useEffect(() => {
        getLogData({});
    }, []);
    return(
        <Typography style={{padding:'30px'}}>
            <Title >Log Configuration</Title>
            <Paragraph>
                <p>Level: {LogConfiguration.VerboseNames}</p>
                <p>Verbose Names:{LogConfiguration.VerboseNames}</p>
                <p>Audit Names: {LogConfiguration.AuditNames}</p>
            </Paragraph>
            <Row>
                <Col span={4}>
                    <Search
                        placeholder="new verbose name"
                        enterButton="Add"
                        onSearch={value => getLogData({add_verbose:value})}
                    />
                </Col>
                <Col span={4} offset={1}>
                    <Search
                        placeholder="del verbose name"
                        enterButton="Delete"
                        onSearch={value => getLogData({del_verbose:value})}
                    />
                </Col>
            </Row>
            <Divider/>
            <Title style={{marginTop:'0px'}}>Log Contents</Title>
            <Paragraph>
                <p>Log path is: {LogContents.logPath}</p>
                <p>{LogContents.showingLast}</p>
            </Paragraph>
            <div ref={container} style={{background: '#f9f9f9',padding: '20px'}}>
                {/* {LogContents.log} */}
            </div>
            <BackTop></BackTop>
        </Typography>
    );
}
 
