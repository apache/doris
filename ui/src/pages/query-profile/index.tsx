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
 
import React, {useState, useEffect, useRef} from 'react';
import {Typography, Button, Row, Col} from 'antd';
const {Text, Title, Paragraph} = Typography;
import {queryProfile} from 'Src/api/api';
import Table from 'Src/components/table';
import {useHistory} from 'react-router-dom';
export default function QueryProfile(params: any) {
    // const [parentUrl, setParentUrl] = useState('');
    const container = useRef();
    const [allTableData, setAllTableData] = useState({});
    const [profile, setProfile] = useState();
    const history = useHistory();
    const doQueryProfile = function(){
        const param = {
            path: getLastPath(),
        };
        queryProfile(param).then(res=>{
            if (res && res.msg === 'success') {
                if(!res.data.column_names){
                    setProfile(res.data);
                    container.current.innerHTML=res.data;
                }else{
                    setProfile('');
                    setAllTableData(res.data);
                }
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
        doQueryProfile();
    }, [location.pathname]);
    function getLastPath(){
        let arr = location.pathname.split('/');
        let str = arr.pop();
        return str === 'QueryProfile' ? '' : str;
    }
    function goPrev(){
        if (location.pathname === '/QueryProfile/') {return;}
        history.push('/QueryProfile/');
    }
    return(
        <Typography style={{padding:'30px'}}>
            <Title >Finished Queries</Title>

            <Row style={{paddingBottom:'15px'}}>
                <Col span={12} ><Text type="strong">This table lists the latest 100 queries</Text></Col>
                <Col span={12} style={{textAlign:'right'}}>
                    {profile?<Button  type="primary" onClick={goPrev}>back</Button>:''}
                </Col>
            </Row>
            {
                profile
                    ?<div ref={container} style={{background: '#f9f9f9',padding: '20px'}}>
                        {/* {profile} */}
                    </div>
                    :<Table
                        isSort={true}
                        isFilter={true}
                        isInner={'/QueryProfile'}
                        allTableData={allTableData}
                    />
            }

        </Typography>
    );
}
 
