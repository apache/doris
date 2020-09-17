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
 
import React from 'react';
import {Typography, Divider, Row, Col, Input, Button} from 'antd';
const {Title, Paragraph, Text} = Typography;
import Table from 'Src/components/table';

export default function Home(params: any) {
    return(
        <Typography style={{padding:'30px'}}>
            <Title >Help Info</Title>
            <Paragraph type="strong">This page lists the help info, like 'help contents' in Mysql client.</Paragraph>
            <Row>
                <Col span={4}>
                    <Input
                        placeholder="new verbose name"
                        addonAfter="Search"
                        // onSearch={value => console.log(value)}
                        style={{color: '#ddd'}}
                    />
                </Col>
                <Col span={4} offset={1}>
                    <Button type="primary">Primary Button</Button>
                </Col>
            </Row>
            <Title style={{marginTop:'0px'}}>Exact Matching Topic</Title>
            <Paragraph>
                <p>Level: INFO</p>
                <p>Verbose Names:</p>
            </Paragraph>
            <Title style={{marginTop:'0px'}}>Fuzzy Matching Topic(By Keyword)</Title>
            <Text type="success">Find only one category, so show you the detail info below.</Text>
            <Text type="success"><p>Find 1 sub topics.</p></Text>
            {/* <Table
                isSort={true}
                isFilter={true}
                columns={[]}
                dataSource={[]}
                rowKey = {'DbId'}
            /> */}
            <Text type="success">Find 2 sub categories.</Text>
            {/* <Table
                isSort={true}
                isFilter={true}
                columns={[]}
                dataSource={[]}
                rowKey = {'DbId'}
            /> */}
        </Typography>
    );
}
 
