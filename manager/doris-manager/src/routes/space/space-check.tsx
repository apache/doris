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
import { Space, Card, Divider, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { Form, Input, Button, Radio } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import styles from './index.module.less';
import Password from 'antd/lib/input/Password';
type RequiredMark = boolean | 'optional';
import { SpaceAPI } from './space.api';
import { useTranslation } from 'react-i18next';

const user = JSON.parse(JSON.stringify(window.localStorage.getItem('user')));
const FormLayoutDemo = () => {
    const { t } = useTranslation();
    const [form] = Form.useForm();
    const [ formData, setFormData ] = useState<any>({});
    const params = useParams<{spaceId: string}>();
    function refresh() {
        SpaceAPI.spaceGet( params.spaceId ).then(res => {
            const { msg, data, code } = res;
            if (code === 0) {
                if (res.data) {
                    setFormData(res.data);
                }
            } else {
                message.error(msg);
            }
        });
    }
    const [requiredMark, setRequiredMarkType] = useState<RequiredMark>('optional');

    const onRequiredTypeChange = ({ requiredMarkValue }: { requiredMarkValue: RequiredMark }) => {
        setRequiredMarkType(requiredMarkValue);
    };
    useEffect(() => {
        refresh();
    }, []);
    return (
        <Form
            form={form}
            layout="vertical"
            initialValues={{ requiredMarkValue: requiredMark }}
            onValuesChange={onRequiredTypeChange}
            requiredMark={requiredMark}
            className={styles['input-gird']}
        >
            <h2>{t`spaceInfo`}</h2>
            <Divider plain></Divider>
            <Form.Item label={t`spaceName`}  required>
                <Input placeholder="input placeholder" value={formData.name} disabled/>
            </Form.Item>
            <Form.Item label={t`spaceIntroduction`} required>
                <Input placeholder="input placeholder" value={formData.description} disabled/>
            </Form.Item>
            {/* {user.authType === "studio" && ( */}
                 <Form.Item label={t`adminName`} required>
                    <Input placeholder="input placeholder" value={formData.spaceAdminUser} disabled/>
                </Form.Item>
            {/* )} */}
            {/* {user.authType === "studio" && ( */}
                {/* <Form.Item label={t`adminpsw`} required>
                    <Input.Password style={{ width: '400px' }} className={styles['input-password']} disabled/>
                </Form.Item> */}
            {/* )} */}
            
            <h2>{t`clusterInfo`}</h2>
            <Divider plain></Divider>
            <Form.Item label={t`clusterAddr`}  required>
                <Input placeholder="input placeholder" value={formData.paloAddress} disabled/>
            </Form.Item>
            <Form.Item label={t`httpPort`} required>
                <Input placeholder="input placeholder" value={formData.httpPort} disabled/>
            </Form.Item>
            <Form.Item label={t`JDBCPort`} required>
                <Input placeholder="input placeholder"  value={formData.queryPort} disabled/>
            </Form.Item>
            <Form.Item label={t`userName`} required>
                <Input placeholder="input placeholder" value={formData.paloAdminUser}  disabled/>
            </Form.Item>
            {/* <Form.Item label={t`userPwd`}  required>
                <Input.Password style={{ width: '400px' }} className={styles['input-password']} disabled/>
            </Form.Item> */}
            {/* <Form.Item>
                <Button type="primary">Submit</Button>
            </Form.Item> */}
        </Form>
    );
};
export default FormLayoutDemo;
