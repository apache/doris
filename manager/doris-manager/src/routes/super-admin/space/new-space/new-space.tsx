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

import { Divider, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { Form, Input, Button, Radio } from 'antd';
import styles from '../space.less';
type RequiredMark = boolean | 'optional';
import { SpaceAPI } from '../space.api';
import { useHistory } from 'react-router-dom';
import { useTranslation } from 'react-i18next';

const SpaceNew = () => {
    const { t } = useTranslation();
    const [form] = Form.useForm();
    const [clusterForm] = Form.useForm();
    const [requiredMark, setRequiredMarkType] = useState<RequiredMark>('optional');
    const history = useHistory();

    const onRequiredTypeChange = ({ requiredMarkValue }: { requiredMarkValue: RequiredMark }) => {
        setRequiredMarkType(requiredMarkValue);
    };
    const handleCreate = () => {
        form.validateFields().then(values => {
            clusterForm.validateFields().then(cluValues => {
                SpaceAPI.spaceCreate({
                    cluster: {
                        address: cluValues.address,
                        httpPort: cluValues.httpPort,
                        passwd: cluValues.passwd || '',
                        queryPort: cluValues.queryPort,
                        user: cluValues.user,
                    },
                    name: values.userName,
                    describe: values.describe,
                    user: {
                        email: values.email,
                        name: values.name,
                        password: values.password,
                    },
                }).then(res => {
                    const { msg, data, code } = res;
                    if (code === 0) {
                        if (res.data) {
                            message.success(msg);
                            history.replace(`/space/list`);
                        }
                    } else {
                        message.error(msg);
                    }
                });
            });
        });
    };
    const handleLinkTest = () => {
        clusterForm.validateFields().then(values => {
            SpaceAPI.spaceValidate({
                address: values.address,
                httpPort: values.httpPort,
                passwd: values.passwd || '',
                queryPort: values.queryPort,
                user: values.user,
            }).then(res => {
                const { msg, data, code } = res;
                if (code === 0) {
                    message.success(msg);
                } else {
                    message.error(msg);
                }
            });
        });
    };

    const handleCheckName = () => {
        SpaceAPI.spaceCheck(form.getFieldValue('name'));
    };

    return (
        <div style={{ padding: '20px 0' }}>
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
                <Form.Item
                    label={t`spaceName`}
                    name="name"
                    rules={[
                        {
                            required: true,
                            validator: async (rule, value) => {
                                if (!value) {
                                    return Promise.reject(new Error(t`required`));
                                }
                                let resData = await SpaceAPI.spaceCheck(value);
                                if (resData.code === 0) {
                                    return Promise.resolve();
                                }
                                return Promise.reject(new Error(resData.msg));
                            },
                        },
                    ]}
                    validateTrigger="onBlur"
                >
                    <Input placeholder={t`spaceName`} onChange={handleCheckName} />
                </Form.Item>
                <Form.Item
                    label={t`spaceIntroduction`}
                    name="describe"
                    rules={[{ required: true, message: t`required` }]}
                >
                    <Input placeholder={t`spaceIntroduction`} />
                </Form.Item>
                <Form.Item label={t`adminName`} name="userName" rules={[{ required: true, message: t`required` }]}>
                    <Input placeholder={t`adminName`} />
                </Form.Item>
                <Form.Item label={t`adminEmail`} name="email" rules={[{ required: true, message: t`required` }]}>
                    <Input placeholder={t`adminEmail`} />
                </Form.Item>
                <Form.Item label={t`adminpsw`} name="password" rules={[{ required: true, message: t`required` }]}>
                    <Input.Password style={{ width: '400px' }} />
                </Form.Item>
                <Form.Item
                    label={t`confirmPassword`}
                    name="passwordCopy"
                    rules={[
                        {
                            required: true,
                            message: t`confirmPassword`,
                        },
                        ({ getFieldValue }) => ({
                            validator(_, value) {
                                if (!value || getFieldValue('password') === value) {
                                    return Promise.resolve();
                                }
                                return Promise.reject(new Error(t`inconsistentPasswords`));
                            },
                        }),
                    ]}
                >
                    <Input.Password style={{ width: '400px' }} />
                </Form.Item>
            </Form>

            <Form
                form={clusterForm}
                layout="vertical"
                initialValues={{ requiredMarkValue: requiredMark }}
                onValuesChange={onRequiredTypeChange}
                requiredMark={requiredMark}
                className={styles['input-gird']}
            >
                <h2>{t`clusterInfo`}</h2>
                <Divider plain></Divider>
                <Form.Item label={t`clusterAddr`} name="address" rules={[{ required: true, message: t`required` }]}>
                    <Input placeholder={t`clusterAddr`} />
                </Form.Item>
                <Form.Item label={t`httpPort`} name="httpPort" rules={[{ required: true, message: t`required` }]}>
                    <Input placeholder={t`httpPort`} />
                </Form.Item>
                <Form.Item label={t`JDBCPort`} name="queryPort" rules={[{ required: true, message: t`required` }]}>
                    <Input placeholder={t`JDBCPort`} />
                </Form.Item>
                <Form.Item label={t`userName`} name="user" rules={[{ required: true, message: t`required` }]}>
                    <Input placeholder={t`userName`} />
                </Form.Item>
                <Form.Item label={t`userPwd`} name="passwd">
                    <Input.Password style={{ width: '400px' }} className={styles['input-password']} />
                </Form.Item>
                <Form.Item>
                    <Button type="primary" onClick={handleLinkTest}>{t`linkTest`}</Button>
                    &nbsp;
                    <Button type="primary" onClick={handleCreate}>{t`submit`}</Button>
                </Form.Item>
            </Form>
        </div>
    );
};
export default SpaceNew;
