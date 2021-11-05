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

/** @format */
import React, { useEffect, useState } from 'react';
import styles from './index.module.less';
import { Row, Tabs, Avatar, Col, Button, Form, Input, message } from 'antd';
import { RequiredMark } from 'antd/lib/form/Form';
import { UserSettingAPI } from './user.api';
const { TabPane } = Tabs;
import { useTranslation } from 'react-i18next';
function UserSetting() {
    useEffect(() => {
        getCurrentInfo();
    }, []);
    const [animated, setAnimated] = useState<string>('');
    const [userName, setUserName] = useState<any>('');
    const [userInfo, setUserInfo] = useState<any>({});
    const [showAccount, setShowAccount] = useState<any>();
    const { t }  = useTranslation();
    function chooseTabs(key: any) {
        console.log(key);
    }
    const [userForm] = Form.useForm();
    const [passwordForm] = Form.useForm();
    const [requiredMark, setRequiredMarkType] = useState<RequiredMark>('optional');
    const onRequiredTypeChange = ({ requiredMarkValue }: { requiredMarkValue: RequiredMark }) => {
        setRequiredMarkType(requiredMarkValue);
    };

    function getCurrentInfo() {
        UserSettingAPI.getCurrentInfo()
            .then(res => {
                setUserInfo(res.data);
                console.log(res.data);
                setUserName(res.data.name != '' ? res.data.name.charAt(0).toUpperCase() : '');
                if (res.data.name === 'Admin') {
                    setShowAccount(false);
                    console.log(showAccount);
                } else {
                    setShowAccount(true);
                    console.log(showAccount);
                }
                userForm.setFieldsValue({
                    name: res.data.name,
                    email: res.data.email,
                });
            })
            .catch(err => {
                console.log(err);
            });
    }
    function accountUser(_value: any) {
        UserSettingAPI.modifyUserInfo(userInfo.id, _value).then(res => {
            if (res.code === 0) {
                message.success(t`Successfully`);
            } else {
                message.warning(res.msg);
            }
        });
    }
    function updatePassword(_value: any) {
        UserSettingAPI.changePassword(userInfo.id, _value).then(res => {
            if (res.code === 0) {
                message.success(t`PasswordResetComplete`);
            } else {
                message.warning(res.msg);
            }
        });
    }
    return (
        <div style={{ textAlign: 'center' }}>
            <Row>
                <Col span={24} style = {{marginTop:'3em'}}>
                    <Avatar
                        style={{
                            color: '#ffffff',
                            backgroundColor: '#3f6cd8',
                            height: '5em',
                            fontWeight: 700,
                            lineHeight: 5,
                            width: '5em',
                        }}
                    >
                        {userName}
                    </Avatar>
                </Col>
            </Row>
            <Row>
                <Col span={24} style={{ marginBottom: '4em', marginTop: '2em' }}>
                    <h2>{ t`AccountSetting`}</h2>
                </Col>
            </Row>
            <Row></Row>
            <Row>
                <Col span={24}>
                    <Tabs
                        defaultActiveKey="1"
                        onChange={chooseTabs}
                        centered
                        style={{ marginBottom: 0, background: '#ffffff' }}
                    >
                        {showAccount}
                        {showAccount ? (
                            <TabPane tab={ t`AccountInformation`} key="1" className={styles['input-gird']}>
                                <Form
                                    form={userForm}
                                    layout="vertical"
                                    initialValues={{ requiredMarkValue: requiredMark }}
                                    onValuesChange={onRequiredTypeChange}
                                    requiredMark={requiredMark}
                                    onFinish={accountUser}
                                >
                                    <Form.Item label={ t`Name`} required name="name">
                                        <Input placeholder={t`PleaseTypeInYourName`} />
                                    </Form.Item>
                                    <Form.Item label={ t`Mail`} required name="email">
                                        <Input placeholder={ t`pleaseInputYourEmail`} />
                                    </Form.Item>
                                    <Form.Item>
                                        <Button
                                            type="primary"
                                            style={{ width: '100%', borderRadius: 10, height: 45 }}
                                            htmlType="submit"
                                        >
                                            { t`update`}
                                        </Button>
                                    </Form.Item>
                                </Form>
                            </TabPane>
                        ) : null}
                        <TabPane tab={ t`password`} key="2" className={styles['input-gird']}>
                            <Form
                                    form={passwordForm}
                                    layout="vertical"
                                    onFinish={updatePassword}
                            >
                                <Form.Item label={ t`OldPassword`} rules={[{ required: true, message: t`OldPasswordCannotBeEmpty` }]} name="oldPsassword">
                                    <Input.Password
                                        placeholder={ t`PleaseEnterTheOldPassword`}
                                        style={{
                                            width: '100%',
                                            borderRadius: 10,
                                            padding: '0.75em',
                                            background: '#ffffff',
                                        }}
                                    />
                                </Form.Item>
                                <Form.Item label={t`NewPassword`} 
                                    rules={[{
                                        required: true,
                                        message: t`OldPasswordCannotBeEmpty`
                                    },
                                        { min: 6, message: t`Least6` },
                                        {pattern:/((^(?=.*[a-z])(?=.*[A-Z])(?=.*\W)[\da-zA-Z\W]{6,}$)|(^(?=.*\d)(?=.*[A-Z])[\da-zA-Z\W]{6,}$)|(^(?=.*\d)(?=.*\W)[\da-zA-Z\W]{6,}$)|(^(?=.*\d)(?=.*[a-z])[\da-zA-Z\W]{6,}$)|(^(?=.*[A-Z])(?=.*\W)[\da-zA-Z\W]{6,}$)|(^(?=.*[a-z])(?=.*\W)[\da-zA-Z\W]{6,}$)|(^(?=.*[a-z])(?=.*[A-Z])[\da-zA-Z\W]{6,}$))/,message:t`ContainAtLeast2Types`}]} name="new_password" >
                                    <Input.Password
                                        placeholder={t`PleaseEnterTheNewPassword`}
                                        style={{
                                            width: '100%',
                                            borderRadius: 10,
                                            padding: '0.75em',
                                            background: '#ffffff',
                                        }}

                                    />
                                </Form.Item>
                                <Form.Item
                                    label={ t`ConfirmPassword`}
                                    name="password"
                                    rules={[{ required: true, message: t`ConfirmPasswordCanNotBeBlank` },
                                        ({ getFieldValue }) => ({
                                            validator(rule, value) {
                                                if (!value || getFieldValue('new_password') === value) {
                                                    return Promise.resolve()
                                                }
                                                return Promise.reject(t`TwoPasswordEntriesAreInconsistent`)
                                            }
                                        })
                                    ]}
                                >
                            
                                    <Input.Password
                                        placeholder={ t`PleaseConfirmYourNewPassword`}
                                        style={{
                                            width: '100%',
                                            borderRadius: 10,
                                            padding: '0.75em',
                                            background: '#ffffff',
                                        }}
                                    />
                                </Form.Item>
                                <Form.Item>
                                    <Button
                                        type="primary"
                                        onClick={updatePassword}
                                        style={{ width: '100%', borderRadius: 10, height: 45 }}
                                        htmlType="submit"
                                    >
                                        { t`Save`}
                                    </Button>
                                </Form.Item>
                            </Form>
                        </TabPane>
                    </Tabs>
                </Col>
            </Row>
        </div>
    );
}
export default UserSetting;
