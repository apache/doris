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

import React, { useEffect, useState } from 'react';
import styles from '../space.less';
import { Button, Form, Input, Row, Space } from 'antd';
import { Divider, message } from 'antd';
import { SpaceAPI } from '../space.api';
import { useHistory, useParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { RequiredMark } from 'antd/lib/form/Form';
import { modal } from '@src/components/doris-modal/doris-modal';

const SpaceDetail = () => {
    const { t } = useTranslation();
    const [form] = Form.useForm();
    const [formData, setFormData] = useState<any>({});
    const params = useParams<{ spaceId: string }>();
    const history = useHistory();
    function refresh() {
        SpaceAPI.spaceGet(params.spaceId).then(res => {
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

    function handleDelete() {
        const spaceId = params.spaceId;
        modal.confirm(
            t`notice`,
            t`SpaceDeleteTips`,
            async () => {
                SpaceAPI.spaceDelete(spaceId).then(result => {
                    if (result && result.code !== 0) {
                        modal.error(t`Failed`, result.msg);
                    } else {
                        modal.success(t`DeleteSuccessTips`).then(result => {
                            if (result.isConfirmed) {
                                history.push(`/super-admin/space/list`);
                            }
                        });
                    }
                });
            },
        );
    }
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
                <Form.Item label={t`spaceName`} required>
                    <Input placeholder="input placeholder" value={formData.name} disabled />
                </Form.Item>
                <Form.Item label={t`spaceIntroduction`} required>
                    <Input placeholder="input placeholder" value={formData.description} disabled />
                </Form.Item>
                <Form.Item label={t`adminName`} required>
                    <Input placeholder="input placeholder" value={formData.spaceAdminUser} disabled />
                </Form.Item>

                <h2>{t`clusterInfo`}</h2>
                <Divider plain></Divider>
                <Form.Item label={t`clusterAddr`} required>
                    <Input placeholder="input placeholder" value={formData.paloAddress} disabled />
                </Form.Item>
                <Form.Item label={t`httpPort`} required>
                    <Input placeholder="input placeholder" value={formData.httpPort} disabled />
                </Form.Item>
                <Form.Item label={t`JDBCPort`} required>
                    <Input placeholder="input placeholder" value={formData.queryPort} disabled />
                </Form.Item>
                <Form.Item label={t`userName`} required>
                    <Input placeholder="input placeholder" value={formData.paloAdminUser} disabled />
                </Form.Item>
            </Form>
            <Row justify="center">
                <Space>
                    <Button type="primary" danger onClick={handleDelete}>
                        {t`Delete`}
                    </Button>
                    <Button
                        type="primary"
                        onClick={() => {
                            history.push(`/super-admin/space/list`);
                        }}
                    >
                        {t`GoBack`}
                    </Button>
                </Space>
            </Row>
        </div>
    );
};
export default SpaceDetail;
