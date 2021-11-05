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
import { Card, Form } from 'antd';
import { message, Space } from 'antd';
import { useHistory } from 'react-router-dom';
import { SpaceAPI } from '../space.api';
import { RequiredMark } from 'antd/lib/form/Form';
import { PlusOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';

const SpaceList = () => {
    const [spaceList, setSpaceList] = useState<{ name: string; description: string; id: string }[]>([]);
    const history = useHistory();
    const { t } = useTranslation();
    function refresh() {
        SpaceAPI.spaceList().then(res => {
            const { msg, data, code } = res;
            if (code === 0) {
                if (res.data) {
                    setSpaceList(res.data);
                }
            } else {
                message.error(msg);
            }
        });
    }
    const [form] = Form.useForm();
    const [requiredMark, setRequiredMarkType] = useState<RequiredMark>('optional');
    const cardStyle = {
        width: 500, marginBottom: 40, cursor: 'pointer', height: 100
    }

    const onRequiredTypeChange = ({ requiredMarkValue }: { requiredMarkValue: RequiredMark }) => {
        setRequiredMarkType(requiredMarkValue);
    };
    useEffect(() => {
        refresh();
    }, []);
    return (
        <div className={styles.dorisSpaceList}>
            <div className={styles.dorisSpaceListContainer}>
                {spaceList &&
                    spaceList.map((item, index) => {
                        return (
                            <Card onClick={() => history.push(`/super-admin/space/detail/${item.id}`)} key={item.name} title={item.name} size="small" style={cardStyle}>
                                <p>{item.description}</p>
                            </Card>
                        );
                    })}
                <Card
                    onClick={() => history.push('/super-admin/space/new')}
                    size="small"
                    style={cardStyle}
                >
                    <Space style={{display: 'flex', alignItems: 'middle', justifyContent: 'center', fontSize: 18, lineHeight: '80px'}}>
                        <PlusOutlined />
                        <span>{t`NewSpace`}</span>
                    </Space>
                </Card>
            </div>
        </div>
    );
};
export default SpaceList;
