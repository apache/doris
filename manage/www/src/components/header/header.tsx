import { Layout, Menu, Tooltip, Col, Row, Dropdown, message, Anchor } from 'antd';
import Sider from 'antd/lib/layout/Sider';
import SubMenu from 'antd/lib/menu/SubMenu';
import { LineChartOutlined, QuestionCircleOutlined,  UserOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import React, { useEffect, useState } from 'react';
import { LayoutAPI } from './header.api';

import { useTranslation } from 'react-i18next';
import { DEFAULT_NAMESPACE_ID } from '@src/config';
import styles from './index.module.less';

export function Header(props: any) {
    const { t } = useTranslation();
    const [statisticInfo, setStatisticInfo] = useState<any>({});

    function getCurrentUser() {
        LayoutAPI.getCurrentUser()
            .then(res => {
                LayoutAPI.getSpaceName(res.data.space_id).then(res1 => {
                    setStatisticInfo(res1.data);
                })
            })
            .catch(err => {
                console.log(err);
            });
    }
    useEffect(() => {
        getCurrentUser();
    }, []);

    return (
        <div
            className="site-layout-background"
            style={{ padding: 0, background: '#f9fbfc', borderBottom: '1px solid #d9d9d9' }}
        > 
            <Row justify="space-between">
                <Col style={{ marginLeft: '2em' }}>
                    <span>{t`namespace`}：{statisticInfo.name}</span>
                </Col>
                <Col style={{ cursor: 'pointer', marginRight: 20, fontSize: 22 }}>
                    <Tooltip placement="bottom" title={t`backTo`+ "Studio"}>
                        <LineChartOutlined
                            className={styles['data-builder-header-items']}
                            style={{ marginRight: 20 }}
                            onClick={() => (window.location.href = `${window.location.origin}`)}
                        />
                    </Tooltip>
                    <Tooltip placement="bottom" title="帮助">
                        <QuestionCircleOutlined
                            // className={styles['data-builder-header-items']}
                            style={{ marginRight: 20 }}
                            onClick={() => {
                                const analyticsUrl = `${window.location.origin}/docs/pages/产品概述/产品介绍.html`;
                                window.open(analyticsUrl);
                            }}
                        />
                    </Tooltip>
                </Col>
            </Row>
        </div>
    );
}
