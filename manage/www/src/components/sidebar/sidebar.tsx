import { Layout, Menu, Tooltip, Col, Row, Dropdown, message, Anchor } from 'antd';
import Sider from 'antd/lib/layout/Sider';
import SubMenu from 'antd/lib/menu/SubMenu';
import { LineChartOutlined, QuestionCircleOutlined,  UserOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import React, { useEffect, useState } from 'react';

import { useTranslation } from 'react-i18next';
import { DEFAULT_NAMESPACE_ID } from '@src/config';
import styles from './sidebar.less';

export function Sidebar(props: any) {
    const { t } = useTranslation();
    const [statisticInfo, setStatisticInfo] = useState<any>({});

    return (
            <Sider width={200} className="DAE-manager-side">
            <Menu
                mode="inline"
                theme="dark"
                defaultSelectedKeys={['1']}
                defaultOpenKeys={['sub1']}
                style={{ height: '100%', borderRight: 0 }}
            >
                {/* <SubMenu key="sub1" icon={<UserOutlined />} title="Doris Manager"> */}
                    <Menu.Item style = {{paddingLeft: '8px',marginTop: '10px'}}>
                        <div
                            className={styles['logo']}
                            onClick={() => props.history.push(`/${DEFAULT_NAMESPACE_ID}/meta/index`)}
                        />
                    </Menu.Item>
                    <Menu.Item key="2">
                        <Link to={`/${DEFAULT_NAMESPACE_ID}/content`}>{t`data`}</Link>
                    </Menu.Item>
                    <SubMenu key="sub2" title="节点" style ={{fontSize: 16}}>
                        <Menu.Item key={`list`}>
                            <Link to={`/${DEFAULT_NAMESPACE_ID}/list`}>列表</Link>
                        </Menu.Item>
                        <Menu.Item key={`configuration`}> 
                            <Link to={`/${DEFAULT_NAMESPACE_ID}/configuration`}>配置项</Link>
                        </Menu.Item>
                        <Menu.Item key={`/node-dash`}>
                            <Link to={`/${DEFAULT_NAMESPACE_ID}/node-dash`}>仪表盘</Link>
                        </Menu.Item>
                    </SubMenu>
                    <Menu.Item key="3">
                        <Link to={`/${DEFAULT_NAMESPACE_ID}/dash`}>集群</Link>
                    </Menu.Item>
                    <Menu.Item key="4">
                        <Link to={`/${DEFAULT_NAMESPACE_ID}/query`}>查询</Link>
                    </Menu.Item>
                {/* </SubMenu> */}
            </Menu>
        </Sider>
    );
}
