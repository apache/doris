import CSSModules from 'react-css-modules';
import React, { useState } from 'react';
import styles from './dashboard.less';
import { CommonHeader } from '@src/components/common-header/header';
import { ConnectInfo } from './connect-info/connect-info';
import { DashboardTabEnum } from './dashboard.data';
import { HomeOutlined } from '@ant-design/icons';
import { Link, match, Redirect, Route, Switch, useHistory, useLocation } from 'react-router-dom';
import { Overview } from './overview/overview';
import { Tabs } from 'antd';

const ICON_HOME = <HomeOutlined />;
const { TabPane } = Tabs;

function Component(props: any) {
    const { match } = props;
    const [refreshToken, setRefreshToken] = useState(new Date().getTime());
    const history = useHistory();
    const location = useLocation();
    const [activeKey, setTabsActiveKey] = useState<DashboardTabEnum>(DashboardTabEnum.Overview);
    React.useEffect( ()=>{
        if(location.pathname.includes(DashboardTabEnum.Overview)) {
            setTabsActiveKey(DashboardTabEnum.Overview);
        } else {
            setTabsActiveKey(DashboardTabEnum.ConnectInfo);
        }
    }, [])
    return (
        <div styleName="home-main">
            <CommonHeader
                title="数据仓库"
                icon={ICON_HOME}
                callback={() => setRefreshToken(new Date().getTime())}
            ></CommonHeader>
            <div styleName="home-content">
                <Tabs activeKey={activeKey} onChange={(key: any) => {
                    setTabsActiveKey(key);
                    if (key === DashboardTabEnum.Overview) {
                        history.push(`${match.path}/overview`);
                    } else {
                        history.push(`${match.path}/connect-info`);
                    }
                }}>
                    <TabPane tab="集群信息概览" key={DashboardTabEnum.Overview}></TabPane>
                    <TabPane tab="连接信息" key={DashboardTabEnum.ConnectInfo}></TabPane>
                </Tabs>
            </div>
            <Switch>
                <Route path={`${match.path}/overview`} component={Overview} />
                <Route path={`${match.path}/connect-info`} component={ConnectInfo} />
                <Redirect to={`${match.path}/overview`} />
            </Switch>
        </div>
    );
}
export const Dashboard = CSSModules(styles)(Component);
