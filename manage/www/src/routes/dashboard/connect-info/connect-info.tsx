import CSSModules from 'react-css-modules';
import React, { useEffect, useState } from 'react';
import styles from './connect-info.less';
import { Card, message } from 'antd';
import { DashboardAPI } from '../dashboard.api';

export function Component(props: any) {
    const [spaceList, setSpaceList] = useState<any>({});
    const [https, setHttps] = useState<any>([]);
    const [mysqls, setMysqls] = useState<any>([]);
    useEffect(() => {
        getOverviewInfo();
    }, []);
    async function getOverviewInfo() {
        DashboardAPI.getSpaceList().then(res1 => {
            const { msg, data, code } = res1;
            let http = '';
            let mysql = '';
            if (code === 0) {
                if (res1.data) {
                    setSpaceList(res1.data);
                    for (let i = 0; i < res1.data.http.length; i++) {
                        http += res1.data.http[i] + '; ';
                    }
                    for (let i = 0; i < res1.data.mysql.length; i++) {
                        mysql += res1.data.mysql[i] + '; ';
                    }
                    setHttps(http);
                    setMysqls(mysql);
                }
            } else if (code === 404) {
                message.error('请升级Doris集群！');
                window.location.href = `${window.location.origin}`;
            } else {
                message.error(msg);
            }
        });
    }
    return (
        <div styleName="connect-info-container">
            <Card style={{ width: '100%' }}>
                <p>HTTP连接信息：{https}</p>
                <p>JDBC连接信息：{mysqls}</p>
            </Card>
        </div>
    );
}

export const ConnectInfo = CSSModules(styles)(Component);
