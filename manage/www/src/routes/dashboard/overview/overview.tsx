import CSSModules from 'react-css-modules';
import React, { useEffect, useState } from 'react';
import styles from './overview.less';
import { DashboardAPI } from '../dashboard.api';
import { DashboardItem } from '../components/dashboard-item';
import { HddOutlined, LaptopOutlined, PieChartOutlined, TableOutlined } from '@ant-design/icons';
import { message } from 'antd';
import { MetaInfoResponse } from '../dashboard.interface';

export function Component(props: any) {
    const [metaInfo, setMetaInfo] = useState<MetaInfoResponse>({
        beCount: 0,
        dbCount: 0,
        diskOccupancy: 0,
        feCount: 0,
        remainDisk: 0,
        tblCount: 0,
    });
    useEffect(() => {
        getOverviewInfo();
    }, []);
    async function getOverviewInfo() {
        const res = await DashboardAPI.getMetaInfo();
        const { msg, data, code } = res;
        if (code === 0) {
            if (res.data) {
                res.data.remainDisk = res.data.remainDisk / 1024;
            }
        } else {
            message.error(msg);
        }
    }
    return (
        <div styleName="overview-container">
            <DashboardItem title="数据库数量" icon={<HddOutlined />} des={metaInfo.dbCount} />
            <DashboardItem title="数据表数量" icon={<TableOutlined />} des={metaInfo.tblCount} />
            <DashboardItem
                title="磁盘占用量"
                icon={<PieChartOutlined />}
                des={
                    metaInfo.diskOccupancy == 0 ? metaInfo.diskOccupancy + '%' : metaInfo.diskOccupancy.toFixed(2) + '%'
                }
            />
            <DashboardItem title="剩余磁盘空间" icon={<LaptopOutlined />} des={metaInfo.remainDisk.toFixed(2) + 'TB'} />
        </div>
    );
}

export const Overview = CSSModules(styles)(Component);
