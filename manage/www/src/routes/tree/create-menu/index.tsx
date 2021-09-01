/** @format */

import React, { useState, useCallback } from 'react';
import styles from './create.module.less';
// import CSSModules from 'react-css-modules';
import { Menu, Dropdown, Button, Space } from 'antd';
import { TableOutlined, HddOutlined, PlusOutlined } from '@ant-design/icons';
import { DEFAULT_NAMESPACE_ID } from '@src/config';
import { useHistory } from 'react-router-dom';
import DatabaseModal from './databaseModal';
const getMenu = (history: any, setIsShow: any) => {
    return (
        <Menu>
            <Menu.Item
                onClick={() => {
                    setIsShow(true);
                }}
            >
                <a style={{ padding: '15px 0', textAlign: 'center', color: '#757272' }}>
                    <HddOutlined style={{ paddingRight: '8px', fontSize: '20px', verticalAlign: 'middle' }} />
                    创建数据库
                </a>
            </Menu.Item>
            <Menu.Item
                onClick={() => {
                    history.push(`/${DEFAULT_NAMESPACE_ID}/new-table`);
                }}
            >
                <a style={{ padding: '15px 0', textAlign: 'center', color: '#757272' }}>
                    <TableOutlined style={{ paddingRight: '8px', fontSize: '20px', verticalAlign: 'middle' }} />
                    创建数据表
                </a>
            </Menu.Item>
        </Menu>
    );
};

function CreateMenu() {
    const history = useHistory();
    const [isShow, setIsShow] = useState();
    const MENU = getMenu(history, setIsShow);

    function refresh() {
        console.log(1111);
    }
    return (
        <div style={{ width: '100%' }}>
            <Dropdown overlay={MENU} placement="bottomCenter" overlayClassName={styles['create-drop']}>
                <Button icon={<PlusOutlined />} type="primary" style={{ width: '100%', borderRadius: '10px' }}>
                    创建数据
                </Button>
            </Dropdown>
            <DatabaseModal
                isShow={isShow}
                changeShow={(e: any) => {
                    setIsShow(e);
                }}
            />
        </div>
    );
}

export default CreateMenu;
