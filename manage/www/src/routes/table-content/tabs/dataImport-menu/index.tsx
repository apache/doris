/** @format */

import React from 'react';
import { Menu, Dropdown, Button } from 'antd';
import { UploadOutlined } from '@ant-design/icons';
import { DEFAULT_NAMESPACE_ID } from '@src/config';
import { useHistory } from 'react-router-dom';
import { FileWordOutlined, CloudServerOutlined } from '@ant-design/icons';
import styles from '../tabs.module.less';
const getMenu = (history: any, props: any) => {
    return (
        <Menu>
            <Menu.Item>
                <a
                    style={{ padding: '10px 25px', textAlign: 'center', color: '#757272' }}
                    onClick={() => {
                        history.push({
                            pathname: `/${DEFAULT_NAMESPACE_ID}/local-import/${localStorage.table_id}`,
                        });
                    }}
                >
                    <FileWordOutlined style={{ paddingRight: '15px', fontSize: '20px', verticalAlign: 'middle' }} />
                    本地上传文件
                </a>
            </Menu.Item>
            <Menu.Item>
                <a
                    style={{ padding: '10px 25px', textAlign: 'center', color: '#757272' }}
                    onClick={() => {
                        history.push({
                            pathname: `/${DEFAULT_NAMESPACE_ID}/system-import/${localStorage.table_id}`,
                        });
                    }}
                >
                    <CloudServerOutlined style={{ paddingRight: '15px', fontSize: '20px', verticalAlign: 'middle' }} />
                    文件系统导入
                </a>
            </Menu.Item>
        </Menu>
    );
};

function ImportMenu(props: any) {
    const history = useHistory();
    const MENU = getMenu(history, props);

    return (
        <div style={{ width: '100%' }}>
            <Dropdown overlay={MENU} placement="bottomCenter" overlayClassName={styles['import-drop']}>
                <Button
                    type="primary"
                    style={{ margin: '0 10px 15px', float: 'right' }}
                    shape="round"
                    icon={<UploadOutlined />}
                >
                    导入数据
                </Button>
            </Dropdown>
        </div>
    );
}

export default ImportMenu;
