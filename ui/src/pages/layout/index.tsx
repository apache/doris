/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
/**
 * @file test cron
 * @author lpx
 * @since 2020/08/19
 */
import React, {useState} from 'react';
import {Layout, Menu, Dropdown, message} from 'antd';
import { CaretDownOutlined, LogoutOutlined} from '@ant-design/icons';
import {renderRoutes} from 'react-router-config';
import {useHistory} from 'react-router-dom';
import {useTranslation} from 'react-i18next';
import routes from 'Src/router';
import {logOut} from 'Utils/api';
import './index.css';
import styles from './index.less';
const {Header, Content, Footer} = Layout;

function Layouts(props: any) {
    let { t } = useTranslation();
    const [route, setRoute] = useState(props.route.routes);
    const [current, setCurrent] = useState(props.location.pathname);
    const history = useHistory();
    //Jump page
    function handleClick(e) {
        setCurrent(e.key);
        if (e.key === '/System' ) {
            history.push(`${e.key}?path=/`);
            return;
        }
        if (location.pathname === e.key) {
            location.reload();
        }
        history.push(e.key);
        if(location.pathname.includes('Playground')){
            location.reload();
        }
    }
    function clearAllCookie() {
        var keys = document.cookie.match(/[^ =;]+(?=\=)/g);
        if(keys) {
            for(var i = keys.length; i--;)
                document.cookie = keys[i] + '=0;expires=' + new Date(0).toUTCString()
        }
    }
    function onLogout(){
        logOut().then((res)=>{
            localStorage.setItem('username','');
            clearAllCookie();
            message.success(t('exitSuccessfully'))
            history.push('/login');
        })
    }
    const menu = (
        <Menu>
            <Menu.Item onClick={onLogout}>
                <LogoutOutlined style={{marginRight: 8}}/>
                {t('signOut')}
            </Menu.Item>
        </Menu>
    );
    return (
        <Layout>
            <Header style={{position: 'fixed', zIndex: 1, width: '100%'}}>
                <div className={styles['logo']} onClick={()=>{history.replace('/home');setCurrent('')}}></div>
                <span className='userSet'>
                    <Dropdown overlay={menu}>
                        <span className="ant-dropdown-link">
                            {/* <img alt="" className='avatar' src=''/> */}
                            {localStorage.getItem('username')} <CaretDownOutlined/>
                        </span>
                    </Dropdown>
                </span>
                <Menu theme="light" onClick={handleClick} selectedKeys={[current]} mode="horizontal">
                    {routes?.routes[1]?.routes?.map(item => {
                        if (item.path !== '/login'&&item.path !== '/home') {
                            return (
                                <Menu.Item key={item.path}>
                                    {item.title}
                                </Menu.Item>
                            );
                        }
                    })}
                </Menu>
            </Header>

            <Content className="site-layout" style={{marginTop: 64}}>
                <div className="site-layout-background" style={{minHeight: 380}}>
                    {renderRoutes(route)}
                </div>
            </Content>

            {/* <Footer style={{textAlign: 'center'}}>xxx</Footer> */}
        </Layout>

    );
}

export default Layouts;/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
