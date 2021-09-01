import React, { useState } from 'react';
import { Form, Input, Button, Radio, Checkbox, message } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import styles from './index.module.less';
import classNames from 'classnames';
import Link from 'antd/lib/typography/Link';
import { DEFAULT_NAMESPACE_ID, VERSION } from 'src/config';
import { PassportAPI } from './passport.api';
import { config } from 'process';
import { METE_INDEX_URL } from '@src/common/common.data';
type RequiredMark = boolean | 'optional';

const FormLayoutDemo = (props: any) => {
    const [form] = Form.useForm();
    const [requiredMark, setRequiredMarkType] = useState<RequiredMark>('optional');

    const onRequiredTypeChange = ({ requiredMarkValue }: { requiredMarkValue: RequiredMark }) => {
        setRequiredMarkType(requiredMarkValue);
    };
    function loginClick(_value: any) {
        PassportAPI.SessionLogin(_value).then(res => {
            if (res.code === 0) {
                window.location.href = METE_INDEX_URL;
            } else {
                message.warn(res.msg);
            }
        });
    }

    return (
        <div className={styles['not-found']}>
            <div className={styles['input-gird']}>
                <Form
                    form={form}
                    layout="vertical"
                    initialValues={{ requiredMarkValue: requiredMark }}
                    onValuesChange={onRequiredTypeChange}
                    requiredMark={requiredMark}
                    onFinish={loginClick}
                >
                    <h2 style={{ textAlign: 'center' }}>登录</h2>
                    <Form.Item label="邮箱" required name="username">
                        <Input placeholder="input placeholder" />
                    </Form.Item>
                    <Form.Item label="密码" required name="password">
                        <Input.Password
                            placeholder="input placeholder"
                            style={{ width: '100%', borderRadius: 4, padding: '0.75em' }}
                        />
                    </Form.Item>
                    <Form.Item name="remember" valuePropName="checked">
                        <Checkbox>Remember me</Checkbox>
                    </Form.Item>
                    <Form.Item>
                        <Button type="primary" style={{ width: '100%', borderRadius: 4, height: 45 }} htmlType="submit">
                            Sign in
                        </Button>
                    </Form.Item>
                    <Form.Item style={{ textAlign: 'center' }}>
                        <Link
                            style={{ fontSize: '14px', color: '#C7CFD4' }}
                            onClick={() => props.history.push(`/forgot`)}
                        >
                            I seem to have forgotten my password
                        </Link>
                    </Form.Item>
                </Form>
            </div>
        </div>
    );
};
export default FormLayoutDemo;
