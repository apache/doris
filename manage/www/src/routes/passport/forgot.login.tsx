/** @format */
import Link from 'antd/lib/typography/Link';
import React, { useState } from 'react';
import styles from './index.module.less';

function ForgotLogin(props: any) {
    return (
        <div className={styles['not-found']}>
            <div className={styles['input-gird']}>
                <h3>Please contact an administrator to have them reset your password</h3>
                <br />
                <Link style={{ fontSize: '14px' }} onClick={() => props.history.push(`/login`)}>
                    Back to login
                </Link>
            </div>
        </div>
    );
}

export default ForgotLogin;
