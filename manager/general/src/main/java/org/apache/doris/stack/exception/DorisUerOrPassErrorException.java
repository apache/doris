package org.apache.doris.stack.exception;

/**
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 *
 * @Author: songchuanyuan@baidu.com
 * @Description：
 * @Date: 2021/10/18
 */
public class DorisUerOrPassErrorException extends Exception {

    public static final String MESSAGE = "用户名/密码错误，请检查后重新输入";

    public DorisUerOrPassErrorException() {
        super(MESSAGE);
    }
}
