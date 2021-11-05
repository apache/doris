package org.apache.doris.stack.exception;

/**
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 *
 * @Author: songchuanyuan@baidu.com
 * @Description：
 * @Date: 2021/10/18
 */
public class DorisHttpPortErrorException extends Exception {

    public static final String MESSAGE = "HTTP端口无法访问，请检查后重新输入";

    public DorisHttpPortErrorException() {
        super(MESSAGE);
    }
}
