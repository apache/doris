package org.apache.doris.stack.exception;

/**
 * Copyright (C) 2020 Baidu, Inc. All Rights Reserved.
 *
 * @Author: songchuanyuan@baidu.com
 * @Description：
 * @Date: 2021/10/18
 */
public class DorisUserNoPermissionException extends Exception {

    public static final String MESSAGE = "用户不具备集群Admin权限，请更换其他具备权限用户";

    public DorisUserNoPermissionException() {
        super(MESSAGE);
    }
}
