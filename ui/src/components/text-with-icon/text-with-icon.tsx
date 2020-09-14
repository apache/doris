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
 
import { Row } from "antd";
import React, { ComponentProps } from "react";
import { FlatBtn } from "Components/flatbtn";

interface TextWithIconProps extends ComponentProps<any> {
    iconPosition?: "left" | "right";
    colorWithIcon?: boolean,
    icon: any;
    isButton?: boolean;
    text: string;
    color?: string;
}

export function TextWithIcon(props: TextWithIconProps) {
    const { iconPosition = "left", colorWithIcon = true,  icon, text, color = "#1890ff", isButton = false, style, ...rest } = props;
    const children = props.children ? props.children : text;
    const styles = { alignItems: "center", ...style };
    if (colorWithIcon) {
        styles.color = color;
    }
    return (
        <Row
            justify="start"
            align="middle"
            style={styles}
            {...rest}
        >
            {iconPosition === "left" && <span style={{marginRight: 6}}>{icon}</span>}
            {isButton ? (
                <FlatBtn style={{color}} onClick={() => props.onClick(true)}>{text}</FlatBtn>
            ) : (
                <span>{children}</span>
            )}
            {iconPosition === "right" && <span style={{marginLeft: 6}}>{icon}</span>}
        </Row>
    );
 
}
