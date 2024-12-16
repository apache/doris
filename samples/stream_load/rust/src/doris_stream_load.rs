// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, AUTHORIZATION, EXPECT};
use base64::encode;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let doris_host = "127.0.0.1";
    let doris_port = "8030";
    let doris_db = "db0";
    let doris_table = "t_user";
    let doris_user = "root";
    let doris_password = "";

    let url = format!(
        "http://{}:{}/api/{}/{}/_stream_load",
        doris_host, doris_port, doris_db, doris_table
    );

    let auth = format!("{}:{}", doris_user, doris_password);
    let encoded_auth = encode(auth);
    let auth_header_value = format!("Basic {}", encoded_auth);

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=UTF-8"));
    headers.insert("format", HeaderValue::from_static("csv"));
    headers.insert("column_separator", HeaderValue::from_static(","));
    headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth_header_value)?);
    headers.insert(EXPECT, HeaderValue::from_static("100-continue"));

    let client = reqwest::Client::new();
    let response = client
        .put(url)
        .headers(headers)
        .body("1,Tom\n2,Jelly")
        .send()
        .await?;

    if response.status().is_success() {
        let resp_text = response.text().await?;
        println!("Response: {}", resp_text);
    } else {
        println!("Failed to load stream: {}", response.status());
    }

    Ok(())
}