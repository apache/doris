-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

SELECT f.*, d.*, t.*, i.*, c.*, cd.*, hd.*, ca.*, p.*, sm.*, w.*, web.*, wp.*
FROM web_sales f
LEFT JOIN date_dim d ON f.ws_sold_date_sk = d.d_date_sk
LEFT JOIN time_dim t ON f.ws_sold_time_sk = t.t_time_sk
LEFT JOIN item i ON f.ws_item_sk = i.i_item_sk
LEFT JOIN customer c ON f.ws_bill_customer_sk = c.c_customer_sk
LEFT JOIN customer_demographics cd ON f.ws_bill_cdemo_sk = cd.cd_demo_sk
LEFT JOIN household_demographics hd ON f.ws_bill_hdemo_sk = hd.hd_demo_sk
LEFT JOIN customer_address ca ON f.ws_bill_addr_sk = ca.ca_address_sk
LEFT JOIN promotion p ON f.ws_promo_sk = p.p_promo_sk
LEFT JOIN ship_mode sm ON f.ws_ship_mode_sk = sm.sm_ship_mode_sk
LEFT JOIN warehouse w ON f.ws_warehouse_sk = w.w_warehouse_sk
LEFT JOIN web_site web ON f.ws_web_site_sk = web.web_site_sk
LEFT JOIN web_page wp ON f.ws_web_page_sk = wp.wp_web_page_sk
