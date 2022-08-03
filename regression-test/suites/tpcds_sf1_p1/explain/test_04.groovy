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

suite("test_regression_test_tpcds_sf1_p1_q04", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  year_total AS (
		   SELECT
		     c_customer_id customer_id
		   , c_first_name customer_first_name
		   , c_last_name customer_last_name
		   , c_preferred_cust_flag customer_preferred_cust_flag
		   , c_birth_country customer_birth_country
		   , c_login customer_login
		   , c_email_address customer_email_address
		   , d_year dyear
		   , sum(((((ss_ext_list_price - ss_ext_wholesale_cost) - ss_ext_discount_amt) + ss_ext_sales_price) / 2)) year_total
		   , 's' sale_type
		   FROM
		     customer
		   , store_sales
		   , date_dim
		   WHERE (c_customer_sk = ss_customer_sk)
		      AND (ss_sold_date_sk = d_date_sk)
		   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
		UNION ALL    SELECT
		     c_customer_id customer_id
		   , c_first_name customer_first_name
		   , c_last_name customer_last_name
		   , c_preferred_cust_flag customer_preferred_cust_flag
		   , c_birth_country customer_birth_country
		   , c_login customer_login
		   , c_email_address customer_email_address
		   , d_year dyear
		   , sum(((((cs_ext_list_price - cs_ext_wholesale_cost) - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) year_total
		   , 'c' sale_type
		   FROM
		     customer
		   , catalog_sales
		   , date_dim
		   WHERE (c_customer_sk = cs_bill_customer_sk)
		      AND (cs_sold_date_sk = d_date_sk)
		   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
		UNION ALL    SELECT
		     c_customer_id customer_id
		   , c_first_name customer_first_name
		   , c_last_name customer_last_name
		   , c_preferred_cust_flag customer_preferred_cust_flag
		   , c_birth_country customer_birth_country
		   , c_login customer_login
		   , c_email_address customer_email_address
		   , d_year dyear
		   , sum(((((ws_ext_list_price - ws_ext_wholesale_cost) - ws_ext_discount_amt) + ws_ext_sales_price) / 2)) year_total
		   , 'w' sale_type
		   FROM
		     customer
		   , web_sales
		   , date_dim
		   WHERE (c_customer_sk = ws_bill_customer_sk)
		      AND (ws_sold_date_sk = d_date_sk)
		   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
		)
		SELECT
		  t_s_secyear.customer_id
		, t_s_secyear.customer_first_name
		, t_s_secyear.customer_last_name
		, t_s_secyear.customer_preferred_cust_flag
		FROM
		  year_total t_s_firstyear
		, year_total t_s_secyear
		, year_total t_c_firstyear
		, year_total t_c_secyear
		, year_total t_w_firstyear
		, year_total t_w_secyear
		WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
		   AND (t_s_firstyear.customer_id = t_c_secyear.customer_id)
		   AND (t_s_firstyear.customer_id = t_c_firstyear.customer_id)
		   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
		   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
		   AND (t_s_firstyear.sale_type = 's')
		   AND (t_c_firstyear.sale_type = 'c')
		   AND (t_w_firstyear.sale_type = 'w')
		   AND (t_s_secyear.sale_type = 's')
		   AND (t_c_secyear.sale_type = 'c')
		   AND (t_w_secyear.sale_type = 'w')
		   AND (t_s_firstyear.dyear = 2001)
		   AND (t_s_secyear.dyear = (2001 + 1))
		   AND (t_c_firstyear.dyear = 2001)
		   AND (t_c_secyear.dyear = (2001 + 1))
		   AND (t_w_firstyear.dyear = 2001)
		   AND (t_w_secyear.dyear = (2001 + 1))
		   AND (t_s_firstyear.year_total > 0)
		   AND (t_c_firstyear.year_total > 0)
		   AND (t_w_firstyear.year_total > 0)
		   AND ((CASE WHEN (t_c_firstyear.year_total > 0) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE null END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE null END))
		   AND ((CASE WHEN (t_c_firstyear.year_total > 0) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE null END) > (CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE null END))
		ORDER BY t_s_secyear.customer_id ASC, t_s_secyear.customer_first_name ASC, t_s_secyear.customer_last_name ASC, t_s_secyear.customer_preferred_cust_flag ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 570> `t_s_secyear`.`customer_id` ASC, <slot 571> `t_s_secyear`.`customer_first_name` ASC, <slot 572> `t_s_secyear`.`customer_last_name` ASC, <slot 573> `t_s_secyear`.`customer_preferred_cust_flag` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 769> = <slot 550> <slot 491> `c_customer_id` <slot 516> `c_customer_id` <slot 541> `c_customer_id`)\n" + 
				"  |  other predicates: ((CASE WHEN (<slot 1068> > 0) THEN (<slot 1067> / <slot 1068>) ELSE NULL END) > (CASE WHEN (<slot 1069> > 0) THEN (<slot 1072> / <slot 1069>) ELSE NULL END))") && 
		explainStr.contains("other predicates: ((CASE WHEN (<slot 1068> > 0) THEN (<slot 1067> / <slot 1068>) ELSE NULL END) > (CASE WHEN (<slot 1069> > 0) THEN (<slot 1072> / <slot 1069>) ELSE NULL END))") && 
		explainStr.contains("vec output tuple id: 101") && 
		explainStr.contains("output slot ids: 826 827 828 829 \n" + 
				"  |  hash output slot ids: 786 773 774 790 775 776 782 558 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 720> = <slot 455> <slot 396> `c_customer_id` <slot 421> `c_customer_id` <slot 446> `c_customer_id`)") && 
		explainStr.contains("vec output tuple id: 98") && 
		explainStr.contains("output slot ids: 769 773 774 775 776 782 786 790 \n" + 
				"  |  hash output slot ids: 720 737 724 725 726 727 733 463 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 675> = <slot 360> <slot 301> `c_customer_id` <slot 326> `c_customer_id` <slot 351> `c_customer_id`)\n" + 
				"  |  other predicates: ((CASE WHEN (<slot 993> > 0) THEN (<slot 990> / <slot 993>) ELSE NULL END) > (CASE WHEN (<slot 984> > 0) THEN (<slot 989> / <slot 984>) ELSE NULL END))") && 
		explainStr.contains("other predicates: ((CASE WHEN (<slot 993> > 0) THEN (<slot 990> / <slot 993>) ELSE NULL END) > (CASE WHEN (<slot 984> > 0) THEN (<slot 989> / <slot 984>) ELSE NULL END))") && 
		explainStr.contains("vec output tuple id: 95") && 
		explainStr.contains("output slot ids: 720 724 725 726 727 733 737 \n" + 
				"  |  hash output slot ids: 688 368 675 677 679 680 681 682 684 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 634> = <slot 265> <slot 206> `c_customer_id` <slot 231> `c_customer_id` <slot 256> `c_customer_id`)") && 
		explainStr.contains("vec output tuple id: 92") && 
		explainStr.contains("output slot ids: 675 677 679 680 681 682 684 688 \n" + 
				"  |  hash output slot ids: 640 641 273 643 634 636 638 639 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (<slot 75> <slot 16> `c_customer_id` <slot 41> `c_customer_id` <slot 66> `c_customer_id` = <slot 170> <slot 111> `c_customer_id` <slot 136> `c_customer_id` <slot 161> `c_customer_id`)") && 
		explainStr.contains("vec output tuple id: 89") && 
		explainStr.contains("output slot ids: 634 636 638 639 640 641 643 \n" + 
				"  |  hash output slot ids: 178 83 170 75 171 172 173 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 549> sum(((((`ws_ext_list_price` - `ws_ext_wholesale_cost`) - `ws_ext_discount_amt`) + `ws_ext_sales_price`) / 2)))\n" + 
				"  |  group by: <slot 541> `c_customer_id`, <slot 542> `c_first_name`, <slot 543> `c_last_name`, <slot 544> `c_preferred_cust_flag`, <slot 545> `c_birth_country`, <slot 546> `c_login`, <slot 547> `c_email_address`, <slot 548> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(((((<slot 806> - <slot 807>) - <slot 808>) + <slot 809>) / 2))\n" + 
				"  |  group by: <slot 812>, <slot 813>, <slot 814>, <slot 815>, <slot 816>, <slot 817>, <slot 818>, <slot 820>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 797> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 100") && 
		explainStr.contains("output slot ids: 806 807 808 809 812 813 814 815 816 817 818 820 \n" + 
				"  |  hash output slot ids: 800 801 802 803 804 532 792 793 794 795 798 799 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_bill_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 99") && 
		explainStr.contains("output slot ids: 792 793 794 795 797 798 799 800 801 802 803 804 \n" + 
				"  |  hash output slot ids: 528 529 530 531 533 534 535 536 539 525 526 527 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF005[in_or_bloom] -> `ws_bill_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2002)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 454> sum(((((`ws_ext_list_price` - `ws_ext_wholesale_cost`) - `ws_ext_discount_amt`) + `ws_ext_sales_price`) / 2)))\n" + 
				"  |  group by: <slot 446> `c_customer_id`, <slot 447> `c_first_name`, <slot 448> `c_last_name`, <slot 449> `c_preferred_cust_flag`, <slot 450> `c_birth_country`, <slot 451> `c_login`, <slot 452> `c_email_address`, <slot 453> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(((((<slot 753> - <slot 754>) - <slot 755>) + <slot 756>) / 2))\n" + 
				"  |  group by: <slot 759>, <slot 760>, <slot 761>, <slot 762>, <slot 763>, <slot 764>, <slot 765>, <slot 767>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 744> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 97") && 
		explainStr.contains("output slot ids: 753 754 755 756 759 760 761 762 763 764 765 767 \n" + 
				"  |  hash output slot ids: 739 740 741 437 742 745 746 747 748 749 750 751 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_bill_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 96") && 
		explainStr.contains("output slot ids: 739 740 741 742 744 745 746 747 748 749 750 751 \n" + 
				"  |  hash output slot ids: 432 433 434 435 436 438 439 440 441 444 430 431 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `ws_bill_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 334> sum(((((`cs_ext_list_price` - `cs_ext_wholesale_cost`) - `cs_ext_discount_amt`) + `cs_ext_sales_price`) / 2)))\n" + 
				"  |  group by: <slot 326> `c_customer_id`, <slot 327> `c_first_name`, <slot 328> `c_last_name`, <slot 329> `c_preferred_cust_flag`, <slot 330> `c_birth_country`, <slot 331> `c_login`, <slot 332> `c_email_address`, <slot 333> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(((((<slot 704> - <slot 705>) - <slot 706>) + <slot 707>) / 2))\n" + 
				"  |  group by: <slot 710>, <slot 711>, <slot 712>, <slot 713>, <slot 714>, <slot 715>, <slot 716>, <slot 718>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 695> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 94") && 
		explainStr.contains("output slot ids: 704 705 706 707 710 711 712 713 714 715 716 718 \n" + 
				"  |  hash output slot ids: 690 691 692 693 696 697 698 699 700 701 317 702 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_bill_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 93") && 
		explainStr.contains("output slot ids: 690 691 692 693 695 696 697 698 699 700 701 702 \n" + 
				"  |  hash output slot ids: 320 321 324 310 311 312 313 314 315 316 318 319 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `cs_bill_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 239> sum(((((`cs_ext_list_price` - `cs_ext_wholesale_cost`) - `cs_ext_discount_amt`) + `cs_ext_sales_price`) / 2)))\n" + 
				"  |  group by: <slot 231> `c_customer_id`, <slot 232> `c_first_name`, <slot 233> `c_last_name`, <slot 234> `c_preferred_cust_flag`, <slot 235> `c_birth_country`, <slot 236> `c_login`, <slot 237> `c_email_address`, <slot 238> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(((((<slot 659> - <slot 660>) - <slot 661>) + <slot 662>) / 2))\n" + 
				"  |  group by: <slot 665>, <slot 666>, <slot 667>, <slot 668>, <slot 669>, <slot 670>, <slot 671>, <slot 673>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 650> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 91") && 
		explainStr.contains("output slot ids: 659 660 661 662 665 666 667 668 669 670 671 673 \n" + 
				"  |  hash output slot ids: 656 657 645 646 647 648 651 652 653 654 222 655 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_bill_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 90") && 
		explainStr.contains("output slot ids: 645 646 647 648 650 651 652 653 654 655 656 657 \n" + 
				"  |  hash output slot ids: 224 225 226 229 215 216 217 218 219 220 221 223 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `cs_bill_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2002)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 119> sum(((((`ss_ext_list_price` - `ss_ext_wholesale_cost`) - `ss_ext_discount_amt`) + `ss_ext_sales_price`) / 2)))\n" + 
				"  |  group by: <slot 111> `c_customer_id`, <slot 112> `c_first_name`, <slot 113> `c_last_name`, <slot 114> `c_preferred_cust_flag`, <slot 115> `c_birth_country`, <slot 116> `c_login`, <slot 117> `c_email_address`, <slot 118> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(((((<slot 618> - <slot 619>) - <slot 620>) + <slot 621>) / 2))\n" + 
				"  |  group by: <slot 624>, <slot 625>, <slot 626>, <slot 627>, <slot 628>, <slot 629>, <slot 630>, <slot 632>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 609> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 88") && 
		explainStr.contains("output slot ids: 618 619 620 621 624 625 626 627 628 629 630 632 \n" + 
				"  |  hash output slot ids: 610 611 612 613 614 102 615 616 604 605 606 607 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 87") && 
		explainStr.contains("output slot ids: 604 605 606 607 609 610 611 612 613 614 615 616 \n" + 
				"  |  hash output slot ids: 96 97 98 99 100 101 103 104 105 106 109 95 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2002)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 24> sum(((((`ss_ext_list_price` - `ss_ext_wholesale_cost`) - `ss_ext_discount_amt`) + `ss_ext_sales_price`) / 2)))\n" + 
				"  |  group by: <slot 16> `c_customer_id`, <slot 17> `c_first_name`, <slot 18> `c_last_name`, <slot 19> `c_preferred_cust_flag`, <slot 20> `c_birth_country`, <slot 21> `c_login`, <slot 22> `c_email_address`, <slot 23> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(((((<slot 588> - <slot 589>) - <slot 590>) + <slot 591>) / 2))\n" + 
				"  |  group by: <slot 594>, <slot 595>, <slot 596>, <slot 597>, <slot 598>, <slot 599>, <slot 600>, <slot 602>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 579> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 86") && 
		explainStr.contains("output slot ids: 588 589 590 591 594 595 596 597 598 599 600 602 \n" + 
				"  |  hash output slot ids: 576 577 580 581 582 583 7 584 585 586 574 575 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 85") && 
		explainStr.contains("output slot ids: 574 575 576 577 579 580 581 582 583 584 585 586 \n" + 
				"  |  hash output slot ids: 0 1 2 3 4 5 6 8 9 10 11 14 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") 
            
        }
    }
}