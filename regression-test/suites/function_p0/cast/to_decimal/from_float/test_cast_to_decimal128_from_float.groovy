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


suite("test_cast_to_decimal128_from_float") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "drop table if exists test_cast_to_decimal128v3_38_0_from_float32;"
    sql "create table test_cast_to_decimal128v3_38_0_from_float32(f1 int, f2 float) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_0_from_float32 values (0, "0.04"),(1, "1.04"),(2, "9.04"),(3, "1e+37"),(4, "9e+37"),(5, "9e+37"),(6, "0.05"),(7, "1.05"),(8, "9.05"),(9, "1e+37"),
      (10, "9e+37"),(11, "9e+37"),(12, "-0.04"),(13, "-1.04"),(14, "-9.04"),(15, "-1e+37"),(16, "-9e+37"),(17, "-9e+37"),(18, "-0.05"),(19, "-1.05"),
      (20, "-9.05"),(21, "-1e+37"),(22, "-9e+37"),(23, "-9e+37");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128v3_38_0_from_float32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as decimalv3(38, 0)) from test_cast_to_decimal128v3_38_0_from_float32 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_19_from_float32;"
    sql "create table test_cast_to_decimal128v3_38_19_from_float32(f1 int, f2 float) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_19_from_float32 values (24, "4e-20"),(25, "1.4e-19"),(26, "9.4e-19"),(27, "0.1"),(28, "0.9"),(29, "0.9"),(30, "1"),(31, "1"),(32, "1"),(33, "1"),
      (34, "1"),(35, "1.1"),(36, "1.9"),(37, "1.9"),(38, "2"),(39, "2"),(40, "9"),(41, "9"),(42, "9"),(43, "9.1"),
      (44, "9.9"),(45, "9.9"),(46, "10"),(47, "10"),(48, "1e+18"),(49, "1e+18"),(50, "1e+18"),(51, "1e+18"),(52, "1e+18"),(53, "1e+18"),
      (54, "1e+18"),(55, "1e+18"),(56, "9e+18"),(57, "9e+18"),(58, "9e+18"),(59, "9e+18"),(60, "9e+18"),(61, "9e+18"),(62, "9e+18"),(63, "9e+18"),
      (64, "9e+18"),(65, "9e+18"),(66, "9e+18"),(67, "9e+18"),(68, "9e+18"),(69, "9e+18"),(70, "9e+18"),(71, "9e+18"),(72, "5e-20"),(73, "1.5e-19"),
      (74, "9.5e-19"),(75, "0.1"),(76, "0.9"),(77, "0.9"),(78, "1"),(79, "1"),(80, "1"),(81, "1"),(82, "1"),(83, "1.1"),
      (84, "1.9"),(85, "1.9"),(86, "2"),(87, "2"),(88, "9"),(89, "9"),(90, "9"),(91, "9.1"),(92, "9.9"),(93, "9.9"),
      (94, "10"),(95, "10"),(96, "1e+18"),(97, "1e+18"),(98, "1e+18"),(99, "1e+18"),(100, "1e+18"),(101, "1e+18"),(102, "1e+18"),(103, "1e+18"),
      (104, "9e+18"),(105, "9e+18"),(106, "9e+18"),(107, "9e+18"),(108, "9e+18"),(109, "9e+18"),(110, "9e+18"),(111, "9e+18"),(112, "9e+18"),(113, "9e+18"),
      (114, "9e+18"),(115, "9e+18"),(116, "9e+18"),(117, "9e+18"),(118, "9e+18"),(119, "9e+18"),(120, "-4e-20"),(121, "-1.4e-19"),(122, "-9.4e-19"),(123, "-0.1"),
      (124, "-0.9"),(125, "-0.9"),(126, "-1"),(127, "-1"),(128, "-1"),(129, "-1"),(130, "-1"),(131, "-1.1"),(132, "-1.9"),(133, "-1.9"),
      (134, "-2"),(135, "-2"),(136, "-9"),(137, "-9"),(138, "-9"),(139, "-9.1"),(140, "-9.9"),(141, "-9.9"),(142, "-10"),(143, "-10"),
      (144, "-1e+18"),(145, "-1e+18"),(146, "-1e+18"),(147, "-1e+18"),(148, "-1e+18"),(149, "-1e+18"),(150, "-1e+18"),(151, "-1e+18"),(152, "-9e+18"),(153, "-9e+18"),
      (154, "-9e+18"),(155, "-9e+18"),(156, "-9e+18"),(157, "-9e+18"),(158, "-9e+18"),(159, "-9e+18"),(160, "-9e+18"),(161, "-9e+18"),(162, "-9e+18"),(163, "-9e+18"),
      (164, "-9e+18"),(165, "-9e+18"),(166, "-9e+18"),(167, "-9e+18"),(168, "-5e-20"),(169, "-1.5e-19"),(170, "-9.5e-19"),(171, "-0.1"),(172, "-0.9"),(173, "-0.9"),
      (174, "-1"),(175, "-1"),(176, "-1"),(177, "-1"),(178, "-1"),(179, "-1.1"),(180, "-1.9"),(181, "-1.9"),(182, "-2"),(183, "-2"),
      (184, "-9"),(185, "-9"),(186, "-9"),(187, "-9.1"),(188, "-9.9"),(189, "-9.9"),(190, "-10"),(191, "-10"),(192, "-1e+18"),(193, "-1e+18"),
      (194, "-1e+18"),(195, "-1e+18"),(196, "-1e+18"),(197, "-1e+18"),(198, "-1e+18"),(199, "-1e+18"),(200, "-9e+18"),(201, "-9e+18"),(202, "-9e+18"),(203, "-9e+18"),
      (204, "-9e+18"),(205, "-9e+18"),(206, "-9e+18"),(207, "-9e+18"),(208, "-9e+18"),(209, "-9e+18"),(210, "-9e+18"),(211, "-9e+18"),(212, "-9e+18"),(213, "-9e+18"),
      (214, "-9e+18"),(215, "-9e+18");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128v3_38_19_from_float32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as decimalv3(38, 19)) from test_cast_to_decimal128v3_38_19_from_float32 order by 1;'

    sql "drop table if exists test_cast_to_decimal128v3_38_37_from_float32;"
    sql "create table test_cast_to_decimal128v3_38_37_from_float32(f1 int, f2 float) properties('replication_num'='1');"
    sql """insert into test_cast_to_decimal128v3_38_37_from_float32 values (216, "4e-38"),(217, "1.4e-37"),(218, "9.4e-37"),(219, "0.1"),(220, "0.9"),(221, "0.9"),(222, "1"),(223, "1"),(224, "1"),(225, "1"),
      (226, "1"),(227, "1.1"),(228, "1.9"),(229, "1.9"),(230, "2"),(231, "2"),(232, "8"),(233, "8"),(234, "8"),(235, "8.1"),
      (236, "8.9"),(237, "8.9"),(238, "9"),(239, "9"),(240, "9"),(241, "9"),(242, "9"),(243, "9.1"),(244, "9.9"),(245, "9.9"),
      (246, "5e-38"),(247, "1.5e-37"),(248, "9.5e-37"),(249, "0.1"),(250, "0.9"),(251, "0.9"),(252, "1"),(253, "1"),(254, "1"),(255, "1"),
      (256, "1"),(257, "1.1"),(258, "1.9"),(259, "1.9"),(260, "2"),(261, "2"),(262, "8"),(263, "8"),(264, "8"),(265, "8.1"),
      (266, "8.9"),(267, "8.9"),(268, "9"),(269, "9"),(270, "9"),(271, "9"),(272, "9"),(273, "9.1"),(274, "9.9"),(275, "9.9"),
      (276, "-4e-38"),(277, "-1.4e-37"),(278, "-9.4e-37"),(279, "-0.1"),(280, "-0.9"),(281, "-0.9"),(282, "-1"),(283, "-1"),(284, "-1"),(285, "-1"),
      (286, "-1"),(287, "-1.1"),(288, "-1.9"),(289, "-1.9"),(290, "-2"),(291, "-2"),(292, "-8"),(293, "-8"),(294, "-8"),(295, "-8.1"),
      (296, "-8.9"),(297, "-8.9"),(298, "-9"),(299, "-9"),(300, "-9"),(301, "-9"),(302, "-9"),(303, "-9.1"),(304, "-9.9"),(305, "-9.9"),
      (306, "-5e-38"),(307, "-1.5e-37"),(308, "-9.5e-37"),(309, "-0.1"),(310, "-0.9"),(311, "-0.9"),(312, "-1"),(313, "-1"),(314, "-1"),(315, "-1"),
      (316, "-1"),(317, "-1.1"),(318, "-1.9"),(319, "-1.9"),(320, "-2"),(321, "-2"),(322, "-8"),(323, "-8"),(324, "-8"),(325, "-8.1"),
      (326, "-8.9"),(327, "-8.9"),(328, "-9"),(329, "-9"),(330, "-9"),(331, "-9"),(332, "-9"),(333, "-9.1"),(334, "-9.9"),(335, "-9.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_float32 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as decimalv3(38, 37)) from test_cast_to_decimal128v3_38_37_from_float32 order by 1;'

}