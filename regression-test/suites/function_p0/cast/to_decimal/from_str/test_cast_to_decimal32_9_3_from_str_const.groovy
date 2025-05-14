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


suite("test_cast_to_decimal32_9_3_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_3618 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 3));"""
    qt_sql_3618_strict "${const_sql_3618}"
    testFoldConst("${const_sql_3618}")
    def const_sql_3619 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 3));"""
    qt_sql_3619_strict "${const_sql_3619}"
    testFoldConst("${const_sql_3619}")
    def const_sql_3620 = """select cast("0" as decimalv3(9, 3));"""
    qt_sql_3620_strict "${const_sql_3620}"
    testFoldConst("${const_sql_3620}")
    def const_sql_3621 = """select cast("1" as decimalv3(9, 3));"""
    qt_sql_3621_strict "${const_sql_3621}"
    testFoldConst("${const_sql_3621}")
    def const_sql_3622 = """select cast("9" as decimalv3(9, 3));"""
    qt_sql_3622_strict "${const_sql_3622}"
    testFoldConst("${const_sql_3622}")
    def const_sql_3623 = """select cast("99999" as decimalv3(9, 3));"""
    qt_sql_3623_strict "${const_sql_3623}"
    testFoldConst("${const_sql_3623}")
    def const_sql_3624 = """select cast("900000" as decimalv3(9, 3));"""
    qt_sql_3624_strict "${const_sql_3624}"
    testFoldConst("${const_sql_3624}")
    def const_sql_3625 = """select cast("900001" as decimalv3(9, 3));"""
    qt_sql_3625_strict "${const_sql_3625}"
    testFoldConst("${const_sql_3625}")
    def const_sql_3626 = """select cast("999998" as decimalv3(9, 3));"""
    qt_sql_3626_strict "${const_sql_3626}"
    testFoldConst("${const_sql_3626}")
    def const_sql_3627 = """select cast("999999" as decimalv3(9, 3));"""
    qt_sql_3627_strict "${const_sql_3627}"
    testFoldConst("${const_sql_3627}")
    def const_sql_3628 = """select cast("0." as decimalv3(9, 3));"""
    qt_sql_3628_strict "${const_sql_3628}"
    testFoldConst("${const_sql_3628}")
    def const_sql_3629 = """select cast("1." as decimalv3(9, 3));"""
    qt_sql_3629_strict "${const_sql_3629}"
    testFoldConst("${const_sql_3629}")
    def const_sql_3630 = """select cast("9." as decimalv3(9, 3));"""
    qt_sql_3630_strict "${const_sql_3630}"
    testFoldConst("${const_sql_3630}")
    def const_sql_3631 = """select cast("99999." as decimalv3(9, 3));"""
    qt_sql_3631_strict "${const_sql_3631}"
    testFoldConst("${const_sql_3631}")
    def const_sql_3632 = """select cast("900000." as decimalv3(9, 3));"""
    qt_sql_3632_strict "${const_sql_3632}"
    testFoldConst("${const_sql_3632}")
    def const_sql_3633 = """select cast("900001." as decimalv3(9, 3));"""
    qt_sql_3633_strict "${const_sql_3633}"
    testFoldConst("${const_sql_3633}")
    def const_sql_3634 = """select cast("999998." as decimalv3(9, 3));"""
    qt_sql_3634_strict "${const_sql_3634}"
    testFoldConst("${const_sql_3634}")
    def const_sql_3635 = """select cast("999999." as decimalv3(9, 3));"""
    qt_sql_3635_strict "${const_sql_3635}"
    testFoldConst("${const_sql_3635}")
    def const_sql_3636 = """select cast("-0" as decimalv3(9, 3));"""
    qt_sql_3636_strict "${const_sql_3636}"
    testFoldConst("${const_sql_3636}")
    def const_sql_3637 = """select cast("-1" as decimalv3(9, 3));"""
    qt_sql_3637_strict "${const_sql_3637}"
    testFoldConst("${const_sql_3637}")
    def const_sql_3638 = """select cast("-9" as decimalv3(9, 3));"""
    qt_sql_3638_strict "${const_sql_3638}"
    testFoldConst("${const_sql_3638}")
    def const_sql_3639 = """select cast("-99999" as decimalv3(9, 3));"""
    qt_sql_3639_strict "${const_sql_3639}"
    testFoldConst("${const_sql_3639}")
    def const_sql_3640 = """select cast("-900000" as decimalv3(9, 3));"""
    qt_sql_3640_strict "${const_sql_3640}"
    testFoldConst("${const_sql_3640}")
    def const_sql_3641 = """select cast("-900001" as decimalv3(9, 3));"""
    qt_sql_3641_strict "${const_sql_3641}"
    testFoldConst("${const_sql_3641}")
    def const_sql_3642 = """select cast("-999998" as decimalv3(9, 3));"""
    qt_sql_3642_strict "${const_sql_3642}"
    testFoldConst("${const_sql_3642}")
    def const_sql_3643 = """select cast("-999999" as decimalv3(9, 3));"""
    qt_sql_3643_strict "${const_sql_3643}"
    testFoldConst("${const_sql_3643}")
    def const_sql_3644 = """select cast("-0." as decimalv3(9, 3));"""
    qt_sql_3644_strict "${const_sql_3644}"
    testFoldConst("${const_sql_3644}")
    def const_sql_3645 = """select cast("-1." as decimalv3(9, 3));"""
    qt_sql_3645_strict "${const_sql_3645}"
    testFoldConst("${const_sql_3645}")
    def const_sql_3646 = """select cast("-9." as decimalv3(9, 3));"""
    qt_sql_3646_strict "${const_sql_3646}"
    testFoldConst("${const_sql_3646}")
    def const_sql_3647 = """select cast("-99999." as decimalv3(9, 3));"""
    qt_sql_3647_strict "${const_sql_3647}"
    testFoldConst("${const_sql_3647}")
    def const_sql_3648 = """select cast("-900000." as decimalv3(9, 3));"""
    qt_sql_3648_strict "${const_sql_3648}"
    testFoldConst("${const_sql_3648}")
    def const_sql_3649 = """select cast("-900001." as decimalv3(9, 3));"""
    qt_sql_3649_strict "${const_sql_3649}"
    testFoldConst("${const_sql_3649}")
    def const_sql_3650 = """select cast("-999998." as decimalv3(9, 3));"""
    qt_sql_3650_strict "${const_sql_3650}"
    testFoldConst("${const_sql_3650}")
    def const_sql_3651 = """select cast("-999999." as decimalv3(9, 3));"""
    qt_sql_3651_strict "${const_sql_3651}"
    testFoldConst("${const_sql_3651}")
    def const_sql_3652 = """select cast(".0004" as decimalv3(9, 3));"""
    qt_sql_3652_strict "${const_sql_3652}"
    testFoldConst("${const_sql_3652}")
    def const_sql_3653 = """select cast(".0014" as decimalv3(9, 3));"""
    qt_sql_3653_strict "${const_sql_3653}"
    testFoldConst("${const_sql_3653}")
    def const_sql_3654 = """select cast(".0094" as decimalv3(9, 3));"""
    qt_sql_3654_strict "${const_sql_3654}"
    testFoldConst("${const_sql_3654}")
    def const_sql_3655 = """select cast(".0994" as decimalv3(9, 3));"""
    qt_sql_3655_strict "${const_sql_3655}"
    testFoldConst("${const_sql_3655}")
    def const_sql_3656 = """select cast(".9004" as decimalv3(9, 3));"""
    qt_sql_3656_strict "${const_sql_3656}"
    testFoldConst("${const_sql_3656}")
    def const_sql_3657 = """select cast(".9014" as decimalv3(9, 3));"""
    qt_sql_3657_strict "${const_sql_3657}"
    testFoldConst("${const_sql_3657}")
    def const_sql_3658 = """select cast(".9984" as decimalv3(9, 3));"""
    qt_sql_3658_strict "${const_sql_3658}"
    testFoldConst("${const_sql_3658}")
    def const_sql_3659 = """select cast(".9994" as decimalv3(9, 3));"""
    qt_sql_3659_strict "${const_sql_3659}"
    testFoldConst("${const_sql_3659}")
    def const_sql_3660 = """select cast(".0005" as decimalv3(9, 3));"""
    qt_sql_3660_strict "${const_sql_3660}"
    testFoldConst("${const_sql_3660}")
    def const_sql_3661 = """select cast(".0015" as decimalv3(9, 3));"""
    qt_sql_3661_strict "${const_sql_3661}"
    testFoldConst("${const_sql_3661}")
    def const_sql_3662 = """select cast(".0095" as decimalv3(9, 3));"""
    qt_sql_3662_strict "${const_sql_3662}"
    testFoldConst("${const_sql_3662}")
    def const_sql_3663 = """select cast(".0995" as decimalv3(9, 3));"""
    qt_sql_3663_strict "${const_sql_3663}"
    testFoldConst("${const_sql_3663}")
    def const_sql_3664 = """select cast(".9005" as decimalv3(9, 3));"""
    qt_sql_3664_strict "${const_sql_3664}"
    testFoldConst("${const_sql_3664}")
    def const_sql_3665 = """select cast(".9015" as decimalv3(9, 3));"""
    qt_sql_3665_strict "${const_sql_3665}"
    testFoldConst("${const_sql_3665}")
    def const_sql_3666 = """select cast(".9985" as decimalv3(9, 3));"""
    qt_sql_3666_strict "${const_sql_3666}"
    testFoldConst("${const_sql_3666}")
    def const_sql_3667 = """select cast(".9994" as decimalv3(9, 3));"""
    qt_sql_3667_strict "${const_sql_3667}"
    testFoldConst("${const_sql_3667}")
    def const_sql_3668 = """select cast("-.0004" as decimalv3(9, 3));"""
    qt_sql_3668_strict "${const_sql_3668}"
    testFoldConst("${const_sql_3668}")
    def const_sql_3669 = """select cast("-.0014" as decimalv3(9, 3));"""
    qt_sql_3669_strict "${const_sql_3669}"
    testFoldConst("${const_sql_3669}")
    def const_sql_3670 = """select cast("-.0094" as decimalv3(9, 3));"""
    qt_sql_3670_strict "${const_sql_3670}"
    testFoldConst("${const_sql_3670}")
    def const_sql_3671 = """select cast("-.0994" as decimalv3(9, 3));"""
    qt_sql_3671_strict "${const_sql_3671}"
    testFoldConst("${const_sql_3671}")
    def const_sql_3672 = """select cast("-.9004" as decimalv3(9, 3));"""
    qt_sql_3672_strict "${const_sql_3672}"
    testFoldConst("${const_sql_3672}")
    def const_sql_3673 = """select cast("-.9014" as decimalv3(9, 3));"""
    qt_sql_3673_strict "${const_sql_3673}"
    testFoldConst("${const_sql_3673}")
    def const_sql_3674 = """select cast("-.9984" as decimalv3(9, 3));"""
    qt_sql_3674_strict "${const_sql_3674}"
    testFoldConst("${const_sql_3674}")
    def const_sql_3675 = """select cast("-.9994" as decimalv3(9, 3));"""
    qt_sql_3675_strict "${const_sql_3675}"
    testFoldConst("${const_sql_3675}")
    def const_sql_3676 = """select cast("-.0005" as decimalv3(9, 3));"""
    qt_sql_3676_strict "${const_sql_3676}"
    testFoldConst("${const_sql_3676}")
    def const_sql_3677 = """select cast("-.0015" as decimalv3(9, 3));"""
    qt_sql_3677_strict "${const_sql_3677}"
    testFoldConst("${const_sql_3677}")
    def const_sql_3678 = """select cast("-.0095" as decimalv3(9, 3));"""
    qt_sql_3678_strict "${const_sql_3678}"
    testFoldConst("${const_sql_3678}")
    def const_sql_3679 = """select cast("-.0995" as decimalv3(9, 3));"""
    qt_sql_3679_strict "${const_sql_3679}"
    testFoldConst("${const_sql_3679}")
    def const_sql_3680 = """select cast("-.9005" as decimalv3(9, 3));"""
    qt_sql_3680_strict "${const_sql_3680}"
    testFoldConst("${const_sql_3680}")
    def const_sql_3681 = """select cast("-.9015" as decimalv3(9, 3));"""
    qt_sql_3681_strict "${const_sql_3681}"
    testFoldConst("${const_sql_3681}")
    def const_sql_3682 = """select cast("-.9985" as decimalv3(9, 3));"""
    qt_sql_3682_strict "${const_sql_3682}"
    testFoldConst("${const_sql_3682}")
    def const_sql_3683 = """select cast("-.9994" as decimalv3(9, 3));"""
    qt_sql_3683_strict "${const_sql_3683}"
    testFoldConst("${const_sql_3683}")
    def const_sql_3684 = """select cast("00004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3684_strict "${const_sql_3684}"
    testFoldConst("${const_sql_3684}")
    def const_sql_3685 = """select cast("00014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3685_strict "${const_sql_3685}"
    testFoldConst("${const_sql_3685}")
    def const_sql_3686 = """select cast("00094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3686_strict "${const_sql_3686}"
    testFoldConst("${const_sql_3686}")
    def const_sql_3687 = """select cast("00994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3687_strict "${const_sql_3687}"
    testFoldConst("${const_sql_3687}")
    def const_sql_3688 = """select cast("09004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3688_strict "${const_sql_3688}"
    testFoldConst("${const_sql_3688}")
    def const_sql_3689 = """select cast("09014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3689_strict "${const_sql_3689}"
    testFoldConst("${const_sql_3689}")
    def const_sql_3690 = """select cast("09984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3690_strict "${const_sql_3690}"
    testFoldConst("${const_sql_3690}")
    def const_sql_3691 = """select cast("09994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3691_strict "${const_sql_3691}"
    testFoldConst("${const_sql_3691}")
    def const_sql_3692 = """select cast("10004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3692_strict "${const_sql_3692}"
    testFoldConst("${const_sql_3692}")
    def const_sql_3693 = """select cast("10014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3693_strict "${const_sql_3693}"
    testFoldConst("${const_sql_3693}")
    def const_sql_3694 = """select cast("10094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3694_strict "${const_sql_3694}"
    testFoldConst("${const_sql_3694}")
    def const_sql_3695 = """select cast("10994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3695_strict "${const_sql_3695}"
    testFoldConst("${const_sql_3695}")
    def const_sql_3696 = """select cast("19004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3696_strict "${const_sql_3696}"
    testFoldConst("${const_sql_3696}")
    def const_sql_3697 = """select cast("19014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3697_strict "${const_sql_3697}"
    testFoldConst("${const_sql_3697}")
    def const_sql_3698 = """select cast("19984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3698_strict "${const_sql_3698}"
    testFoldConst("${const_sql_3698}")
    def const_sql_3699 = """select cast("19994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3699_strict "${const_sql_3699}"
    testFoldConst("${const_sql_3699}")
    def const_sql_3700 = """select cast("90004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3700_strict "${const_sql_3700}"
    testFoldConst("${const_sql_3700}")
    def const_sql_3701 = """select cast("90014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3701_strict "${const_sql_3701}"
    testFoldConst("${const_sql_3701}")
    def const_sql_3702 = """select cast("90094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3702_strict "${const_sql_3702}"
    testFoldConst("${const_sql_3702}")
    def const_sql_3703 = """select cast("90994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3703_strict "${const_sql_3703}"
    testFoldConst("${const_sql_3703}")
    def const_sql_3704 = """select cast("99004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3704_strict "${const_sql_3704}"
    testFoldConst("${const_sql_3704}")
    def const_sql_3705 = """select cast("99014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3705_strict "${const_sql_3705}"
    testFoldConst("${const_sql_3705}")
    def const_sql_3706 = """select cast("99984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3706_strict "${const_sql_3706}"
    testFoldConst("${const_sql_3706}")
    def const_sql_3707 = """select cast("99994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3707_strict "${const_sql_3707}"
    testFoldConst("${const_sql_3707}")
    def const_sql_3708 = """select cast("999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3708_strict "${const_sql_3708}"
    testFoldConst("${const_sql_3708}")
    def const_sql_3709 = """select cast("999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3709_strict "${const_sql_3709}"
    testFoldConst("${const_sql_3709}")
    def const_sql_3710 = """select cast("999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3710_strict "${const_sql_3710}"
    testFoldConst("${const_sql_3710}")
    def const_sql_3711 = """select cast("999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3711_strict "${const_sql_3711}"
    testFoldConst("${const_sql_3711}")
    def const_sql_3712 = """select cast("999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3712_strict "${const_sql_3712}"
    testFoldConst("${const_sql_3712}")
    def const_sql_3713 = """select cast("999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3713_strict "${const_sql_3713}"
    testFoldConst("${const_sql_3713}")
    def const_sql_3714 = """select cast("999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3714_strict "${const_sql_3714}"
    testFoldConst("${const_sql_3714}")
    def const_sql_3715 = """select cast("999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3715_strict "${const_sql_3715}"
    testFoldConst("${const_sql_3715}")
    def const_sql_3716 = """select cast("9000000004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3716_strict "${const_sql_3716}"
    testFoldConst("${const_sql_3716}")
    def const_sql_3717 = """select cast("9000000014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3717_strict "${const_sql_3717}"
    testFoldConst("${const_sql_3717}")
    def const_sql_3718 = """select cast("9000000094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3718_strict "${const_sql_3718}"
    testFoldConst("${const_sql_3718}")
    def const_sql_3719 = """select cast("9000000994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3719_strict "${const_sql_3719}"
    testFoldConst("${const_sql_3719}")
    def const_sql_3720 = """select cast("9000009004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3720_strict "${const_sql_3720}"
    testFoldConst("${const_sql_3720}")
    def const_sql_3721 = """select cast("9000009014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3721_strict "${const_sql_3721}"
    testFoldConst("${const_sql_3721}")
    def const_sql_3722 = """select cast("9000009984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3722_strict "${const_sql_3722}"
    testFoldConst("${const_sql_3722}")
    def const_sql_3723 = """select cast("9000009994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3723_strict "${const_sql_3723}"
    testFoldConst("${const_sql_3723}")
    def const_sql_3724 = """select cast("9000010004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3724_strict "${const_sql_3724}"
    testFoldConst("${const_sql_3724}")
    def const_sql_3725 = """select cast("9000010014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3725_strict "${const_sql_3725}"
    testFoldConst("${const_sql_3725}")
    def const_sql_3726 = """select cast("9000010094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3726_strict "${const_sql_3726}"
    testFoldConst("${const_sql_3726}")
    def const_sql_3727 = """select cast("9000010994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3727_strict "${const_sql_3727}"
    testFoldConst("${const_sql_3727}")
    def const_sql_3728 = """select cast("9000019004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3728_strict "${const_sql_3728}"
    testFoldConst("${const_sql_3728}")
    def const_sql_3729 = """select cast("9000019014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3729_strict "${const_sql_3729}"
    testFoldConst("${const_sql_3729}")
    def const_sql_3730 = """select cast("9000019984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3730_strict "${const_sql_3730}"
    testFoldConst("${const_sql_3730}")
    def const_sql_3731 = """select cast("9000019994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3731_strict "${const_sql_3731}"
    testFoldConst("${const_sql_3731}")
    def const_sql_3732 = """select cast("9999980004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3732_strict "${const_sql_3732}"
    testFoldConst("${const_sql_3732}")
    def const_sql_3733 = """select cast("9999980014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3733_strict "${const_sql_3733}"
    testFoldConst("${const_sql_3733}")
    def const_sql_3734 = """select cast("9999980094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3734_strict "${const_sql_3734}"
    testFoldConst("${const_sql_3734}")
    def const_sql_3735 = """select cast("9999980994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3735_strict "${const_sql_3735}"
    testFoldConst("${const_sql_3735}")
    def const_sql_3736 = """select cast("9999989004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3736_strict "${const_sql_3736}"
    testFoldConst("${const_sql_3736}")
    def const_sql_3737 = """select cast("9999989014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3737_strict "${const_sql_3737}"
    testFoldConst("${const_sql_3737}")
    def const_sql_3738 = """select cast("9999989984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3738_strict "${const_sql_3738}"
    testFoldConst("${const_sql_3738}")
    def const_sql_3739 = """select cast("9999989994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3739_strict "${const_sql_3739}"
    testFoldConst("${const_sql_3739}")
    def const_sql_3740 = """select cast("9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3740_strict "${const_sql_3740}"
    testFoldConst("${const_sql_3740}")
    def const_sql_3741 = """select cast("9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3741_strict "${const_sql_3741}"
    testFoldConst("${const_sql_3741}")
    def const_sql_3742 = """select cast("9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3742_strict "${const_sql_3742}"
    testFoldConst("${const_sql_3742}")
    def const_sql_3743 = """select cast("9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3743_strict "${const_sql_3743}"
    testFoldConst("${const_sql_3743}")
    def const_sql_3744 = """select cast("9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3744_strict "${const_sql_3744}"
    testFoldConst("${const_sql_3744}")
    def const_sql_3745 = """select cast("9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3745_strict "${const_sql_3745}"
    testFoldConst("${const_sql_3745}")
    def const_sql_3746 = """select cast("9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3746_strict "${const_sql_3746}"
    testFoldConst("${const_sql_3746}")
    def const_sql_3747 = """select cast("9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3747_strict "${const_sql_3747}"
    testFoldConst("${const_sql_3747}")
    def const_sql_3748 = """select cast("00005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3748_strict "${const_sql_3748}"
    testFoldConst("${const_sql_3748}")
    def const_sql_3749 = """select cast("00015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3749_strict "${const_sql_3749}"
    testFoldConst("${const_sql_3749}")
    def const_sql_3750 = """select cast("00095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3750_strict "${const_sql_3750}"
    testFoldConst("${const_sql_3750}")
    def const_sql_3751 = """select cast("00995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3751_strict "${const_sql_3751}"
    testFoldConst("${const_sql_3751}")
    def const_sql_3752 = """select cast("09005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3752_strict "${const_sql_3752}"
    testFoldConst("${const_sql_3752}")
    def const_sql_3753 = """select cast("09015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3753_strict "${const_sql_3753}"
    testFoldConst("${const_sql_3753}")
    def const_sql_3754 = """select cast("09985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3754_strict "${const_sql_3754}"
    testFoldConst("${const_sql_3754}")
    def const_sql_3755 = """select cast("09995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3755_strict "${const_sql_3755}"
    testFoldConst("${const_sql_3755}")
    def const_sql_3756 = """select cast("10005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3756_strict "${const_sql_3756}"
    testFoldConst("${const_sql_3756}")
    def const_sql_3757 = """select cast("10015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3757_strict "${const_sql_3757}"
    testFoldConst("${const_sql_3757}")
    def const_sql_3758 = """select cast("10095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3758_strict "${const_sql_3758}"
    testFoldConst("${const_sql_3758}")
    def const_sql_3759 = """select cast("10995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3759_strict "${const_sql_3759}"
    testFoldConst("${const_sql_3759}")
    def const_sql_3760 = """select cast("19005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3760_strict "${const_sql_3760}"
    testFoldConst("${const_sql_3760}")
    def const_sql_3761 = """select cast("19015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3761_strict "${const_sql_3761}"
    testFoldConst("${const_sql_3761}")
    def const_sql_3762 = """select cast("19985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3762_strict "${const_sql_3762}"
    testFoldConst("${const_sql_3762}")
    def const_sql_3763 = """select cast("19995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3763_strict "${const_sql_3763}"
    testFoldConst("${const_sql_3763}")
    def const_sql_3764 = """select cast("90005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3764_strict "${const_sql_3764}"
    testFoldConst("${const_sql_3764}")
    def const_sql_3765 = """select cast("90015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3765_strict "${const_sql_3765}"
    testFoldConst("${const_sql_3765}")
    def const_sql_3766 = """select cast("90095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3766_strict "${const_sql_3766}"
    testFoldConst("${const_sql_3766}")
    def const_sql_3767 = """select cast("90995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3767_strict "${const_sql_3767}"
    testFoldConst("${const_sql_3767}")
    def const_sql_3768 = """select cast("99005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3768_strict "${const_sql_3768}"
    testFoldConst("${const_sql_3768}")
    def const_sql_3769 = """select cast("99015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3769_strict "${const_sql_3769}"
    testFoldConst("${const_sql_3769}")
    def const_sql_3770 = """select cast("99985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3770_strict "${const_sql_3770}"
    testFoldConst("${const_sql_3770}")
    def const_sql_3771 = """select cast("99995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3771_strict "${const_sql_3771}"
    testFoldConst("${const_sql_3771}")
    def const_sql_3772 = """select cast("999990005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3772_strict "${const_sql_3772}"
    testFoldConst("${const_sql_3772}")
    def const_sql_3773 = """select cast("999990015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3773_strict "${const_sql_3773}"
    testFoldConst("${const_sql_3773}")
    def const_sql_3774 = """select cast("999990095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3774_strict "${const_sql_3774}"
    testFoldConst("${const_sql_3774}")
    def const_sql_3775 = """select cast("999990995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3775_strict "${const_sql_3775}"
    testFoldConst("${const_sql_3775}")
    def const_sql_3776 = """select cast("999999005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3776_strict "${const_sql_3776}"
    testFoldConst("${const_sql_3776}")
    def const_sql_3777 = """select cast("999999015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3777_strict "${const_sql_3777}"
    testFoldConst("${const_sql_3777}")
    def const_sql_3778 = """select cast("999999985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3778_strict "${const_sql_3778}"
    testFoldConst("${const_sql_3778}")
    def const_sql_3779 = """select cast("999999995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3779_strict "${const_sql_3779}"
    testFoldConst("${const_sql_3779}")
    def const_sql_3780 = """select cast("9000000005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3780_strict "${const_sql_3780}"
    testFoldConst("${const_sql_3780}")
    def const_sql_3781 = """select cast("9000000015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3781_strict "${const_sql_3781}"
    testFoldConst("${const_sql_3781}")
    def const_sql_3782 = """select cast("9000000095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3782_strict "${const_sql_3782}"
    testFoldConst("${const_sql_3782}")
    def const_sql_3783 = """select cast("9000000995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3783_strict "${const_sql_3783}"
    testFoldConst("${const_sql_3783}")
    def const_sql_3784 = """select cast("9000009005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3784_strict "${const_sql_3784}"
    testFoldConst("${const_sql_3784}")
    def const_sql_3785 = """select cast("9000009015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3785_strict "${const_sql_3785}"
    testFoldConst("${const_sql_3785}")
    def const_sql_3786 = """select cast("9000009985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3786_strict "${const_sql_3786}"
    testFoldConst("${const_sql_3786}")
    def const_sql_3787 = """select cast("9000009995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3787_strict "${const_sql_3787}"
    testFoldConst("${const_sql_3787}")
    def const_sql_3788 = """select cast("9000010005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3788_strict "${const_sql_3788}"
    testFoldConst("${const_sql_3788}")
    def const_sql_3789 = """select cast("9000010015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3789_strict "${const_sql_3789}"
    testFoldConst("${const_sql_3789}")
    def const_sql_3790 = """select cast("9000010095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3790_strict "${const_sql_3790}"
    testFoldConst("${const_sql_3790}")
    def const_sql_3791 = """select cast("9000010995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3791_strict "${const_sql_3791}"
    testFoldConst("${const_sql_3791}")
    def const_sql_3792 = """select cast("9000019005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3792_strict "${const_sql_3792}"
    testFoldConst("${const_sql_3792}")
    def const_sql_3793 = """select cast("9000019015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3793_strict "${const_sql_3793}"
    testFoldConst("${const_sql_3793}")
    def const_sql_3794 = """select cast("9000019985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3794_strict "${const_sql_3794}"
    testFoldConst("${const_sql_3794}")
    def const_sql_3795 = """select cast("9000019995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3795_strict "${const_sql_3795}"
    testFoldConst("${const_sql_3795}")
    def const_sql_3796 = """select cast("9999980005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3796_strict "${const_sql_3796}"
    testFoldConst("${const_sql_3796}")
    def const_sql_3797 = """select cast("9999980015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3797_strict "${const_sql_3797}"
    testFoldConst("${const_sql_3797}")
    def const_sql_3798 = """select cast("9999980095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3798_strict "${const_sql_3798}"
    testFoldConst("${const_sql_3798}")
    def const_sql_3799 = """select cast("9999980995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3799_strict "${const_sql_3799}"
    testFoldConst("${const_sql_3799}")
    def const_sql_3800 = """select cast("9999989005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3800_strict "${const_sql_3800}"
    testFoldConst("${const_sql_3800}")
    def const_sql_3801 = """select cast("9999989015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3801_strict "${const_sql_3801}"
    testFoldConst("${const_sql_3801}")
    def const_sql_3802 = """select cast("9999989985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3802_strict "${const_sql_3802}"
    testFoldConst("${const_sql_3802}")
    def const_sql_3803 = """select cast("9999989995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3803_strict "${const_sql_3803}"
    testFoldConst("${const_sql_3803}")
    def const_sql_3804 = """select cast("9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3804_strict "${const_sql_3804}"
    testFoldConst("${const_sql_3804}")
    def const_sql_3805 = """select cast("9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3805_strict "${const_sql_3805}"
    testFoldConst("${const_sql_3805}")
    def const_sql_3806 = """select cast("9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3806_strict "${const_sql_3806}"
    testFoldConst("${const_sql_3806}")
    def const_sql_3807 = """select cast("9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3807_strict "${const_sql_3807}"
    testFoldConst("${const_sql_3807}")
    def const_sql_3808 = """select cast("9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3808_strict "${const_sql_3808}"
    testFoldConst("${const_sql_3808}")
    def const_sql_3809 = """select cast("9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3809_strict "${const_sql_3809}"
    testFoldConst("${const_sql_3809}")
    def const_sql_3810 = """select cast("9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3810_strict "${const_sql_3810}"
    testFoldConst("${const_sql_3810}")
    def const_sql_3811 = """select cast("9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3811_strict "${const_sql_3811}"
    testFoldConst("${const_sql_3811}")
    def const_sql_3812 = """select cast("-00004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3812_strict "${const_sql_3812}"
    testFoldConst("${const_sql_3812}")
    def const_sql_3813 = """select cast("-00014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3813_strict "${const_sql_3813}"
    testFoldConst("${const_sql_3813}")
    def const_sql_3814 = """select cast("-00094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3814_strict "${const_sql_3814}"
    testFoldConst("${const_sql_3814}")
    def const_sql_3815 = """select cast("-00994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3815_strict "${const_sql_3815}"
    testFoldConst("${const_sql_3815}")
    def const_sql_3816 = """select cast("-09004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3816_strict "${const_sql_3816}"
    testFoldConst("${const_sql_3816}")
    def const_sql_3817 = """select cast("-09014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3817_strict "${const_sql_3817}"
    testFoldConst("${const_sql_3817}")
    def const_sql_3818 = """select cast("-09984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3818_strict "${const_sql_3818}"
    testFoldConst("${const_sql_3818}")
    def const_sql_3819 = """select cast("-09994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3819_strict "${const_sql_3819}"
    testFoldConst("${const_sql_3819}")
    def const_sql_3820 = """select cast("-10004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3820_strict "${const_sql_3820}"
    testFoldConst("${const_sql_3820}")
    def const_sql_3821 = """select cast("-10014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3821_strict "${const_sql_3821}"
    testFoldConst("${const_sql_3821}")
    def const_sql_3822 = """select cast("-10094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3822_strict "${const_sql_3822}"
    testFoldConst("${const_sql_3822}")
    def const_sql_3823 = """select cast("-10994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3823_strict "${const_sql_3823}"
    testFoldConst("${const_sql_3823}")
    def const_sql_3824 = """select cast("-19004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3824_strict "${const_sql_3824}"
    testFoldConst("${const_sql_3824}")
    def const_sql_3825 = """select cast("-19014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3825_strict "${const_sql_3825}"
    testFoldConst("${const_sql_3825}")
    def const_sql_3826 = """select cast("-19984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3826_strict "${const_sql_3826}"
    testFoldConst("${const_sql_3826}")
    def const_sql_3827 = """select cast("-19994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3827_strict "${const_sql_3827}"
    testFoldConst("${const_sql_3827}")
    def const_sql_3828 = """select cast("-90004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3828_strict "${const_sql_3828}"
    testFoldConst("${const_sql_3828}")
    def const_sql_3829 = """select cast("-90014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3829_strict "${const_sql_3829}"
    testFoldConst("${const_sql_3829}")
    def const_sql_3830 = """select cast("-90094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3830_strict "${const_sql_3830}"
    testFoldConst("${const_sql_3830}")
    def const_sql_3831 = """select cast("-90994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3831_strict "${const_sql_3831}"
    testFoldConst("${const_sql_3831}")
    def const_sql_3832 = """select cast("-99004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3832_strict "${const_sql_3832}"
    testFoldConst("${const_sql_3832}")
    def const_sql_3833 = """select cast("-99014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3833_strict "${const_sql_3833}"
    testFoldConst("${const_sql_3833}")
    def const_sql_3834 = """select cast("-99984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3834_strict "${const_sql_3834}"
    testFoldConst("${const_sql_3834}")
    def const_sql_3835 = """select cast("-99994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3835_strict "${const_sql_3835}"
    testFoldConst("${const_sql_3835}")
    def const_sql_3836 = """select cast("-999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3836_strict "${const_sql_3836}"
    testFoldConst("${const_sql_3836}")
    def const_sql_3837 = """select cast("-999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3837_strict "${const_sql_3837}"
    testFoldConst("${const_sql_3837}")
    def const_sql_3838 = """select cast("-999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3838_strict "${const_sql_3838}"
    testFoldConst("${const_sql_3838}")
    def const_sql_3839 = """select cast("-999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3839_strict "${const_sql_3839}"
    testFoldConst("${const_sql_3839}")
    def const_sql_3840 = """select cast("-999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3840_strict "${const_sql_3840}"
    testFoldConst("${const_sql_3840}")
    def const_sql_3841 = """select cast("-999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3841_strict "${const_sql_3841}"
    testFoldConst("${const_sql_3841}")
    def const_sql_3842 = """select cast("-999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3842_strict "${const_sql_3842}"
    testFoldConst("${const_sql_3842}")
    def const_sql_3843 = """select cast("-999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3843_strict "${const_sql_3843}"
    testFoldConst("${const_sql_3843}")
    def const_sql_3844 = """select cast("-9000000004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3844_strict "${const_sql_3844}"
    testFoldConst("${const_sql_3844}")
    def const_sql_3845 = """select cast("-9000000014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3845_strict "${const_sql_3845}"
    testFoldConst("${const_sql_3845}")
    def const_sql_3846 = """select cast("-9000000094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3846_strict "${const_sql_3846}"
    testFoldConst("${const_sql_3846}")
    def const_sql_3847 = """select cast("-9000000994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3847_strict "${const_sql_3847}"
    testFoldConst("${const_sql_3847}")
    def const_sql_3848 = """select cast("-9000009004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3848_strict "${const_sql_3848}"
    testFoldConst("${const_sql_3848}")
    def const_sql_3849 = """select cast("-9000009014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3849_strict "${const_sql_3849}"
    testFoldConst("${const_sql_3849}")
    def const_sql_3850 = """select cast("-9000009984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3850_strict "${const_sql_3850}"
    testFoldConst("${const_sql_3850}")
    def const_sql_3851 = """select cast("-9000009994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3851_strict "${const_sql_3851}"
    testFoldConst("${const_sql_3851}")
    def const_sql_3852 = """select cast("-9000010004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3852_strict "${const_sql_3852}"
    testFoldConst("${const_sql_3852}")
    def const_sql_3853 = """select cast("-9000010014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3853_strict "${const_sql_3853}"
    testFoldConst("${const_sql_3853}")
    def const_sql_3854 = """select cast("-9000010094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3854_strict "${const_sql_3854}"
    testFoldConst("${const_sql_3854}")
    def const_sql_3855 = """select cast("-9000010994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3855_strict "${const_sql_3855}"
    testFoldConst("${const_sql_3855}")
    def const_sql_3856 = """select cast("-9000019004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3856_strict "${const_sql_3856}"
    testFoldConst("${const_sql_3856}")
    def const_sql_3857 = """select cast("-9000019014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3857_strict "${const_sql_3857}"
    testFoldConst("${const_sql_3857}")
    def const_sql_3858 = """select cast("-9000019984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3858_strict "${const_sql_3858}"
    testFoldConst("${const_sql_3858}")
    def const_sql_3859 = """select cast("-9000019994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3859_strict "${const_sql_3859}"
    testFoldConst("${const_sql_3859}")
    def const_sql_3860 = """select cast("-9999980004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3860_strict "${const_sql_3860}"
    testFoldConst("${const_sql_3860}")
    def const_sql_3861 = """select cast("-9999980014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3861_strict "${const_sql_3861}"
    testFoldConst("${const_sql_3861}")
    def const_sql_3862 = """select cast("-9999980094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3862_strict "${const_sql_3862}"
    testFoldConst("${const_sql_3862}")
    def const_sql_3863 = """select cast("-9999980994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3863_strict "${const_sql_3863}"
    testFoldConst("${const_sql_3863}")
    def const_sql_3864 = """select cast("-9999989004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3864_strict "${const_sql_3864}"
    testFoldConst("${const_sql_3864}")
    def const_sql_3865 = """select cast("-9999989014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3865_strict "${const_sql_3865}"
    testFoldConst("${const_sql_3865}")
    def const_sql_3866 = """select cast("-9999989984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3866_strict "${const_sql_3866}"
    testFoldConst("${const_sql_3866}")
    def const_sql_3867 = """select cast("-9999989994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3867_strict "${const_sql_3867}"
    testFoldConst("${const_sql_3867}")
    def const_sql_3868 = """select cast("-9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3868_strict "${const_sql_3868}"
    testFoldConst("${const_sql_3868}")
    def const_sql_3869 = """select cast("-9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3869_strict "${const_sql_3869}"
    testFoldConst("${const_sql_3869}")
    def const_sql_3870 = """select cast("-9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3870_strict "${const_sql_3870}"
    testFoldConst("${const_sql_3870}")
    def const_sql_3871 = """select cast("-9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3871_strict "${const_sql_3871}"
    testFoldConst("${const_sql_3871}")
    def const_sql_3872 = """select cast("-9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3872_strict "${const_sql_3872}"
    testFoldConst("${const_sql_3872}")
    def const_sql_3873 = """select cast("-9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3873_strict "${const_sql_3873}"
    testFoldConst("${const_sql_3873}")
    def const_sql_3874 = """select cast("-9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3874_strict "${const_sql_3874}"
    testFoldConst("${const_sql_3874}")
    def const_sql_3875 = """select cast("-9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3875_strict "${const_sql_3875}"
    testFoldConst("${const_sql_3875}")
    def const_sql_3876 = """select cast("-00005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3876_strict "${const_sql_3876}"
    testFoldConst("${const_sql_3876}")
    def const_sql_3877 = """select cast("-00015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3877_strict "${const_sql_3877}"
    testFoldConst("${const_sql_3877}")
    def const_sql_3878 = """select cast("-00095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3878_strict "${const_sql_3878}"
    testFoldConst("${const_sql_3878}")
    def const_sql_3879 = """select cast("-00995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3879_strict "${const_sql_3879}"
    testFoldConst("${const_sql_3879}")
    def const_sql_3880 = """select cast("-09005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3880_strict "${const_sql_3880}"
    testFoldConst("${const_sql_3880}")
    def const_sql_3881 = """select cast("-09015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3881_strict "${const_sql_3881}"
    testFoldConst("${const_sql_3881}")
    def const_sql_3882 = """select cast("-09985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3882_strict "${const_sql_3882}"
    testFoldConst("${const_sql_3882}")
    def const_sql_3883 = """select cast("-09995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3883_strict "${const_sql_3883}"
    testFoldConst("${const_sql_3883}")
    def const_sql_3884 = """select cast("-10005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3884_strict "${const_sql_3884}"
    testFoldConst("${const_sql_3884}")
    def const_sql_3885 = """select cast("-10015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3885_strict "${const_sql_3885}"
    testFoldConst("${const_sql_3885}")
    def const_sql_3886 = """select cast("-10095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3886_strict "${const_sql_3886}"
    testFoldConst("${const_sql_3886}")
    def const_sql_3887 = """select cast("-10995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3887_strict "${const_sql_3887}"
    testFoldConst("${const_sql_3887}")
    def const_sql_3888 = """select cast("-19005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3888_strict "${const_sql_3888}"
    testFoldConst("${const_sql_3888}")
    def const_sql_3889 = """select cast("-19015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3889_strict "${const_sql_3889}"
    testFoldConst("${const_sql_3889}")
    def const_sql_3890 = """select cast("-19985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3890_strict "${const_sql_3890}"
    testFoldConst("${const_sql_3890}")
    def const_sql_3891 = """select cast("-19995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3891_strict "${const_sql_3891}"
    testFoldConst("${const_sql_3891}")
    def const_sql_3892 = """select cast("-90005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3892_strict "${const_sql_3892}"
    testFoldConst("${const_sql_3892}")
    def const_sql_3893 = """select cast("-90015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3893_strict "${const_sql_3893}"
    testFoldConst("${const_sql_3893}")
    def const_sql_3894 = """select cast("-90095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3894_strict "${const_sql_3894}"
    testFoldConst("${const_sql_3894}")
    def const_sql_3895 = """select cast("-90995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3895_strict "${const_sql_3895}"
    testFoldConst("${const_sql_3895}")
    def const_sql_3896 = """select cast("-99005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3896_strict "${const_sql_3896}"
    testFoldConst("${const_sql_3896}")
    def const_sql_3897 = """select cast("-99015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3897_strict "${const_sql_3897}"
    testFoldConst("${const_sql_3897}")
    def const_sql_3898 = """select cast("-99985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3898_strict "${const_sql_3898}"
    testFoldConst("${const_sql_3898}")
    def const_sql_3899 = """select cast("-99995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3899_strict "${const_sql_3899}"
    testFoldConst("${const_sql_3899}")
    def const_sql_3900 = """select cast("-999990005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3900_strict "${const_sql_3900}"
    testFoldConst("${const_sql_3900}")
    def const_sql_3901 = """select cast("-999990015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3901_strict "${const_sql_3901}"
    testFoldConst("${const_sql_3901}")
    def const_sql_3902 = """select cast("-999990095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3902_strict "${const_sql_3902}"
    testFoldConst("${const_sql_3902}")
    def const_sql_3903 = """select cast("-999990995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3903_strict "${const_sql_3903}"
    testFoldConst("${const_sql_3903}")
    def const_sql_3904 = """select cast("-999999005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3904_strict "${const_sql_3904}"
    testFoldConst("${const_sql_3904}")
    def const_sql_3905 = """select cast("-999999015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3905_strict "${const_sql_3905}"
    testFoldConst("${const_sql_3905}")
    def const_sql_3906 = """select cast("-999999985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3906_strict "${const_sql_3906}"
    testFoldConst("${const_sql_3906}")
    def const_sql_3907 = """select cast("-999999995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3907_strict "${const_sql_3907}"
    testFoldConst("${const_sql_3907}")
    def const_sql_3908 = """select cast("-9000000005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3908_strict "${const_sql_3908}"
    testFoldConst("${const_sql_3908}")
    def const_sql_3909 = """select cast("-9000000015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3909_strict "${const_sql_3909}"
    testFoldConst("${const_sql_3909}")
    def const_sql_3910 = """select cast("-9000000095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3910_strict "${const_sql_3910}"
    testFoldConst("${const_sql_3910}")
    def const_sql_3911 = """select cast("-9000000995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3911_strict "${const_sql_3911}"
    testFoldConst("${const_sql_3911}")
    def const_sql_3912 = """select cast("-9000009005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3912_strict "${const_sql_3912}"
    testFoldConst("${const_sql_3912}")
    def const_sql_3913 = """select cast("-9000009015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3913_strict "${const_sql_3913}"
    testFoldConst("${const_sql_3913}")
    def const_sql_3914 = """select cast("-9000009985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3914_strict "${const_sql_3914}"
    testFoldConst("${const_sql_3914}")
    def const_sql_3915 = """select cast("-9000009995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3915_strict "${const_sql_3915}"
    testFoldConst("${const_sql_3915}")
    def const_sql_3916 = """select cast("-9000010005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3916_strict "${const_sql_3916}"
    testFoldConst("${const_sql_3916}")
    def const_sql_3917 = """select cast("-9000010015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3917_strict "${const_sql_3917}"
    testFoldConst("${const_sql_3917}")
    def const_sql_3918 = """select cast("-9000010095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3918_strict "${const_sql_3918}"
    testFoldConst("${const_sql_3918}")
    def const_sql_3919 = """select cast("-9000010995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3919_strict "${const_sql_3919}"
    testFoldConst("${const_sql_3919}")
    def const_sql_3920 = """select cast("-9000019005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3920_strict "${const_sql_3920}"
    testFoldConst("${const_sql_3920}")
    def const_sql_3921 = """select cast("-9000019015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3921_strict "${const_sql_3921}"
    testFoldConst("${const_sql_3921}")
    def const_sql_3922 = """select cast("-9000019985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3922_strict "${const_sql_3922}"
    testFoldConst("${const_sql_3922}")
    def const_sql_3923 = """select cast("-9000019995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3923_strict "${const_sql_3923}"
    testFoldConst("${const_sql_3923}")
    def const_sql_3924 = """select cast("-9999980005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3924_strict "${const_sql_3924}"
    testFoldConst("${const_sql_3924}")
    def const_sql_3925 = """select cast("-9999980015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3925_strict "${const_sql_3925}"
    testFoldConst("${const_sql_3925}")
    def const_sql_3926 = """select cast("-9999980095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3926_strict "${const_sql_3926}"
    testFoldConst("${const_sql_3926}")
    def const_sql_3927 = """select cast("-9999980995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3927_strict "${const_sql_3927}"
    testFoldConst("${const_sql_3927}")
    def const_sql_3928 = """select cast("-9999989005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3928_strict "${const_sql_3928}"
    testFoldConst("${const_sql_3928}")
    def const_sql_3929 = """select cast("-9999989015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3929_strict "${const_sql_3929}"
    testFoldConst("${const_sql_3929}")
    def const_sql_3930 = """select cast("-9999989985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3930_strict "${const_sql_3930}"
    testFoldConst("${const_sql_3930}")
    def const_sql_3931 = """select cast("-9999989995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3931_strict "${const_sql_3931}"
    testFoldConst("${const_sql_3931}")
    def const_sql_3932 = """select cast("-9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3932_strict "${const_sql_3932}"
    testFoldConst("${const_sql_3932}")
    def const_sql_3933 = """select cast("-9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3933_strict "${const_sql_3933}"
    testFoldConst("${const_sql_3933}")
    def const_sql_3934 = """select cast("-9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3934_strict "${const_sql_3934}"
    testFoldConst("${const_sql_3934}")
    def const_sql_3935 = """select cast("-9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3935_strict "${const_sql_3935}"
    testFoldConst("${const_sql_3935}")
    def const_sql_3936 = """select cast("-9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3936_strict "${const_sql_3936}"
    testFoldConst("${const_sql_3936}")
    def const_sql_3937 = """select cast("-9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3937_strict "${const_sql_3937}"
    testFoldConst("${const_sql_3937}")
    def const_sql_3938 = """select cast("-9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3938_strict "${const_sql_3938}"
    testFoldConst("${const_sql_3938}")
    def const_sql_3939 = """select cast("-9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 3));"""
    qt_sql_3939_strict "${const_sql_3939}"
    testFoldConst("${const_sql_3939}")
    sql "set enable_strict_cast=false;"
    qt_sql_3618_non_strict "${const_sql_3618}"
    testFoldConst("${const_sql_3618}")
    qt_sql_3619_non_strict "${const_sql_3619}"
    testFoldConst("${const_sql_3619}")
    qt_sql_3620_non_strict "${const_sql_3620}"
    testFoldConst("${const_sql_3620}")
    qt_sql_3621_non_strict "${const_sql_3621}"
    testFoldConst("${const_sql_3621}")
    qt_sql_3622_non_strict "${const_sql_3622}"
    testFoldConst("${const_sql_3622}")
    qt_sql_3623_non_strict "${const_sql_3623}"
    testFoldConst("${const_sql_3623}")
    qt_sql_3624_non_strict "${const_sql_3624}"
    testFoldConst("${const_sql_3624}")
    qt_sql_3625_non_strict "${const_sql_3625}"
    testFoldConst("${const_sql_3625}")
    qt_sql_3626_non_strict "${const_sql_3626}"
    testFoldConst("${const_sql_3626}")
    qt_sql_3627_non_strict "${const_sql_3627}"
    testFoldConst("${const_sql_3627}")
    qt_sql_3628_non_strict "${const_sql_3628}"
    testFoldConst("${const_sql_3628}")
    qt_sql_3629_non_strict "${const_sql_3629}"
    testFoldConst("${const_sql_3629}")
    qt_sql_3630_non_strict "${const_sql_3630}"
    testFoldConst("${const_sql_3630}")
    qt_sql_3631_non_strict "${const_sql_3631}"
    testFoldConst("${const_sql_3631}")
    qt_sql_3632_non_strict "${const_sql_3632}"
    testFoldConst("${const_sql_3632}")
    qt_sql_3633_non_strict "${const_sql_3633}"
    testFoldConst("${const_sql_3633}")
    qt_sql_3634_non_strict "${const_sql_3634}"
    testFoldConst("${const_sql_3634}")
    qt_sql_3635_non_strict "${const_sql_3635}"
    testFoldConst("${const_sql_3635}")
    qt_sql_3636_non_strict "${const_sql_3636}"
    testFoldConst("${const_sql_3636}")
    qt_sql_3637_non_strict "${const_sql_3637}"
    testFoldConst("${const_sql_3637}")
    qt_sql_3638_non_strict "${const_sql_3638}"
    testFoldConst("${const_sql_3638}")
    qt_sql_3639_non_strict "${const_sql_3639}"
    testFoldConst("${const_sql_3639}")
    qt_sql_3640_non_strict "${const_sql_3640}"
    testFoldConst("${const_sql_3640}")
    qt_sql_3641_non_strict "${const_sql_3641}"
    testFoldConst("${const_sql_3641}")
    qt_sql_3642_non_strict "${const_sql_3642}"
    testFoldConst("${const_sql_3642}")
    qt_sql_3643_non_strict "${const_sql_3643}"
    testFoldConst("${const_sql_3643}")
    qt_sql_3644_non_strict "${const_sql_3644}"
    testFoldConst("${const_sql_3644}")
    qt_sql_3645_non_strict "${const_sql_3645}"
    testFoldConst("${const_sql_3645}")
    qt_sql_3646_non_strict "${const_sql_3646}"
    testFoldConst("${const_sql_3646}")
    qt_sql_3647_non_strict "${const_sql_3647}"
    testFoldConst("${const_sql_3647}")
    qt_sql_3648_non_strict "${const_sql_3648}"
    testFoldConst("${const_sql_3648}")
    qt_sql_3649_non_strict "${const_sql_3649}"
    testFoldConst("${const_sql_3649}")
    qt_sql_3650_non_strict "${const_sql_3650}"
    testFoldConst("${const_sql_3650}")
    qt_sql_3651_non_strict "${const_sql_3651}"
    testFoldConst("${const_sql_3651}")
    qt_sql_3652_non_strict "${const_sql_3652}"
    testFoldConst("${const_sql_3652}")
    qt_sql_3653_non_strict "${const_sql_3653}"
    testFoldConst("${const_sql_3653}")
    qt_sql_3654_non_strict "${const_sql_3654}"
    testFoldConst("${const_sql_3654}")
    qt_sql_3655_non_strict "${const_sql_3655}"
    testFoldConst("${const_sql_3655}")
    qt_sql_3656_non_strict "${const_sql_3656}"
    testFoldConst("${const_sql_3656}")
    qt_sql_3657_non_strict "${const_sql_3657}"
    testFoldConst("${const_sql_3657}")
    qt_sql_3658_non_strict "${const_sql_3658}"
    testFoldConst("${const_sql_3658}")
    qt_sql_3659_non_strict "${const_sql_3659}"
    testFoldConst("${const_sql_3659}")
    qt_sql_3660_non_strict "${const_sql_3660}"
    testFoldConst("${const_sql_3660}")
    qt_sql_3661_non_strict "${const_sql_3661}"
    testFoldConst("${const_sql_3661}")
    qt_sql_3662_non_strict "${const_sql_3662}"
    testFoldConst("${const_sql_3662}")
    qt_sql_3663_non_strict "${const_sql_3663}"
    testFoldConst("${const_sql_3663}")
    qt_sql_3664_non_strict "${const_sql_3664}"
    testFoldConst("${const_sql_3664}")
    qt_sql_3665_non_strict "${const_sql_3665}"
    testFoldConst("${const_sql_3665}")
    qt_sql_3666_non_strict "${const_sql_3666}"
    testFoldConst("${const_sql_3666}")
    qt_sql_3667_non_strict "${const_sql_3667}"
    testFoldConst("${const_sql_3667}")
    qt_sql_3668_non_strict "${const_sql_3668}"
    testFoldConst("${const_sql_3668}")
    qt_sql_3669_non_strict "${const_sql_3669}"
    testFoldConst("${const_sql_3669}")
    qt_sql_3670_non_strict "${const_sql_3670}"
    testFoldConst("${const_sql_3670}")
    qt_sql_3671_non_strict "${const_sql_3671}"
    testFoldConst("${const_sql_3671}")
    qt_sql_3672_non_strict "${const_sql_3672}"
    testFoldConst("${const_sql_3672}")
    qt_sql_3673_non_strict "${const_sql_3673}"
    testFoldConst("${const_sql_3673}")
    qt_sql_3674_non_strict "${const_sql_3674}"
    testFoldConst("${const_sql_3674}")
    qt_sql_3675_non_strict "${const_sql_3675}"
    testFoldConst("${const_sql_3675}")
    qt_sql_3676_non_strict "${const_sql_3676}"
    testFoldConst("${const_sql_3676}")
    qt_sql_3677_non_strict "${const_sql_3677}"
    testFoldConst("${const_sql_3677}")
    qt_sql_3678_non_strict "${const_sql_3678}"
    testFoldConst("${const_sql_3678}")
    qt_sql_3679_non_strict "${const_sql_3679}"
    testFoldConst("${const_sql_3679}")
    qt_sql_3680_non_strict "${const_sql_3680}"
    testFoldConst("${const_sql_3680}")
    qt_sql_3681_non_strict "${const_sql_3681}"
    testFoldConst("${const_sql_3681}")
    qt_sql_3682_non_strict "${const_sql_3682}"
    testFoldConst("${const_sql_3682}")
    qt_sql_3683_non_strict "${const_sql_3683}"
    testFoldConst("${const_sql_3683}")
    qt_sql_3684_non_strict "${const_sql_3684}"
    testFoldConst("${const_sql_3684}")
    qt_sql_3685_non_strict "${const_sql_3685}"
    testFoldConst("${const_sql_3685}")
    qt_sql_3686_non_strict "${const_sql_3686}"
    testFoldConst("${const_sql_3686}")
    qt_sql_3687_non_strict "${const_sql_3687}"
    testFoldConst("${const_sql_3687}")
    qt_sql_3688_non_strict "${const_sql_3688}"
    testFoldConst("${const_sql_3688}")
    qt_sql_3689_non_strict "${const_sql_3689}"
    testFoldConst("${const_sql_3689}")
    qt_sql_3690_non_strict "${const_sql_3690}"
    testFoldConst("${const_sql_3690}")
    qt_sql_3691_non_strict "${const_sql_3691}"
    testFoldConst("${const_sql_3691}")
    qt_sql_3692_non_strict "${const_sql_3692}"
    testFoldConst("${const_sql_3692}")
    qt_sql_3693_non_strict "${const_sql_3693}"
    testFoldConst("${const_sql_3693}")
    qt_sql_3694_non_strict "${const_sql_3694}"
    testFoldConst("${const_sql_3694}")
    qt_sql_3695_non_strict "${const_sql_3695}"
    testFoldConst("${const_sql_3695}")
    qt_sql_3696_non_strict "${const_sql_3696}"
    testFoldConst("${const_sql_3696}")
    qt_sql_3697_non_strict "${const_sql_3697}"
    testFoldConst("${const_sql_3697}")
    qt_sql_3698_non_strict "${const_sql_3698}"
    testFoldConst("${const_sql_3698}")
    qt_sql_3699_non_strict "${const_sql_3699}"
    testFoldConst("${const_sql_3699}")
    qt_sql_3700_non_strict "${const_sql_3700}"
    testFoldConst("${const_sql_3700}")
    qt_sql_3701_non_strict "${const_sql_3701}"
    testFoldConst("${const_sql_3701}")
    qt_sql_3702_non_strict "${const_sql_3702}"
    testFoldConst("${const_sql_3702}")
    qt_sql_3703_non_strict "${const_sql_3703}"
    testFoldConst("${const_sql_3703}")
    qt_sql_3704_non_strict "${const_sql_3704}"
    testFoldConst("${const_sql_3704}")
    qt_sql_3705_non_strict "${const_sql_3705}"
    testFoldConst("${const_sql_3705}")
    qt_sql_3706_non_strict "${const_sql_3706}"
    testFoldConst("${const_sql_3706}")
    qt_sql_3707_non_strict "${const_sql_3707}"
    testFoldConst("${const_sql_3707}")
    qt_sql_3708_non_strict "${const_sql_3708}"
    testFoldConst("${const_sql_3708}")
    qt_sql_3709_non_strict "${const_sql_3709}"
    testFoldConst("${const_sql_3709}")
    qt_sql_3710_non_strict "${const_sql_3710}"
    testFoldConst("${const_sql_3710}")
    qt_sql_3711_non_strict "${const_sql_3711}"
    testFoldConst("${const_sql_3711}")
    qt_sql_3712_non_strict "${const_sql_3712}"
    testFoldConst("${const_sql_3712}")
    qt_sql_3713_non_strict "${const_sql_3713}"
    testFoldConst("${const_sql_3713}")
    qt_sql_3714_non_strict "${const_sql_3714}"
    testFoldConst("${const_sql_3714}")
    qt_sql_3715_non_strict "${const_sql_3715}"
    testFoldConst("${const_sql_3715}")
    qt_sql_3716_non_strict "${const_sql_3716}"
    testFoldConst("${const_sql_3716}")
    qt_sql_3717_non_strict "${const_sql_3717}"
    testFoldConst("${const_sql_3717}")
    qt_sql_3718_non_strict "${const_sql_3718}"
    testFoldConst("${const_sql_3718}")
    qt_sql_3719_non_strict "${const_sql_3719}"
    testFoldConst("${const_sql_3719}")
    qt_sql_3720_non_strict "${const_sql_3720}"
    testFoldConst("${const_sql_3720}")
    qt_sql_3721_non_strict "${const_sql_3721}"
    testFoldConst("${const_sql_3721}")
    qt_sql_3722_non_strict "${const_sql_3722}"
    testFoldConst("${const_sql_3722}")
    qt_sql_3723_non_strict "${const_sql_3723}"
    testFoldConst("${const_sql_3723}")
    qt_sql_3724_non_strict "${const_sql_3724}"
    testFoldConst("${const_sql_3724}")
    qt_sql_3725_non_strict "${const_sql_3725}"
    testFoldConst("${const_sql_3725}")
    qt_sql_3726_non_strict "${const_sql_3726}"
    testFoldConst("${const_sql_3726}")
    qt_sql_3727_non_strict "${const_sql_3727}"
    testFoldConst("${const_sql_3727}")
    qt_sql_3728_non_strict "${const_sql_3728}"
    testFoldConst("${const_sql_3728}")
    qt_sql_3729_non_strict "${const_sql_3729}"
    testFoldConst("${const_sql_3729}")
    qt_sql_3730_non_strict "${const_sql_3730}"
    testFoldConst("${const_sql_3730}")
    qt_sql_3731_non_strict "${const_sql_3731}"
    testFoldConst("${const_sql_3731}")
    qt_sql_3732_non_strict "${const_sql_3732}"
    testFoldConst("${const_sql_3732}")
    qt_sql_3733_non_strict "${const_sql_3733}"
    testFoldConst("${const_sql_3733}")
    qt_sql_3734_non_strict "${const_sql_3734}"
    testFoldConst("${const_sql_3734}")
    qt_sql_3735_non_strict "${const_sql_3735}"
    testFoldConst("${const_sql_3735}")
    qt_sql_3736_non_strict "${const_sql_3736}"
    testFoldConst("${const_sql_3736}")
    qt_sql_3737_non_strict "${const_sql_3737}"
    testFoldConst("${const_sql_3737}")
    qt_sql_3738_non_strict "${const_sql_3738}"
    testFoldConst("${const_sql_3738}")
    qt_sql_3739_non_strict "${const_sql_3739}"
    testFoldConst("${const_sql_3739}")
    qt_sql_3740_non_strict "${const_sql_3740}"
    testFoldConst("${const_sql_3740}")
    qt_sql_3741_non_strict "${const_sql_3741}"
    testFoldConst("${const_sql_3741}")
    qt_sql_3742_non_strict "${const_sql_3742}"
    testFoldConst("${const_sql_3742}")
    qt_sql_3743_non_strict "${const_sql_3743}"
    testFoldConst("${const_sql_3743}")
    qt_sql_3744_non_strict "${const_sql_3744}"
    testFoldConst("${const_sql_3744}")
    qt_sql_3745_non_strict "${const_sql_3745}"
    testFoldConst("${const_sql_3745}")
    qt_sql_3746_non_strict "${const_sql_3746}"
    testFoldConst("${const_sql_3746}")
    qt_sql_3747_non_strict "${const_sql_3747}"
    testFoldConst("${const_sql_3747}")
    qt_sql_3748_non_strict "${const_sql_3748}"
    testFoldConst("${const_sql_3748}")
    qt_sql_3749_non_strict "${const_sql_3749}"
    testFoldConst("${const_sql_3749}")
    qt_sql_3750_non_strict "${const_sql_3750}"
    testFoldConst("${const_sql_3750}")
    qt_sql_3751_non_strict "${const_sql_3751}"
    testFoldConst("${const_sql_3751}")
    qt_sql_3752_non_strict "${const_sql_3752}"
    testFoldConst("${const_sql_3752}")
    qt_sql_3753_non_strict "${const_sql_3753}"
    testFoldConst("${const_sql_3753}")
    qt_sql_3754_non_strict "${const_sql_3754}"
    testFoldConst("${const_sql_3754}")
    qt_sql_3755_non_strict "${const_sql_3755}"
    testFoldConst("${const_sql_3755}")
    qt_sql_3756_non_strict "${const_sql_3756}"
    testFoldConst("${const_sql_3756}")
    qt_sql_3757_non_strict "${const_sql_3757}"
    testFoldConst("${const_sql_3757}")
    qt_sql_3758_non_strict "${const_sql_3758}"
    testFoldConst("${const_sql_3758}")
    qt_sql_3759_non_strict "${const_sql_3759}"
    testFoldConst("${const_sql_3759}")
    qt_sql_3760_non_strict "${const_sql_3760}"
    testFoldConst("${const_sql_3760}")
    qt_sql_3761_non_strict "${const_sql_3761}"
    testFoldConst("${const_sql_3761}")
    qt_sql_3762_non_strict "${const_sql_3762}"
    testFoldConst("${const_sql_3762}")
    qt_sql_3763_non_strict "${const_sql_3763}"
    testFoldConst("${const_sql_3763}")
    qt_sql_3764_non_strict "${const_sql_3764}"
    testFoldConst("${const_sql_3764}")
    qt_sql_3765_non_strict "${const_sql_3765}"
    testFoldConst("${const_sql_3765}")
    qt_sql_3766_non_strict "${const_sql_3766}"
    testFoldConst("${const_sql_3766}")
    qt_sql_3767_non_strict "${const_sql_3767}"
    testFoldConst("${const_sql_3767}")
    qt_sql_3768_non_strict "${const_sql_3768}"
    testFoldConst("${const_sql_3768}")
    qt_sql_3769_non_strict "${const_sql_3769}"
    testFoldConst("${const_sql_3769}")
    qt_sql_3770_non_strict "${const_sql_3770}"
    testFoldConst("${const_sql_3770}")
    qt_sql_3771_non_strict "${const_sql_3771}"
    testFoldConst("${const_sql_3771}")
    qt_sql_3772_non_strict "${const_sql_3772}"
    testFoldConst("${const_sql_3772}")
    qt_sql_3773_non_strict "${const_sql_3773}"
    testFoldConst("${const_sql_3773}")
    qt_sql_3774_non_strict "${const_sql_3774}"
    testFoldConst("${const_sql_3774}")
    qt_sql_3775_non_strict "${const_sql_3775}"
    testFoldConst("${const_sql_3775}")
    qt_sql_3776_non_strict "${const_sql_3776}"
    testFoldConst("${const_sql_3776}")
    qt_sql_3777_non_strict "${const_sql_3777}"
    testFoldConst("${const_sql_3777}")
    qt_sql_3778_non_strict "${const_sql_3778}"
    testFoldConst("${const_sql_3778}")
    qt_sql_3779_non_strict "${const_sql_3779}"
    testFoldConst("${const_sql_3779}")
    qt_sql_3780_non_strict "${const_sql_3780}"
    testFoldConst("${const_sql_3780}")
    qt_sql_3781_non_strict "${const_sql_3781}"
    testFoldConst("${const_sql_3781}")
    qt_sql_3782_non_strict "${const_sql_3782}"
    testFoldConst("${const_sql_3782}")
    qt_sql_3783_non_strict "${const_sql_3783}"
    testFoldConst("${const_sql_3783}")
    qt_sql_3784_non_strict "${const_sql_3784}"
    testFoldConst("${const_sql_3784}")
    qt_sql_3785_non_strict "${const_sql_3785}"
    testFoldConst("${const_sql_3785}")
    qt_sql_3786_non_strict "${const_sql_3786}"
    testFoldConst("${const_sql_3786}")
    qt_sql_3787_non_strict "${const_sql_3787}"
    testFoldConst("${const_sql_3787}")
    qt_sql_3788_non_strict "${const_sql_3788}"
    testFoldConst("${const_sql_3788}")
    qt_sql_3789_non_strict "${const_sql_3789}"
    testFoldConst("${const_sql_3789}")
    qt_sql_3790_non_strict "${const_sql_3790}"
    testFoldConst("${const_sql_3790}")
    qt_sql_3791_non_strict "${const_sql_3791}"
    testFoldConst("${const_sql_3791}")
    qt_sql_3792_non_strict "${const_sql_3792}"
    testFoldConst("${const_sql_3792}")
    qt_sql_3793_non_strict "${const_sql_3793}"
    testFoldConst("${const_sql_3793}")
    qt_sql_3794_non_strict "${const_sql_3794}"
    testFoldConst("${const_sql_3794}")
    qt_sql_3795_non_strict "${const_sql_3795}"
    testFoldConst("${const_sql_3795}")
    qt_sql_3796_non_strict "${const_sql_3796}"
    testFoldConst("${const_sql_3796}")
    qt_sql_3797_non_strict "${const_sql_3797}"
    testFoldConst("${const_sql_3797}")
    qt_sql_3798_non_strict "${const_sql_3798}"
    testFoldConst("${const_sql_3798}")
    qt_sql_3799_non_strict "${const_sql_3799}"
    testFoldConst("${const_sql_3799}")
    qt_sql_3800_non_strict "${const_sql_3800}"
    testFoldConst("${const_sql_3800}")
    qt_sql_3801_non_strict "${const_sql_3801}"
    testFoldConst("${const_sql_3801}")
    qt_sql_3802_non_strict "${const_sql_3802}"
    testFoldConst("${const_sql_3802}")
    qt_sql_3803_non_strict "${const_sql_3803}"
    testFoldConst("${const_sql_3803}")
    qt_sql_3804_non_strict "${const_sql_3804}"
    testFoldConst("${const_sql_3804}")
    qt_sql_3805_non_strict "${const_sql_3805}"
    testFoldConst("${const_sql_3805}")
    qt_sql_3806_non_strict "${const_sql_3806}"
    testFoldConst("${const_sql_3806}")
    qt_sql_3807_non_strict "${const_sql_3807}"
    testFoldConst("${const_sql_3807}")
    qt_sql_3808_non_strict "${const_sql_3808}"
    testFoldConst("${const_sql_3808}")
    qt_sql_3809_non_strict "${const_sql_3809}"
    testFoldConst("${const_sql_3809}")
    qt_sql_3810_non_strict "${const_sql_3810}"
    testFoldConst("${const_sql_3810}")
    qt_sql_3811_non_strict "${const_sql_3811}"
    testFoldConst("${const_sql_3811}")
    qt_sql_3812_non_strict "${const_sql_3812}"
    testFoldConst("${const_sql_3812}")
    qt_sql_3813_non_strict "${const_sql_3813}"
    testFoldConst("${const_sql_3813}")
    qt_sql_3814_non_strict "${const_sql_3814}"
    testFoldConst("${const_sql_3814}")
    qt_sql_3815_non_strict "${const_sql_3815}"
    testFoldConst("${const_sql_3815}")
    qt_sql_3816_non_strict "${const_sql_3816}"
    testFoldConst("${const_sql_3816}")
    qt_sql_3817_non_strict "${const_sql_3817}"
    testFoldConst("${const_sql_3817}")
    qt_sql_3818_non_strict "${const_sql_3818}"
    testFoldConst("${const_sql_3818}")
    qt_sql_3819_non_strict "${const_sql_3819}"
    testFoldConst("${const_sql_3819}")
    qt_sql_3820_non_strict "${const_sql_3820}"
    testFoldConst("${const_sql_3820}")
    qt_sql_3821_non_strict "${const_sql_3821}"
    testFoldConst("${const_sql_3821}")
    qt_sql_3822_non_strict "${const_sql_3822}"
    testFoldConst("${const_sql_3822}")
    qt_sql_3823_non_strict "${const_sql_3823}"
    testFoldConst("${const_sql_3823}")
    qt_sql_3824_non_strict "${const_sql_3824}"
    testFoldConst("${const_sql_3824}")
    qt_sql_3825_non_strict "${const_sql_3825}"
    testFoldConst("${const_sql_3825}")
    qt_sql_3826_non_strict "${const_sql_3826}"
    testFoldConst("${const_sql_3826}")
    qt_sql_3827_non_strict "${const_sql_3827}"
    testFoldConst("${const_sql_3827}")
    qt_sql_3828_non_strict "${const_sql_3828}"
    testFoldConst("${const_sql_3828}")
    qt_sql_3829_non_strict "${const_sql_3829}"
    testFoldConst("${const_sql_3829}")
    qt_sql_3830_non_strict "${const_sql_3830}"
    testFoldConst("${const_sql_3830}")
    qt_sql_3831_non_strict "${const_sql_3831}"
    testFoldConst("${const_sql_3831}")
    qt_sql_3832_non_strict "${const_sql_3832}"
    testFoldConst("${const_sql_3832}")
    qt_sql_3833_non_strict "${const_sql_3833}"
    testFoldConst("${const_sql_3833}")
    qt_sql_3834_non_strict "${const_sql_3834}"
    testFoldConst("${const_sql_3834}")
    qt_sql_3835_non_strict "${const_sql_3835}"
    testFoldConst("${const_sql_3835}")
    qt_sql_3836_non_strict "${const_sql_3836}"
    testFoldConst("${const_sql_3836}")
    qt_sql_3837_non_strict "${const_sql_3837}"
    testFoldConst("${const_sql_3837}")
    qt_sql_3838_non_strict "${const_sql_3838}"
    testFoldConst("${const_sql_3838}")
    qt_sql_3839_non_strict "${const_sql_3839}"
    testFoldConst("${const_sql_3839}")
    qt_sql_3840_non_strict "${const_sql_3840}"
    testFoldConst("${const_sql_3840}")
    qt_sql_3841_non_strict "${const_sql_3841}"
    testFoldConst("${const_sql_3841}")
    qt_sql_3842_non_strict "${const_sql_3842}"
    testFoldConst("${const_sql_3842}")
    qt_sql_3843_non_strict "${const_sql_3843}"
    testFoldConst("${const_sql_3843}")
    qt_sql_3844_non_strict "${const_sql_3844}"
    testFoldConst("${const_sql_3844}")
    qt_sql_3845_non_strict "${const_sql_3845}"
    testFoldConst("${const_sql_3845}")
    qt_sql_3846_non_strict "${const_sql_3846}"
    testFoldConst("${const_sql_3846}")
    qt_sql_3847_non_strict "${const_sql_3847}"
    testFoldConst("${const_sql_3847}")
    qt_sql_3848_non_strict "${const_sql_3848}"
    testFoldConst("${const_sql_3848}")
    qt_sql_3849_non_strict "${const_sql_3849}"
    testFoldConst("${const_sql_3849}")
    qt_sql_3850_non_strict "${const_sql_3850}"
    testFoldConst("${const_sql_3850}")
    qt_sql_3851_non_strict "${const_sql_3851}"
    testFoldConst("${const_sql_3851}")
    qt_sql_3852_non_strict "${const_sql_3852}"
    testFoldConst("${const_sql_3852}")
    qt_sql_3853_non_strict "${const_sql_3853}"
    testFoldConst("${const_sql_3853}")
    qt_sql_3854_non_strict "${const_sql_3854}"
    testFoldConst("${const_sql_3854}")
    qt_sql_3855_non_strict "${const_sql_3855}"
    testFoldConst("${const_sql_3855}")
    qt_sql_3856_non_strict "${const_sql_3856}"
    testFoldConst("${const_sql_3856}")
    qt_sql_3857_non_strict "${const_sql_3857}"
    testFoldConst("${const_sql_3857}")
    qt_sql_3858_non_strict "${const_sql_3858}"
    testFoldConst("${const_sql_3858}")
    qt_sql_3859_non_strict "${const_sql_3859}"
    testFoldConst("${const_sql_3859}")
    qt_sql_3860_non_strict "${const_sql_3860}"
    testFoldConst("${const_sql_3860}")
    qt_sql_3861_non_strict "${const_sql_3861}"
    testFoldConst("${const_sql_3861}")
    qt_sql_3862_non_strict "${const_sql_3862}"
    testFoldConst("${const_sql_3862}")
    qt_sql_3863_non_strict "${const_sql_3863}"
    testFoldConst("${const_sql_3863}")
    qt_sql_3864_non_strict "${const_sql_3864}"
    testFoldConst("${const_sql_3864}")
    qt_sql_3865_non_strict "${const_sql_3865}"
    testFoldConst("${const_sql_3865}")
    qt_sql_3866_non_strict "${const_sql_3866}"
    testFoldConst("${const_sql_3866}")
    qt_sql_3867_non_strict "${const_sql_3867}"
    testFoldConst("${const_sql_3867}")
    qt_sql_3868_non_strict "${const_sql_3868}"
    testFoldConst("${const_sql_3868}")
    qt_sql_3869_non_strict "${const_sql_3869}"
    testFoldConst("${const_sql_3869}")
    qt_sql_3870_non_strict "${const_sql_3870}"
    testFoldConst("${const_sql_3870}")
    qt_sql_3871_non_strict "${const_sql_3871}"
    testFoldConst("${const_sql_3871}")
    qt_sql_3872_non_strict "${const_sql_3872}"
    testFoldConst("${const_sql_3872}")
    qt_sql_3873_non_strict "${const_sql_3873}"
    testFoldConst("${const_sql_3873}")
    qt_sql_3874_non_strict "${const_sql_3874}"
    testFoldConst("${const_sql_3874}")
    qt_sql_3875_non_strict "${const_sql_3875}"
    testFoldConst("${const_sql_3875}")
    qt_sql_3876_non_strict "${const_sql_3876}"
    testFoldConst("${const_sql_3876}")
    qt_sql_3877_non_strict "${const_sql_3877}"
    testFoldConst("${const_sql_3877}")
    qt_sql_3878_non_strict "${const_sql_3878}"
    testFoldConst("${const_sql_3878}")
    qt_sql_3879_non_strict "${const_sql_3879}"
    testFoldConst("${const_sql_3879}")
    qt_sql_3880_non_strict "${const_sql_3880}"
    testFoldConst("${const_sql_3880}")
    qt_sql_3881_non_strict "${const_sql_3881}"
    testFoldConst("${const_sql_3881}")
    qt_sql_3882_non_strict "${const_sql_3882}"
    testFoldConst("${const_sql_3882}")
    qt_sql_3883_non_strict "${const_sql_3883}"
    testFoldConst("${const_sql_3883}")
    qt_sql_3884_non_strict "${const_sql_3884}"
    testFoldConst("${const_sql_3884}")
    qt_sql_3885_non_strict "${const_sql_3885}"
    testFoldConst("${const_sql_3885}")
    qt_sql_3886_non_strict "${const_sql_3886}"
    testFoldConst("${const_sql_3886}")
    qt_sql_3887_non_strict "${const_sql_3887}"
    testFoldConst("${const_sql_3887}")
    qt_sql_3888_non_strict "${const_sql_3888}"
    testFoldConst("${const_sql_3888}")
    qt_sql_3889_non_strict "${const_sql_3889}"
    testFoldConst("${const_sql_3889}")
    qt_sql_3890_non_strict "${const_sql_3890}"
    testFoldConst("${const_sql_3890}")
    qt_sql_3891_non_strict "${const_sql_3891}"
    testFoldConst("${const_sql_3891}")
    qt_sql_3892_non_strict "${const_sql_3892}"
    testFoldConst("${const_sql_3892}")
    qt_sql_3893_non_strict "${const_sql_3893}"
    testFoldConst("${const_sql_3893}")
    qt_sql_3894_non_strict "${const_sql_3894}"
    testFoldConst("${const_sql_3894}")
    qt_sql_3895_non_strict "${const_sql_3895}"
    testFoldConst("${const_sql_3895}")
    qt_sql_3896_non_strict "${const_sql_3896}"
    testFoldConst("${const_sql_3896}")
    qt_sql_3897_non_strict "${const_sql_3897}"
    testFoldConst("${const_sql_3897}")
    qt_sql_3898_non_strict "${const_sql_3898}"
    testFoldConst("${const_sql_3898}")
    qt_sql_3899_non_strict "${const_sql_3899}"
    testFoldConst("${const_sql_3899}")
    qt_sql_3900_non_strict "${const_sql_3900}"
    testFoldConst("${const_sql_3900}")
    qt_sql_3901_non_strict "${const_sql_3901}"
    testFoldConst("${const_sql_3901}")
    qt_sql_3902_non_strict "${const_sql_3902}"
    testFoldConst("${const_sql_3902}")
    qt_sql_3903_non_strict "${const_sql_3903}"
    testFoldConst("${const_sql_3903}")
    qt_sql_3904_non_strict "${const_sql_3904}"
    testFoldConst("${const_sql_3904}")
    qt_sql_3905_non_strict "${const_sql_3905}"
    testFoldConst("${const_sql_3905}")
    qt_sql_3906_non_strict "${const_sql_3906}"
    testFoldConst("${const_sql_3906}")
    qt_sql_3907_non_strict "${const_sql_3907}"
    testFoldConst("${const_sql_3907}")
    qt_sql_3908_non_strict "${const_sql_3908}"
    testFoldConst("${const_sql_3908}")
    qt_sql_3909_non_strict "${const_sql_3909}"
    testFoldConst("${const_sql_3909}")
    qt_sql_3910_non_strict "${const_sql_3910}"
    testFoldConst("${const_sql_3910}")
    qt_sql_3911_non_strict "${const_sql_3911}"
    testFoldConst("${const_sql_3911}")
    qt_sql_3912_non_strict "${const_sql_3912}"
    testFoldConst("${const_sql_3912}")
    qt_sql_3913_non_strict "${const_sql_3913}"
    testFoldConst("${const_sql_3913}")
    qt_sql_3914_non_strict "${const_sql_3914}"
    testFoldConst("${const_sql_3914}")
    qt_sql_3915_non_strict "${const_sql_3915}"
    testFoldConst("${const_sql_3915}")
    qt_sql_3916_non_strict "${const_sql_3916}"
    testFoldConst("${const_sql_3916}")
    qt_sql_3917_non_strict "${const_sql_3917}"
    testFoldConst("${const_sql_3917}")
    qt_sql_3918_non_strict "${const_sql_3918}"
    testFoldConst("${const_sql_3918}")
    qt_sql_3919_non_strict "${const_sql_3919}"
    testFoldConst("${const_sql_3919}")
    qt_sql_3920_non_strict "${const_sql_3920}"
    testFoldConst("${const_sql_3920}")
    qt_sql_3921_non_strict "${const_sql_3921}"
    testFoldConst("${const_sql_3921}")
    qt_sql_3922_non_strict "${const_sql_3922}"
    testFoldConst("${const_sql_3922}")
    qt_sql_3923_non_strict "${const_sql_3923}"
    testFoldConst("${const_sql_3923}")
    qt_sql_3924_non_strict "${const_sql_3924}"
    testFoldConst("${const_sql_3924}")
    qt_sql_3925_non_strict "${const_sql_3925}"
    testFoldConst("${const_sql_3925}")
    qt_sql_3926_non_strict "${const_sql_3926}"
    testFoldConst("${const_sql_3926}")
    qt_sql_3927_non_strict "${const_sql_3927}"
    testFoldConst("${const_sql_3927}")
    qt_sql_3928_non_strict "${const_sql_3928}"
    testFoldConst("${const_sql_3928}")
    qt_sql_3929_non_strict "${const_sql_3929}"
    testFoldConst("${const_sql_3929}")
    qt_sql_3930_non_strict "${const_sql_3930}"
    testFoldConst("${const_sql_3930}")
    qt_sql_3931_non_strict "${const_sql_3931}"
    testFoldConst("${const_sql_3931}")
    qt_sql_3932_non_strict "${const_sql_3932}"
    testFoldConst("${const_sql_3932}")
    qt_sql_3933_non_strict "${const_sql_3933}"
    testFoldConst("${const_sql_3933}")
    qt_sql_3934_non_strict "${const_sql_3934}"
    testFoldConst("${const_sql_3934}")
    qt_sql_3935_non_strict "${const_sql_3935}"
    testFoldConst("${const_sql_3935}")
    qt_sql_3936_non_strict "${const_sql_3936}"
    testFoldConst("${const_sql_3936}")
    qt_sql_3937_non_strict "${const_sql_3937}"
    testFoldConst("${const_sql_3937}")
    qt_sql_3938_non_strict "${const_sql_3938}"
    testFoldConst("${const_sql_3938}")
    qt_sql_3939_non_strict "${const_sql_3939}"
    testFoldConst("${const_sql_3939}")
}