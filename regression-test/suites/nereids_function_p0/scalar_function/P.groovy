suite("nereids_scalar_fn_P") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_pmod_BigInt_BigInt "select pmod(kbint, kbint) from fn_test order by kbint, kbint"
	qt_sql_pmod_BigInt_BigInt "select pmod(kbint, kbint) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_pmod_Double_Double "select pmod(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_pmod_Double_Double "select pmod(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	qt_sql_positive_BigInt "select positive(kbint) from fn_test order by kbint"
	qt_sql_positive_BigInt "select positive(kbint) from fn_test_not_nullable order by kbint"
	qt_sql_positive_Double "select positive(kdbl) from fn_test order by kdbl"
	qt_sql_positive_Double "select positive(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_positive_DecimalV2 "select positive(kdcmls1) from fn_test order by kdcmls1"
	qt_sql_positive_DecimalV2 "select positive(kdcmls1) from fn_test_not_nullable order by kdcmls1"
	qt_sql_pow_Double_Double "select pow(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_pow_Double_Double "select pow(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	qt_sql_power_Double_Double "select power(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_power_Double_Double "select power(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	qt_sql_protocol_String "select protocol(kstr) from fn_test order by kstr"
	qt_sql_protocol_String "select protocol(kstr) from fn_test_not_nullable order by kstr"
}