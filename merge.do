// Define the base path to OneDrive
global ONEDRIVE_PATH "D:/OneDrive"  // Change this to match your OneDrive path

use "${ONEDRIVE_PATH}/MA/acquisition/multiple/acq_mul.dta", clear
sort deal_num
merge 1:1 deal_num using "${ONEDRIVE_PATH}/MA/acquisition/structure_date/acq_d.dta",keepusing(rumour_d_yr announced_d_yr expected_comp_d_yr assumed_comp_d_yr completed_d_yr postponed_d_yr withdrawn_d_yr)
keep if _merge==3
drop _merge
gen length= assumed_comp_d_yr - announced_d_yr
drop  rumour_d_yr expected_comp_d_yr assumed_comp_d_yr completed_d_yr postponed_d_yr withdrawn_d_yr

sort deal_num 
merge 1:1 deal_num using "${ONEDRIVE_PATH}\MA\acquisition\structure_date\acq_status"
keep if _merge==3
drop _merge

sort deal_num
merge 1:1 deal_num using "${ONEDRIVE_PATH}/MA/acquisition/value/acq_value_with_ln.dta",keepusing( ln_value ln_value_native_currency ln_equity_value ln_equity_value_native_currenc ln_enterprise_value ln_enterprise_value_native_cur ln_modelled_enterprise_value ln_modelled_enterprise_value_n ln_total_target_value ln_total_target_value_native_c)
keep if _merge==3
drop _merge


save "${ONEDRIVE_PATH}\MA\acquisition\output\mergedtemp_deal.dta",replace
/////////////////////////////////////////////////
use "${ONEDRIVE_PATH}\MA\acquisition\output\mergedtemp_deal.dta",clear
sort deal_num 
merge 1:m deal_num using "${ONEDRIVE_PATH}/MA/acquisition/financial/acq_fin_with_ln.dta",keepusing(tar_name tar_bvd_id_num tar_orbis_id_num acq_name acq_bvd_id_num acq_orbis_id_num ven_name ven_bvd_id_num ven_orbis_id_num ln_pre_t_rev ln_pre_t_ebd ln_pre_t_ebt ln_pre_t_pbt ln_pre_t_pat ln_pre_t_np ln_pre_t_ta ln_pre_t_ta1 ln_pre_t_ta2 ln_pre_t_ta3 ln_pre_t_ta4 ln_pre_a_rev ln_pre_a_ebd ln_pre_a_ebt ln_pre_a_pbt ln_pre_a_pat ln_pre_a_np ln_pre_a_ta ln_pre_a_na ln_pre_a_cl ln_pre_a_eq ln_pre_a_cap ln_pre_v_rev ln_pre_v_ebd ln_pre_v_ebt ln_pre_v_pbt ln_pre_v_pat ln_pre_v_np ln_pre_v_ta ln_pre_v_na ln_pre_v_cl ln_pre_v_eq ln_pre_v_cap ln_post_t_rev ln_post_t_ebd ln_post_t_ebt ln_post_t_pbt ln_post_t_pat ln_post_t_np ln_post_t_ta ln_post_t_ta1 ln_post_t_ta2 ln_post_t_ta3 ln_post_t_ta4 ln_post_a_rev ln_post_a_ebd ln_post_a_ebt ln_post_a_pbt ln_post_a_pat ln_post_a_np ln_post_a_ta ln_post_a_na ln_post_a_sh ln_post_a_cap ln_post_v_rev ln_post_v_ebd ln_post_v_ebt ln_post_v_pbt ln_post_v_pat ln_post_v_np ln_post_v_ta ln_post_v_na ln_post_v_sh ln_post_v_cap)
keep if _merge==3
drop _merge

sort deal_num tar_name tar_bvd_id_num tar_orbis_id_num acq_name acq_bvd_id_num acq_orbis_id_num ven_name ven_bvd_id_num ven_orbis_id_num

merge 1:1 deal_num tar_name tar_bvd_id_num tar_orbis_id_num acq_name acq_bvd_id_num acq_orbis_id_num ven_name ven_bvd_id_num ven_orbis_id_num using "${ONEDRIVE_PATH}/MA/acquisition/financial/acq_com_fin_with_ln.dta",keepusing(ln_t_rev_0 ln_t_rev_1 ln_t_rev_2 ln_t_ebd_0 ln_t_ebd_1 ln_t_ebd_2 ln_t_ebt_0 ln_t_ebt_1 ln_t_ebt_2 ln_t_pbt_0 ln_t_pbt_1 ln_t_pbt_2 ln_t_pat_0 ln_t_pat_1 ln_t_pat_2 ln_t_np_0 ln_t_np_1 ln_t_np_2 ln_t_ta_0 ln_t_ta_1 ln_t_ta_2 ln_t_na_0 ln_t_na_1 ln_t_na_2 ln_t_eq_0 ln_t_eq_1 ln_t_eq_2 ln_t_cap_0 ln_t_cap_1 ln_t_cap_2 ln_t_emp_0 ln_t_emp_1 ln_t_emp_2 ln_t_ev_0 ln_t_ev_1 ln_t_ev_2 ln_t_eps_0 ln_t_eps_1 ln_t_eps_2 ln_t_cfps_0 ln_t_cfps_1 ln_t_cfps_2 ln_t_dps_0 ln_t_dps_1 ln_t_dps_2 ln_t_bvps_0 ln_t_bvps_1 ln_t_bvps_2 ln_a_rev_0 ln_a_rev_1 ln_a_rev_2 ln_a_ebd_0 ln_a_ebd_1 ln_a_ebd_2 ln_a_ebt_0 ln_a_ebt_1 ln_a_ebt_2 ln_a_pbt_0 ln_a_pbt_1 ln_a_pbt_2 ln_a_pat_0 ln_a_pat_1 ln_a_pat_2 ln_a_np_0 ln_a_np_1 ln_a_np_2 ln_a_ta_0 ln_a_ta_1 ln_a_ta_2 ln_a_na_0 ln_a_na_1 ln_a_na_2 ln_a_eq_0 ln_a_eq_1 ln_a_eq_2 ln_a_cap_0 ln_a_cap_1 ln_a_cap_2 ln_a_emp_0 ln_a_emp_1 ln_a_emp_2 ln_a_ev_0 ln_a_ev_1 ln_a_ev_2 ln_a_eps_0 ln_a_eps_1 ln_a_eps_2 ln_a_cfps_0 ln_a_cfps_1 ln_a_cfps_2 ln_a_dps_0 ln_a_dps_1 ln_a_dps_2 ln_a_bvps_0 ln_a_bvps_1 ln_a_bvps_2 ln_v_rev_0 ln_v_rev_1 ln_v_rev_2 ln_v_ebd_0 ln_v_ebd_1 ln_v_ebd_2 ln_v_ebt_0 ln_v_ebt_1 ln_v_ebt_2 ln_v_pbt_0 ln_v_pbt_1 ln_v_pbt_2 ln_v_pat_0 ln_v_pat_1 ln_v_pat_2 ln_v_np_0 ln_v_np_1 ln_v_np_2 ln_v_ta_0 ln_v_ta_1 ln_v_ta_2 ln_v_na_0 ln_v_na_1 ln_v_na_2 ln_v_eq_0 ln_v_eq_1 ln_v_eq_2 ln_v_cap_0 ln_v_cap_1 ln_v_cap_2 ln_v_emp_0 ln_v_emp_1 ln_v_emp_2 ln_v_ev_0 ln_v_ev_1 ln_v_ev_2 ln_v_eps_0 ln_v_eps_1 ln_v_eps_2 ln_v_cfps_0 ln_v_cfps_1 ln_v_cfps_2 ln_v_dps_0 ln_v_dps_1 ln_v_dps_2 ln_v_bvps_0 ln_v_bvps_1 ln_v_bvps_2)
keep if _merge==3
drop _merge

duplicates tag deal_num tar_name tar_bvd_id_num tar_orbis_id_num acq_name acq_bvd_id_num acq_orbis_id_num ven_name ven_bvd_id_num ven_orbis_id_num, gen(dup)
tab dup
list tar_name acq_name ven_name if dup > 0, abbreviate(5)
bysort deal_num tar_name tar_bvd_id_num tar_orbis_id_num acq_name acq_bvd_id_num acq_orbis_id_num ven_name ven_bvd_id_num ven_orbis_id_num: keep if _n == 1
drop dup


sort deal_num tar_name tar_bvd_id_num tar_orbis_id_num
merge m:1 deal_num tar_name tar_bvd_id_num tar_orbis_id_num using "${ONEDRIVE_PATH}/MA/acquisition/industry/acq_tar_ind.dta",keepusing(tar_name tar_bvd_id_num tar_orbis_id_num tar_primary_sic_code)
keep if _merge==3
drop _merge
sort deal_num tar_name 
merge m:1 deal_num tar_name using "${ONEDRIVE_PATH}/MA/acquisition/overview/acq_tar_country.dta",keepusing(tar_name tar_bvd_id_num tar_orbis_id_num tar_country_code)
keep if _merge==3
drop _merge

sort deal_num acq_name acq_bvd_id_num acq_orbis_id_num
merge m:1 deal_num acq_name acq_bvd_id_num acq_orbis_id_num using "${ONEDRIVE_PATH}/MA/acquisition/industry/acq_acq_ind.dta",keepusing(acq_name acq_bvd_id_num acq_orbis_id_num acq_primary_sic_code)
keep if _merge==3
drop _merge

sort deal_num acq_name 
merge m:1 deal_num acq_name using "${ONEDRIVE_PATH}/MA/acquisition/overview/acq_acq_country.dta",keepusing(acq_name acq_country_code acq_bvd_id_num acq_orbis_id_num)
keep if _merge==3
drop _merge
save "${ONEDRIVE_PATH}/MA/acquisition/output/mergedtemp_deal_com.dta", replace

//////////////////////////
sort deal_num ven_name ven_bvd_id_num ven_orbis_id_num
merge m:1 deal_num ven_name ven_bvd_id_num ven_orbis_id_num using "${ONEDRIVE_PATH}/MA/acquisition/industry/acq_ven_ind.dta",keepusing(ven_name ven_bvd_id_num ven_orbis_id_num ven_primary_sic_code)
keep if _merge==3
drop _merge

sort deal_num ven_name 
merge m:1 deal_num ven_name using "${ONEDRIVE_PATH}/MA/acquisition/overview/acq_ven_country.dta",keepusing(ven_name ven_bvd_id_num ven_orbis_id_num ven_country_code)
keep if _merge==3
drop _merge
//////////////////////////
use "${ONEDRIVE_PATH}/MA/acquisition/output/mergedtemp_deal_com.dta", clear
sort deal_num
merge m:1 deal_num using "${ONEDRIVE_PATH}\MA\acquisition\legal\acq_legal.dta",keepusing(tar_incorp_d_year acq_incorp_d_year ven_incorp_d_year)
keep if _merge==3
drop _merge
gen tar_age=announced_d_yr-tar_incorp_d_year
gen acq_age=announced_d_yr-acq_incorp_d_year
gen ven_age=announced_d_yr-ven_incorp_d_year
drop tar_incorp_d_year acq_incorp_d_year ven_incorp_d_year 
save "${ONEDRIVE_PATH}/MA/acquisition/output/mergedtemp.dta", replace

////////////////////////
use "${ONEDRIVE_PATH}/MA/acquisition/output/mergedtemp.dta",clear
sort deal_num 
merge m:1 deal_num  using "${ONEDRIVE_PATH}\MA\acquisition\structure_date\acq_pay_method_val_S.dta"
keep if _merge==3
drop _merge

sort deal_num deal_struct deal_pay_method deal_pay_method_val_usd
merge m:1 deal_num  deal_struct deal_pay_method deal_pay_method_val_usd using "${ONEDRIVE_PATH}\MA\acquisition\structure_date\acq_stru.dta"
keep if _merge==3
drop _merge


save "${ONEDRIVE_PATH}/MA/acquisition/output/mergedtemp_S.dta", replace



