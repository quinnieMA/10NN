// Define the base path to OneDrive
global ONEDRIVE_PATH "D:/OneDrive"  // Change this to match your OneDrive path

// Define the base path to OneDrive
global ONEDRIVE_PATH "D:/OneDrive"  // Change this to match your OneDrive path

use "${ONEDRIVE_PATH}/MA/acquisition/multiple/acq_mul.dta", clear
sort deal_num 
merge 1:1 deal_num using "${ONEDRIVE_PATH}\MA\acquisition\structure_date\acq_status"
keep if _merge==3
drop _merge

sort deal_num
merge 1:1 deal_num using "${ONEDRIVE_PATH}/MA/acquisition/value/acq_value.dta",force
keep if _merge==3
drop _merge

sort deal_num
merge 1:1 deal_num using "${ONEDRIVE_PATH}/MA/acquisition/structure_date/acq_d.dta"
keep if _merge==3
drop _merge
gen length= assumed_comp_d_yr - announced_d_yr
drop  rumour_d_yr expected_comp_d_yr assumed_comp_d_yr completed_d_yr postponed_d_yr withdrawn_d_yr

save "${ONEDRIVE_PATH}\MA\acquisition\output\mergedtable_deal.dta",replace
/////////////////////////////////////////////////
use "${ONEDRIVE_PATH}\MA\acquisition\output\mergedtable_deal.dta",clear
sort deal_num 
merge 1:m deal_num using "${ONEDRIVE_PATH}/MA/acquisition/financial/acq_fin.dta"
drop _merge

sort deal_num tar_name tar_bvd_id_num tar_orbis_id_num acq_name acq_bvd_id_num acq_orbis_id_num ven_name ven_bvd_id_num ven_orbis_id_num

merge 1:1 deal_num tar_name tar_bvd_id_num tar_orbis_id_num acq_name acq_bvd_id_num acq_orbis_id_num ven_name ven_bvd_id_num ven_orbis_id_num using "${ONEDRIVE_PATH}/MA/acquisition/financial/acq_com_fin.dta"
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
save "${ONEDRIVE_PATH}/MA/acquisition/output/mergedtable_deal_com.dta", replace

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
use "${ONEDRIVE_PATH}/MA/acquisition/output/mergedtable_deal_com.dta", clear
sort deal_num
merge m:1 deal_num using "${ONEDRIVE_PATH}\MA\acquisition\legal\acq_legal.dta",keepusing(tar_incorp_d_year acq_incorp_d_year ven_incorp_d_year)
keep if _merge==3
drop _merge
gen tar_age=announced_d_yr-tar_incorp_d_year
gen acq_age=announced_d_yr-acq_incorp_d_year
gen ven_age=announced_d_yr-ven_incorp_d_year
drop tar_incorp_d_year acq_incorp_d_year ven_incorp_d_year 
save "${ONEDRIVE_PATH}/MA/acquisition/output/mergedtable.dta", replace



use "${ONEDRIVE_PATH}\MA\acquisition\output\mergedtable.dta", clear
* 正确的方法：分别独立统计deal_count和acq_count
preserve

* 步骤1：统计每个国家的不重复交易数量
keep tar_country_code deal_num
duplicates drop  // 去除重复的国家-交易组合
bysort tar_country_code: gen deal_count = _N
bysort tar_country_code: keep if _n == 1
keep tar_country_code deal_count
save temp_deal_count, replace
restore

preserve
* 步骤2：统计每个国家的不重复收购公司数量（在整个国家范围内去重）
keep tar_country_code acq_bvd_id_num
duplicates drop  // 去除重复的国家-收购公司组合
bysort tar_country_code: gen acq_count = _N
bysort tar_country_code: keep if _n == 1
keep tar_country_code acq_count
save temp_acq_count, replace
restore

* 合并结果
use temp_deal_count, clear
merge 1:1 tar_country_code using temp_acq_count
drop _merge

* 按交易数量排序
gsort -deal_count

* 显示结果
list tar_country_code deal_count acq_count, clean noobs abbreviate(15)

* 清理临时文件
cap erase temp_deal_count.dta
cap erase temp_acq_count.dta



global ONEDRIVE_PATH "D:/OneDrive"  // Change this to match your OneDrive path

use "${ONEDRIVE_PATH}/MA/acquisition/multiple/acq_mul.dta", clear
sort deal_num
merge 1:1 deal_num using "${ONEDRIVE_PATH}/MA/acquisition/structure_date/acq_d.dta",keepusing(rumour_d_yr announced_d_yr expected_comp_d_yr assumed_comp_d_yr completed_d_yr postponed_d_yr withdrawn_d_yr)
keep if _merge==3
drop _merge
gen length= assumed_comp_d_yr - announced_d_yr
drop  rumour_d_yr expected_comp_d_yr assumed_comp_d_yr completed_d_yr postponed_d_yr withdrawn_d_yr

* 1. 统计每年的不重复deal_num数量
preserve
keep announced_d_yr deal_num
duplicates drop
bysort announced_d_yr: gen deal_count = _N
bysort announced_d_yr: keep if _n == 1
keep announced_d_yr deal_count
save temp_deal_count, replace
restore

* 2. 统计每年的不重复pre_ebitda_mul_ly数量
preserve
keep announced_d_yr pre_ebitda_mul_ly
drop if missing(pre_ebitda_mul_ly)
duplicates drop
bysort announced_d_yr: gen ebitda_count = _N
bysort announced_d_yr: keep if _n == 1
keep announced_d_yr ebitda_count
save temp_ebitda_count, replace
restore

* 3. 合并结果并保存为dta文件
use temp_deal_count, clear
merge 1:1 announced_d_yr using temp_ebitda_count
drop _merge

* 计算覆盖率和比例
gen coverage_rate = ebitda_count / deal_count * 100
gen coverage_pct = round(coverage_rate, 0.1)
label variable announced_d_yr "Announcement Year"
label variable deal_count "Number of Deals"
label variable ebitda_count "EBITDA Multiple Observations"
label variable coverage_rate "Coverage Rate (%)"

* 按年份排序
gsort announced_d_yr

* 保存结果
save "${ONEDRIVE_PATH}/MA/acquisition/deal_ebitda_stats.dta", replace

* 清理临时文件
cap erase temp_deal_count.dta
cap erase temp_ebitda_count.dta

* 简单列表展示
use "${ONEDRIVE_PATH}/MA/acquisition/deal_ebitda_stats.dta", clear
list announced_d_yr deal_count ebitda_count coverage_pct, clean noobs
