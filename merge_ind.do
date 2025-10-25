// Define the base path to OneDrive
global ONEDRIVE_PATH "D:/OneDrive"  // Change this to match your OneDrive path
/*total 19380 D:\OneDrive\MA\acquisition\NLP_tar_overview\ind_similarity_yr_adjusted.py */
import delimited ${ONEDRIVE_PATH}/MA/acquisition/similarity_results/target_top10_neighbors.csv, bindquote(strict) clear 
generate deal_num = substr(neighbor_target, 11, .)

* 整理代码：合并行业数据并计算中位数乘数

* 首先处理行业数据合并
sort deal_num
merge m:1 deal_num using "${ONEDRIVE_PATH}/MA/acquisition/industry/acq_tar_ind_to_merge.dta", ///
    keepusing(tar_primary_sic_code)

* 保留匹配成功的观测值
keep if _merge == 3
drop _merge

* 重命名变量以区分邻居和目标
rename tar_primary_sic_code neighbor_tar_primary_sic_code
drop deal_num

* 从source_target中提取deal_num
generate deal_num = substr(source_target, 11, .)

* 再次合并获取源目标的行业代码
merge m:1 deal_num using "${ONEDRIVE_PATH}/MA/acquisition/industry/acq_tar_ind_to_merge.dta", ///
    keepusing(tar_primary_sic_code)
keep if _merge == 3
drop _merge

* 重命名变量
rename tar_primary_sic_code source_tar_primary_sic_code

* 将数值型SIC代码转换为字符串进行比较
tostring neighbor_tar_primary_sic_code, gen(neighbor_sic_str)
tostring source_tar_primary_sic_code, gen(source_sic_str)

* 提取SIC代码的前三位进行比较
gen neighbor_sic3 = substr(neighbor_sic_str, 1, 3)
gen source_sic3 = substr(source_sic_str, 1, 3)

* 创建比较变量
gen sic3_match = (neighbor_sic3 == source_sic3) if !missing(neighbor_sic3, source_sic3)

* 统计相同和不同的数量
tab sic3_match


* 清理临时变量
*drop neighbor_sic_str source_sic_str neighbor_sic3 source_sic3

* 合并乘数数据
sort deal_num
merge m:1 deal_num using "${ONEDRIVE_PATH}/MA/acquisition/multiple/acq_mul.dta", ///
    
* 保留匹配成功的观测值
keep if _merge == 3
drop _merge

* 计算每个deal_num的EBITDA乘数中位数
bysort source_sic3  year: egen median_pre_ebitda_mul_ly = median(pre_ebitda_mul_ly)
bysort neighbor_sic3 year: egen median_pre_ebitda_mul_ly_1 = median(pre_ebitda_mul_ly)


winsor2 pre_ebitda_mul_ly, suffix(_w) cuts(1 99)
gen diff= pre_ebitda_mul_ly_w - median_pre_ebitda_mul_ly
gen diff1= pre_ebitda_mul_ly_w - median_pre_ebitda_mul_ly_1
* 保留每个deal_num的第一条观测值
bysort deal_num: keep if _n == 1

* 保留所需变量
keep diff diff1 deal_num year pre_rev_mul_ly pre_ebitda_mul_ly pre_ebitda_mul_ly_w pre_ebit_mul_ly pre_pbt_mul_ly pre_pat_mul_ly pre_np_mul_ly pre_ta_mul_ly pre_na_mul_ly pre_cl_mul_ly pre_eq_mul_ly pre_cap_mul_ly post_rev_mul_fy post_ebitda_mul_fy post_ebit_mul_fy post_pbt_mul_fy post_pat_mul_fy post_np_mul_fy post_ta_mul_fy post_na_mul_fy post_cl_mul_fy post_eq_mul_fy post_cap_mul_fy  neighbor_sic3 source_sic3 source_tar_primary_sic_code neighbor_tar_primary_sic_code median_pre_ebitda_mul_ly median_pre_ebitda_mul_ly_1

* 保存结果
save "D:\OneDrive\MA\acquisition\industry\ovneighbor_ind.dta", replace

use "D:\OneDrive\MA\acquisition\industry\ovneighbor_ind.dta",clear

sort deal_num 
merge 1:1 deal_num using "${ONEDRIVE_PATH}\MA\dta\count.dta"
keep if _merge==3
drop _merge


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

save "${ONEDRIVE_PATH}\MA\dta\indmerge_deal.dta",replace
/////////////////////////////////////////////////
use "${ONEDRIVE_PATH}\MA\dta\indmerge_deal.dta",clear
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
save "${ONEDRIVE_PATH}/MA/dta/indmerge_deal_com.dta", replace

gen temp = (tar_country_code != acq_country_code) if !missing(tar_country_code, acq_country_code)
replace temp = 0 if missing(temp)
sort deal_num
by deal_num : egen crossborder=sum(temp)
replace crossborder=1 if crossborder!=0
bysort  deal_num  tar_bvd_id_num tar_orbis_id_num acq_bvd_id_num acq_orbis_id_num :keep if _n == 1
save "${ONEDRIVE_PATH}/MA/dta/crossborder.dta",replace
//////////////////////////
use "${ONEDRIVE_PATH}/MA/dta/indmerge_deal_com.dta", clear
sort deal_num
merge m:1 deal_num using "${ONEDRIVE_PATH}\MA\acquisition\legal\acq_legal.dta",keepusing(tar_incorp_d_year acq_incorp_d_year ven_incorp_d_year)
keep if _merge==3
drop _merge
gen tar_age=announced_d_yr-tar_incorp_d_year
gen acq_age=announced_d_yr-acq_incorp_d_year
gen ven_age=announced_d_yr-ven_incorp_d_year
drop tar_incorp_d_year acq_incorp_d_year ven_incorp_d_year 
save "${ONEDRIVE_PATH}/MA/dta/ind_merged1.dta", replace


////////////////////////
* 定义支付方式列表
local payment_types "Dividend Cash Debt Earnout Stock Other_Assets"

* 初始化一个空的数据集用于append
clear
save "${ONEDRIVE_PATH}/MA/dta/ind_merged_all_payments.dta", replace emptyok

* 循环处理每个支付方式
foreach pay in `payment_types' {
    di "Processing payment type: `pay'"
    
    * 加载主数据集
    use "${ONEDRIVE_PATH}/MA/dta/ind_merged1.dta", clear
    sort deal_num 
    
    * 合并支付方式子集
    merge m:1 deal_num using "${ONEDRIVE_PATH}\MA\dta\payment_`pay'.dta"
    keep if _merge == 3
    drop _merge
    
    * 添加支付方式标识变量
    *gen payment_type = "`pay'"
    * 保存单个支付方式文件
    save "${ONEDRIVE_PATH}/MA/dta/ind_merged_payment_`pay'.dta", replace
    
    * 添加到合并数据集
    append using "${ONEDRIVE_PATH}/MA/dta/ind_merged_all_payments.dta"
    save "${ONEDRIVE_PATH}/MA/dta/ind_merged_all_payments.dta", replace
    
    di "Completed: `pay' - `_N' total observations so far"
}

erase ${ONEDRIVE_PATH}\MA\dta\indmerge_deal.dta
erase ${ONEDRIVE_PATH}\MA\dta\indmerge_deal_com.dta


///////////////////////////////////////
use "${ONEDRIVE_PATH}/MA/dta/ind_merged_all_payments.dta",clear
* 计算目标公司财务比率（统一用总资产标准化）

* 1. 盈利能力比率
gen tar_profit_margin_ly = tar_pat_last_avail_yr / tar_rev_rev_last_avail_yr if tar_rev_rev_last_avail_yr > 0
gen tar_profit_margin_y1 = tar_pat_yr__1 / tar_rev_rev_yr__1 if tar_rev_rev_yr__1 > 0
gen tar_profit_margin_y2 = tar_pat_yr__2 / tar_rev_rev_yr__2 if tar_rev_rev_yr__2 > 0

* 2. 资产收益率 (ROA)
gen tar_roa_ly =  tar_rev_rev_last_avail_yr / tar_ta_last_avail_yr if tar_ta_last_avail_yr > 0
gen tar_roa_y1 = tar_rev_rev_yr__1 / tar_ta_yr__1 if tar_ta_yr__1 > 0
gen tar_roa_y2 = tar_rev_rev_yr__2 / tar_ta_yr__2 if tar_ta_yr__2 > 0

* 3. 杠杆比率 (负债/总资产)
gen tar_leverage_ly = (tar_ta_last_avail_yr - tar_eq_last_avail_yr) / tar_ta_last_avail_yr if tar_ta_last_avail_yr > 0
gen tar_leverage_y1 = (tar_ta_yr__1 - tar_eq_yr__1) / tar_ta_yr__1 if tar_ta_yr__1 > 0
gen tar_leverage_y2 = (tar_ta_yr__2 - tar_eq_yr__2) / tar_ta_yr__2 if tar_ta_yr__2 > 0

* 4. EBITDA利润率
gen tar_ebitda_margin_ly = tar_ebitda_last_avail_yr / tar_rev_rev_last_avail_yr if tar_rev_rev_last_avail_yr > 0
gen tar_ebitda_margin_y1 = tar_ebitda_yr__1 / tar_rev_rev_yr__1 if tar_rev_rev_yr__1 > 0
gen tar_ebitda_margin_y2 = tar_ebitda_yr__2 / tar_rev_rev_yr__2 if tar_rev_rev_yr__2 > 0

* 5. 资产周转率
gen tar_asset_turnover_ly = tar_rev_rev_last_avail_yr / tar_ta_last_avail_yr if tar_ta_last_avail_yr > 0
gen tar_asset_turnover_y1 = tar_rev_rev_yr__1 / tar_ta_yr__1 if tar_ta_yr__1 > 0
gen tar_asset_turnover_y2 = tar_rev_rev_yr__2 / tar_ta_yr__2 if tar_ta_yr__2 > 0

* 6. 流动比率 (如果有流动资产数据)
* gen tar_current_ratio_ly = tar_current_assets_ly / tar_current_liabilities_ly

* 7. 人均指标
gen tar_rev_per_emp_ly = tar_rev_rev_last_avail_yr / tar_emp_last_avail_yr if tar_emp_last_avail_yr > 0
gen tar_rev_per_emp_y1 = tar_rev_rev_yr__1 / tar_emp_yr__1 if tar_emp_yr__1 > 0
gen tar_rev_per_emp_y2 = tar_rev_rev_yr__2 / tar_emp_yr__2 if tar_emp_yr__2 > 0

* 7.5 size指标
gen tar_ta = ln(tar_ta_last_avail_yr) if tar_ta_last_avail_yr > 0
gen tar_ta_y1 = ln(tar_ta_yr__1) if tar_ta_yr__1 > 0
gen tar_ta_y2 = ln(tar_ta_yr__2) if tar_ta_yr__2 > 0
gen tar_emp = ln(tar_emp_last_avail_yr) if tar_emp_last_avail_yr > 0
gen tar_emp_y1 = ln(tar_emp_yr__1) if tar_emp_yr__1 > 0
gen tar_emp_y2 = ln(tar_emp_yr__2) if tar_emp_yr__2 > 0

* 8. 估值比率 (如果EV数据可用)
gen tar_cap = ln(tar_cap_last_avail_yr) if tar_cap_last_avail_yr > 0
gen tar_cap_y1 = ln(tar_cap_yr__1) if tar_cap_yr__1 > 0
gen tar_cap_y2 = ln(tar_cap_yr__2) if tar_cap_yr__2 > 0
gen tar_ev = ln(tar_ev_last_avail_yr) if tar_ev_last_avail_yr > 0
gen tar_ev_y1 = ln(tar_ev_yr__1) if tar_ev_yr__1 > 0
gen tar_ev_y2 = ln(tar_ev_yr__2) if tar_ev_yr__2 > 0

*gen tar_ev_to_rev_ly = tar_ev_last_avail_yr / tar_rev_rev_last_avail_yr if tar_rev_rev_last_avail_yr > 0
*gen tar_ev_to_ebitda_ly = tar_ev_last_avail_yr / tar_ebitda_last_avail_yr if tar_ebitda_last_avail_yr > 0


* 9. 检查计算结果的统计特征
sum tar_profit_margin_ly tar_roa_ly tar_leverage_ly tar_ebitda_margin_ly tar_asset_turnover_ly

* 10. 标签变量
label variable tar_profit_margin_ly "Target Profit Margin (Last Year)"
label variable tar_roa_ly "Target ROA (Last Year)" 
label variable tar_leverage_ly "Target Leverage Ratio (Last Year)"
label variable tar_ebitda_margin_ly "Target EBITDA Margin (Last Year)"
label variable tar_asset_turnover_ly "Target Asset Turnover (Last Year)"
label variable tar_rev_per_emp_ly "Target Revenue per Employee (Last Year)"


* 计算acq公司财务比率（统一用总资产标准化）

* 1. 盈利能力比率
gen acq_profit_margin_ly = acq_pat_last_avail_yr / acq_rev_rev_last_avail_yr if acq_rev_rev_last_avail_yr > 0
gen acq_profit_margin_y1 = acq_pat_yr__1 / acq_rev_rev_yr__1 if acq_rev_rev_yr__1 > 0
gen acq_profit_margin_y2 = acq_pat_yr__2 / acq_rev_rev_yr__2 if acq_rev_rev_yr__2 > 0

* 2. 资产收益率 (ROA)
gen acq_roa_ly =  acq_rev_rev_last_avail_yr/ acq_ta_last_avail_yr if acq_ta_last_avail_yr > 0
gen acq_roa_y1 =  acq_rev_rev_yr__1 / acq_ta_yr__1 if acq_ta_yr__1 > 0
gen acq_roa_y2 =  acq_rev_rev_yr__2 / acq_ta_yr__2 if acq_ta_yr__2 > 0

* 3. 杠杆比率 (负债/总资产)
gen acq_leverage_ly = (acq_ta_last_avail_yr - acq_eq_last_avail_yr) / acq_ta_last_avail_yr if acq_ta_last_avail_yr > 0
gen acq_leverage_y1 = (acq_ta_yr__1 - acq_eq_yr__1) / acq_ta_yr__1 if acq_ta_yr__1 > 0
gen acq_leverage_y2 = (acq_ta_yr__2 - acq_eq_yr__2) / acq_ta_yr__2 if acq_ta_yr__2 > 0

* 4. EBITDA利润率
gen acq_ebitda_margin_ly = acq_ebitda_last_avail_yr / acq_rev_rev_last_avail_yr if acq_rev_rev_last_avail_yr > 0
gen acq_ebitda_margin_y1 = acq_ebitda_yr__1 / acq_rev_rev_yr__1 if acq_rev_rev_yr__1 > 0
gen acq_ebitda_margin_y2 = acq_ebitda_yr__2 / acq_rev_rev_yr__2 if acq_rev_rev_yr__2 > 0

* 5. 资产周转率
gen acq_asset_turnover_ly = acq_rev_rev_last_avail_yr / acq_ta_last_avail_yr if acq_ta_last_avail_yr > 0
gen acq_asset_turnover_y1 = acq_rev_rev_yr__1 / acq_ta_yr__1 if acq_ta_yr__1 > 0
gen acq_asset_turnover_y2 = acq_rev_rev_yr__2 / acq_ta_yr__2 if acq_ta_yr__2 > 0

* 6. 流动比率 (如果有流动资产数据)
*gen acq_current_ratio_ly = acq_current_assets_ly / acq_current_liabilities_ly

* 7. 人均指标
*gen acq_rev_per_emp_ly = acq_rev_rev_last_avail_yr / acq_emp_last_avail_yr if acq_emp_last_avail_yr > 0
*gen acq_rev_per_emp_y1 = acq_rev_rev_yr__1 / acq_emp_yr__1 if acq_emp_yr__1 > 0
*gen acq_rev_per_emp_y2 = acq_rev_rev_yr__2 / acq_emp_yr__2 if acq_emp_yr__2 > 0

* 7.5 size指标
gen acq_ta = ln(acq_ta_last_avail_yr) if acq_ta_last_avail_yr > 0
gen acq_ta_y1 = ln(acq_ta_yr__1) if acq_ta_yr__1 > 0
gen acq_ta_y2 = ln(acq_ta_yr__2) if acq_ta_yr__2 > 0
gen acq_emp = ln(acq_emp_last_avail_yr) if acq_emp_last_avail_yr > 0
gen acq_emp_y1 = ln(acq_emp_yr__1) if tar_emp_yr__1 > 0
gen acq_emp_y2 = ln(acq_emp_yr__2) if tar_emp_yr__2 > 0

* 8. 估值比率 (如果EV数据可用)
gen acq_cap = ln(acq_cap_last_avail_yr) if acq_cap_last_avail_yr > 0
gen acq_cap_y1 = ln(acq_cap_yr__1) if acq_cap_yr__1 > 0
gen acq_cap_y2 = ln(acq_cap_yr__2) if acq_cap_yr__2 > 0
gen acq_ev = ln(acq_ev_last_avail_yr) if acq_ev_last_avail_yr > 0
gen acq_ev_y1 = ln(acq_ev_yr__1) if acq_ev_yr__1 > 0
gen acq_ev_y2 = ln(acq_ev_yr__2) if acq_ev_yr__2 > 0
*gen acq_ev_to_rev_ly = acq_ev_last_avail_yr / acq_rev_rev_last_avail_yr if acq_rev_rev_last_avail_yr > 0
*gen acq_ev_to_ebitda_ly = acq_ev_last_avail_yr / acq_ebitda_last_avail_yr if acq_ebitda_last_avail_yr > 0

* 9. 检查计算结果的统计特征
sum acq_profit_margin_ly acq_roa_ly acq_leverage_ly acq_ebitda_margin_ly acq_asset_turnover_ly

* 10. 标签变量
label variable acq_profit_margin_ly "Acquirer Profit Margin (Last Year)"
label variable acq_roa_ly "Acquirer ROA (Last Year)" 
label variable acq_leverage_ly "Acquirer Leverage Ratio (Last Year)"
label variable acq_ebitda_margin_ly "Acquirer EBITDA Margin (Last Year)"
label variable acq_asset_turnover_ly "Acquirer Asset Turnover (Last Year)"
*label variable acq_rev_per_emp_ly "Acquirer per Employee (Last Year)"

drop  tar_rev_rev_last_avail_yr tar_rev_rev_yr__1 tar_rev_rev_yr__2 tar_ebitda_last_avail_yr tar_ebitda_yr__1 tar_ebitda_yr__2 tar_ebit_last_avail_yr tar_ebit_yr__1 tar_ebit_yr__2 tar_pbt_last_avail_yr tar_pbt_yr__1 tar_pbt_yr__2 tar_pat_last_avail_yr tar_pat_yr__1 tar_pat_yr__2 tar_np_last_avail_yr tar_np_yr__1 tar_np_yr__2 tar_ta_last_avail_yr tar_ta_yr__1 tar_ta_yr__2 tar_na_last_avail_yr tar_na_yr__1 tar_na_yr__2 tar_eq_last_avail_yr tar_eq_yr__1 tar_eq_yr__2 tar_cap_last_avail_yr tar_cap_yr__1 tar_cap_yr__2 tar_emp_last_avail_yr tar_emp_yr__1 tar_emp_yr__2 tar_ev_last_avail_yr tar_ev_yr__1 tar_ev_yr__2 tar_eps_last_avail_yr tar_eps_yr__1 tar_eps_yr__2 tar_cfps_last_avail_yr tar_cfps_yr__1 tar_cfps_yr__2 tar_dps_last_avail_yr tar_dps_yr__1 tar_dps_yr__2 tar_bvps_last_avail_yr tar_bvps_yr__1 tar_bvps_yr__2 acq_rev_rev_last_avail_yr acq_rev_rev_yr__1 acq_rev_rev_yr__2 acq_ebitda_last_avail_yr acq_ebitda_yr__1 acq_ebitda_yr__2 acq_ebit_last_avail_yr acq_ebit_yr__1 acq_ebit_yr__2 acq_pbt_last_avail_yr acq_pbt_yr__1 acq_pbt_yr__2 acq_pat_last_avail_yr acq_pat_yr__1 acq_pat_yr__2 acq_np_last_avail_yr acq_np_yr__1 acq_np_yr__2 acq_ta_last_avail_yr acq_ta_yr__1 acq_ta_yr__2 acq_na_last_avail_yr acq_na_yr__1 acq_na_yr__2 acq_eq_last_avail_yr acq_eq_yr__1 acq_eq_yr__2 acq_cap_last_avail_yr acq_cap_yr__1 acq_cap_yr__2 acq_emp_last_avail_yr acq_emp_yr__1 acq_emp_yr__2 acq_ev_last_avail_yr acq_ev_yr__1 acq_ev_yr__2 acq_eps_last_avail_yr acq_eps_yr__1 acq_eps_yr__2 acq_cfps_last_avail_yr acq_cfps_yr__1 acq_cfps_yr__2 acq_dps_last_avail_yr acq_dps_yr__1 acq_dps_yr__2 acq_bvps_last_avail_yr acq_bvps_yr__1 acq_bvps_yr__2 ven_rev_rev_last_avail_yr ven_rev_rev_yr__1 ven_rev_rev_yr__2 ven_ebitda_last_avail_yr ven_ebitda_yr__1 ven_ebitda_yr__2 ven_ebit_last_avail_yr ven_ebit_yr__1 ven_ebit_yr__2 ven_pbt_last_avail_yr ven_pbt_yr__1 ven_pbt_yr__2 ven_pat_last_avail_yr ven_pat_yr__1 ven_pat_yr__2 ven_np_last_avail_yr ven_np_yr__1 ven_np_yr__2 ven_ta_last_avail_yr ven_ta_yr__1 ven_ta_yr__2 ven_na_last_avail_yr ven_na_yr__1 ven_na_yr__2 ven_eq_last_avail_yr ven_eq_yr__1 ven_eq_yr__2 ven_cap_last_avail_yr ven_cap_yr__1 ven_cap_yr__2 ven_emp_last_avail_yr ven_emp_yr__1 ven_emp_yr__2 ven_ev_last_avail_yr ven_ev_yr__1 ven_ev_yr__2 ven_eps_last_avail_yr ven_eps_yr__1 ven_eps_yr__2 ven_cfps_last_avail_yr ven_cfps_yr__1 ven_cfps_yr__2 ven_dps_last_avail_yr ven_dps_yr__1 ven_dps_yr__2 ven_bvps_last_avail_yr ven_bvps_yr__1 ven_bvps_yr__2
drop  pre_pbt_mul_ly pre_pat_mul_ly pre_np_mul_ly pre_ta_mul_ly pre_na_mul_ly pre_cl_mul_ly pre_eq_mul_ly pre_cap_mul_ly  post_pbt_mul_fy post_pat_mul_fy post_np_mul_fy post_ta_mul_fy post_na_mul_fy post_cl_mul_fy post_eq_mul_fy post_cap_mul_fy  deal_value_native_currency deal_equity_value_native_currenc deal_enterprise_value_native_cur  deal_modelled_enterprise_value_n deal_total_target_value_native_c modelled_fee_income irr_pct currency last_deal_status_d_yr last_deal_val_up_d_yr last_deal_status_up_d_yr last_pct_stake_up_d_yr last_acq_tar_ven_up_d_yr last_advisor_up_d_yr last_comment_up_d_yr last_up_yr pre_deal_tar_rev_rev_last_avail_ pre_deal_tar_ebitda_last_avail_y pre_deal_tar_ebit_last_avail_yr pre_deal_tar_pbt_last_avail_yr pre_deal_tar_pat_last_avail_yr pre_deal_tar_np_last_avail_yr pre_deal_tar_ta_last_avail_yr pre_deal_tar_na_last_avail_yr pre_deal_tar_current_liabilities pre_deal_tar_eq_last_avail_yr pre_deal_tar_cap pre_deal_acq_rev_rev_last_avail_ pre_deal_acq_ebitda_last_avail_y pre_deal_acq_ebit_last_avail_yr pre_deal_acq_pbt_last_avail_yr pre_deal_acq_pat_last_avail_yr pre_deal_acq_np_last_avail_yr pre_deal_acq_ta_last_avail_yr pre_deal_acq_na_last_avail_yr pre_deal_acq_current_liabilities pre_deal_acq_eq_last_avail_yr pre_deal_acq_cap pre_deal_ven_rev_rev_last_avail_ pre_deal_ven_ebitda_last_avail_y pre_deal_ven_ebit_last_avail_yr pre_deal_ven_pbt_last_avail_yr pre_deal_ven_pat_last_avail_yr pre_deal_ven_np_last_avail_yr pre_deal_ven_ta_last_avail_yr pre_deal_ven_na_last_avail_yr pre_deal_ven_current_liabilities pre_deal_ven_eq_last_avail_yr pre_deal_ven_cap 
bysort  deal_num  tar_bvd_id_num tar_orbis_id_num acq_bvd_id_num acq_orbis_id_num :keep if _n == 1

save "${ONEDRIVE_PATH}\MA\dta\ind_merged2.dta", replace

use "${ONEDRIVE_PATH}\MA\dta\ind_merged2.dta", clear
keep if year >= 1996 & year <= 2024

encode deal_struct,gen(deal_category)

gen Country= tar_country_code

sort Country year
merge m:1 Country year using ${ONEDRIVE_PATH}\MA\macrodata\world_stock_traded_gdp_long.dta,keepusing(cap_pct_gdp)
keep if _merge == 3
drop _merge

/*id 可以对应很多name，冗余，之前merge为了精确用name bvdid orbisid 三个标志物来merge，但是真正计算的时候不需要这么多标志物，会出现太多重复值，因此最后只保留唯一id*/
bysort  deal_num  tar_bvd_id_num tar_orbis_id_num acq_bvd_id_num acq_orbis_id_num ven_bvd_id_num ven_orbis_id_num:keep if _n == 1



* 定义需要winsorize的变量列表
local winsor_vars  deal_value deal_total_target_value deal_equity_value deal_enterprise_value deal_modelled_enterprise_valuedeal_pay_method_val_usd post_deal_tar_rev_rev_1st_avail_ post_deal_tar_ebitda_1st_avail_y post_deal_tar_ebit_1st_avail_yr post_deal_tar_pbt_1st_avail_yr post_deal_tar_pat_1st_avail_yr post_deal_tar_np_1st_avail_yr post_deal_tar_ta_1st_avail_yr post_deal_tar_na_1st_avail_yr post_deal_tar_current_liabilitie post_deal_tar_shareholder_funds_ post_deal_tar_cap post_deal_acq_rev_rev_1st_avail_ post_deal_acq_ebitda_1st_avail_y post_deal_acq_ebit_1st_avail_yr post_deal_acq_pbt_1st_avail_yr post_deal_acq_pat_1st_avail_yr post_deal_acq_np_1st_avail_yr post_deal_acq_ta_1st_avail_yr post_deal_acq_na_1st_avail_yr post_deal_acq_shareholder_funds_ post_deal_acq_cap post_deal_ven_rev_rev_1st_avail_ post_deal_ven_ebitda_1st_avail_y post_deal_ven_ebit_1st_avail_yr post_deal_ven_pbt_1st_avail_yr post_deal_ven_pat_1st_avail_yr post_deal_ven_np_1st_avail_yr post_deal_ven_ta_1st_avail_yr post_deal_ven_na_1st_avail_yr post_deal_ven_shareholder_funds_ post_deal_ven_cap pre_rev_mul_ly post_rev_mul_fy post_ebitda_mul_fy post_ebit_mul_fy
* 对存在的变量进行winsorize处理
foreach var of local winsor_vars {
    capture confirm variable `var'
    if _rc == 0 {
        di "Winsorizing `var' at 1st and 99th percentiles"
        winsor2 `var', replace cuts(1 99)
    }
    else {
        di "Variable `var' not found in the dataset"
    }
}
* 描述性统计
di "Available controls: `available_controls'"
describe `available_controls'



gen ind=int( tar_primary_sic_code/1000)
gen ln_value=ln(deal_value)
gen ln_pay=ln(deal_pay_method_val_usd)


save "${ONEDRIVE_PATH}\MA\dta\ind_regress.dta", replace

