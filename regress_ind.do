// Define the base path to OneDrive
global ONEDRIVE_PATH "D:/OneDrive"  // Change this to match your OneDrive path

use "${ONEDRIVE_PATH}\MA\acquisition\output\mergedtemp.dta", clear
*append using "${ONEDRIVE_PATH}\MA\acquisition\output\mergedtemp_S.dta"

sort deal_num
merge m:1 deal_num using "${ONEDRIVE_PATH}\MA\acquisition\NLP_rationale\sentiment_results\ALL_SENTIMENT_RESULTS.dta"
keep if _merge == 3
drop _merge

/*id 可以对应很多name，冗余，之前merge为了精确用name bvdid orbisid 三个标志物来merge，但是真正计算的时候不需要这么多标志物，会出现太多重复值，因此最后只保留唯一id*/
bysort  deal_num  tar_bvd_id_num tar_orbis_id_num acq_bvd_id_num acq_orbis_id_num /*ven_bvd_id_num ven_orbis_id_num*/:keep if _n == 1


* 安装winsor2命令（如果未安装）
capture which winsor2
if _rc != 0 {
    ssc install winsor2
}

* 定义需要winsorize的变量列表
local winsor_vars  pre_rev_mul_ly pre_ebitda_mul_ly pre_ebit_mul_ly ///
                 pre_pbt_mul_ly pre_pat_mul_ly pre_np_mul_ly pre_ta_mul_ly ///
                 pre_na_mul_ly pre_cl_mul_ly pre_eq_mul_ly pre_cap_mul_ly ///
                 post_rev_mul_fy post_ebitda_mul_fy post_ebit_mul_fy ///
                 post_pbt_mul_fy post_pat_mul_fy post_np_mul_fy post_ta_mul_fy ///
                 post_na_mul_fy post_cl_mul_fy post_eq_mul_fy post_cap_mul_fy

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

* 创建差异变量并运行第二个回归
bysort tar_primary_sic_code announced_d_yr: egen median_pre_ebitda_mul_ly1 = median(pre_ebitda_mul_ly)
*bysort tar_primary_sic_code announced_d_yr: egen median_pre_rev_mul_ly = median(pre_rev_mul_ly)
gen diff = pre_ebitda_mul_ly - median_pre_ebitda_mul_ly1 
gen diff1= pre_ebitda_mul_ly -median_pre_ebitda_mul_ly
reg diff LM_litigious_percent ln_value ln_pre_t_ta ln_pre_a_ta ln_t_emp_0 ln_a_emp_0 tar_age acq_age /*ven_ta_last_avail_yr ven_emp_last_avail_yr*/,r
* 显示结果摘要
di "Regression 1: pre_ebitda_mul_ly on LM_litigious_percent and mean_pre_ebitda_mul_ly"
di "Regression 2: diff (pre_ebitda_mul_ly - mean) on LM_litigious_percent"

* 保存处理后的数据
save "${ONEDRIVE_PATH}\MA\acquisition\output\processed_data.dta", replace
