
* 定义需要winsorize的变量列表
local winsor_vars deal_value pre_tar_ta  pre_acq_ta acq_emp tar_emp pre_rev_mul_ly pre_ebitda_mul_ly pre_ebit_mul_ly pre_pbt_mul_ly pre_pat_mul_ly pre_np_mul_ly pre_ta_mul_ly pre_na_mul_ly pre_cl_mul_ly pre_eq_mul_ly pre_cap_mul_ly post_rev_mul_fy post_ebitda_mul_fy post_ebit_mul_fy post_pbt_mul_fy post_pat_mul_fy post_np_mul_fy post_ta_mul_fy post_na_mul_fy post_cl_mul_fy post_eq_mul_fy post_cap_mul_fy
* 确保安装了winsor2命令（如果没有安装的话）
capture which winsor2
if _rc != 0 {
    ssc install winsor2
}

* 对每个变量进行1%和99%分位的winsorize处理
foreach var of local winsor_vars {
    * 检查变量是否存在
    capture confirm variable `var'
    if _rc == 0 {
        di "Winsorizing `var' at 1st and 99th percentiles"
        winsor2 `var', replace cuts(1 99)
    }
    else {
        di "Variable `var' not found in the dataset"
    }
}
* 缩短变量名（使用rename或者使用局部宏）
* 如果变量名太长，可以先重命名，或者直接在循环中使用短名称
* 先重命名长变量名（如果需要）
rename pre_ebitda_mul_ly pre_ebitda
rename pre_deal_tar_ebitda_last_avail_y pre_tar_ebitda
rename pre_deal_tar_ta_last_avail_yr pre_tar_ta
rename pre_deal_acq_ebitda_last_avail_y pre_acq_ebitda
rename pre_deal_acq_ta_last_avail_yr pre_acq_ta
rename acq_emp_last_avail_yr acq_emp
rename tar_emp_last_avail_yr tar_emp
rename pct_contr_shareholder pct_contr
rename arethereshareownershiplimitation share_limit
rename arethereothercommonrestrictionso other_restr
rename mustsharesbedepositedorblockedfr share_deposit
rename arederivativesuitscommonlyusedin derivative

* 定义缩短的变量名
local yvar pre_ebitda     // 因变量
local endog LM_litigious_percent       // 内生变量（缩短名称）
local controls  i.tar_primary_sic_code deal_value pre_tar_ta  pre_acq_ta acq_emp tar_emp tar_incorp_d_year acq_incorp_d_year

* 工具变量列表（使用短名称）
local ivlist pct_contr indipendency share_limit other_restr share_deposit voting oppressed antitakeover fairprice classaction derivative


* 初始化存储
eststo clear

* 运行所有回归
reg `yvar' `controls', vce(robust)
eststo m1

reg `yvar' `endog' `controls', vce(robust)
eststo m2

local i 3
foreach iv of local ivlist {
    ivregress 2sls `yvar' (`endog' = `iv') `controls', vce(robust)
    eststo m`i'
    local i = `i' + 1
}

ivregress 2sls `yvar' (`endog' = `ivlist') `controls', vce(robust)
eststo m_all

* 输出表格 - 只显示内生变量，完全去掉所有控制变量和常数项
esttab m1 m2 m3 m4 m5 m6 m7 m8 m9 m10 m11 m12 m13 m_all, ///
    keep(`endog' LM_litigious_percent) ///  // 只保留内生变量
    star(* 0.10 ** 0.05 *** 0.01) ///
    b(3) se(3) ///
    mtitle("Controls Only" "OLS" "IV: `:word 1 of `ivlist''" "IV: `:word 2 of `ivlist''" "IV: `:word 3 of `ivlist''" "IV: `:word 4 of `ivlist''" "IV: `:word 5 of `ivlist''" "IV: `:word 6 of `ivlist''" "IV: `:word 7 of `ivlist''" "IV: `:word 8 of `ivlist''" "IV: `:word 9 of `ivlist''" "IV: `:word 10 of `ivlist''" "IV: `:word 11 of `ivlist''" "All IVs") ///
    stats(N r2, fmt(0 3))
