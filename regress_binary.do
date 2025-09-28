* 方案1：二元分类（最常用）
gen deal_success = 0
replace deal_success = 1 if inlist(deal_status, "Completed", "Completed Assumed", "Unconditional")
label variable deal_success "Deal Success (1=Completed/Assumed/Unconditional, 0=Other)"
tab deal_success

* 方案2：三元分类
gen deal_status3 = 0 if inlist(deal_status, "Withdrawn", "Rumour - Withdrawn", "Postponed")
replace deal_status3 = 1 if inlist(deal_status, "Announced", "Pending", "Pending - awaiting regulatory approval", "Pending - awaiting shareholder approval", "Rumour", "Rumour - Expired", "Rumour - informal offer/non-binding")
replace deal_status3 = 2 if inlist(deal_status, "Completed", "Completed Assumed", "Unconditional")
label define status3 0 "Failed" 1 "Ongoing/Pending" 2 "Successful"
label values deal_status3 status3
tab deal_status3

* 方案3：四元分类（更细致）
gen deal_status4 = 0 if inlist(deal_status, "Withdrawn", "Rumour - Withdrawn")
replace deal_status4 = 1 if inlist(deal_status, "Postponed", "Rumour - Expired")
replace deal_status4 = 2 if inlist(deal_status, "Announced", "Pending", "Pending - awaiting regulatory approval", "Pending - awaiting shareholder approval", "Rumour", "Rumour - informal offer/non-binding")
replace deal_status4 = 3 if inlist(deal_status, "Completed", "Completed Assumed", "Unconditional")
label define status4 0 "Withdrawn" 1 "Postponed/Expired" 2 "Ongoing" 3 "Completed"
label values deal_status4 status4
tab deal_status4

* 方案4：按deal_status原始值（多项分类）
encode deal_status, gen(deal_status_encoded)

* 定义基础变量
local base_vars LM_litigious_percent ln_value ln_pre_t_ta ln_pre_a_ta ln_t_emp_0 ln_a_emp_0 tar_age acq_age 

* 1. 二元Logit模型（成功 vs 失败）
logit deal_success `base_vars' i.tar_primary_sic_code, vce(robust)
eststo binary_logit
margins, dydx(*) post
eststo binary_margins

* 2. 多项Logit模型（四元分类）
mlogit deal_status4 `base_vars' i.tar_primary_sic_code, vce(robust) base(0)
eststo mlogit_4cat

* 计算边际效应（以Completed为参照）
margins, dydx(*) predict(outcome(3)) post
eststo mlogit_margins

* 3. 有序Logit模型（如果认为状态有顺序性）
* 假设顺序：Withdrawn < Postponed/Expired < Ongoing < Completed
ologit deal_status4 `base_vars' i.tar_primary_sic_code, vce(robust)
eststo ologit_4cat

* 4. 如果关注特定对比，比如Completed vs Withdrawn
gen completed_vs_withdrawn = .
replace completed_vs_withdrawn = 0 if deal_status4 == 0  // Withdrawn
replace completed_vs_withdrawn = 1 if deal_status4 == 3  // Completed
logit completed_vs_withdrawn `base_vars' i.tar_primary_sic_code if !missing(completed_vs_withdrawn), vce(robust)
eststo comp_vs_withdrawn

* 5. 逐个变量分析（二元成功变量）
local single_vars boardindependency pct_contr_shareholder indipendency ///
                 arethereshareownershiplimitation arethereothercommonrestrictionso ///
                 mustsharesbedepositedorblockedfr votingpolicy oppressed ///
                 antitakeover fairprice classaction arederivativesuitscommonlyusedin

eststo clear
foreach var of local single_vars {
    eststo: logit deal_success `base_vars' `var' i.tar_primary_sic_code, vce(robust)
    margins, dydx(`var') post
    eststo `var'_margins
}

* 显示结果
esttab binary_margins, ///
    b(3) se(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    stats(N, labels("Observations")) ///
    title("Marginal Effects on Deal Success Probability") ///
    compress

* 比较不同模型
esttab binary_logit mlogit_4cat ologit_4cat comp_vs_withdrawn, ///
    b(3) se(3) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    stats(N r2_p ll, labels("Observations" "Pseudo R2" "Log Likelihood")) ///
    title("Comparison of Different Deal Status Models") ///
    compress

* 保存处理后的数据
save "deal_status_analysis_comprehensive.dta", replace
