use "D:\OneDrive\MA\dta\samplebias.dta", clear
sort deal_num
merge 1:1 deal_num using "${ONEDRIVE_PATH}/MA/acquisition/structure_date/acq_d.dta"
keep if _merge==3
drop _merge

* 去重处理 - 基于deal_num去除重复交易记录
duplicates tag deal_num, gen(dup_deal)
drop if dup_deal > 0
drop dup_deal
drop if announced_d_yr>=2025
* 生成交易计数变量
gen deal_count = 1

* 生成EBITDA倍数存在指标
gen overview_count = !missing(tar_overview)
gen overview=1 if !missing(tar_overview)

* 按年份统计
collapse (sum) deal_count overview, by(announced_d_yr)

save "${ONEDRIVE_PATH}/MA/dta/deal_overview_stats.dta",replace

use "${ONEDRIVE_PATH}\MA\dta\ind_merged2.dta", clear
keep if year >= 1996 & year <= 2024

encode deal_struct,gen(deal_category)

gen Country= tar_country_code

*sort Country year
*merge m:1 Country year using ${ONEDRIVE_PATH}\MA\macrodata\world_stock_traded_gdp_long.dta,keepusing(cap_pct_gdp)
*keep if _merge == 3
*drop _merge

sort deal_num
merge m:1 deal_num using ${ONEDRIVE_PATH}\MA\dta\count.dta
keep if _merge == 3
drop _merge


/*id 可以对应很多name，冗余，之前merge为了精确用name bvdid orbisid 三个标志物来merge，但是真正计算的时候不需要这么多标志物，会出现太多重复值，因此最后只保留唯一id*/
bysort  deal_num  tar_bvd_id_num tar_orbis_id_num acq_bvd_id_num acq_orbis_id_num :keep if _n == 1

save "${ONEDRIVE_PATH}\MA\dta\ind_mergedtable.dta",replace
/////////////////////////////////
use "${ONEDRIVE_PATH}\MA\dta\ind_regress.dta", clear
* 设置输出路径
global OUTPUT_PATH "D:/OneDrive/MA/writing"

* 控制变量定义
local deal_controls "deal_pay_method_val_usd deal_value tar_count acq_count ven_count"
local company_controls_ly "tar_roa_ly tar_leverage_ly  acq_roa_ly acq_leverage_ly  acq_emp tar_emp tar_age acq_age "
local company_controls_y1 "tar_roa_y1 tar_leverage_y1  acq_roa_y1 acq_leverage_y1 "
local company_controls_y2 "tar_roa_y2 tar_leverage_y2   acq_roa_y2 acq_leverage_y2 "
local other_controls "cap_pct_gdp i.year i.ind payment_category deal_category "
sum diff diff1 pre_ebitda_mul_ly post_ebitda_mul_fy post_deal_tar_ebitda_1st_avail_y post_deal_acq_ebitda_1st_avail_y `deal_controls' `company_controls_ly' `company_controls_y1' `company_controls_y2'  `other_controls'

/////////////////////////////////////
use "${ONEDRIVE_PATH}\MA\dta\ind_mergedtable.dta",clear
////////////////////////////////////////////////
* 去重处理 - 基于deal_num去除重复交易记录
duplicates tag deal_num, gen(dup_deal)
drop if dup_deal > 0
drop dup_deal
* 生成交易计数变量
gen deal_count = 1

* 生成EBITDA倍数存在指标
gen ebitda_count = !missing(pre_ebitda_mul_ly)

* 按年份统计
collapse (sum) deal_count ebitda_count, by(announced_d_yr)

* 计算覆盖率和百分比
gen coverage_rate = (ebitda_count / deal_count) * 100
gen coverage_pct = round(coverage_rate, 0.1)

* 只保留1996-2024年的数据
keep if announced_d_yr >= 1996 & announced_d_yr <= 2024

* 格式化显示
format coverage_rate %9.2f
format coverage_pct %4.1f

* 排序并显示结果
sort announced_d_yr
list announced_d_yr deal_count ebitda_count coverage_rate coverage_pct, clean noobs
//////////////////////////////////////////////////////
use "${ONEDRIVE_PATH}\MA\dta\ind_mergedtable.dta",clear
keep if year >= 1996 & year <= 2024

gen temp = (tar_country_code != acq_country_code) if !missing(tar_country_code, acq_country_code)
replace temp = 0 if missing(temp)
sort deal_num
by deal_num : egen crossborder=sum(temp)
replace crossborder=1 if crossborder!=0
bysort  deal_num  tar_bvd_id_num tar_orbis_id_num acq_bvd_id_num acq_orbis_id_num :keep if _n == 1

*drop if pre_ebitda_mul_ly==. | Country==""
bysort deal_num: gen deal_count = _N
bysort deal_num: keep if _n == 1

*sum pre_ebitda_mul_ly pre_ebit_mul_ly  deal_value deal_pay_method_val_usd  cap_pct_gdp tar_profit_margin_ly tar_profit_margin_y1 tar_profit_margin_y2 tar_roa_ly tar_roa_y1 tar_roa_y2 tar_leverage_ly tar_leverage_y1 tar_leverage_y2 tar_asset_turnover_ly tar_asset_turnover_y1 tar_asset_turnover_y2 tar_rev_per_emp_ly tar_rev_per_emp_y1 tar_rev_per_emp_y2 acq_profit_margin_ly acq_profit_margin_y1 acq_profit_margin_y2 acq_roa_ly acq_roa_y1 acq_roa_y2 acq_leverage_ly acq_leverage_y1 acq_leverage_y2 acq_asset_turnover_ly acq_asset_turnover_y1 acq_asset_turnover_y2 acq_rev_per_emp_ly acq_rev_per_emp_y1 acq_rev_per_emp_y2 tar_age acq_age
drop deal_count
///////////////////////////
* 正确的方法：分别独立统计deal_count、acq_count、ven_count和crossborder数量
preserve

* 步骤1：统计每个国家的不重复交易数量
keep Country deal_num crossborder
duplicates drop  // 去除重复的国家-交易组合
bysort Country: gen deal_count = _N
bysort Country: egen crossborder_count = total(crossborder)
bysort Country: keep if _n == 1
keep Country deal_count crossborder_count
save temp_deal_count, replace
restore

preserve
* 步骤2：统计每个国家的不重复收购公司数量（在整个国家范围内去重）
keep Country tar_bvd_id_num
duplicates drop  // 去除重复的国家-收购公司组合
bysort Country: gen tar_count = _N
bysort Country: keep if _n == 1
keep Country tar_count
save temp_tar_count, replace
restore


preserve
* 步骤2：统计每个国家的不重复收购公司数量（在整个国家范围内去重）
keep Country acq_bvd_id_num
duplicates drop  // 去除重复的国家-收购公司组合
bysort Country: gen acq_count = _N
bysort Country: keep if _n == 1
keep Country acq_count
save temp_acq_count, replace
restore

preserve
* 步骤3：统计每个国家的不重复vendor公司数量（在整个国家范围内去重）
keep Country ven_bvd_id_num
duplicates drop  // 去除重复的国家-目标公司组合
bysort Country: gen ven_count = _N
bysort Country: keep if _n == 1
keep Country ven_count
save temp_ven_count, replace
restore

* 合并结果
use temp_deal_count, clear
merge 1:1 Country using temp_acq_count
drop _merge
merge 1:1 Country using temp_ven_count
drop _merge
merge 1:1 Country using temp_tar_count
drop _merge

* 计算crossborder比例
gen crossborder_ratio = crossborder_count / deal_count

* 按交易数量排序
gsort -deal_count

* 保存原始数据
save temp_all_countries, replace

* 处理前20名和others
use temp_all_countries, clear

* 生成排名变量
gen rank = _n

* 创建新变量用于显示
gen display_country = Country
gen display_deal_count = deal_count
gen display_tar_count = tar_count
gen display_acq_count = acq_count
gen display_ven_count = ven_count
gen display_crossborder_count = crossborder_count
gen display_crossborder_ratio = crossborder_ratio

* 将排名20之后的国家替换为"others"
replace display_country = "others" if rank > 20

* 计算others的汇总值
preserve
keep if rank > 20
collapse (sum) display_deal_count = deal_count  display_tar_count = tar_count display_acq_count = acq_count ///
         display_ven_count = ven_count display_crossborder_count = crossborder_count, ///
         by(display_country)
* 重新计算others的crossborder比例
gen display_crossborder_ratio = display_crossborder_count / display_deal_count
tempfile others_data
save `others_data'
restore

* 保留前20名
keep if rank <= 20
keep display_country display_deal_count  display_tar_count display_acq_count display_ven_count ///
     display_crossborder_count display_crossborder_ratio

* 添加others行
append using `others_data'

* 计算样本总数
preserve
use temp_all_countries, clear
gen total_deals = sum(deal_count)
gen total_tars = sum(tar_count)
gen total_acqs = sum(acq_count)
gen total_vens = sum(ven_count)
gen total_crossborder = sum(crossborder_count)
local total_deal_count = total_deals[_N]
local total_tar_count = total_tars[_N]
local total_acq_count = total_acqs[_N]
local total_ven_count = total_vens[_N]
local total_crossborder_count = total_crossborder[_N]
local total_crossborder_ratio = `total_crossborder_count' / `total_deal_count'
restore

* 显示结果
list display_country display_deal_count display_tar_count  display_acq_count display_ven_count ///
     display_crossborder_count display_crossborder_ratio, clean noobs abbreviate(15)

* 显示样本总数
display "样本总数 - 交易数量: `total_deal_count', 收购公司数量: `total_acq_count', 供应商公司数量: `total_ven_count'"
display "跨境交易数量: `total_crossborder_count', 跨境交易比例: " %4.3f `total_crossborder_ratio'

* 清理临时文件
cap erase temp_deal_count.dta
cap erase temp_tar_count.dta
cap erase temp_acq_count.dta
cap erase temp_ven_count.dta
cap erase temp_all_countries.dta
////////////////////////////////////////////////////////////////
use "${ONEDRIVE_PATH}\MA\dta\ind_mergedtable.dta",clear
* 只保留1996-2021年的数据
keep if year >= 1996 & year <= 2024

* 生成完成交易指标
gen completed = 0
replace completed = 1 if inlist(deal_status, "Completed", "Completed Assumed", "Unconditional")

* 生成交易计数变量（所有交易都计数为1）
gen deal_count = 1

* 按年份统计
collapse (sum) num_deals = deal_count (mean) pct_completed = completed (sum) agg_deal_value = deal_value, by(year)

* 计算平均交易价值
gen avg_deal_value = agg_deal_value / num_deals

* 格式化百分比
gen pct_completed_pct = pct_completed * 100


* 格式化数值显示
format agg_deal_value %15.0fc
format avg_deal_value %15.0fc
format pct_completed_pct %6.2f

* 排序并显示结果
sort year
list year num_deals pct_completed_pct agg_deal_value avg_deal_value, clean noobs

display "\hline"
display "\end{tabular}"
display "\end{table}"
///////////////////////////////////////////
use "${ONEDRIVE_PATH}\MA\dta\ind_mergedtable.dta",clear
* 只保留1996-2021年的数据
keep if year >= 1996 & year <= 2024

* 从source_tar_primary_sic_code提取第一个数字作为9大分类
gen sic_first_digit = int(source_tar_primary_sic_code/1000)
*destring sic_first_digit, replace

* 创建详细的9大分类
gen sic_category = .
replace sic_category = 1 if sic_first_digit == 0  // 农业、林业和渔业
replace sic_category = 2 if sic_first_digit == 1  // 矿业和建筑业
replace sic_category = 3 if sic_first_digit == 2  // 制造业：食品、纺织、木材、造纸等
replace sic_category = 4 if sic_first_digit == 3  // 制造业：石油、化工、机械、电子等
replace sic_category = 5 if sic_first_digit == 4  // 交通运输、通信、电力、燃气和卫生服务
replace sic_category = 6 if sic_first_digit == 5  // 批发和零售贸易
replace sic_category = 7 if sic_first_digit == 6  // 金融、保险和房地产
replace sic_category = 8 if sic_first_digit == 7  // 服务业：酒店、维修、商业服务等
replace sic_category = 9 if sic_first_digit == 8 | sic_first_digit == 9  // 公共服务

* 设置分类标签
label define sic_cat_lbl 1 "农业林业渔业(0)" 2 "矿业建筑业(1)" 3 "制造业-轻工(2)" ///
                         4 "制造业-重工(3)" 5 "交通通信公用事业(4)" 6 "批发零售贸易(5)" ///
                         7 "金融保险房地产(6)" 8 "服务业(7)" 9 "公共服务(8-9)"
label values sic_category sic_cat_lbl

* 确保变量为字符串格式
tostring neighbor_sic3 source_sic3, replace

* 生成是否不同的标识变量
gen sic_diff = (neighbor_sic3 != source_sic3) if !missing(neighbor_sic3, source_sic3)

* 统计每个分类的不同数量
preserve
collapse (sum) diff_count = sic_diff (count) total_count = sic_diff, by(sic_category)

* 计算比例和排序
gen diff_ratio = diff_count / total_count
gsort -diff_count

* 格式化
format diff_ratio %4.3f

* 显示最终结果
list sic_category diff_count total_count diff_ratio, clean noobs
restore

* 显示样本总数
count if !missing(neighbor_sic3, source_sic3)
local total_obs = r(N)
count if neighbor_sic3 != source_sic3 & !missing(neighbor_sic3, source_sic3)
local total_diff = r(N)
display "总样本数: `total_obs', 不同SIC3数量: `total_diff', 总体比例: " %4.3f `total_diff'/`total_obs'

