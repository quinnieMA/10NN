// Define the base path to OneDrive
global ONEDRIVE_PATH "D:/OneDrive"  // Change this to match your OneDrive path

import delimited ${ONEDRIVE_PATH}/MA/acquisition/similarity_results/target_top10_neighbors1.csv, bindquote(strict) clear 
generate deal_num = substr(neighbor_target, 12, .)

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
generate deal_num = substr(source_target, 12, .)

* 再次合并获取源目标的行业代码
merge m:1 deal_num using "${ONEDRIVE_PATH}/MA/acquisition/industry/acq_tar_ind_to_merge.dta", ///
    keepusing(tar_primary_sic_code)

* 保留匹配成功的观测值
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

* 显示详细统计结果
display "前三位SIC代码相同的观测值数量: " r(N)
display "其中:"
display "  相同的数量: " r(N)[1]
display "  不同的数量: " r(N)[2]
display "  匹配比例: " round(r(N)[1]/r(N)*100, 0.1) "%"

* 清理临时变量
drop neighbor_sic_str source_sic_str neighbor_sic3 source_sic3

* 合并乘数数据
sort deal_num 
merge m:1 deal_num  using "D:\OneDrive\MA\acquisition\structure_date\acq_status"
keep if _merge==3
drop _merge

* 计算每个deal_num的EBITDA乘数中位数
bysort deal_num year: egen median_pre_ebitda_mul_ly = median(pre_ebitda_mul_ly)
bysort source_tar_primary_sic_code year: egen median_pre_ebitda_mul_ly_1 = median(pre_ebitda_mul_ly)

* 保留每个deal_num的第一条观测值
bysort deal_num: keep if _n == 1

* 保留所需变量
keep deal_num year median_pre_ebitda_mul_ly median_pre_ebitda_mul_ly_1 source_tar_primary_sic_code

* 保存结果
save "D:\OneDrive\MA\acquisition\industry\neibor_ind.dta", replace

