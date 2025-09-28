clear all

// 设置输出路径（唯一修改的部分）
local output_path "C:\Users\FM\OneDrive\MA\acquisition\NLP\comments\"

// 确保输出目录存在 - 添加强制创建
capture mkdir "`output_path'", public
display "输出目录: `output_path'"

// 步骤1：导入数据（完全不变）
import delimited "C:\Users\FM\OneDrive\MA\acquisition\comments\acquisition_comments.csv", bindquote(strict) 

// 确保dealnumber是干净字符串格式（完全不变）
tostring dealnumber, replace
replace dealnumber = trim(itrim(dealnumber))  // 去除所有空格
replace dealnumber = subinstr(dealnumber, `"""', "", .) // 去除引号

// 获取所有唯一的dealnumber（完全不变）
quietly levelsof dealnumber, local(deals) clean

// 循环处理每个dealnumber（完全不变）
foreach deal of local deals {
    preserve
    quietly keep if dealnumber == "`deal'"
    
    // 检查是否有数据且存在dealcomments变量
    capture confirm variable dealcomments
    if _rc == 0 & _N > 0 {
        // 创建安全的文件名 - 只需要dealnumber作为文件名
        local filename "`deal'.txt"
        local fullpath "`output_path'`filename'"
        display "正在处理: `fullpath'"
        
        // 导出dealcomments内容到文本文件
        file open myfile using "`fullpath'", write text replace
        file write myfile (dealcomments[1]) _n // 添加索引[1]确保获取内容
        file close myfile
        
        display "已导出: `filename'"
    }
    else if _N == 0 {
        display "警告: dealnumber `deal' 无匹配数据"
    }
    else {
        display "警告: dealnumber `deal' 没有dealcomments变量"
    }
    restore
}
