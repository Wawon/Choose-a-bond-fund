"""
可申购低价债券基金筛选程序

功能概述：
- 对成立超过3年的债券基金进行二次筛选
- 查询基金的申购状态和购买起点信息
- 筛选出当前可申购且价格合理的基金

筛选条件：
1. 申购状态：排除"封闭期"和"暂停申购"的基金
2. 购买起点：不超过1000元
3. 数据来源：使用AKShare的fund_purchase_em接口获取实时申购信息

处理流程：
1. 读取前期筛选出的债券基金代码文件
2. 获取全市场基金申购状态数据
3. 并发查询每只基金的具体信息
4. 应用筛选条件过滤基金
5. 将筛选结果保存回原文件

技术特点：
- 使用多线程并发查询提高效率
- 每15次查询间隔0.15秒避免请求过频
- 自动处理数据类型转换和异常情况
"""

import akshare as ak
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def query_fund_info(code, all_fund_data):
    """
    查询单只基金的信息
    :param code: 基金代码
    :param all_fund_data: 所有基金的申购状态数据
    :return: 单只基金的信息字典
    """
    # 确保基金代码为 6 位字符串
    code = str(code).zfill(6)
    # 查找对应基金的数据
    fund_info = all_fund_data[all_fund_data["基金代码"] == code]

    if not fund_info.empty:
        # 提取需要的信息
        purchase_status = fund_info.iloc[0]["申购状态"]
        purchase_start = fund_info.iloc[0]["购买起点"]
        fund_name = fund_info.iloc[0]["基金简称"]

        return {
            "基金代码": code,
            "基金简称": fund_name,
            "申购状态": purchase_status,
            "购买起点": purchase_start
        }
    else:
        print(f"基金代码 {code} 未查询到相关数据")
        return {
            "基金代码": code,
            "基金简称": "未查询到",
            "申购状态": "未查询到",
            "购买起点": "未查询到"
        }


def query_fund_purchase_status():
    # 读取包含基金代码的Excel文件
    input_file = r"C:\Users\wawon\PycharmProjects\PythonProject1\.venv\Bond All in One\大于三年的债券基金代码.xlsx"
    try:
        # 读取原文件数据
        df = pd.read_excel(input_file)
        # 确保基金代码列转换为 6 位字符串类型
        df["基金代码"] = df.iloc[:, 0].astype(str).str.zfill(6)
        # 读取第一列基金代码
        fund_codes = df["基金代码"].tolist()
        print(f"成功读取基金代码，共{len(fund_codes)}只基金")
    except Exception as e:
        print(f"读取基金代码失败：{e}")
        return

    # 获取所有基金的申购状态数据
    try:
        print("正在获取所有基金的申购状态数据...")
        all_fund_data = ak.fund_purchase_em()
        # 将基金代码转换为字符串类型，确保匹配
        all_fund_data["基金代码"] = all_fund_data["基金代码"].astype(str)
        print("所有基金申购状态数据获取完成")
    except Exception as e:
        print(f"获取申购状态数据失败：{e}")
        return

    # 准备存储结果的列表
    result_list = []
    total = len(fund_codes)
    start_time = time.time()
    request_count = 0

    # 使用线程池并发处理基金信息查询
    with ThreadPoolExecutor() as executor:
        # 提交所有任务
        future_to_code = {executor.submit(query_fund_info, code, all_fund_data): code for code in fund_codes}

        for i, future in enumerate(as_completed(future_to_code), 1):
            # 打印进度
            print(f"查询进度：{i}/{total}")
            try:
                result = future.result()
                result_list.append(result)
                request_count += 1
                # 每查询15只基金休息0.15秒，避免请求过于频繁
                if request_count % 15 == 0:
                    time.sleep(0.15)
            except Exception as e:
                print(f"查询基金代码 {future_to_code[future]} 时出错: {e}")

    # 将结果转换为DataFrame
    result_df = pd.DataFrame(result_list)
    # 确保结果中的基金代码也是字符串类型
    result_df["基金代码"] = result_df["基金代码"].astype(str)
    # 合并原数据和查询结果
    merged_df = pd.merge(df, result_df, on="基金代码", how="left")

    # 删除购买起点大于1000元的基金，同时转换非数字值为NaN后处理
    try:
        merged_df["购买起点"] = pd.to_numeric(merged_df["购买起点"], errors="coerce")
        merged_df = merged_df[merged_df["购买起点"] <= 1000]
    except Exception as e:
        print(f"处理购买起点数据时出错：{e}")

    # 删除申购状态为“封闭期”“暂停申购”的基金
    if "申购状态" in merged_df.columns:
        merged_df = merged_df[~merged_df["申购状态"].isin(["封闭期", "暂停申购"])]

    # 将结果保存回原文件
    try:
        merged_df.to_excel(input_file, index=False)
        print(f"查询完成，结果已保存至：{input_file}")
    except Exception as e:
        print(f"保存结果失败：{e}")


if __name__ == "__main__":
    query_fund_purchase_status()
