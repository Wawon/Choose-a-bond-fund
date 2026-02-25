"""
债券基金风险收益评估程序

功能概述：
- 计算债券基金的Ulcer指数（痛苦指数）和Martin比率
- 评估基金在指定时间区间内的风险调整后收益表现
- 筛选出收益风险比较优的基金产品

核心指标：
1. Ulcer指数：衡量基金回撤风险的指标，数值越小风险越低
2. 年化收益率：基金在指定期间的复合年化收益
3. Martin比率：(年化收益率-无风险利率)/Ulcer指数，衡量风险调整后收益

处理流程：
1. 读取筛选后的债券基金代码列表
2. 获取每只基金在指定时间区间的累计净值数据
3. 并发计算各基金的Ulcer指数和Martin比率
4. 应用筛选条件：年化收益率3.5%-10% 且 Martin比率≥3.5
5. 将计算结果与原数据合并后保存

技术特点：
- 使用多线程并发处理提高计算效率
- 实现重试机制应对网络不稳定情况
- 添加随机延迟避免请求过于频繁
- 自动适配CPU核心数优化线程池大小
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_fund_data(fund_code, max_retries=3, retry_delay=5):
    """获取基金数据，包含重试机制"""
    for attempt in range(max_retries):
        try:
            # 添加随机延迟，降低请求频率
            time.sleep(random.uniform(0.5, 2))
            fund_data = ak.fund_open_fund_info_em(
                symbol=str(fund_code).zfill(6),  # 确保基金代码为 6 位字符串
                indicator="累计净值走势"  # 修改指标为累计净值走势
            )
            return fund_data
        except Exception as e:
            print(f"获取基金 {fund_code} 数据时出错（尝试 {attempt + 1}/{max_retries}）: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    return None


def calculate_ulcer_index(net_values):
    # 直接使用净值比例计算累计增长
    cumulative_returns = net_values / net_values.iloc[0]

    # 计算峰值
    peak = cumulative_returns.cummax()

    # 计算回撤百分比，取正值表示下跌比例
    drawdown = (peak - cumulative_returns) / peak

    # 计算回撤百分比的平方
    drawdown_squared = drawdown ** 2

    # 计算 Ulcer 指数
    ulcer_index = np.sqrt(drawdown_squared.mean()) * 100
    return ulcer_index


def calculate_annualized_return(net_values, start_date, end_date):
    # 计算投资年限
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    years = (end_date - start_date).days / 365

    # 获取期初和期末净值
    start_value = net_values.iloc[0]
    end_value = net_values.iloc[-1]

    # 使用正确公式计算年化收益率
    annualized_return = ((end_value / start_value) ** (1 / years) - 1) * 100
    return annualized_return


def calculate_martin_ratio(annualized_return, risk_free_rate, ulcer_index):
    """计算 Martin Ratio"""
    return (annualized_return - risk_free_rate) / ulcer_index


def process_fund(fund_code, start_date_str, end_date_str, risk_free_rate):
    """处理单个基金的计算逻辑"""
    fund_data = get_fund_data(fund_code)
    result = None

    if fund_data is not None and not fund_data.empty:
        # 将净值日期转换为日期类型并设置为索引
        fund_data['净值日期'] = pd.to_datetime(fund_data['净值日期'])
        fund_data.set_index('净值日期', inplace=True)
        fund_data.sort_index(inplace=True)

        try:
            # 获取指定日期的净值数据，修改为获取累计净值
            start_value = fund_data.loc[start_date_str]['累计净值']
            end_value = fund_data.loc[end_date_str]['累计净值']
        except KeyError:
            print(f"未找到基金 {fund_code} 指定日期的净值数据，请检查数据是否包含对应日期。")
            return result
        else:
            # 提取指定日期范围内的累计净值列（复权净值）
            net_values = fund_data.loc[start_date_str:end_date_str]['累计净值']
            if net_values.empty:
                print(f"基金 {fund_code} 指定日期范围内未获取到有效的累计净值数据。")
                return result

            # 计算年化收益率
            start_date = pd.to_datetime(start_date_str)
            end_date = pd.to_datetime(end_date_str)
            annualized_return = calculate_annualized_return(net_values, start_date, end_date)

            # 计算 Ulcer 指数
            ulcer_index = calculate_ulcer_index(net_values)

            # 计算 Martin Ratio
            martin_ratio = calculate_martin_ratio(annualized_return, risk_free_rate, ulcer_index)

            # 记录结果
            result = {
                '基金代码': str(fund_code).zfill(6),
                '年化收益率(%)': annualized_return,
                'Ulcer指数(%)': ulcer_index,
                'Martin Ratio': martin_ratio
            }

            print(
                f"债券基金 {fund_code} 在 {start_date_str} 至 {end_date_str} 的年化收益率为: {annualized_return:.4f}%，"
                f"Ulcer 指数为: {ulcer_index:.4f}%，Martin Ratio 为: {martin_ratio:.4f}")
    else:
        print(f"未获取到基金 {fund_code} 有效的净值数据，请检查基金代码或网络连接。")

    return result


if __name__ == "__main__":
    # 设置默认无风险利率为 1.5%
    risk_free_rate = 1.5

    # 定义目标日期
    start_date_str = "2023-02-24"
    end_date_str = "2026-02-24"

    # 读取基金代码文件
    input_path = r"C:\Users\wawon\PycharmProjects\PythonProject1\.venv\Bond All in One\大于三年的债券基金代码.xlsx"
    try:
        # 先读取文件获取列名
        temp_df = pd.read_excel(input_path, nrows=0)
        first_column = temp_df.columns[0]
        # 重新读取文件并指定第一列的数据类型为字符串
        fund_codes_df = pd.read_excel(input_path, dtype={first_column: str})
        fund_codes = fund_codes_df.iloc[:, 0].tolist()
    except Exception as e:
        print(f"读取文件 {input_path} 出错: {str(e)}")
        exit(1)

    # 初始化结果列表
    results = []

    # 计算最佳线程数，通常设置为 CPU 核心数的 2 - 4 倍
    import multiprocessing

    max_workers = multiprocessing.cpu_count() * 4

    # 使用线程池并发处理基金数据，指定最大线程数
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for fund_code in fund_codes:
            future = executor.submit(process_fund, fund_code, start_date_str, end_date_str, risk_free_rate)
            futures.append(future)

        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    # 将结果转换为 DataFrame
    results_df = pd.DataFrame(results)

    # 添加筛选逻辑，只保留年化收益率在3.5%到10%之间且Martin比率≥3.5的基金
    if not results_df.empty:
        results_df = results_df[
            (results_df['年化收益率(%)'] >= 3.5) & 
            (results_df['年化收益率(%)'] <= 10) & 
            (results_df['Martin Ratio'] >= 3.5)
        ]

    # 将结果合并到原数据中，使用内连接只保留匹配的行
    merged_df = pd.merge(fund_codes_df, results_df, left_on=fund_codes_df.columns[0], right_on='基金代码', how='inner')

    # 将结果保存回原文件
    try:
        merged_df.to_excel(input_path, index=False)
        print(f"结果已成功保存到 {input_path}")
    except Exception as e:
        print(f"保存文件 {input_path} 出错: {str(e)}")
