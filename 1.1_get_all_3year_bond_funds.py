"""
债券基金筛选程序

功能概述：
- 筛选成立时间超过3年的债券型公募基金
- 从全市场基金中筛选名称包含"债"字的基金
- 验证每只基金的成立时间是否满足3年以上条件
- 使用多线程并发处理提高数据获取效率
- 输出符合条件的基金代码列表到Excel文件

处理流程：
1. 获取全市场公募基金列表（使用fund_name_em接口）
2. 筛选基金名称中包含"债"字的债券型基金
3. 计算3年前的日期作为筛选阈值
4. 并发验证每只基金的成立时间（使用fund_individual_basic_info_xq接口）
5. 保存符合条件的基金代码到Excel文件
6. 记录处理失败的基金信息用于后续排查

输出文件：
- 大于三年的债券基金代码.xlsx：包含所有符合条件的基金代码
- 基金处理错误记录.xlsx：记录处理过程中出现错误的基金信息
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue


# 定义一个辅助函数，用于处理单个基金的成立时间验证
def check_fund_establishment(fund_code, fund_name, three_years_ago):
    try:
        # 官网指定接口：获取基金基本信息（含成立时间）
        # 文档地址：https://akshare.akfamily.xyz/data/fund/fund_public.html#fund-individual-basic-info-xq
        fund_info = ak.fund_individual_basic_info_xq(symbol=fund_code)

        # 提取成立时间（官网返回格式为DataFrame，项目列含"成立时间"）
        establish_row = fund_info[fund_info['item'] == '成立时间']
        if establish_row.empty:
            raise ValueError("未找到'成立时间'字段")

        establish_date_str = establish_row['value'].values[0]
        # 解析日期（官网示例格式：2015-01-01）
        establish_date = datetime.strptime(establish_date_str, '%Y-%m-%d')

        # 判断是否符合条件
        if establish_date <= three_years_ago:
            print(f"✅ 符合条件（成立日期：{establish_date_str}）")
            return fund_code, None
        else:
            print(f"❌ 成立时间不足3年（成立日期：{establish_date_str}）")
            return None, None

    except Exception as e:
        error_msg = f"❌ 处理失败：{str(e)}"
        print(error_msg)
        return None, {
            '基金代码': fund_code,
            '基金名称': fund_name,
            '错误信息': str(e)
        }


def get_valid_bond_funds():
    # 1. 获取公募基金列表（官网推荐：fund_name_em）
    print("===== 步骤1：获取基金列表（fund_name_em） =====")
    try:
        # 官网接口：获取全市场公募基金代码和名称
        all_funds = ak.fund_name_em()
        all_funds['基金代码'] = all_funds['基金代码'].astype(str).str.zfill(6)  # 6位代码格式化
        print(f"成功获取 {len(all_funds)} 只基金")
    except Exception as e:
        print(f"获取基金列表失败：{e}")
        return None

    # 2. 筛选名称含"债"的基金
    print("\n===== 步骤2：筛选债券类基金 =====")
    bond_funds = all_funds[all_funds['基金简称'].str.contains('债', na=False)]
    total_bond = len(bond_funds)
    print(f"名称含'债'的基金共 {total_bond} 只")

    # 3. 计算3年前日期阈值
    three_years_ago = datetime.now() - timedelta(days=3 * 365)
    valid_codes = []
    error_records = []

    # 4. 验证成立时间（官网接口：fund_individual_basic_info_xq）
    print(f"\n===== 步骤3：验证成立时间（fund_individual_basic_info_xq） =====")
    print(f"成立时间阈值：{three_years_ago.strftime('%Y-%m-%d')}")

    # 使用线程池并发处理基金验证
    # 限制并发数以避免高频访问问题
    max_workers = 15
    print(f"使用线程池并发处理，最大并发数：{max_workers}")

    # 记录处理进度
    processed_count = 0
    start_time = time.time()
    # 用于控制请求频率的队列
    request_queue = Queue()

    def controlled_submit(executor, func, *args):
        # 如果队列中有10个请求，等待0.2秒
        if request_queue.qsize() >= 15:
            print("--- 暂停0.15秒，降低请求频率 ---")
            time.sleep(0.15)
            # 清空队列
            while not request_queue.empty():
                request_queue.get()
        request_queue.put(1)
        return executor.submit(func, *args)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        # 使用 enumerate 替代 iterrows 的索引，确保进度计数连续
        for current_progress, row in enumerate(bond_funds.iterrows(), start=1):
            _, row_data = row
            fund_code = row_data['基金代码']
            fund_name = row_data['基金简称']

            print(f"\n[{current_progress}/{total_bond}] 提交处理：{fund_code} {fund_name}")

            future = controlled_submit(executor, check_fund_establishment, fund_code, fund_name, three_years_ago)
            futures.append(future)

        for future in as_completed(futures):
            valid_code, error_record = future.result()
            if valid_code:
                valid_codes.append(valid_code)
            if error_record:
                error_records.append(error_record)
            processed_count += 1
            elapsed_time = time.time() - start_time
            if processed_count % 10 == 0:
                print(f"已处理 {processed_count}/{total_bond} 只基金，耗时 {elapsed_time:.2f} 秒")

    # 5. 保存结果
    print("\n===== 处理完成 =====")
    print(f"符合条件的基金：{len(valid_codes)} 只 | 处理失败：{len(error_records)} 只")

    pd.DataFrame(valid_codes, columns=['基金代码']).to_excel(
        '大于三年的债券基金代码.xlsx', index=False
    )
    if error_records:
        pd.DataFrame(error_records).to_excel('基金处理错误记录.xlsx', index=False)
        print("错误记录已保存，可用于排查个别基金问题")
    print("结果文件：大于三年的债券基金代码.xlsx")


if __name__ == "__main__":
    get_valid_bond_funds()
