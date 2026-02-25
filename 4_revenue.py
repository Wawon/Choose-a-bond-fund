"""
债券基金收益率查询程序

功能概述：
- 查询筛选后债券基金的近期收益率表现
- 获取每只基金近1年和近3月的收益率数据
- 将收益率信息整合到基金基础数据中

数据来源：
- 使用AKShare的fund_individual_achievement_xq接口
- 获取基金的阶段业绩数据

处理流程：
1. 读取经过前期筛选的债券基金代码列表
2. 遍历每只基金代码查询收益率数据
3. 提取近1年收益率和近3月收益率指标
4. 处理查询异常和数据缺失情况
5. 将收益率数据添加到原数据框中
6. 保存更新后的完整数据到Excel文件

异常处理机制：
- 网络请求异常的重试机制
- 数据结构变化的兼容性处理
- 查询频率控制避免接口限制
- 备用文件保存机制确保数据不丢失
"""

import akshare as ak
import pandas as pd
import time
import json
from requests.exceptions import RequestException


def query_fund_returns():
    # 输入文件路径
    input_file = r"C:\Users\wawon\PycharmProjects\PythonProject1\.venv\Bond All in One\大于三年的债券基金代码.xlsx"

    try:
        # 读取Excel文件
        df = pd.read_excel(input_file)
        # 获取第一列的基金代码并转换为字符串
        fund_codes = df.iloc[:, 0].astype(str).tolist()
        total = len(fund_codes)
        print(f"成功读取Excel文件，共{total}只基金需要查询")
    except Exception as e:
        print(f"读取Excel文件失败：{e}")
        return

    # 准备存储收益率的列表
    returns_1y = []
    returns_3m = []

    # 遍历每个基金代码查询数据
    for i, code in enumerate(fund_codes, 1):
        # 确保基金代码是6位格式（补全前导零）
        code = code.zfill(6)
        # 打印进度信息
        print(f"查询进度：{i}/{total}，当前基金代码：{code}")

        try:
            # 调用接口获取基金业绩数据，增加超时设置
            fund_data = ak.fund_individual_achievement_xq(symbol=code, timeout=10)

            # 提取近1年收益率（阶段业绩中的近1年）
            cond_1y = (fund_data["业绩类型"] == "阶段业绩") & (fund_data["周期"] == "近1年")
            ret_1y = fund_data.loc[cond_1y, "本产品区间收益"].values[0] if not fund_data[cond_1y].empty else None

            # 提取近3月收益率（阶段业绩中的近3月）
            cond_3m = (fund_data["业绩类型"] == "阶段业绩") & (fund_data["周期"] == "近3月")
            ret_3m = fund_data.loc[cond_3m, "本产品区间收益"].values[0] if not fund_data[cond_3m].empty else None

            returns_1y.append(ret_1y)
            returns_3m.append(ret_3m)
            print(f"基金{code}查询成功：近1年收益率={ret_1y}%，近3月收益率={ret_3m}%")

        except KeyError as e:
            # 处理数据结构变化导致的KeyError
            print(f"基金{code}数据结构异常：{e}，尝试兼容处理")
            try:
                # 尝试直接获取接口原始数据进行解析
                fund_data = ak.fund_individual_achievement_xq(symbol=code, timeout=10)
                # 转换为字典查看结构
                data_dict = fund_data.to_dict('records')

                # 重新尝试查找近1年和近3月数据
                ret_1y = None
                ret_3m = None
                for item in data_dict:
                    if item.get('业绩类型') == '阶段业绩':
                        if item.get('周期') == '近1年':
                            ret_1y = item.get('本产品区间收益')
                        if item.get('周期') == '近3月':
                            ret_3m = item.get('本产品区间收益')

                returns_1y.append(ret_1y if ret_1y is not None else "未找到数据")
                returns_3m.append(ret_3m if ret_3m is not None else "未找到数据")
            except Exception as e2:
                print(f"兼容处理失败：{e2}")
                returns_1y.append("查询失败")
                returns_3m.append("查询失败")

        except RequestException as e:
            print(f"基金{code}网络请求失败：{e}，将重试一次")
            # 重试一次
            try:
                time.sleep(2)  # 等待2秒后重试
                fund_data = ak.fund_individual_achievement_xq(symbol=code, timeout=15)

                cond_1y = (fund_data["业绩类型"] == "阶段业绩") & (fund_data["周期"] == "近1年")
                ret_1y = fund_data.loc[cond_1y, "本产品区间收益"].values[0] if not fund_data[cond_1y].empty else None

                cond_3m = (fund_data["业绩类型"] == "阶段业绩") & (fund_data["周期"] == "近3月")
                ret_3m = fund_data.loc[cond_3m, "本产品区间收益"].values[0] if not fund_data[cond_3m].empty else None

                returns_1y.append(ret_1y)
                returns_3m.append(ret_3m)
                print(f"基金{code}重试成功")
            except Exception as e2:
                print(f"基金{code}重试也失败：{e2}")
                returns_1y.append("查询失败")
                returns_3m.append("查询失败")

        except Exception as e:
            print(f"基金{code}查询失败：{e}")
            returns_1y.append("查询失败")
            returns_3m.append("查询失败")

        # 调整查询频率，避免请求过于频繁
        if i % 15 == 0:
            print("稍作休息，避免请求过于频繁...")
            time.sleep(0.75)  # 延长休息时间

    # 将收益率数据添加到原DataFrame的右侧
    df["近1年收益率(%)"] = returns_1y
    df["近3月收益率(%)"] = returns_3m

    # 保存回原Excel文件
    try:
        df.to_excel(input_file, index=False)
        print(f"所有查询完成，结果已添加到原文件：{input_file}")
    except Exception as e:
        print(f"保存文件失败：{e}")
        # 保存到备用文件
        backup_file = input_file.replace(".xlsx", "_backup.xlsx")
        try:
            df.to_excel(backup_file, index=False)
            print(f"已将结果保存到备用文件：{backup_file}")
        except Exception as e2:
            print(f"备用文件保存也失败：{e2}")


if __name__ == "__main__":
    query_fund_returns()
