import os

import requests
import pandas as pd

cookies = {
    'Hm_lvt_66c58514bc6f9bc81f2ca3f9d9fa7a75': '1760172427',
    'HMACCOUNT': 'A89E656BF367E71C',
    'Hm_lpvt_66c58514bc6f9bc81f2ca3f9d9fa7a75': '1760172597',
}

headers = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Referer': 'https://easynomad.cn/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
    'sec-ch-ua': '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}


def get_data(page):
    params = (
        ('limit', '100'),
        ('page', page),
        ('jobCategory', ''),
        ('contractType', ''),
    )

    response = requests.get('https://easynomad.cn/api/posts/list', headers=headers, params=params, cookies=cookies)
    data = response.json()['data']
    print(data)
    return data


def get_list():
    page = 1
    all_data = []
    while True:
        data = get_data(page)
        page += 1
        if not data:
            break
        all_data.extend(data)
        # break

    return all_data


def dict_list_to_md_table(data_list, md_file_path, remove_columns=None):
    """
    将列表中的字典数据转换为Markdown表格（支持移除指定列）并写入文件

    :param data_list: 包含字典的列表
    :param md_file_path: 输出的Markdown文件路径
    :param remove_columns: 需要移除的列名列表（如 ["gender", "city"]），默认None（不移除）
    """
    if not data_list:
        print("数据列表为空，不生成表格")
        return

    # 处理移除列参数（默认空列表）
    remove_columns = remove_columns or []

    # 创建pandas DataFrame
    df = pd.DataFrame(data_list)

    # 移除指定的列
    for col in remove_columns:
        if col in df.columns:
            df = df.drop(columns=[col])

    # # 对列进行排序，保持一致性
    # df = df[sorted(df.columns)]

    # 构建Markdown内容
    md_content = """
# remote_work
remote work
每天零点自动更新列表内容

"""
    
    # 使用pandas的to_markdown方法生成表格
    md_content += df.to_markdown(index=False, tablefmt="pipe")

    # 写入文件
    with open(md_file_path, 'w', encoding='utf-8') as f:
        f.write(md_content)
    print(f"Markdown表格已写入（已移除指定列）：{md_file_path}")


if __name__ == '__main__':
    # # 清空或创建CSV文件
    # if os.path.exists('data.csv'):
    #     os.remove('data.csv')
    
    # 获取数据
    data = get_list()
    
    # 使用pandas将数据写入CSV
    if data:
        df = pd.DataFrame(data)
        df.to_csv('data.csv', index=False, encoding='utf-8')
        print(f"数据已保存到data.csv，共{len(df)}条记录")
    
    # 生成Markdown文件
    dict_list_to_md_table(data, 'README.md', remove_columns=["descContent","companyLogoUrl","companyIndustry","jobType","source","contact","tags","jobCode"])
