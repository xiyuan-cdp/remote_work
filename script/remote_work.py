import requests
from DataRecorder import Recorder

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
        r.add_data(data)
        all_data.extend(data)

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

    # 步骤1：提取所有唯一的键，然后过滤掉需要移除的列
    all_headers = set()
    for item in data_list:
        all_headers.update(item.keys())

    # 过滤表头：排除需要移除的列
    headers = [header for header in all_headers if header not in remove_columns]
    headers = sorted(headers)  # 排序表头，保持一致性

    # 步骤2：构建Markdown表格内容
    md_lines = []
    # 表头行
    md_lines.append("| " + " | ".join(headers) + " |")
    # 分隔线
    md_lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    # 步骤3：添加每行数据（只包含过滤后的列）
    for item in data_list:
        row = [str(item.get(header, "")) for header in headers]
        md_lines.append("| " + " | ".join(row) + " |")

    # 步骤4：写入文件
    with open(md_file_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(md_lines))
    print(f"Markdown表格已写入（已移除指定列）：{md_file_path}")


if __name__ == '__main__':
    r = Recorder('data.csv')
    data = get_list()
    r.record()  # 记录数据
    dict_list_to_md_table(data, 'data.md', remove_columns=["descContent"])
