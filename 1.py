def dict_list_to_md_table(data_list, md_file_path):
    """
    将列表中的字典数据转换为Markdown表格并写入文件

    :param data_list: 包含字典的列表（字典键可不一致，会自动合并所有键作为表头）
    :param md_file_path: 输出的Markdown文件路径
    """
    if not data_list:
        print("数据列表为空，不生成表格")
        return

    # 步骤1：提取所有唯一的键作为表头（确保包含所有字典的键）
    headers = set()
    for item in data_list:
        headers.update(item.keys())
    headers = sorted(headers)  # 排序表头，保持一致性

    # 步骤2：构建Markdown表格内容
    md_lines = []
    # 表头行（用|分隔）
    md_lines.append("| " + " | ".join(headers) + " |")
    # 分隔线（表头与内容之间的横线，用---分隔）
    md_lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    # 步骤3：添加每行数据
    for item in data_list:
        # 对每个键取值，若不存在则用空字符串填充
        row = [str(item.get(header, "")) for header in headers]
        md_lines.append("| " + " | ".join(row) + " |")

    # 步骤4：写入文件
    with open(md_file_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(md_lines))
    print(f"Markdown表格已写入：{md_file_path}")


# 示例用法
if __name__ == "__main__":
    # 示例数据（字典键可不一致）
    data = [
        {"name": "张三", "age": 20, "gender": "男"},
        {"name": "李四", "age": 25, "city": "北京"},  # 缺少gender，多了city
        {"name": "王五", "age": 30, "gender": "男", "city": "上海"}
    ]

    # 生成表格到data_table.md
    dict_list_to_md_table(data, "data_table.md")