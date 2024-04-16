import os
import sys
import re
import pandas as pd
from extract_content import extract_file_content


def extract_ID_with_context(root_directory):
    """
    递归地遍历指定根目录及其所有子目录下的所有.docx、.xlsx、.xls、.pdf文件，并提取学号及其上下文信息。
    输入：根目录
    输出：学号、前文、后文、来源文件名
    """

    all_data = []  # 存储所有文件的数据
    files_to_process = [os.path.join(dirpath, file)
                        for dirpath, _, files in os.walk(root_directory)
                        for file in files if file.endswith(('.docx', '.xlsx', '.xls', '.pdf'))
                        and not file.startswith('~$')]

    print("初始化...", end="")
    for i, file_path in enumerate(files_to_process, 1):
        file_name = os.path.basename(file_path)
        # 使用\r来返回行首，并使用end=""来避免换行
        sys.stdout.write(f"\r[{i}/{len(files_to_process)}] 正在处理: {file_name} ")
        sys.stdout.flush()
        content = extract_file_content(file_path)
        student_numbers_df = extract_student(content, file_path)
        all_data.append(student_numbers_df)

    # 合并所有DataFrame
    if len(all_data) > 0:
        return pd.concat(all_data, ignore_index=True)
    else:
        # 如果 all_data 为空，返回一个空的 DataFrame
        return pd.DataFrame(columns=['学号', '前文', '后文', '来源文件名'])


def extract_student(content, file_name):
    """
    从文本中提取本科生和研究生的学号及其前后的单词。
    如果前文或后文是单个中文字符，则提取其后的一个单词。
    """

    pattern = r'(\b\w+\b)?\s+(\b\d{9}\b|M\d{9}\b)\s+(\b\w+\b)?'
    matches = re.finditer(pattern, content)

    data = []
    for match in matches:
        before, number, after = match.groups()
        before_start = match.start(1) if before else match.start(2)
        after_end = match.end(3) if after else None

        # 如果前文是单个中文字符或不存在，则向前搜索
        if not before or (len(before) == 1 and '\u4e00' <= before <= '\u9fff'):
            # 从当前位置向前搜索一个单词或中文字符
            prev_content = content[max(before_start - 10, 0):before_start]
            prev_word_match = re.search(r'(\b\w+\b|\b[\u4e00-\u9fff]\b)\s*$', prev_content)
            if prev_word_match:
                before = prev_word_match.group() + (before if before else "")
            else:
                before = ""
        # 删除前文中的额外空格
        before = "".join(before.split())

        # 处理单个中文字符的后文
        if after and len(after) == 1 and '\u4e00' <= after <= '\u9fff':
            # 仅提取后文后面紧跟的单词
            next_word_match = re.search(r'\s+(\b\w+\b)', content[after_end:])
            if next_word_match:
                after += next_word_match.group(1)

        data.append({'学号': number,
                     '前文': before or '',
                     '后文': after or '',
                     '来源文件名': file_name})

    return pd.DataFrame(data)


def unknown_header_excel_reader(file_path):
    # 读取文件的前五行，不设置表头
    hd = pd.read_excel(file_path, header=None, nrows=5)
    # 检查哪一行同时包含'姓名'和'学号',如果没有找到同时包含'姓名'和'学号'的行，默认为第一行是表头
    header_row = 0
    for i in range(len(hd)):
        row = hd.iloc[i].apply(lambda x: x.replace(" ", "") if isinstance(x, str) else x)
        if '姓名' in row.values and '学号' in row.values:
            header_row = i
    # 读取整个表格，从表头行开始读取
    df = pd.read_excel(file_path, header=header_row)
    # 处理表头：去除所有空格
    df.columns = [col.replace(" ", "") if isinstance(col, str) else col for col in df.columns]
    return df


def extract_tables(full_dir_path):
    data_tables = []
    for root, dirs, files in os.walk(full_dir_path):
        for file in files:
            if file.endswith(('.xlsx', '.xls')) and not file.startswith('~$'):
                file_path = os.path.join(root, file)
                try:
                    data_table = unknown_header_excel_reader(file_path)
                    data_tables.append(data_table)
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
    return data_tables
