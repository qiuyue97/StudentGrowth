from extract_tools import extract_ID_with_context, extract_tables
import os
import pandas as pd
import numpy as np
# 设置显示所有行列
pd.set_option('display.max_columns', None)
pd.set_option('display.max_row', None)


def process_data_table(key_table, data_table):
    # 基于学号将key_table中的姓名合并到data_table
    merged_table = pd.merge(data_table, key_table[['学号', '姓名']], on='学号', how='left')
    # 筛选出同时存在于两个表的学号的行
    processed_data_table = merged_table[merged_table['学号'].isin(key_table['学号'])]
    # 删除前文或后文列包含对应姓名的行
    check_table = processed_data_table[
        ~((processed_data_table['前文'] == processed_data_table['姓名']) | (processed_data_table['后文'] == processed_data_table['姓名']))]
    return processed_data_table.copy(), check_table.copy()


def process_1(key_table):
    temp_table = key_table.copy()
    file_path = r'数据源文件\0-database\T_JW_ZB_ENROL_STU.csv'
    print(f'正在从{file_path}中读取高考信息...')
    marks_input = input('请输入满分非750的省份及对应分数，省份与分数之间用逗号隔开，各省份之间用分号隔开：\n（例如：上海,660;海南,900）\n')
    marks_input = marks_input.replace('；', ';').replace('，', ',')
    marks_dict = dict(item.split(',') for item in marks_input.split(';'))
    data_table = pd.read_csv(file_path, header=0)
    data_table.rename(columns={'XH': '学号', 'KSH': '考生号', 'XM': '姓名', 'XB': '性别', 'NF': '录取年份', 'SYD': '生源地省市', 'ZSLX': '招生类型', 'ZSKL': '招生科类', 'ZY': '录取专业', 'CC': '层次', 'GKCJ': '高考成绩（投档）', 'KFX_L1': '地区一本控分线(自招线)'}, inplace=True)
    data_table = data_table.drop(columns=['考生号', '性别', '姓名'])
    temp_table = temp_table.loc[temp_table['学号'].isin(data_table['学号'])]
    check_table = temp_table.loc[~temp_table['学号'].isin(data_table['学号'])]
    data_table['高考满分成绩'] = data_table['生源地省市'].apply(
        lambda x: int(marks_dict[x]) if x in marks_dict else 750)
    data_table['高考超本科线当量成绩'] = (data_table['高考成绩（投档）'] - data_table['地区一本控分线(自招线)']) / (750 - data_table['地区一本控分线(自招线)']) * 100
    temp_table = pd.merge(temp_table, data_table, on='学号', how='left')
    print(f'读取完成，共有{len(check_table)}名学生未获得高考信息。')
    return temp_table.copy(), check_table.copy()


def process_3_1(key_table):
    temp_table = key_table.copy()
    temp_table['3_1_point'] = 0
    file_path = r'数据源文件\0-database\T_DZZ_STU_STAFF.csv'
    print(f'正在从{file_path}中读取入党信息...')
    data_table = pd.read_csv(file_path, header=0)
    data_table.rename(columns={'XGH': '学号', 'XM': '姓名'}, inplace=True)
    temp_table.loc[temp_table['学号'].isin(data_table['学号']), '3_1_point'] = 4
    counter = temp_table.loc[temp_table['3_1_point'] == 4]
    print(f'读取完成，共获得{len(counter)}条党员信息。')
    return temp_table.copy()


def process_3_2(key_table):
    temp_table = key_table.copy()
    folder_path = r'数据源文件\3-评奖评优、入党\2-评优'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    data_table = extract_ID_with_context(full_dir_path)
    processed_data_table, check_table = process_data_table(key_table, data_table)
    print(f'"{folder_path}"文件夹抽取完成，共抽取到{len(processed_data_table)}条目标学生数据，其中{len(check_table)}条数据可能需要人工核对。')
    temp_table['3_2_point'] = temp_table['学号'].isin(processed_data_table['学号']) * 4
    return temp_table.copy(), check_table.copy()


def process_3_3(key_table):
    temp_table = key_table.copy()
    temp_table['3_3_point'] = 0
    folder_path_1 = r'数据源文件\3-评奖评优、入党\3-奖学金\国、省市级奖学金（4分）'
    folder_path_2 = r'数据源文件\3-评奖评优、入党\3-奖学金\其他奖学金（2分）'
    full_dir_path_1 = os.path.join(os.getcwd(), folder_path_1)
    full_dir_path_2 = os.path.join(os.getcwd(), folder_path_2)
    print(f'开始抽取{folder_path_1}中的数据...')
    data_table_1 = extract_ID_with_context(full_dir_path_1)
    processed_data_table_1, check_table_1 = process_data_table(key_table, data_table_1)
    print(f'"{folder_path_1}"文件夹抽取完成，共抽取到{len(processed_data_table_1)}条目标学生数据，其中{len(check_table_1)}条数据可能需要人工核对。')
    temp_table.loc[temp_table['学号'].isin(processed_data_table_1['学号']), '3_3_point'] = 4
    print(f'开始抽取{folder_path_2}中的数据...')
    data_table_2 = extract_ID_with_context(full_dir_path_2)
    processed_data_table_2, check_table_2 = process_data_table(key_table, data_table_2)
    print(f'"{folder_path_2}"文件夹抽取完成，共抽取到{len(processed_data_table_2)}条目标学生数据，其中{len(check_table_2)}条数据可能需要人工核对。')
    temp_table.loc[temp_table['学号'].isin(processed_data_table_2['学号']) & (temp_table['3_3_point'] != 4), '3_3_point'] = 2
    check_table = pd.concat([check_table_1, check_table_2], ignore_index=True)
    return temp_table.copy(), check_table.copy()


def process_3(key_table):
    table_3_1 = process_3_1(key_table)
    table_3_2, check_table_3_2 = process_3_2(key_table)
    table_3_3, check_table_3_3 = process_3_3(key_table)
    table_3 = key_table.copy()
    table_3 = table_3.merge(table_3_1[['学号', '3_1_point']], on='学号', how='left')
    table_3 = table_3.merge(table_3_2[['学号', '3_2_point']], on='学号', how='left')
    table_3 = table_3.merge(table_3_3[['学号', '3_3_point']], on='学号', how='left')
    table_3['评奖评优、入党'] = table_3[['3_1_point', '3_2_point', '3_3_point']].max(axis=1)
    check_table_3 = pd.concat([check_table_3_2, check_table_3_3], ignore_index=True)
    return table_3.copy(), check_table_3.copy()


def process_4_1(key_table):
    temp_table = key_table.copy()
    folder_path = r'数据源文件\4-奉献精神\1-献血'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    print(f'---------注意！献血统计表格中可能存在全部在校学生信息页，请删除后再启动程序抽取！---------')
    data_table = extract_ID_with_context(full_dir_path)
    processed_data_table, check_table = process_data_table(key_table, data_table)
    print(f'"{folder_path}"文件夹抽取完成，共抽取到{len(processed_data_table)}条目标学生数据，其中{len(check_table)}条数据可能需要人工核对。')
    temp_table['4_1_point'] = temp_table['学号'].isin(processed_data_table['学号']) * 2
    return temp_table.copy(), check_table.copy()


def process_4_2(key_table):
    temp_table = key_table.copy()
    folder_path = r'数据源文件\4-奉献精神\2-入伍'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    data_table = extract_ID_with_context(full_dir_path)
    processed_data_table, check_table = process_data_table(key_table, data_table)
    print(f'"{folder_path}"文件夹抽取完成，共抽取到{len(processed_data_table)}条目标学生数据，其中{len(check_table)}条数据可能需要人工核对。')
    temp_table['4_2_point'] = temp_table['学号'].isin(processed_data_table['学号']) * 2
    return temp_table.copy(), check_table.copy()


def process_4_3(key_table):
    temp_table = key_table.copy()
    folder_path = r'数据源文件\4-奉献精神\3-援疆、援藏'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    data_table = extract_ID_with_context(full_dir_path)
    processed_data_table, check_table = process_data_table(key_table, data_table)
    print(f'"{folder_path}"文件夹抽取完成，共抽取到{len(processed_data_table)}条目标学生数据，其中{len(check_table)}条数据可能需要人工核对。')
    temp_table['4_3_point'] = temp_table['学号'].isin(processed_data_table['学号']) * 2
    return temp_table.copy(), check_table.copy()


def process_4_4(key_table):
    temp_table = key_table.copy()
    temp_table['4_4_point'] = 0
    file_path = r'数据源文件\0-database\T_SDXG_DEKT.csv'
    print(f'正在从{file_path}中读取志愿者信息...')
    data_table = pd.read_csv(file_path, header=0)
    data_table.rename(columns={'XH': '学号', 'XM': '姓名'}, inplace=True)
    data_table = data_table[data_table['学号'].isin(temp_table['学号'])]
    data_table = data_table.loc[(data_table['CGMC'] == '参加志愿服务活动大于等于40小时') |
                                   (data_table['CGMC'] == '志愿者小时数大于等于40')]
    temp_table.loc[temp_table['学号'].isin(data_table['学号']), '4_4_point'] = 2
    print(f'读取完成，共获得{len(data_table)}条志愿者信息。')
    return temp_table.copy()


def process_4(key_table):
    table_4_1, check_table_4_1 = process_4_1(key_table)
    table_4_2, check_table_4_2 = process_4_2(key_table)
    table_4_3, check_table_4_3 = process_4_3(key_table)
    table_4_4 = process_4_4(key_table)
    table_4 = key_table.copy()
    table_4 = table_4.merge(table_4_1[['学号', '4_1_point']], on='学号', how='left')
    table_4 = table_4.merge(table_4_2[['学号', '4_2_point']], on='学号', how='left')
    table_4 = table_4.merge(table_4_3[['学号', '4_3_point']], on='学号', how='left')
    table_4 = table_4.merge(table_4_4[['学号', '4_4_point']], on='学号', how='left')
    table_4['奉献精神'] = table_4[['4_1_point', '4_2_point', '4_3_point', '4_4_point']].max(axis=1)
    check_table_4 = pd.concat([check_table_4_1, check_table_4_2, check_table_4_3], ignore_index=True)
    return table_4.copy(), check_table_4.copy()


def process_5(key_table):
    temp_table = key_table.copy()
    temp_table['违法违纪'] = 0
    folder_path = r'数据源文件\5-违法违纪'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    data_tables = extract_tables(full_dir_path)
    check_table_rows = []
    # 遍历 data_tables 中的每个 DataFrame
    for data_table in data_tables:
        # 查找包含“处分”的列
        for column in data_table.columns:
            if "处分" in column:
                # 检索含有特定处分的行
                for _, row in data_table.iterrows():
                    punishment = row[column]
                    score = 0
                    if '留校' in punishment:
                        score = min(score, -8)
                    if '记过' in punishment:
                        score = min(score, -6)
                    if '严重警告' in punishment:
                        score = min(score, -4)
                    if '警告' in punishment and '严重警告' not in punishment:
                        score = min(score, -2)
                    # 在 temp_table 中更新得分
                    student_id = row['学号']
                    student_name = row['姓名']
                    temp_table_row = temp_table[temp_table['学号'] == student_id]
                    if not temp_table_row.empty:
                        current_score = temp_table_row['违法违纪'].iloc[0]
                        temp_table.loc[temp_table['学号'] == student_id, '违法违纪'] = min(current_score, score)
                        # 如果姓名不匹配，将行添加到 check_table
                        if temp_table_row['姓名'].iloc[0] != student_name:
                            check_table_rows.append(row)
    # 重置 check_table 的索引
    check_table = pd.DataFrame(check_table_rows)
    print(f'抽取完毕，{len(check_table)}条数据可能需要人工核对。')
    return temp_table.copy(), check_table.copy()


def process_6(key_table):
    temp_table = key_table.copy()
    file_path = r'数据源文件\0-database\T_JW_TOTAL_SCORE.csv'
    print(f'正在从{file_path}中读取总评成绩绩点信息...')
    data_table = pd.read_csv(file_path, header=0)
    data_table.rename(columns={'XH': '学号', 'XM': '姓名'}, inplace=True)
    data_table = data_table[data_table['学号'].isin(temp_table['学号'])]
    conditions = [
        data_table['ZJD'] >= 3.7,
        (data_table['ZJD'] >= 3.3) & (data_table['ZJD'] < 3.7),
        (data_table['ZJD'] >= 3.0) & (data_table['ZJD'] < 3.3),
        (data_table['ZJD'] >= 2.7) & (data_table['ZJD'] < 3.0),
        (data_table['ZJD'] >= 2.3) & (data_table['ZJD'] < 2.7),
        (data_table['ZJD'] >= 2.0) & (data_table['ZJD'] < 2.3),
        data_table['ZJD'] < 2.0,
    ]
    scores = [14, 12, 10, 8, 6, 4, 2]
    data_table['总评成绩绩点'] = np.select(conditions, scores, default=2)
    temp_table = temp_table.merge(data_table[['学号', '总评成绩绩点', 'ZJD']], on='学号', how='left')
    temp_table['总评成绩绩点'].fillna(0, inplace=True)  # 对于未找到绩点的学生，默认分数为0
    missing_students = temp_table[~temp_table['学号'].isin(data_table['学号'])]
    print(f'读取完成，共有{len(missing_students)}位学生在数据库中未查得总绩点信息。')
    return temp_table.copy()


def process_7(key_table):
    temp_table = key_table.copy()
    file_path = r'数据源文件\0-database\T_JW_GRADE_SCORE.csv'
    print(f'正在从{file_path}中读取CET信息...')
    data_table = pd.read_csv(file_path, header=0)
    data_table.rename(columns={'XH': '学号', 'XM': '姓名'}, inplace=True)
    data_table = data_table[data_table['学号'].isin(temp_table['学号'])]
    data_table = data_table.loc[(data_table['KSKM'] == '全国大学英语四级') |
                                   (data_table['KSKM'] == '全国大学英语六级')]
    data_table['CJ'] = pd.to_numeric(data_table['CJ'], errors='coerce')
    data_table.loc[(data_table['KSKM'] == '全国大学英语四级') & (data_table['CJ'] >= 425), 'CET 4/CET 6'] = 1
    data_table.loc[(data_table['KSKM'] == '全国大学英语六级') & (data_table['CJ'] >= 425), 'CET 4/CET 6'] = 2
    # 根据学号分组，找到7_point最大的分数
    max_points = data_table.groupby('学号')['CET 4/CET 6'].max().reset_index()
    # 将计算得到的最大分数更新回temp_table
    temp_table = temp_table.merge(max_points, on='学号', how='left')
    # 对于data_table中不存在的学号，保留初始值0
    temp_table['CET 4/CET 6'].fillna(0, inplace=True)
    counter = temp_table.loc[temp_table['CET 4/CET 6'] != 0]
    print(f'读取完成，共获得{len(counter)}位同学的有效四六级成绩信息。')
    return temp_table.copy()


def process_8(key_table):
    folder_path = r'数据源文件\8-体质测试成绩'
    temp_table = key_table.copy()
    # 遍历体测数据文件夹中的所有.xls和.xlsx文件
    for file in os.listdir(folder_path):
        print(f"正在处理{file}...")
        if file.endswith('.xls') or file.endswith('.xlsx'):
            # 读取体测数据表
            pe_table = pd.read_excel(os.path.join(folder_path, file))
            # 初始化新列名，基于文件名前5个字符
            column_name = file[:5] + '体测总分'
            # 添加新列，初始值设为NaN
            temp_table[column_name] = np.NaN
            # 对于每个学号，在体测表中查找并取“总分”数据
            for index, row in temp_table.iterrows():
                student_number = row['学号']
                score_row = pe_table[pe_table['学号'] == student_number]
                if not score_row.empty:
                    temp_table.at[index, column_name] = score_row['总分'].iloc[0]
    temp_table['体测总分个数'] = temp_table.filter(regex='体测总分$').notna().sum(axis=1)
    temp_table['体测平均分'] = temp_table.filter(regex='体测总分$').mean(axis=1)
    def calculate_physical_test_score(avg_score):
        if avg_score >= 90:
            return 5
        elif 80 <= avg_score < 90:
            return 4
        elif 70 <= avg_score < 80:
            return 3
        elif 60 <= avg_score < 70:
            return 2
        else:
            return 1
    temp_table['体质测试成绩'] = temp_table['体测平均分'].apply(calculate_physical_test_score)
    return temp_table.copy()


def process_9(key_table):
    temp_table = key_table.copy()
    folder_path = r'数据源文件\9-体育赛事、运动会'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    data_table = extract_ID_with_context(full_dir_path)
    processed_data_table, check_table = process_data_table(key_table, data_table)
    print(f'"{folder_path}"文件夹抽取完成，共抽取到{len(processed_data_table)}条目标学生数据，其中{len(check_table)}条数据可能需要人工核对。')
    temp_table['体育赛事、运动会'] = temp_table['学号'].isin(processed_data_table['学号']) * 1
    return temp_table.copy(), check_table.copy()


def process_10(key_table):
    temp_table = key_table.copy()
    file_path = r'数据源文件\0-database\T_JW_SCORE.csv'
    print(f'正在从{file_path}中读取艺术审美类课程信息...')
    data_table = pd.read_csv(file_path, header=0, low_memory=False)
    data_table = data_table.drop(columns=['SCORE_ID', 'TASK_ID'])
    data_table = data_table.drop_duplicates()
    data_table.rename(columns={'XH': '学号', 'XM': '姓名'}, inplace=True)
    data_table = data_table[data_table['学号'].isin(temp_table['学号'])]
    data_table = data_table[(data_table['KCDM'].str[2] == 'Y') & (data_table['QMKSQK'] == '正常')]
    jd_weighted_means = data_table.groupby('学号').agg(
        JD_Mean=(
        'JD', lambda x: (x * data_table.loc[x.index, 'XF']).sum() / data_table.loc[x.index, 'XF'].sum()),
        Count=('JD', 'count')
    ).reset_index()
    conditions = [
        (jd_weighted_means['JD_Mean'] >= 3.7),
        (jd_weighted_means['JD_Mean'] >= 3.3) & (jd_weighted_means['JD_Mean'] < 3.7),
        (jd_weighted_means['JD_Mean'] >= 3.0) & (jd_weighted_means['JD_Mean'] < 3.3),
        (jd_weighted_means['JD_Mean'] >= 2.0) & (jd_weighted_means['JD_Mean'] < 3.0),
        (jd_weighted_means['JD_Mean'] < 2.0)
    ]
    choices = [5, 4, 3, 2, 1]
    jd_weighted_means['艺术审美类课程平均绩点'] = np.select(conditions, choices, default=0)
    temp_table = temp_table.merge(jd_weighted_means, on='学号', how='left')
    counter = temp_table.loc[pd.notna(temp_table['艺术审美类课程平均绩点'])]
    print(f'读取完成，共获得{len(counter)}位同学的有效艺术审美类课程平均绩点信息。')
    # 对于data_table中不存在的学号，保留初始值1
    temp_table['艺术审美类课程平均绩点'].fillna(1, inplace=True)
    return temp_table.copy()


def process_11(key_table):
    temp_table = key_table.copy()
    temp_table['参加校艺术团活动'] = 0
    folder_path = r'数据源文件\11-参加校艺术团活动'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    data_tables = extract_tables(full_dir_path)
    check_table_rows = []
    # 遍历 data_tables 中的每个 DataFrame
    for data_table in data_tables:
        for _, row in data_table.iterrows():
            student_id = row['学号']
            student_name = row['姓名']
            # 检查学号是否在 temp_table 中
            if student_id in temp_table['学号'].values:
                temp_table.loc[temp_table['学号'] == student_id, '参加校艺术团活动'] = 1
                # 检查姓名是否不匹配
                if temp_table.loc[temp_table['学号'] == student_id, '姓名'].iloc[0] != student_name:
                    check_table_rows.append(row)
    check_table = pd.DataFrame(check_table_rows)
    check_table = check_table.reset_index(drop=True)
    print(f'抽取完毕，{len(check_table)}条数据可能需要人工核对。')
    return temp_table.copy(), check_table.copy()


def process_12(key_table):
    temp_table = key_table.copy()
    file_path = r'数据源文件\0-database\T_JW_SCORE.csv'
    print(f'正在从{file_path}中读取劳动教育课程信息...')
    data_table = pd.read_csv(file_path, header=0, low_memory=False)
    data_table = data_table.drop(columns=['SCORE_ID', 'TASK_ID'])
    data_table = data_table.drop_duplicates()
    data_table.rename(columns={'XH': '学号', 'XM': '姓名'}, inplace=True)
    data_table = data_table[data_table['学号'].isin(temp_table['学号'])]
    data_table = data_table[(data_table['KCMC'].str.contains('劳动教育')) & (data_table['QMKSQK'] == '正常')]
    jd_weighted_means = data_table.groupby('学号').agg(
        JD_Mean=(
        'JD', lambda x: (x * data_table.loc[x.index, 'XF']).sum() / data_table.loc[x.index, 'XF'].sum()),
        Count=('JD', 'count')
    ).reset_index()
    conditions = [
        (jd_weighted_means['JD_Mean'] >= 3.0),
        (jd_weighted_means['JD_Mean'] < 3.0)
    ]
    choices = [1, 0.5]
    jd_weighted_means['劳动教育平均绩点'] = np.select(conditions, choices, default=0)
    temp_table = temp_table.merge(jd_weighted_means, on='学号', how='left')
    counter = temp_table.loc[pd.notna(temp_table['劳动教育平均绩点'])]
    print(f'读取完成，共获得{len(counter)}位同学的有效劳动教育平均绩点信息。')
    # 对于data_table中不存在的学号，保留初始值0.5
    temp_table['劳动教育平均绩点'].fillna(0.5, inplace=True)
    return temp_table.copy()


def process_13(key_table):
    temp_table = key_table.copy()
    temp_table['工程实践操作能力'] = np.nan
    temp_table['专业'] = temp_table['专业'].str.replace('（', '(').str.replace('）', ')')
    folder_path = r'数据源文件\13-工程实践操作能力'
    columns = {'专业', '集中实践教学环节学分'}
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    files = [entry.name for entry in os.scandir(full_dir_path) if
             entry.is_file() and entry.name.endswith('.xlsx') and not entry.name.startswith('~$')]
    for file in files:
        dfs = pd.read_excel(os.path.join(full_dir_path, file), sheet_name=None)
        for _, df in dfs.items():
            if columns.issubset(df.columns):
                df['专业'] = df['专业'].str.replace('（', '(').str.replace('）', ')')
                practice_scores_mapping = df.set_index('专业')['集中实践教学环节学分'].to_dict()
                mapped_scores = temp_table['专业'].map(practice_scores_mapping)
                temp_table['工程实践操作能力'] = temp_table['工程实践操作能力'].fillna(mapped_scores)
    check_table = temp_table[temp_table['工程实践操作能力'].isna()]
    temp_table['工程实践操作能力'] = temp_table['工程实践操作能力'].apply(
        lambda x: 10 if x >= 34 else (8 if x >= 30 else 6))
    return temp_table.copy(), check_table.copy()


def process_14(key_table):
    temp_table = key_table.copy()
    file_path = r'数据源文件\0-database\T_JW_SCORE.csv'
    print(f'正在从{file_path}中读取毕业设计信息...')
    data_table = pd.read_csv(file_path, header=0, low_memory=False)
    data_table = data_table.drop(columns=['SCORE_ID', 'TASK_ID'])
    data_table = data_table.drop_duplicates()
    data_table.rename(columns={'XH': '学号', 'XM': '姓名'}, inplace=True)
    data_table = data_table[data_table['学号'].isin(temp_table['学号'])]
    data_table = data_table[data_table['KCMC'].str.contains('毕业设计')]
    conditions = [
        (data_table['ZPCJ'] >= 90),
        (data_table['ZPCJ'] >= 80) & (data_table['ZPCJ'] < 90),
        (data_table['ZPCJ'] >= 70) & (data_table['ZPCJ'] < 80),
        (data_table['ZPCJ'] < 70)
    ]
    choices = [3, 2, 1, 0]
    data_table['工程设计能力'] = np.select(conditions, choices, default=0)
    data_table = data_table.loc[data_table.groupby('学号')['工程设计能力'].idxmax()]
    temp_table = temp_table.merge(data_table[['学号', 'KCMC', '工程设计能力']], on='学号', how='left')
    counter = temp_table.loc[pd.notna(temp_table['KCMC'])]
    print(f'读取完成，共获得{len(counter)}位同学的有效毕业设计成绩信息。')
    # 对于data_table中不存在的学号，保留初始值0
    temp_table['工程设计能力'].fillna(0, inplace=True)
    return temp_table.copy()


def process_15(key_table):
    temp_table = key_table.copy()
    file_path = r'数据源文件\0-database\T_SDXG_XKJS.csv'
    print(f'正在从{file_path}中读取学科竞赛获奖信息...')
    data_table = pd.read_csv(file_path, header=0, low_memory=False)
    data_table.rename(columns={'XH': '学号', 'XM': '姓名'}, inplace=True)
    data_table = data_table[data_table['学号'].isin(temp_table['学号'])]
    def calculate_score(row):
        if row['JSFL'].startswith('A') and row['GJHJMC'] == '一等奖':
            return 4
        elif row['JSFL'].startswith('A') and (row['GJHJMC'] == '二等奖' or row['SSHJMC'] == '特等奖'):
            return 3.5
        elif (row['JSFL'].startswith('A') and row['SSHJMC'] == '一等奖') or (
                row['JSFL'].startswith('B') and row['GJHJMC'] == '特等奖'):
            return 3
        elif row['JSFL'].startswith('A') and (row['GJHJMC'] == '三等奖' or row['SSHJMC'] == '二等奖') or (
                row['JSFL'].startswith('B') and row['GJHJMC'] == '一等奖'):
            return 2.5
        elif (row['JSFL'].startswith('A') and row['SSHJMC'] == '三等奖') or (
                row['JSFL'].startswith('B') and (row['GJHJMC'] == '二等奖' or row['SSHJMC'] == '一等奖')):
            return 2
        elif row['JSFL'].startswith('B') and (row['GJHJMC'] == '三等奖' or row['SSHJMC'] == '二等奖'):
            return 1.5
        elif row['JSFL'].startswith('B') and row['SSHJMC'] == '三等奖':
            return 1
        else:
            return 0
    data_table['工程创新能力'] = data_table.apply(calculate_score, axis=1)
    data_table = data_table.loc[data_table.groupby('学号')['工程创新能力'].idxmax()]
    temp_table = temp_table.merge(data_table[['学号', 'JSMC', 'JSFL', 'SSHJMC', 'GJHJMC', '工程创新能力']], on='学号', how='left')
    counter = temp_table.loc[pd.notna(temp_table['工程创新能力'])]
    print(f'读取完成，共获得{len(counter)}位同学的有效学科竞赛获奖信息。')
    # 对于data_table中不存在的学号，保留初始值0
    temp_table['工程创新能力'].fillna(0, inplace=True)
    return temp_table.copy()


def process_16(key_table):
    temp_table = key_table.copy()
    file_path = r'数据源文件\0-database\T_JW_ACHIEVEMENT.csv'
    print(f'正在从{file_path}中读取工程研究能力信息...')
    data_table = pd.read_csv(file_path, header=0, low_memory=False)
    data_table.rename(columns={'XH': '学号', 'XM': '姓名'}, inplace=True)
    data_table = data_table[data_table['学号'].isin(temp_table['学号'])]
    score_map = {
        '发表学术论文核心期刊第一作者': 4, '发表学术论文核心期刊通讯作者': 4,
        '专利申请与授权发明专利授权（第一位）': 4, '专利申请与授权实用新型专利授权（第一位）': 4,
        '发表学术论文核心期刊第二作者': 2, '专利申请与授权发明专利申请（第一位）': 2,
        '专利申请与授权实用新型专利申请（第一位）': 2, '专利申请与授权外观设计专利授权（第一位）': 2,
        '大学生创新创业训练计划(不用申请）请查看备注信息进行申请（该申请无效）': 2,
        '发表学术论文核心期刊第三作者': 1, '发表学术论文非核心期刊第一作者': 1,
        '发表学术论文非核心期刊通讯作者': 1, '专利申请与授权外观设计专利申请（第一位）': 1,
        '参与教师科研项目': 1
    }
    data_table['工程研究能力'] = data_table['CGLX'].map(score_map).fillna(0)
    data_table = data_table.loc[data_table.groupby('学号')['工程研究能力'].idxmax()]
    temp_table = temp_table.merge(data_table[['学号', 'CGLX', 'CGMC', '工程研究能力']], on='学号', how='left')
    counter = temp_table.loc[pd.notna(temp_table['工程研究能力'])]
    print(f'读取完成，共获得{len(counter)}位同学的有效工程研究能力信息。')
    # 对于data_table中不存在的学号，保留初始值0
    temp_table['工程研究能力'].fillna(0, inplace=True)
    return temp_table.copy()


def process_17_2(key_table):
    temp_table = key_table.copy()
    temp_table['17_2_point'] = 0
    folder_path = r'数据源文件\17-管理协作能力\2-校级学生会'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    data_tables = extract_tables(full_dir_path)
    check_table_rows = []
    # 遍历 data_tables 中的每个 DataFrame
    for data_table in data_tables:
        for _, row in data_table.iterrows():
            student_id = row['学号']
            student_name = row['姓名']
            # 检查学号是否在 temp_table 中
            if student_id in temp_table['学号'].values:
                # 根据“职务”列内容确定分数
                duty = row.get('职务', '')  # 使用 get 方法以防“职务”列不存在
                score = 3 if '干事' in duty else 4
                temp_table.loc[temp_table['学号'] == student_id, '17_2_point'] = score
                # 检查姓名是否不匹配
                if temp_table.loc[temp_table['学号'] == student_id, '姓名'].iloc[0] != student_name:
                    check_table_rows.append(row)
    check_table = pd.DataFrame(check_table_rows)
    check_table = check_table.reset_index(drop=True)
    print(f'抽取完毕，{len(check_table)}条数据可能需要人工核对。')
    return temp_table.copy(), check_table.copy()


def process_17_3(key_table):
    temp_table = key_table.copy()
    temp_table['17_3_point'] = 0
    folder_path = r'数据源文件\17-管理协作能力\3-院级学生会'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    data_tables = extract_tables(full_dir_path)
    check_table_rows = []
    for data_table in data_tables:
        for _, row in data_table.iterrows():
            student_id = row['学号']
            student_name = row['姓名']
            # 检查学号是否在 temp_table 中
            if student_id in temp_table['学号'].values:
                score = row.get('分数', '')
                temp_table.loc[temp_table['学号'] == student_id, '17_3_point'] = score
                # 检查姓名是否不匹配
                if temp_table.loc[temp_table['学号'] == student_id, '姓名'].iloc[0] != student_name:
                    check_table_rows.append(row)
    check_table = pd.DataFrame(check_table_rows)
    check_table = check_table.reset_index(drop=True)
    check_table['来源文件名'] = full_dir_path
    print(f'抽取完毕，{len(check_table)}条数据可能需要人工核对。')
    return temp_table.copy(), check_table.copy()


def process_17_5(key_table):
    temp_table = key_table.copy()
    folder_path = r'数据源文件\17-管理协作能力\5-社团团长'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    data_table = extract_ID_with_context(full_dir_path)
    processed_data_table, check_table = process_data_table(key_table, data_table)
    print(f'"{folder_path}"文件夹抽取完成，共抽取到{len(processed_data_table)}条目标学生数据，其中{len(check_table)}条数据可能需要人工核对。')
    temp_table['17_5_point'] = temp_table['学号'].isin(processed_data_table['学号']) * 2
    return temp_table.copy(), check_table.copy()


def process_17(key_table):
    table_17_2, check_table_17_2 = process_17_2(key_table)
    table_17_3, check_table_17_3 = process_17_3(key_table)
    table_17_5, check_table_17_5 = process_17_5(key_table)
    table_17 = key_table.copy()
    table_17 = table_17.merge(table_17_2[['学号', '17_2_point']], on='学号', how='left')
    table_17 = table_17.merge(table_17_3[['学号', '17_3_point']], on='学号', how='left')
    table_17 = table_17.merge(table_17_5[['学号', '17_5_point']], on='学号', how='left')
    table_17['管理协作能力'] = table_17[['17_2_point', '17_3_point', '17_5_point']].max(axis=1)
    check_table_17 = pd.concat([check_table_17_2, check_table_17_3, check_table_17_5], ignore_index=True)
    return table_17.copy(), check_table_17.copy()


def process_18(key_table):
    temp_table = key_table.copy()
    folder_path = r'数据源文件\18-国际交流'
    full_dir_path = os.path.join(os.getcwd(), folder_path)
    print(f'开始抽取{folder_path}中的数据...')
    data_table = extract_ID_with_context(full_dir_path)
    processed_data_table, check_table = process_data_table(key_table, data_table)
    print(f'"{folder_path}"文件夹抽取完成，共抽取到{len(processed_data_table)}条目标学生数据，其中{len(check_table)}条数据可能需要人工核对。')
    temp_table['国际交流'] = temp_table['学号'].isin(processed_data_table['学号']) * 1
    return temp_table.copy(), check_table.copy()


def process_19(key_table):
    temp_table = key_table.copy()
    file_path = r'数据源文件\0-database\T_JOB_STU_BYQX.csv'
    print(f'正在从{file_path}中读取可持续发展能力信息...')
    qs_table_path = r'数据源文件\19-可持续发展能力\qs_table.xlsx'
    print(f'正在从{qs_table_path}中读取高校QS排名信息...')
    data_table = pd.read_csv(file_path, header=0, low_memory=False)
    qs_table = pd.read_excel(qs_table_path)
    data_table.rename(columns={'XH': '学号', 'XM': '姓名'}, inplace=True)
    data_table = data_table[data_table['学号'].isin(temp_table['学号'])]
    conditions_zczj = [
        (data_table['ZCZJ'] >= 1500000),
        (data_table['ZCZJ'] < 1500000) & (data_table['ZCZJ'] >= 1000000),
        (data_table['ZCZJ'] < 1000000) & (data_table['ZCZJ'] >= 500000),
        (data_table['ZCZJ'] < 500000) & (data_table['ZCZJ'] >= 100000),
        (data_table['ZCZJ'] < 100000)
    ]
    conditions_sqgz = [
        (data_table['SQGZ'] >= 15000),
        (data_table['SQGZ'] < 15000) & (data_table['SQGZ'] >= 10000),
        (data_table['SQGZ'] < 10000) & (data_table['SQGZ'] >= 5000),
        (data_table['SQGZ'] < 7000) & (data_table['SQGZ'] >= 5000),
        (data_table['SQGZ'] < 5000)
    ]
    choices = [28, 26, 24, 22, 20]
    data_table['ZCZJ_point'] = np.select(conditions_zczj, choices, default=20)
    data_table['SQGZ_point'] = np.select(conditions_sqgz, choices, default=20)
    # 假设需要一个函数来判断 data_table 中每行的某列值是否存在于 qs_table 中，并返回相应的 qs_rank 分值
    def get_qs_rank_score(row, qs_table):
        scores = []
        # 转换data_table中相关列为小写进行匹配
        for col in ['XX', 'DXM', 'GWDXMC']:
            name = row[col].lower() if pd.notna(row[col]) else None
            if name:
                # 转换qs_table中的列为小写进行匹配
                matched_ranks = qs_table.loc[
                    qs_table['original_name'].str.lower().eq(name) | qs_table['chinese_name'].str.lower().eq(
                        name), 'qs_rank']
                for rank in matched_ranks:
                    if rank <= 100:
                        scores.append(28)
                    elif rank <= 500:
                        scores.append(26)
                    elif rank <= 1000:
                        scores.append(24)
                    elif rank <= 1500:
                        scores.append(22)
                    else:
                        scores.append(20)
        return max(scores) if scores else 20  # 如果有匹配分数，取最高分；否则返回20分
    data_table['QS_point'] = data_table.apply(get_qs_rank_score, qs_table=qs_table, axis=1)
    data_table['可持续发展能力'] = data_table[['ZCZJ_point', 'SQGZ_point', 'QS_point']].max(axis=1)
    temp_table = temp_table.merge(data_table[['学号', 'ZCZJ', 'SQGZ', 'XX', 'DXM', 'GWDXMC', 'ZCZJ_point', 'SQGZ_point', 'QS_point', '可持续发展能力']], on='学号', how='left')
    counter = temp_table.loc[pd.notna(temp_table['可持续发展能力'])]
    print(f'读取完成，共获得{len(counter)}位同学的有效可持续发展能力信息。')
    # 对于data_table中不存在的学号，保留初始值20
    temp_table['可持续发展能力'].fillna(20, inplace=True)
    return temp_table.copy()


def process_main(full_key_table):
    key_table = full_key_table[['学号', '姓名']].copy()
    table_1, check_table_1 = process_1(key_table)
    table_3, check_table_3 = process_3(key_table)
    table_4, check_table_4 = process_4(key_table)
    table_5, check_table_5 = process_5(key_table)
    table_6 = process_6(key_table)
    table_7 = process_7(key_table)
    table_8 = process_8(key_table)
    table_9, check_table_9 = process_9(key_table)
    table_10 = process_10(key_table)
    table_11, check_table_11 = process_11(key_table)
    table_12 = process_12(key_table)
    table_13, check_table_13 = process_13(full_key_table)
    table_14 = process_14(key_table)
    table_15 = process_15(key_table)
    table_16 = process_16(key_table)
    table_17, check_table_17 = process_17(key_table)
    table_18, check_table_18 = process_18(key_table)
    table_19 = process_19(key_table)
    key_table = key_table.merge(table_1[['学号', '高考超本科线当量成绩']], on='学号', how='left')
    key_table['道德素养'] = ~key_table['学号'].isin(table_5[table_5['违法违纪'] != 0]['学号']) * 10
    key_table = key_table.merge(table_3[['学号', '评奖评优、入党']], on='学号', how='left')
    key_table = key_table.merge(table_4[['学号', '奉献精神']], on='学号', how='left')
    key_table = key_table.merge(table_5[['学号', '违法违纪']], on='学号', how='left')
    key_table = key_table.merge(table_6[['学号', '总评成绩绩点']], on='学号', how='left')
    key_table = key_table.merge(table_7[['学号', 'CET 4/CET 6']], on='学号', how='left')
    key_table = key_table.merge(table_8[['学号', '体质测试成绩']], on='学号', how='left')
    key_table = key_table.merge(table_9[['学号', '体育赛事、运动会']], on='学号', how='left')
    key_table = key_table.merge(table_10[['学号', '艺术审美类课程平均绩点']], on='学号', how='left')
    key_table = key_table.merge(table_11[['学号', '参加校艺术团活动']], on='学号', how='left')
    key_table = key_table.merge(table_12[['学号', '劳动教育平均绩点']], on='学号', how='left')
    key_table = key_table.merge(table_13[['学号', '工程实践操作能力']], on='学号', how='left')
    key_table = key_table.merge(table_14[['学号', '工程设计能力']], on='学号', how='left')
    key_table = key_table.merge(table_15[['学号', '工程创新能力']], on='学号', how='left')
    key_table = key_table.merge(table_16[['学号', '工程研究能力']], on='学号', how='left')
    key_table = key_table.merge(table_17[['学号', '管理协作能力']], on='学号', how='left')
    key_table = key_table.merge(table_18[['学号', '国际交流']], on='学号', how='left')
    key_table = key_table.merge(table_19[['学号', '可持续发展能力']], on='学号', how='left')
    key_table['总分'] = key_table.drop(columns=['学号', '姓名']).sum(axis=1)
    folder_path = os.path.join(os.getcwd(), r'供检查的过程文件')
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    table_details_path = r'供检查的过程文件\各小分细表.xlsx'
    check_table_path= r'供检查的过程文件\可能需要人工检查的数据.xlsx'
    with pd.ExcelWriter(os.path.join(os.getcwd(), table_details_path)) as writer:
        table_1.to_excel(writer, index=False, sheet_name='高考超本科线当量成绩')
        table_3.to_excel(writer, index=False, sheet_name='评奖评优、入党')
        table_4.to_excel(writer, index=False, sheet_name='奉献精神')
        table_5.to_excel(writer, index=False, sheet_name='违法违纪')
        table_8.to_excel(writer, index=False, sheet_name='体质测试成绩')
        table_9.to_excel(writer, index=False, sheet_name='体育赛事、运动会')
        table_10.to_excel(writer, index=False, sheet_name='艺术审美类课程平均绩点')
        table_11.to_excel(writer, index=False, sheet_name='参加校艺术团活动')
        table_12.to_excel(writer, index=False, sheet_name='劳动教育平均绩点')
        table_13.to_excel(writer, index=False, sheet_name='工程实践操作能力')
        table_14.to_excel(writer, index=False, sheet_name='工程设计能力')
        table_15.to_excel(writer, index=False, sheet_name='工程创新能力')
        table_16.to_excel(writer, index=False, sheet_name='工程研究能力')
        table_17.to_excel(writer, index=False, sheet_name='管理协作能力')
        table_18.to_excel(writer, index=False, sheet_name='国际交流')
        table_19.to_excel(writer, index=False, sheet_name='可持续发展能力')
    with pd.ExcelWriter(os.path.join(os.getcwd(), check_table_path)) as writer:
        check_table_1.to_excel(writer, index=False, sheet_name='高考超本科线当量成绩')
        check_table_3.to_excel(writer, index=False, sheet_name='评奖评优、入党')
        check_table_4.to_excel(writer, index=False, sheet_name='奉献精神')
        check_table_5.to_excel(writer, index=False, sheet_name='违法违纪')
        check_table_9.to_excel(writer, index=False, sheet_name='体育赛事、运动会')
        check_table_11.to_excel(writer, index=False, sheet_name='参加校艺术团活动')
        check_table_13.to_excel(writer, index=False, sheet_name='工程实践操作能力')
        check_table_17.to_excel(writer, index=False, sheet_name='管理协作能力')
        check_table_18.to_excel(writer, index=False, sheet_name='国际交流')
    return key_table.copy()
