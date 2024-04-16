import os
import pandas as pd
import sys
import configparser
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from datetime import datetime


def verify_folders(folders):
    """ 验证当前目录下是否存在指定的文件夹结构及特定文件和格式。 """
    missing_folders = []
    incorrect_files = []
    for _, folder_path in folders:
        full_path = os.path.join(os.getcwd(), folder_path)
        if not os.path.exists(full_path):
            missing_folders.append(folder_path)

    # 特定文件的检查逻辑
    qs_table_path = os.path.join(os.getcwd(), '数据源文件\\19-可持续发展能力', 'qs_table.xlsx')
    if os.path.exists(qs_table_path):
        try:
            df = pd.read_excel(qs_table_path)
            required_columns = ['original_name', 'chinese_name', 'qs_rank']
            if not all(column in df.columns for column in required_columns):
                raise ValueError("qs_table.xlsx列名不符合要求。")
        except Exception as e:
            incorrect_files.append("数据源文件\\19-可持续发展能力\\qs_table.xlsx: " + str(e))
    else:
        incorrect_files.append("数据源文件\\19-可持续发展能力\\qs_table.xlsx: 文件不存在。")

    if missing_folders or incorrect_files:
        missing_str = "\n".join(missing_folders + incorrect_files)
        raise FileNotFoundError(f"缺失或不正确的文件夹/文件:\n{missing_str}")
    else:
        return True


def create_folders(folders):
    """ 在当前目录下创建文件夹，并确保特定文件存在且格式正确。 """
    for _, folder_path in folders:
        full_path = os.path.join(os.getcwd(), folder_path)
        if not os.path.exists(full_path):
            os.makedirs(full_path)

    # 创建或更新qs_table.xlsx
    qs_table_path = os.path.join(os.getcwd(), '数据源文件\\19-可持续发展能力', 'qs_table.xlsx')
    required_columns = ['original_name', 'chinese_name', 'qs_rank']
    try:
        df = pd.read_excel(qs_table_path)
        if not all(column in df.columns for column in required_columns):
            raise ValueError
    except Exception:
        # 文件不存在或列名不符合要求，创建新文件
        df = pd.DataFrame(columns=required_columns)
        df.to_excel(qs_table_path, index=False)
        print(f"已创建或更新 qs_table.xlsx 在 {qs_table_path}")


def download():
    # 创建数据库连接
    config = configparser.ConfigParser()
    config.read('config.ini')

    user = config['database']['user']
    password = config['database']['password']
    host = config['database']['host']
    port = config['database']['port']
    dbname = config['database']['dbname']

    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"

    engine = create_engine(connection_string)

    try:
        # 创建Inspector对象
        inspector = inspect(engine)

        # 获取数据库中所有表的名称
        tables = inspector.get_table_names()
        print(f"找到数据库中的表：{tables}")

        # 确保数据源文件夹存在
        data_folder = "./数据源文件/0-database/"
        os.makedirs(data_folder, exist_ok=True)

        # 遍历所有表名，下载并保存为CSV文件
        for table_name in tables:
            if table_name == 'test':
                continue

            if table_name == 'T_JW_SCORE':
                # 对于T_JW_SCORE表，执行特定的查询
                query = text(
                    "SELECT * FROM T_JW_SCORE WHERE KCDM LIKE '__Y%' OR KCMC LIKE '%毕业%' OR KCMC LIKE '%劳动教育%'")
                df = pd.read_sql_query(query, engine)
            else:
                # 对于其他表，直接下载整张表
                df = pd.read_sql_table(table_name, engine)

            # 保存DataFrame到CSV文件
            df.to_csv(f"./数据源文件/0-database/{table_name}.csv", index=False, encoding='utf-8-sig')
            print(f"表 {table_name} 已保存至本地。")

        # 创建或更新EDITTIME.txt文件
        with open(os.path.join(data_folder, "EDITTIME.txt"), "w") as edit_time_file:
            edit_time_file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print("EDITTIME.txt已更新。")

    except Exception as e:
        print("操作失败：", e)


def check_csv_files_exist(files, directory):
    """检查指定目录下是否存在所有给定的文件"""
    return all(os.path.exists(os.path.join(directory, f"{file}.csv")) for file in files)


def get_last_update_time(directory):
    """获取最后一次更新的时间"""
    try:
        with open(os.path.join(directory, "EDITTIME.txt"), "r") as file:
            return file.read().strip()
    except FileNotFoundError:
        return "未知"


def check_connection_and_proceed():
    required_tables = ['T_DZZ_STU_STAFF', 'T_JOB_STU_BYQX', 'T_JW_ACHIEVEMENT', 'T_JW_GRADE_SCORE', 'T_JW_SCORE',
                       'T_JW_TOTAL_SCORE', 'T_SDXG_DEKT', 'T_SDXG_XKJS', 'T_JW_ZB_ENROL_STU']
    data_directory = "数据源文件\\0-database"

    # 尝试连接数据库
    try:
        engine = create_engine('mysql+pymysql://root:Sues%40123@202.121.127.225:3306/student_growth')
        with engine.connect() as connection:
            print("数据库已连接。")
            download()  # 调用下载函数
    except OperationalError:
        # 数据库连接失败，检查CSV文件
        if check_csv_files_exist(required_tables, data_directory):
            last_update_time = get_last_update_time(data_directory)
            print(f"数据库无法连接，将使用本地现有的数据进行后续计算，最近一次更新时间为 {last_update_time}")
        else:
            print("服务器连接失败，请确认已连接学校内网或开启VPN。")
            exit()  # 退出程序


def builder():
    folders = [
        (r'0-database', r'数据源文件\\0-database'),
        (r'3-评奖评优、入党', r'数据源文件\\3-评奖评优、入党'),
        (r'2-评优', r'数据源文件\\3-评奖评优、入党\\2-评优'),
        (r'3-奖学金', r'数据源文件\\3-评奖评优、入党\\3-奖学金'),
        (r'国、省市级奖学金（4分）', r'数据源文件\\3-评奖评优、入党\\3-奖学金\\国、省市级奖学金（4分）'),
        (r'其他奖学金（2分）', r'数据源文件\\3-评奖评优、入党\\3-奖学金\\其他奖学金（2分）'),
        (r'4-奉献精神', r'数据源文件\\4-奉献精神'),
        (r'1-献血', r'数据源文件\\4-奉献精神\\1-献血'),
        (r'2-入伍', r'数据源文件\\4-奉献精神\\2-入伍'),
        (r'3-援疆、援藏', r'数据源文件\\4-奉献精神\\3-援疆、援藏'),
        (r'5-违法违纪', r'数据源文件\\5-违法违纪'),
        (r'8-体质测试成绩', r'数据源文件\\8-体质测试成绩'),
        (r'9-体育赛事、运动会', r'数据源文件\\9-体育赛事、运动会'),
        (r'11-参加校艺术团活动', r'数据源文件\\11-参加校艺术团活动'),
        (r'13-工程实践操作能力', r'数据源文件\\13-工程实践操作能力'),
        (r'17-管理协作能力', r'数据源文件\\17-管理协作能力'),
        (r'2-校级学生会', r'数据源文件\\17-管理协作能力\\2-校级学生会'),
        (r'3-院级学生会', r'数据源文件\\17-管理协作能力\\3-院级学生会'),
        (r'5-社团团长', r'数据源文件\\17-管理协作能力\\5-社团团长'),
        (r'18-国际交流', r'数据源文件\\18-国际交流'),
        (r'19-可持续发展能力', r'数据源文件\\19-可持续发展能力'),
    ]
    while True:
        create_flag = input(r'是否创建空的“数据源文件”目录？（Y/N)：')
        if create_flag.upper() == 'Y':
            create_folders(folders)
            print("完整的“数据源文件”目录已在当前文件夹创建。")
            check_connection_and_proceed()
            break
        elif create_flag.upper() == 'N':
            print('正在验证当前目录下“数据源文件”完整性...')
            try:
                if verify_folders(folders):
                    print("“数据源文件”完整性验证通过。")
                    check_connection_and_proceed()
                    break
            except FileNotFoundError as e:
                print(e)
                complete_flag = input("是否补全缺失的文件夹？（Y/N）: ")
                if complete_flag.upper() == 'Y':
                    create_folders(folders)
                    print("完整的“数据源文件”目录已在当前文件夹创建，请补全文件夹中的内容后重新运行程序。")
                    sys.exit(0)
                else:
                    raise Exception("文件夹结构不完整，程序已退出。")
        else:
            print("请输入 'Y' 或 'N'")
