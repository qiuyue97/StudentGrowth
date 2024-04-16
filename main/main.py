import pandas as pd
from builder import builder
from file_processer import process_main
import warnings
import os
warnings.filterwarnings('ignore')


def main():
    builder()
    main_table_path = r'学籍信息v2.xlsx'
    print(f"已成功读取“{main_table_path}”。")
    main_table = pd.read_excel(main_table_path)
    add_table = process_main(main_table[['学号', '姓名', '专业']].copy())
    result_table = pd.merge(main_table, add_table, on=['学号', '姓名'], how='left')
    output_path = r'学生成长增值评价研究得分结果.xlsx'
    with pd.ExcelWriter(os.path.join(os.getcwd(), output_path)) as writer:
        result_table.to_excel(writer, index=False)
    print('学生成长增值评价研究得分结果表格已储存至当前文件夹。')

if __name__ == '__main__':
    main()
