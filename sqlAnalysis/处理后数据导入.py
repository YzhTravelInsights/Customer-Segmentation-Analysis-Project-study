#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025/8/23 09:21
# @Author  : yzh
# @Site    : 
# @File    : 处理后数据导入.py
# @Version：V 0.1
# @desc :
import pandas as pd
from sqlalchemy import create_engine

# 创建数据库连接
engine = create_engine('mysql+mysqlconnector://root:040706@localhost/cohort_rem')

# 读取清理后的parquet文件
df = pd.read_parquet(r'F:\Customer-Segmentation-Analysis-Project-study\retail_cleaned.parquet')

# 导入到MySQL
df.to_sql('OnlineRetail', con=engine, if_exists='append', index=False)