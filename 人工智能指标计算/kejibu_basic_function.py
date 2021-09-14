import sqlalchemy
import pandas as pd
import pymysql

pymysql.install_as_MySQLdb()
import warnings

warnings.filterwarnings('ignore')


# 创建一个专门读取SQL并且
def SQL_data_processing(input_SQL):
    engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152: 3306/knowledge_graph')
    sql_cmd = input_SQL
    df = pd.read_sql(sql=sql_cmd, con=engine)
    df["最大值"] = df["number"].max()
    df.sort_values(by="number", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["指标值"] = " "
    for i in range(len(df)):
        df["指标值"][i] = (df["number"][i] / df["最大值"][i]) * 100
        df["指标值"][i] = ('%.1f' % df["指标值"][i])
    list = []
    i = 0
    while True:
        if i < len(df):
            list.append("2020年")
            i += 1
        else:
            break
    df.insert(loc=0, column="年份", value=list)
    print(df)
    return df
#------------------------------1-1 核心企业总数---------------------------------------------------------------------------
input_SQL = '''
select province,count(*) as number from ai_company_info where industry_all like '%人工智能%'and found_year<='2020' 
group by province'''
df1_1 = SQL_data_processing(input_SQL)

# ------------------------------1-3 平台、基地数量-------------------------------------------------------------------------
input_SQL = '''SELECT
	province,count(*) as number
FROM
	ai_platform_base_lab 
WHERE
	classify IN ( '国家级', '省市级' ) 
	AND field = '人工智能' 
	AND found_year <= '2020年' 
	AND type IN ( '平台', '基地' )
	group by province
	order by number desc'''
df1_3 = SQL_data_processing(input_SQL)

# ------------------------------1-9 核心企业规模------------------------------------------------------------------------
input_SQL = '''select province, economy_scale as number from ai_province_economy_scale_table where industry='人工智能' and type=0 and year='2020年' '''
df1_9 = SQL_data_processing(input_SQL)

# ------------------------------1-12  投融资数量------------------------------------------------------------------------
input_SQL = '''select a.province, sum(b.financing_amount) as number
from ai_company_info a, ai_financing_amount_dup b 
where a.com_registered_name=b.com_registered_name 
and b.field like '%人工智能%' 
and b.invest_year='2020' 
group by a.province '''
df1_12 = SQL_data_processing(input_SQL)





# ------------------------------1-15  带动传统企业产出的增加值--------------------------------------------------------------
input_SQL = '''select province, economy_scale as number
from ai_province_economy_scale_table 
where industry='人工智能' 
and type=1 
and year='2020年' '''
df1_15 = SQL_data_processing(input_SQL)




# ------------------------------2-1 成熟AI企业数量增长率 -------------------------------------------------------------


