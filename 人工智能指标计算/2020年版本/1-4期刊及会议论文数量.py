import sqlalchemy
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
import warnings
warnings.filterwarnings('ignore')

engine = sqlalchemy.create_engine('mysql+pymysql://root:123456@172.16.16.152:3306/knowledge_graph')
sql_cmd = '''select province from ai_journal_article_managed where country like "%中国%" and pub_year<='2020' and field like '%人工智能%' '''
df = pd.read_sql(sql=sql_cmd, con=engine)
df.head()

