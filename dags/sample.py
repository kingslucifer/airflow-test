# -*- coding: utf-8 -*-
"""
Created on Sat Jun 23 16:20:45 2018

@author: zy259
"""

from datetime import datetime
from time import time
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from sqlachemy import create_engine, MetaData
from sqlachemy.orm import sessionmaker
from coinmarketcap import Market
import pandas as pd

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Date, String, Float

Base = declarative_base()

class Top10WeeklyRecord(Base):
    __tablename__ = 'Top10WeeklyRecord'
    DATE = Column(Date, primary_key=True)
    NAME =  Column(String, primary_key=True)
    SYMBOL = Column(String)
    MARKET_CAP_USD = Column(Float)
    PERCENT_CHANGE_1H = Column(Float)
    PERCENT_CHANGE_24H = Column(Float)
    PERCENT_CHANGE_7D = Column(Float)
    PRICE_USD = Column(Float)
    INDEX_PORTION = Column(Float)
    
engine = create_engine('postgresql://ccm:Password123@devdbinstance.cueh4niuygr6.us-west-2.rds.amazonaws.com:5432/thanos')
metadata = MetaData()
Session = sessionmaker(bind=engine)

conversion_list = ['market_cap_usd', 'price_usd']

coinmarketcap = Market()
#top_10
top_10 = coinmarketcap.ticker(start=0, limit=10, sort='Rank')
df_top10 = pd.DataFrame(top_10)
df_top10[conversion_list] = df_top10[conversion_list].apply(pd.to_numeric)
total_top10_marketcap = df_top10['market_cap_usd'].sum()
df_top10 = df_top10[['name', 'symbol', 'market_cap_usd', 'percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'price_usd']]

#get the index portion for each underlying security
df_top10['index_portion'] = df_top10['market_cap_usd'] / total_top10_marketcap

def sum_product(df):
    sum_product = 0
    for index, row in df.iterrows():
        sum_product = sum_product + row['index_portion'] * row['price_usd']
    return sum_product

initial_price_top10 = sum_product(df_top10)

def write_df_record_to_db(df_record, model, session):
    try:
        for index, row in df_record.iterrows():
            record = model(**{
                'DATE': datetime.now(),
                'NAME': row['name'],
                'SYMBOL': row['symbol'],
                'MARKET_CAP_USD': row['market_cap_usd'],
                'PERCENT_CHANGE_1H': row['percent_change_1h'],
                'PERCENT_CHANGE_24H': row['percent_change_24h'],
                'PERCENT_CHANGE_7D': row['percent_change_7d'],
                'PRICE_USD': row['price_usd'],
                'INDEX_PORTION': row['index_portion'],
        })
            print(record)
            session.add(record) #add all records
        session.commit() #attempt to commit all changes
    except:
        session.rollback() # rollback the changes on error
    finally:
        session.close() #close the connection  
        
def ingest_top10_daily_data():
    t = time()
    session = Session()
    
    write_df_record_to_db(df_top10, Top10WeeklyRecord, session)
    return "Done recording top10 daily record at %s" % (t)


start = datetime(day=23, month=6, year=2018)

args = {'owner': 'airflow',
        'start_date': start
        }

dag = DAG('ingest_daily_record',
          default_args=args,
          schedule_interval='0 5 * * 1-5')

t1 = PythonOperator(
        task_id='ingest_top10_daily_data',
        python_callable=ingest_top10_daily_data,
        provide_context=True,
        dag=dag)
t1 