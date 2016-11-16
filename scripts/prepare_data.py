import pandas as pd
import numpy as np

technical = pd.read_csv('/Users/jiaqizhang/Documents/innovation_day/stock/data/Technical.csv')
technical['transaction_date'] = pd.to_datetime(technical['Tradedate'])
technical['ipo_date'] = pd.to_datetime(technical['ipodate'], format="%Y/%m/%d")
technical = technical.drop(['Tradedate','ipodate','Unnamed: 13'],1)
technical['weekday'] = technical.transaction_date.dt.dayofweek
technical['out_date'] = technical.transaction_date + pd.Timedelta('7 days')
technical_r = technical.drop(['turn', 'mkt_cap', 'vol', 'adjfactor', 'amount',
                                  'swing','high', 'low','out_date','ipo_date','weekday'], 1)

technical_r = technical_r.rename(columns = {'transaction_date':'out_date'})

technical_tag = technical.join(technical_r.set_index(['code', 'out_date']),
                                       on=['code', 'out_date'], rsuffix='_7d')
technical_tag['closed_diff'] = (technical_tag['close'] - technical_tag['close_7d']) / technical_tag['close']

tech_key = technical_tag[['code','transaction_date']]

fund = pd.read_csv('/Users/jiaqizhang/Documents/innovation_day/stock/data/Fundamental.csv')
fund['issue_date'] = pd.to_datetime(fund['issuedate'])
fund = fund.drop(['reportdate', 'issuedate', 'ipodate', 'Unnamed: 61'], 1)
fund_key = fund[['code','issue_date']]


base_key = tech_key.join(fund_key.set_index(['code']), on = 'code')
base_key_f = base_key[ base_key.transaction_date > base_key.issue_date  ]
base_key_m = base_key_f.groupby(['code', 'transaction_date'], as_index=False)['issue_date'].max()

base_0 = pd.merge(technical_tag, base_key_m, on=['code', 'transaction_date'], how='left')
base = base_0.merge(fund, how='left', on=['code', 'issue_date'], suffixes=('','_f') )

base.to_csv('/Users/jiaqizhang/Documents/innovation_day/stock/data/base.csv')




