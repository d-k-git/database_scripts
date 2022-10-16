import pandas as pd
import cx_Oracle
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

import warnings
warnings.filterwarnings('ignore')

from sqlalchemy import create_engine


I. GET DATA 

try:
    engine = sqlalchemy.create_engine("oracle+cx_oracle://login:pass@host/?service_name=DB") #, arraysize=1000)
    orders_sql = """SELECT 
    MULTICHANNEL_NUMBER, DURATION/1000 AS DURATION_SEC,
    to_char(date '1970-01-01' + (START_TIME/86400000), 'YYYY-MM-DD HH24:MI:SS') as new_START_TIME,
    to_char(date '1970-01-01' + (RELEASE_TIME/86400000), 'YYYY-MM-DD HH24:MI:SS') as new_RELEASE_TIME
    FROM
        DB.RECORDS
    WHERE
        START_TIME >= '1651363200000' AND START_TIME < '1652313600000'
        AND MULTICHANNEL_NUMBER LIKE '+%'
        AND DURATION > 0
    ORDER BY
        MULTICHANNEL_NUMBER,NEW_START_TIME""";

    df_RECORDS_0 = pd.read_sql(orders_sql, engine)

except SQLAlchemyError as e:
    print(e)
df_RECORDS_0.head()


###  II. PROCESSING DATA 


# create a copy to keep the original df
df100 = df_RECORDS_0

#remove +7 and create a column with a short name
df100['mnс'] = df100['multichannel_number'].str[2:]

#leave three cols
df00 = df100[['mnс','new_start_time','new_release_time']]

 
df = df00.drop_duplicates()

# create duplicate string columns in datetime format
df['open'] = pd.to_datetime(df['new_start_time'])
df['close'] = pd.to_datetime(df['new_release_time'])

# filter rare numbers ( > 3, 5 etc):
df1 = df[df.groupby('mcn')['mcn'].transform('size') > 0]

# to iterate over  numbers split the dataframe into dictionaries - number: dates
dfs = dict(tuple(df1.groupby('mcn')))


#iterate over numbers

from itertools import chain
data_list = []
for number in dfs:
    print(number)
    session_starts = (x - pd.Timedelta(seconds=x.second) for x in dfs[number]['open'])
    session_ends = (x - pd.Timedelta(seconds=x.second) for x in dfs[number]['close'])
    ranges = (pd.date_range(x,y,freq='1T') for x,y in zip(session_starts,session_ends)) # 1T - minutes split  https://pandas.pydata.org/pandas-docs/version/0.9.1/timeseries.html
    ranges = pd.Series(chain.from_iterable(ranges))
    output = ranges.value_counts(sort=False).sort_index()

    df10 = output.to_frame()
    df10.rename(columns={0 :'calls'}, inplace=True)

    u = df10['calls'].astype(str)
    df10['calls'] = u.str[-4:]
    df15 = df10.sort_values(['calls'], ascending=False)
    df15['number'] = number
    max_momemt_calls = df15['calls']
    print(df15)
    #data.append(df15)
    data_list.append((number,
                      max_momemt_calls))


# build df
result = pd.DataFrame(data_list)
result = result.rename(columns={0: 'mcn', 1:'datas'})

# transform for splitting
result['mystring'] = pd.Series(result['datas'], dtype="string")
result = result.replace('\n',';', regex=True)

# select a column  select the maximum number of calls to each number
result['max_col'] = result['mystring'].str[:30]

# splitting by the max_col column separator to create auxiliary columns
result[['first_col','second_col']] = result.max_col.str.split(';',expand=True)

# one more split  - col 'first' by space
result[['date','time','max_momemt_calls']] = result.first_col.str.split(expand=True)

# leave the desired columns
df_fin = result[['mnс','date','time','max_momemt_calls']]

# convert calls to a number for sorting
df_fin['calls'] = pd.to_numeric(df_fin['max_momemt_calls'])
df_RECORDS_fin = df_fin.sort_values(['max_momemt_calls'], ascending=False)

# final df
df_RECORDS_fin.head()



[...]

df_pre_end_merge_ABONENT = pd.merge(df_pre_end_merge, df_ABONENT, how='left', left_on=['mcn'], right_on=['last_name'])

## Processing the final df

#  select and rename the desired columns in the final df
df_final = df_pre_end_merge_ABONENT[['client_id_x','package_id','name','mnc','price_plan','date','time','max_momemt_calls','used_abonent_number','call_capacity']]
df_final.rename(columns = {'client_id_x':'client_id','name':'package_name','mnc':'multichannel_number'}, inplace = True)


#df_final.notnull().sum()
#df_final.head()



# III. Load result into excel


writer = pd.ExcelWriter("df_final.xlsx", engine='xlsxwriter')
df_final.to_excel(writer, index=False)
writer.save()
print('Ready!')
