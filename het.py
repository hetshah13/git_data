import dask.dataframe as dd
from datetime import datetime, timezone
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

file_loc = ['data/uber-raw-data-apr14.csv','data/uber-raw-data-aug14.csv',
            'data/uber-raw-data-jul14.csv','data/uber-raw-data-jun14.csv',
            'data/uber-raw-data-may14.csv','data/uber-raw-data-sep14.csv']

dfs = [dd.read_csv(file) for file in file_loc]
#print(dfs)
# date:string  Lat:float64	Lon:float64	Base_code:string

l15 = dd.read_csv(r'data/uber-raw-data-janjune-15.csv/uber-raw-data-janjune-15.csv')
l1 = dd.read_csv(r"data/taxi-zone-lookup.csv")

#concatinating files
df = dd.concat(dfs)

#renaming columns
l1 = l1.rename(columns={'LocationID': 'locationID'})
result = dd.merge(l15,l1 ,on='locationID')

print(df.tail())
print(result.tail())

df['Date/Time'] = dd.to_datetime(df['Date/Time'])
result['Pickup_date'] = dd.to_datetime(result['Pickup_date'])

# Convert to local time
df['Date/Time'] = df['Date/Time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
result['Pickup_date'] = result['Pickup_date'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')

#tweeking :replacing base code with base name

base_name= {
    'B02512': 'Unter','B02598': 'Hinter','B02617': 'Weiter','B02682': 'Schmecken',
    'B02764': 'Danach-NY','B02765': 'Grun','B02835': 'Dreist','B02836': 'Drinnen'}

df['Base'] = df['Base'].replace(base_name)
result['Dispatching_base_num'] = result['Dispatching_base_num'].replace(base_name)

#connect to database
engine = db.create_engine('mysql+mysqlconnector://root:hetshah13@localhost:3307/mno')

# testing a connection
conn = engine.connect()
metadata = db.MetaData()
print(repr(metadata))

df.to_sql('uber1',uri ='mysql+mysqlconnector://root:hetshah13@localhost:3307/mno',index=False)
result.to_sql('uber2',uri ='mysql+mysqlconnector://root:hetshah13@localhost:3307/mno',index=False)

uber= db.Table('uber1', metadata, autoload=True, 
autoload_with=engine)

print(repr(metadata.tables['uber1']))

'''
query = uber.select()
print(query)
'''
