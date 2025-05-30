# %%
import pandas as pd
from sqlalchemy import create_engine

# %%
dtype_dict = {'column_name': str, 'another_column': float}
df = pd.read_csv(r"C:\Users\user\Desktop\JKR\Data\03_year_two.csv", dtype=dtype_dict)

# %%
def extract_data(file_path):
    df = pd.read_csv(file_path)
    return df

# %%
def transform_data(df):
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    df.dropna(inplace=True)  
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df

# %%
dbname =" RVT"
user = "user"
password = "password"
host = "host"
port = " port"

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')


df.to_sql(
    'table_name',
    engine,
    schema='bronze/silver/gold',
    if_exists='replace',    
    index=False
)

# %%
print('loading data successful')



