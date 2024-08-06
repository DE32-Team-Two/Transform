import pandas as pd

def save2parqeut(ds_nodash, df):
    df.to_parquet(f'~/t2/test_parquet/ordered_parquet/', partition_cols=['year','month'])

def merge(load_dt):
    m = str(int(load_dt[4:6]) - 1)

    if len(m) == 1:
        m = '0' + str(m)

    read_df = pd.read_parquet(f'~/t2/test_parquet/year={load_dt[0:4]}/month={m}')
    print(read_df.dtypes())
    cols = ['movieCd', 'movieNm', 'audiCnt', 'year', 'month']
    df = read_df[cols].copy()
    
    # Convert audiCnt to integer
    df['audiCnt'] = df['audiCnt'].astype(int)
    
    # Group by movieCd and sum the audiCnt, keeping the first occurrence of movieNm
    df_grouped = df.groupby('movieCd').agg({
        'movieNm': 'first',
        'audiCnt': 'sum',
        'year': 'first',
        'month': 'first'
    }).reset_index()
    
    save2parqeut(load_dt,df_grouped)

