import pandas as pd

def save2parqeut(ds_nodash, df):
    df.to_parquet(f'~/t2/test_parquet/ordered_parquet/{ds_nodash[4:6]}')

def merge(load_dt):
    m = str(int(load_dt[4:6]) - 1)

    if len(m) == 1:
        m = '0' + str(m)

    read_df = pd.read_parquet(f'~/t2/test_parquet/year={load_dt[0:4]}/month={m}')
    cols = ['movieCd', 'movieNm', 'audiCnt']
    df = read_df[cols].copy()
    
    # Convert audiCnt to integer
    df['audiCnt'] = df['audiCnt'].astype(int)
    
    # Group by movieCd and sum the audiCnt, keeping the first occurrence of movieNm
    df_grouped = df.groupby('movieCd').agg({
        'movieNm': 'first',
        'audiCnt': 'sum'
    }).reset_index()
    
    save2parqeut(load_dt,df_grouped)

