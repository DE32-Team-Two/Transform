import pandas as pd

def merge(load_dt):
    read_df = pd.read_parquet('~/t2/test_parquet/year=load_dt[0:4]/month=load_dt[4:6]')
    cols = ['movieCd', 'movieNm', 'audiCnt']
    df = read_df[cols].copy()
    
    # Convert audiCnt to integer
    df['audiCnt'] = df['audiCnt'].astype(int)
    
    # Group by movieCd and sum the audiCnt, keeping the first occurrence of movieNm
    df_grouped = df.groupby('movieCd').agg({
        'movieNm': 'first',
        'audiCnt': 'sum'
    }).reset_index()
    
    return df_grouped
