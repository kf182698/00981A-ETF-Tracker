import pandas as pd


def standardize_columns(df, columns_types):
    """
    批量標準化 DataFrame 欄位型別
    
    Args:
        df: DataFrame
        columns_types: dict，例如 {
            'int': ['欄位1', '欄位2'],
            'float': ['欄位3', '欄位4'],
            'numeric': ['欄位5']  # 使用 pd.to_numeric，不轉型
        }
    
    Returns:
        修改後的 DataFrame
    """
    # 處理 int 型別欄位
    if 'int' in columns_types:
        for col in columns_types['int']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            else:
                df[col] = 0
    
    # 處理 float 型別欄位
    if 'float' in columns_types:
        for col in columns_types['float']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            else:
                df[col] = 0.0
    
    # 處理 numeric 型別欄位（只轉換，不填充）
    if 'numeric' in columns_types:
        for col in columns_types['numeric']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df
