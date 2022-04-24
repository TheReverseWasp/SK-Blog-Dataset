import pandas as pd

df = pd.read_parquet("SK_Blog_Dataset.parquet")
for df_it, row in df.iterrows():
    print(row)
print(len(df))
