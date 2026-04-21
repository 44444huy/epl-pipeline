import pandas as pd

# Đọc file parquet
df = pd.read_parquet(r'D:\Downloads\part-00000-83ea06b2-a578-4d00-9a08-11dd4d6f325c.c000.snappy.parquet')

# Xem 5 dòng đầu tiên
print(df.shape)


