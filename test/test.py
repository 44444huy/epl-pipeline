import pandas as pd

# Đọc file parquet
df = pd.read_parquet(r'D:\Downloads\part-00000-a895e3bc-b167-447a-af81-b8947a600fd5.c000.snappy.parquet')

# Xem 5 dòng đầu tiên
print(df.head(20))

# Xem cấu trúc các cột (Schema)
print(df.info())