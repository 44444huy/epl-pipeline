import pandas as pd

# Đọc file parquet
df = pd.read_parquet(r'D:\Downloads\part-00000-08e3ff40-32a8-4ad6-94e6-04ee1493c490.c000.snappy.parquet')

# Xem 5 dòng đầu tiên
print(df.head(20))

# Xem cấu trúc các cột (Schema)
print(df.info())