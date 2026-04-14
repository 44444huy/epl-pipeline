import pandas as pd

# Đọc file parquet
df = pd.read_parquet(r'D:\Downloads\part-00000-9e25d743-6fa9-4075-8fcd-988f949a53cb.c000.snappy.parquet')

# Xem 5 dòng đầu tiên
print(df.head(20))

# Xem cấu trúc các cột (Schema)
print(df.info())