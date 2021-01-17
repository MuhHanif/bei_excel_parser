import time
import excel_parser as xp

start = time.perf_counter()

list_data = xp.load_excel("excel_data").list_all_files()
df = xp.create_df("excel_data",list_data).run(20,30)

finish = time.perf_counter()

print(finish-start)
