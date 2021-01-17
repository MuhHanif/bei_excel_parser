import time
import excel_parser as xp
import multiprocessing as mp
import pandas as pd

list_data = xp.load_excel("excel_data").list_all_files()

def task(lower,upper):
    df = xp.create_df("excel_data",list_data).run(lower,upper)
    df.to_csv(str(lower)+"-"+str(upper)+".csv")

if __name__ == '__main__':
    start = time.perf_counter()
    print("multiprocessing 3 task")
    p1 = mp.Process(target=task,args=[0,5])
    p2 = mp.Process(target=task,args=[5,10])
    p3 = mp.Process(target=task,args=[10,15])

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()
    finish = time.perf_counter()

    print(finish-start)
    start = time.perf_counter()
    print("solo task")
    task(0,15)
    finish = time.perf_counter()
    print(finish-start)
