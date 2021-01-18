import time
import excel_parser as xp
import multiprocessing as mp
import pandas as pd

#get list of all excel files
list_data = xp.load_excel("F:\skripsi\data_folder\data_folder").list_all_files()
"""
this function is creating dataframe using
excel_parser lib. this function return none
but creating new csv files as output.
this function use limit to bound retrieval
of excel files.
"""
def task(lower,upper):
    df = xp.create_df("F:\skripsi\data_folder\data_folder",list_data).run(lower,upper)
    df.to_csv("result/" + str(lower)+"-"+str(upper)+".csv")
offset = 2500
batch = 250
if __name__ == '__main__':
    start = time.perf_counter()
    print("multiprocessing 3 task")
    process_pool = []
    for each_process in range(5):
        #preparing function process
        p = mp.Process(target=task,args=[each_process*batch + offset, each_process*batch + batch + offset])
        #go !
        p.start()
        #appending process to be executed together
        process_pool.append(p)
        pass
    #execute together all process inside process_pool list
    for process in process_pool:
        process.join()
        pass
    finish = time.perf_counter()
    print(finish-start)
