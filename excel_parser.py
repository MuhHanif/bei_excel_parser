import pandas as pd
from collections import OrderedDict
import os
import time

"""
this code follow waterflow steps, every class is for
specific steps.
"""
class load_excel(object):
    """
    this class is the (1st) first pipelines.
    this class spesified for all the task from load excel files
    into dictionary. This class should return dictionary filled
    with dataframe to be processed later.
    """
    def __init__(self, bulk_excel_dir):
        self.bulk_excel_dir = bulk_excel_dir

    def open_load(self, filename):
        #read all excel data from directory by concatenanting it
        path = self.bulk_excel_dir + "/" + filename
        #read all excel files
        xlsx = pd.ExcelFile(path)
        #create empty oordered dict for keeping loaded data
        file_sheet_dict = OrderedDict()
        #load all sheet into odered dictionary
        for sheet in xlsx.sheet_names:
            file_sheet_dict[filename +" "+ sheet] = pd.read_excel(xlsx, sheet_name=sheet).T
            print(file_sheet_dict[filename +" "+ sheet].shape)
        return(file_sheet_dict)

    def list_all_files(self):
        #get all excel file names
        return(os.listdir(self.bulk_excel_dir))
#there's should be another class here to combine and flatten data from previous dict

#this class might be 3rd pr second to last pipelines
class create_dict(object):
    """
    this class is the (2nd) second pipelines.
    this class sepecified for creating dictionary of multiple sheet
    extracted from prev class. this class should return at least 2
    dictionary from excel sheet and transposed for further pipelines.
    """
    def __init__(self, dir):
        self.dir = dir

    def dict_pool(self):
        """
        pooling all sheet into 1 monolithic dictionary
        """
        #use previous function from pipelines to load list of excel files
        excel_file_list = load_excel(self.dir).list_all_files()
        #create ordered dictionary for storing the sheet retreived
        dict = OrderedDict()
        #get all sheet from the list of excel files
        for excel_file in excel_file_list:
            y = load_excel(self.dir).open_load(excel_file)
            print(excel_file)
            z = dict.update(y)
        print(dict.keys())
        return(dict)



start = time.perf_counter()

x = create_dict("excel_data").dict_pool()
finish = time.perf_counter()

print(finish-start)
