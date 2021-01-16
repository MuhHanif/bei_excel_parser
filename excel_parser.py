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
            #print(file_sheet_dict[filename +" "+ sheet].keys)
        #print(file_sheet_dict.keys())
        return(file_sheet_dict)

    def list_all_files(self):
        #get all excel file names
        return(os.listdir(self.bulk_excel_dir))
#there's should be another class here to combine and flatten data from previous dict

class clean_sheet(object):
    """
    this class is the (2nd) second pipelines.
    this class spesified for rearanging the sheet into one
    chunk of tabular (dataframe) format. This class should
    return single dataframe containing all data necessary
    for further analysis.
    """
    def __init__(self, dir):
        self.dir = dir

    def flatten(self, excel_file):
        """
        get exctracted sheet from previous steps.
        this code require both load excel class function
        to run properly. highlighting this to track when
        the code start turn into spaghetti.
        """
        #read dict
        sheet_dict = load_excel(self.dir).open_load(excel_file)
        #get keyname for addressing specific sheet
        list_key = list(sheet_dict.keys())
        """
        general information sheet
        drop the unnececary data and set some of the rows
        into column names.
        """
        general_info = sheet_dict.get(list_key[1])
        general_info.columns = general_info.iloc[2]
        general_info = general_info.iloc[1].to_frame().T
        sheet_dict[list_key[1]] = general_info

        """
        Same process with general information sheet
        """
        sheet_list = [2,3,6]
        for sheet in sheet_list:
            df = sheet_dict.get(list_key[sheet])
            df.columns = df.iloc[3]
            df = df.iloc[1].to_frame().T
            sheet_dict[list_key[sheet]] = df
            pass

        """
        create flattening method for equity statement.
        equity statement consist of 2d array and need
        to be flattenend into 1d array to create final
        dataframe for analysis.
        """
        sheet_list_stack = [4]
        for sheet_stack in sheet_list_stack:
            #first transpose to original table
            df_stack = sheet_dict.get(list_key[sheet_stack]).T
            #reseting index ? for unknown reason
            df_stack.reset_index(inplace=True,drop=True)
            #crop out uneccesary header
            df_stack.columns = df_stack.iloc[5]
            #set index with the rightmost data
            df_stack.index = df_stack.iloc[:,25]
            #flattening df into single column
            df_stack = df_stack.iloc[6:,1:25]
            df_stack = df_stack.stack().to_frame()
            #removing multiindex and turn it into ordinary values
            df_stack.reset_index(inplace=True)
            #merging previously multiindex into single value
            df_stack[2] = df_stack.iloc[:,0] +" "+ df_stack.iloc[:,1]
            #get numeric values only then transpose
            df_stack.index = df_stack[2]
            df_stack = df_stack.iloc[:,2].to_frame().T
            sheet_dict[list_key[sheet_stack]] = df_stack
            pass

        """
        combining all of the dataframe inside the dict.
        turn into single monolithic dataframe.
        """
        #emoving leftover index for transforming dataframe
        df_sheet = [1,2,3,4,6]
        for df in df_sheet:
            sheet_df = sheet_dict.get(list_key[df])
            sheet_df.reset_index(inplace=True,drop=True)
            sheet_dict[list_key[df]] = sheet_df
            pass
        #concatenating all of dataframe
        completed_df = pd.DataFrame()
        for data in df_sheet:
            data_concat = sheet_dict.get(list_key[data])
            completed_df = pd.concat([completed_df,data_concat], axis=1)
            pass
        #drop uneccesary columns fill with N/A
        completed_df = completed_df[completed_df.columns.dropna()]
        """
        https://stackoverflow.com/questions/30650474/
        python-rename-duplicates-in-list-with-progressive-numbers-without-sorting-list
        this below code copied from stackoverflow (link above).
        i simply to lazy to create column name duplicate check.
        """
        list_col = list(completed_df.columns)
        mylist = list_col[:]
        dups = {}
        for i, val in enumerate(mylist):
            if val not in dups:
                # Store index of first occurrence and occurrence value
                dups[val] = [i, 1]
            else:
                # Special case for first occurrence
                if dups[val][1] == 1:
                    mylist[dups[val][0]] += str(dups[val][1])
                # Increment occurrence value, index value doesn't matter anymore
                dups[val][1] += 1
                # Use stored occurrence value
                mylist[i] += str(dups[val][1])
        completed_df.columns = mylist
        #removing dictionary to free the memory
        del sheet_dict
        return(completed_df)


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

#x = create_dict("excel_data").dict_pool()
x = clean_sheet("excel_data").flatten("AALI_2018_II.xlsx")

finish = time.perf_counter()

print(finish-start)
