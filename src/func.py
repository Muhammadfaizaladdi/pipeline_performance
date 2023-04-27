import pandas as pd
import numpy as np
from datetime import datetime
import os
import yaml
import re

# Get env vars
def get_list_of_env(file, ret="values", env_cat=None):
    """
    Function to get specific value of environment variable

    Args:
        env_cat (str): environment variable categories : "branch" / "cluster" 
        file (str): file name or path+filename
        ret (str): type of returns, keys: keys of env vars. values: values of env vars
        
    Return
        vars (dict): key:value of env vars
    """

    with open(f'{file}',mode="r") as file:
        all = yaml.load(file, Loader=yaml.FullLoader)
        if ret == "values":
            result = all[env_cat]
        elif ret == "keys":
            result = all.keys()
        file.close()
    return result
        
# Get list files
def get_list_files(path):
    list_files = os.listdir(path)
    return list_files

# Delete File 
def delete_file(file,
                path=None):
    if os.path.exists(f"{path}/{file}"):
        os.remove(f"{path}/{file}")
        print("file has been removed")
    else:
        print("The file does not exist")

# get file's date
def get_file_date(filename, 
                  start=3, 
                  end=6):
    """
    File to get date and month of the files

    Args:
        filename (str): name of the file
        start (int): start index of the date in filename after splitting
        end (int): start index of the date in filename after splitting

    Returns:
        date_of_file (date) = date of the file
        month (int) : month of the file

    """

    # Get the date component from file's name
    date_of_file = " ".join(filename.split()[3:6])

    # Convert date in string type to date type
    date_of_file = datetime.strptime(date_of_file, '%d %b %Y').date()

    # Get the month of from date
    month = date_of_file.strftime("%B")
 
    # Get year-month of file 
    year_month = date_of_file.strftime("%Y-%m")

    # Convert proper date into string
    date_of_file = date_of_file.strftime("%Y-%m-%d")
   
    # Return date and month of the file
    return date_of_file, month, year_month

def read_data(file_name,
              raw_path,
              sheet_name, 
              save_file=False):
    """
    Function to read data from excel file

    Args:
        file (string): file name or path+file name in string
        path (string): path of raw data file
        sheet_name (string): name of the sheet in excel file
        save_file (string): "Y" if the file will be saved "N" if will not be saved 

    Return:
        data_ori (dataframe): the original data
        copy_data (dataframe): copied data
    """

    # Read data from excel and store it to variable
    data_ori = pd.read_excel(f"{raw_path}/{file_name}", sheet_name)
    
    # create the copy of the data
    copy_data = data_ori.copy()

    # return the original and the copy data
    return data_ori, copy_data


def slice_data(data,
               start_row=0,
               end_row=0,
               start_col=0,
               end_col=0):
    """
    Function to slice the data into specific row and columns
    
    Args:
        data (dataframe): Data that will be saved
        start_row (int): first row index that will be sliced
        end_row (int): last row index that will be sliced
        start_col (int): first column index that will be sliced
        end_col (int): last column index that will be sliced

    Return:
        sliced_data (dataframe): Data which have been sliced
    """
    sliced_data = data.iloc[start_row:end_row, start_col:end_col]
    return sliced_data


def flattening_col(data,
                   year_month_file,
                   start_row=0,
                   end_row=2,
                   is_transposed=True,
                   first_idx_col=0,
                   scnd_idx_col=1,
                   return_raw_col=False):
    
    """Function to merge multi columns into single columns
    
    Args:
        data (dataframe): data with multi columns
        start_row (int): index of first columns
        end_row (int): index of second columns
        transposed (bool): True if the columns need to be transposed
        idx_first_head (int): index of first columns
        idx_scnd_head (int): index of second columns
        return_raw_col (bool): True if the array want to be returned from function

    Return:
        data (dataframe): data which columns has been merged
        columns_ (array): array of columns name
    """

    

    if is_transposed:
        columns_ = data.iloc[start_row:end_row].T
        # print(columns_)
        columns_[first_idx_col] = columns_[first_idx_col].apply(lambda x: x.strip() if type(x) == str else x)
        columns_[first_idx_col] = columns_[first_idx_col].apply(lambda x: re.split("[0-9]{2}", x)[0] if len(re.findall("[0-9]{2}\s[\w]{2,5}", str(x))) > 0 else x)
        columns_[first_idx_col] = columns_[first_idx_col].ffill()
        columns_[scnd_idx_col] = columns_[scnd_idx_col].apply(lambda x: "current" if len(re.findall("[0-9]{2}\s[\w]{2,5}", str(x))) >0  else x)
        columns_["new_columns"] = columns_[first_idx_col] + "_" + columns_[scnd_idx_col].astype(str)
        columns_ = columns_["new_columns"].T.values
        data.columns = columns_
    
    else:
        columns_[first_idx_col] = columns_[first_idx_col].ffill()
        columns_[scnd_idx_col] = columns_[scnd_idx_col].apply(lambda x: x if year_month_file not in str(x) else "current")
        columns_["new_columns"] = columns_[first_idx_col] + "_" + columns_[scnd_idx_col].astype(str)
        columns_ = columns_["new_columns"].T.values
        data.columns = columns_

    if return_raw_col:
        return data, columns_
    else:
        return data
    

def filter_data(data,
                list_cat,
                columns_name,
                method="or",
                ):
    """
    Filtering data with one or two category in one columns

    Args:
        data (dataframe): data that will be filtered
        list_cat (list): list of category that will be used as filetering parameter
        columns_name (str): name of the columns as reference of filtering
        method (str): if more than one category, "and" / "or" will be used to filter data

    Return:
        filtered_data (dataframe): data that has been filtered

    """
    
    if len(list_cat)==1:
        filtered_data = data[data[columns_name] == list_cat[0]]
    else:
        if method == "or":
            filtered_data = data[(data[columns_name] == list_cat[0]) | (data[columns_name] == list_cat[1])]
        elif method == "and":
            filtered_data = data[(data[columns_name] == list_cat[0]) & (data[columns_name] == list_cat[1])]
        else:
            print('Need to specify method using "or" / "and"')
    
    return filtered_data


def save_excel_file(data,
                    saved_file_name,
                    path_saved,
                    list_clean_files,
                    sheet_name="Sheet 1",
                    saved_index= False):
    
    if len(list_clean_files)>0:
        list_sheet_name = pd.ExcelFile(f"{path_saved}{saved_file_name}").sheet_names

    if saved_file_name not in list_clean_files:
        data.to_excel(f"{path_saved}{saved_file_name}", 
                             index=saved_index,
                             sheet_name=sheet_name)
    
    elif sheet_name not in list_sheet_name:
        with pd.ExcelWriter(f"{path_saved}{saved_file_name}", 
                            engine="openpyxl", 
                            mode="a", 
                            if_sheet_exists="replace") as writer:

            data.to_excel(writer, sheet_name=sheet_name, index=False)


    else:
        previous_perf = pd.read_excel(f"{path_saved}{saved_file_name}", sheet_name=sheet_name)
        new_perf = pd.concat([previous_perf, data], ignore_index=True)

        with pd.ExcelWriter(f"{path_saved}{saved_file_name}", 
                            engine="openpyxl", 
                            mode="a", 
                            if_sheet_exists="replace") as writer:
            
            new_perf.to_excel(writer, sheet_name=sheet_name, index=False) 

