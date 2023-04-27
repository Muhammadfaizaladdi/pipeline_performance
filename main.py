import os
from get_l1_perf import run_branch_perf
from src.func import get_list_files, get_list_of_env, delete_file


BASE_DIR = os.getcwd()
RAW_FOLDER_PATH = BASE_DIR + "/data/raw"
env_file_name = "/config/main.yaml"

# Get list files that will be read
list_read_files = get_list_files(RAW_FOLDER_PATH)

# Get list keys env
keys = get_list_of_env(file=f"{BASE_DIR}{env_file_name}",
                       ret="keys")

if len(list_read_files) > 0:
    for file in list_read_files:
        for key in keys:
            print(f"Now sheets: {key}")
            env_vars_ = get_list_of_env(env_cat=key, 
                                            file=f"{BASE_DIR}{env_file_name}")        
            run_branch_perf(read_file_name=file, 
                            env_vars=env_vars_)
            
            print(f"file {file}, sheet {key} sudah selesai")


for file in list_read_files: 
    # delete raw file 
    try:
        delete_file(file=file, 
                    path=RAW_FOLDER_PATH)
    except:
        print("can't delete the files")