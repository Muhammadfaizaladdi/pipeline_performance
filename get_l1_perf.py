import os
from src.func import get_file_date, \
                     read_data, \
                     slice_data, \
                     flattening_col, \
                     filter_data, \
                     save_excel_file, \
                     delete_file


def run_branch_perf(read_file_name, 
                    env_vars):

    # Get necessary value and save it to env variable
    BASE_DIR = os.getcwd()
    RAW_DATA_PATH = BASE_DIR + "/data/raw/"
    LIST_RAW_FILES = os.listdir(RAW_DATA_PATH)
    CLEAN_DATA_PATH = BASE_DIR + "/data/clean/"
    LIST_CLEAN_FILES = os.listdir(CLEAN_DATA_PATH)
    date_of_file, month, year_month = get_file_date(read_file_name)
    CLEAN_FILE_NAME = f"performance_{month}.xlsx"

    # Read Data
    data_ori, all_data = read_data(file_name=read_file_name,
                                   raw_path=RAW_DATA_PATH, 
                                   sheet_name=env_vars["READ_SHEET_NAME"])

    # Get branch data
    data_branch = slice_data(data=all_data, 
                            start_row=env_vars["START_ROW"],
                            end_row=env_vars["END_ROW"],
                            start_col=env_vars["START_COL"],
                            end_col=env_vars["END_COL"])

    # Flattening multi columns
    data_branch = flattening_col(data=data_branch,
                                 year_month_file=year_month,
                                start_row=env_vars["FLAT_START_ROW"],
                                end_row=env_vars["FLAT_END_ROW"],
                                first_idx_col=env_vars["FIRST_IDX_COL"],
                                scnd_idx_col=env_vars["SCND_IDX_COL"])

    # Add date of file to the dataframe
    data_branch["Tanggal"] = date_of_file

    # Filter branch Gorontalo's Data
    data_branch_gto = filter_data(data_branch, 
                                env_vars["FILTERS_CAT"],
                                env_vars["FILTERS_COL"])

    # Save clean file
    save_excel_file(data=data_branch_gto, 
                    saved_file_name=CLEAN_FILE_NAME,
                    path_saved=CLEAN_DATA_PATH,
                    list_clean_files = LIST_CLEAN_FILES,
                    sheet_name=env_vars["CLEAN_SHEET_NAME"])
    
