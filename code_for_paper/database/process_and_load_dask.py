#!python3
# -*- coding: utf-8 -*-
"""
This script processes and loads the zipped FTP files
into a Hadoop cluster and makes data available via Impala
"""

#%% Libraries
import os
import pathlib
import traceback
import subprocess
import shutil
import pandas as pd
import numpy as np
import sys
import pickle
import platform
import multiprocessing
from datetime import datetime, date, timedelta, timezone
from dateutil.parser import isoparse
import psycopg2
import random
import re
from functools import partial
import itertools
import gc
import zipfile


def mail_bot(self, recipient, subject, text):
    return_code = os.system('echo "' + text + '" | mutt -s "' + subject + '" ' + recipient)
    if return_code != 0:
        print('mail failed')
        return False
    else:
        print('mail sent!')
        return True

import dask
from dask import bag
from dask.distributed import Client, LocalCluster

# who to recieve email alerts
script_maintainer = 'email1,email2,email3'

# how old a file should be before deleting to save space
recent_cutoff = timedelta(days = 5)
#%% Functions


def process_record(file_i_split, col_table):
    """
    Process raw Panjiva records and apply appropriate structure

    Parameters
    ----------
    file_i_split : list
        List of records.
    col_table : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    # make empty result list of appropriate size
    list_dict = [None] * len(file_i_split)
    
    num = 0
    # loop through individual entries
    for input_param_i in file_i_split:
        
        # split on unique character
        entry_i = input_param_i.split("'~'")
        
        # ensure appropriate number of columns in the record
        if len(entry_i) != len(col_table): 
            if entry_i == ['']:
                continue
            else:
                print('malformed entry:',entry_i)
                continue
                
        # build out proper data type of each column
        dict_item = {}
        for i in range(len(col_table)):
            
            if col_table.loc[i,'PythonColType'] == 'int':
                val = entry_i[i].replace("'",'')
                if val is None or val == '':
                    dict_item[col_table.loc[i,'columnName']] = None
                else:
                    dict_item[col_table.loc[i,'columnName']] = int(val)
            if col_table.loc[i,'PythonColType'] in ['str','datetime']:
            # remove leading quote mark if it exists
                if len(entry_i[i]) > 0:
                    if entry_i[i][0] == "'": 
                        entry_i[i] = entry_i[i][1:]
                else:
                    entry_i[i] = None
                dict_item[col_table.loc[i,'columnName']] = entry_i[i]
            elif col_table.loc[i,'PythonColType'] in ['bool']:
            # remove leading quote mark if it exists
                if len(entry_i[i]) > 0:
                    if entry_i[i][0] == "'": 
                        entry_i[i] = entry_i[i][1:]
                    entry_i[i] = bool(entry_i[i])
                else:
                    entry_i[i] = None                    
                dict_item[col_table.loc[i,'columnName']] = entry_i[i]
                
            else:
                if len(entry_i[i]) > 0:
                    if entry_i[i][0] == "'": 
                        entry_i[i] = entry_i[i][1:]
                else:
                    entry_i[i] = None                    
                dict_item[col_table.loc[i,'columnName']] = entry_i[i]
                
        # store result of single record process
        # place into appropriate index location
        # only store complete records
        list_dict[num] = dict_item
        num += 1
    
    # delete original object to save RAM
    del file_i_split
    gc.collect()
    
    # return list with number of complete records
    return list_dict[:num]

def partitionIndexes(totalsize, numberofpartitions):
    """ Partition a number to apprporiate sub partition size """
    # Compute the chunk size
    chunksize = int(totalsize / numberofpartitions)
    # How many chunks need an extra 1 added to the size?
    remainder = totalsize - chunksize * numberofpartitions
    a = 0
    for i in range(numberofpartitions):
        b = a + chunksize + (i < remainder)
        # Yield the inclusive-inclusive range
        yield [a, b]
        a = b

def sql_create_fn(file_i_table_name, dtype_view, dtype_sql, db_location, table_type, save_type):
    """
    Create SQL create statement for Impala to execute

    Parameters
    ----------
    file_i_table_name : str
        Name of table for Impala.
    dtype_view : dict or pandas series
        sample of data or metadata to determine structure for statement.
    dtype_sql : pandas dataframe
        metadata object.
    db_location : str
        database location.
    table_type : split or single
        whether to place files in single folder or multiple subfolders for partitions.
    save_type : str
        Parquet or csv.

    Returns
    -------
    sql_create_statement : str
        prepared SQL statement to execute on Hadoop cluster with Impala.

    """
    if type(dtype_view) == pd.core.series.Series:
        dict_col_info = dict(dtype_view)
    else:
        dict_col_info = dtype_view
    # set up empty strings for variable info
    table_vars = ''
    if save_type == 'parquet':
        parquet_vars = ''
    
    # loop through items in the dictionary
    i = 0
    for col_name,col_type in dict_col_info.items():
        # convert to string for easier comparison
        col_type = str(col_type)
        
        # determine which data type to use for each variable
        if col_type.find('nvarchar') != -1:
            col_type = 'STRING'
        elif col_type == 'object' and all(dtype_sql[dtype_sql['columnName'] == col_name]['PythonColType'] == 'datetime64[s]'):
            col_type = 'TIMESTAMP'
        elif col_type.find('datetime') != -1:
            col_type = 'TIMESTAMP'
        elif col_type == 'object':
            col_type = 'STRING'
        elif str.lower(col_type).find('int64') != -1:
            col_type = 'BIGINT'
        elif str.lower(col_type).find('int32') != -1:
            col_type = 'INTEGER'
        elif col_type.find('float') != -1:
            col_type = 'DOUBLE'
        elif col_type.find('bit') != -1 or col_type.find('bool') != -1:
            col_type = 'BOOLEAN'
        else:
            col_type = col_type
        

        # if col name ends in _partition then add to partition var info
        if save_type == 'parquet':
            if col_name.endswith('_partition'):
                parquet_vars += str.lower(col_name) + " " + col_type + ", "
            else:
                # add variable command to growing string
                table_vars += str.lower(col_name) + " " + col_type + ", "
        else:
            # add variable command to growing string
            table_vars += str.lower(col_name) + " " + col_type + ", "
        
        # at the end take out the last comma 
        i += 1    
        if i == len(dict_col_info):
            if table_vars[-2:] == ', ':
                table_vars = table_vars[:-2]
            if save_type == 'parquet':
                if parquet_vars[-2:] == ', ':
                    parquet_vars = parquet_vars[:-2]
    
    ### make sql statement
    sql_create_statement = f'CREATE EXTERNAL TABLE IF NOT EXISTS {str.lower(file_i_table_name)} ({table_vars})'
    if save_type == 'text':
        sql_create_statement += f" row format delimited FIELDS TERMINATED BY '1' STORED AS TEXTFILE LOCATION"
    elif save_type == 'parquet':
        if table_type == 'split':
            sql_create_statement += f" PARTITIONED BY ({parquet_vars})"
        sql_create_statement += f" STORED AS PARQUET"
    
    # location section
    sql_create_statement += f" LOCATION 'hdfs://{db_location}/{file_i_table_name}';"
    
    return sql_create_statement

def get_file_paths():

    path_ftp_base = './ftp'
    path_hiveload = './load'
    path_hiveold = './already_loaded'
    path_tempfiles = '/./temp'
    
    return path_ftp_base, path_hiveload, path_hiveold, path_tempfiles

def process_df_part(mapped_tuple, df_table_i):
    """ Process a chunk of data according to appropriate data type rules from metadata """
    mapped_chunk, chunk_i = mapped_tuple
    
    try:
        df_trim = pd.DataFrame(list(mapped_chunk))
        del mapped_chunk
        gc.collect()
        df_trim = df_trim.dropna(how='all') # there might be one or two observations lost here!
        df_trim.columns = [str.lower(x) for x in df_trim.columns] 
        
        # loop through columns of the data to force data type and fix odd problems
        for i in range(len(df_table_i)):
            col_name = str.lower(df_table_i.loc[i,'columnName'])
    
            if df_table_i.loc[i,'PythonColType'].find('int') != -1:
                
                if df_trim[col_name].dtype == 'float64':
                    df_trim[col_name] = pd.array(df_trim[col_name], dtype = pd.Int64Dtype())
                elif df_trim[col_name].dtype == 'int64':
                    if sum(df_trim[col_name].isna()) > 0:
                        print("More than 0 missing values")
                elif df_trim[col_name].dtype == 'str':
                    df_trim[col_name] = pd.array([int(x) if x != '' else None for x in df_trim[col_name]], dtype = pd.Int64Dtype())
                elif df_trim[col_name].dtype == 'O':
                    try:
                        df_trim[col_name] = pd.array(df_trim[col_name], dtype = 'int64')
                    except:
                        try:
                            test = [np.nan if pd.isna(x) else int(x) for x in df_trim[col_name]]
                            df_trim[col_name] = pd.array(test, dtype = pd.Int64Dtype())
                        except:
                            test = [np.nan if x == '' else int(x) for x in df_trim[col_name]]
                            df_trim[col_name] = pd.array(test, dtype = pd.Int64Dtype())
                
            elif df_table_i.loc[i,'PythonColType'].find('datetime') != -1:
                if df_trim[col_name].dtype == 'datetime64[s]':
                    df_trim[col_name] = df_trim[col_name].dt.to_pydatetime()
                else:
                    try:
                        df_trim[col_name] = [pd.to_datetime(x) if x is not None else None for x in df_trim[col_name]]
                    except:
                        date_list = []
                        for i in df_trim[col_name]:
                            if i is None:
                                date_list.append(None)
                            else:
                                try:
                                    date_list.append(isoparse(i))
                                except:
                                    date_list.append(isoparse('2600-12-31 00:00:00'))
                        df_trim[col_name] = date_list
                    df_trim[col_name] = df_trim[col_name].astype(df_table_i.loc[i,'PythonColType'])
            else:
                df_trim[col_name] = df_trim[col_name].astype(df_table_i.loc[i,'PythonColType'])
    except Exception as e:
        raise ValueError(f'chunk {chunk_i} error: {repr(e)}')
    return df_trim

#%% Main script
        
if __name__ == '__main__':
    
    try:
            
        # set up local cluster
        cluster = LocalCluster(threads_per_worker = 1,
                           local_directory='/tmp/')
        client = Client(cluster)
        client
            
        # get appropriate file paths
        path_ftp_base, path_hiveload, path_hiveold, path_tempfiles = get_file_paths()
        
        time_start = datetime.now()
        
        load_type = 'full+change'
        
        # getting list of tables
        list_tables = os.listdir(path_ftp_base)
        
        table_name = list_tables[0]
        # find last full file
        
        
#%% Build file list
        def get_data_file_info(data_file):
            """ Function to build file information """
            
            if data_file.find('Full') != -1:
                file_type = 'full'
                dt_str = data_file.lower().split(file_type)[1].split('.zip')[0]
                if dt_str.find('-') != -1:
                    file_part = int(dt_str.split('-')[1])
                    dt_str = dt_str.split('-')[0] 
                else:
                    file_part = 1
                dt_file = pd.to_datetime(dt_str).to_pydatetime()
                
            elif data_file.find('Change') != -1:
                file_type = 'change'
                dt_str = data_file.lower().split(file_type)[1].split('.zip')[0]
                file_part = int(dt_str.split('-')[1])
                dt_str = dt_str.split('-')[0]
                dt_file = pd.to_datetime(dt_str).to_pydatetime()            
                
            
            return file_type, dt_file, file_part
        
        
        data_dict = []
        for table_name in list_tables:
            for folder in os.listdir(os.path.join(path_ftp_base, table_name)):
                for data_file in os.listdir(os.path.join(path_ftp_base, table_name,folder)):
                    if data_file.endswith('.zip'):
                        file_type, dt_file, file_part = get_data_file_info(data_file)
                        file_path = os.path.join(path_ftp_base, table_name,folder,data_file)
                        
                        data_dict.append({'table'     : table_name,
                                          'date'      : folder,
                                          'file_path' : file_path,
                                          'file_type' : file_type,
                                          'file_part' : file_part,
                                          'dt_file'   : dt_file,
                                          'dt_mtime'  : datetime.fromtimestamp(pathlib.Path(file_path).stat().st_mtime),
                                          'file_size' : pathlib.Path(file_path).stat().st_size
                                          })
        
        df_data_dict = pd.DataFrame(data_dict)
        
        df_grouped_table_part = df_data_dict[df_data_dict['file_type'] == 'full'].groupby(['table','file_part'])
        df_most_recent_tables = df_grouped_table_part.apply(lambda g: g[g['date'] == g['date'].max()]).reset_index(drop = True)

#%% set prioritization for tables
  
        list_tables = [x for x in df_most_recent_tables.table.unique() if 
                       (x.find('US') != -1 and ((x.find('USExport') != -1 or (x.find('USImportDates') != -1)) or (x.find('USImport20') != -1 and x.find('To') != -1)))
                       or x.find('US') == -1]
        
        list_tables = [x for x in list_tables if x not in ['PanjivaMXImportDates']]
        if date.today().day % 2:
            list_tables.reverse()
        list_tables = [x for x in list_tables if x.find('USImport2020To2024') != -1] + [x for x in list_tables if x.find('USImport2020To2024') == -1]
        
        list_tables = [x for x in list_tables if (x.find('Company') != -1)] +  [x for x in list_tables if (x.find('US') != -1) or (x.find('MX') != -1)] + \
            [x for x in list_tables if (x.find('IN') != -1)] + \
                [x for x in list_tables if (x.find('MX') == -1) and (x.find('US') == -1) and (x.find('IN') == -1)]
        #list_tables.reverse()
        #list_tables_reorder = [x for x in list_tables[158:] if x.find('US') == -1]
        #[x for x in list_tables if x.find('US') != -1]

#%% Table processing

        for table in list_tables:
            df_table_update = df_most_recent_tables[df_most_recent_tables['table'] == table].reset_index(drop=True)
    #            df_table_update = df_table_update[::-1]
            
            # loop through files within a single update
            for i, update_file in df_table_update.iterrows():
                
                # if i<10:
                #     continue
                
                print(update_file)
    
                # unzip into memory
                # this saves disk space to not store expanded file
                try:
                    zip_obj = zipfile.ZipFile(update_file['file_path'], 'r')
                except:
                    print('BROKEN!',update_file['file_path'])  
                # loop through the files in the zipped archive
                zip_files = zip_obj.namelist()
                if len(zip_files) != 2:
                    print("MORE FILES THAN EXPECTED!!!!")
                
                count_file = [x for x in zip_files if x.endswith('.cnt')][0]
                count_raw = zip_obj.open(count_file).read().decode('utf-8')
                
                
                #data_file = [x for x in zip_files if x.endswith('.txt')][0]     
                for table_file in [x for x in zip_files if x.endswith('.txt')]:     
                        
                    table_year = None
                    table_name = None
                    file_part = None
                    
                    file_part = update_file['file_part']
                    data_file_obj = zip_obj.open(table_file)
                    
                    
                    # settings for appropriate table name, year, part for
                    # connecting to metadata
                                        # dates modifier
                    if 'DatesData' in table_file:
                        table_file = update_file['table']
                        datestable_flag = True
                        sheet_name = 'Dates Data Add-On'
                    elif 'CompanyCrossRef' in table_file:
                        datestable_flag = False
                        table_file = update_file['table']
                        sheet_name = 'panjivaCompanyCrossRef'
                    else:
                        datestable_flag = False
                        sheet_name = 'Import-Export Files 5-2020'
                        
                    try:
                        hive_type = 'split'
                        table_name = re.match('([A-Z,a-z]*)(2\d\d\d)?(?:to\d\d\d\d)?',table_file).group(1).lower()
                        table_year = int(re.match('(?:[A-Z,a-z]*)(2\d\d\d)?(?:to\d\d\d\d)?',table_file).group(1))
                        year_file_part = str(table_year) + '-'
                    except:
                        hive_type = 'single'
                    
                    if table_year is None:
                        hive_type = 'single'
                        year_file_part = ''
                    
                    # setting file name and save location
                    out_file_name = table_name + '-' + year_file_part + str(file_part) + '.parquet'
                    out_file_path = os.path.join(path_hiveload,out_file_name)
                    
                    # skip if a recent file was already saved
                    if os.path.exists(os.path.join(path_hiveold, out_file_name)):
                        if os.stat(os.path.join(path_hiveold, out_file_name)).st_mtime > datetime.timestamp(datetime.now() - recent_cutoff):
                            print("recent file loaded, skipping...")
                            continue
                    
                    
                    # read the file and split into records
                    # drop original object to save RAM space
                    try:
                        data_file_split = data_file_obj.read().decode('utf-8').replace('\n', ' ').split('#@#@#')
                    except UnicodeDecodeError: # got this error UnicodeDecodeError: 'utf-8' codec can't decode byte 0x92 in position 2832: invalid start byte
                        data_file_obj = data_file_obj.read().decode('Latin-1')
                        try:
                            data_file_split = data_file_obj.replace('\n', ' ').split('#@#@#')     
                        except UnicodeDecodeError: # got this error UnicodeDecodeError: 'utf-8' codec can't decode byte 0x92 in position 2832: invalid start byte
                            data_file_obj = data_file_obj.read().decode('cp1252')
                            data_file_split = data_file_obj.read().replace('\n', ' ').split('#@#@#')
            
                    del data_file_obj
                    
                    # read in metadata file
                    df_table_info = pd.read_excel('PANJIVA_METADATA_FILE.xlsx', sheet_name = sheet_name)
                    
                    # prep metadata file
                    for col_i in ['Table Name', 'Column Name', 'SQL Data Type']:
                        df_table_info[col_i] = df_table_info[col_i].str.lower()
                        
                    if datestable_flag == True:
                        df_table_info['Table Name Mod'] = ('Panjiva' + df_table_info['ISO 2 Country Code'] + df_table_info['File Type'] + 'Dates').str.lower()
                        df_table_i = df_table_info[df_table_info['Table Name Mod'] == table_name.lower()].reset_index(drop = True)
                        
                    else:
                        df_table_i = df_table_info[df_table_info['Table Name'] == table_name.lower()].reset_index(drop = True)
                        year_table = False
                        if len(df_table_i) == 0:
                            df_table_i = df_table_info[df_table_info['Table Name'] == table_name[:-4].lower()].reset_index(drop = True)
                            year_table = True
    
                        
                    def make_python_datatypes(col_type):
                        if col_type.find('varchar') != -1:
                            col_type = 'str'
                        elif col_type == 'object':
                            col_type = 'datetime'
                        elif col_type.find('datetime') != -1:
                            col_type = 'datetime64[s]'
                        elif col_type == 'object':
                            col_type = 'str'
                        elif str.lower(col_type).find('bigint') != -1:
                            col_type = 'int64'
                        elif str.lower(col_type).find('int') != -1:
                            col_type = 'int32'
                        elif col_type.find('float') != -1:
                            col_type = 'float'
                        elif col_type.find('bit') != -1:
                            col_type = 'bool'
                        else:
                            col_type = col_type
                        
                        return col_type
                        
                    df_table_i['PythonColType'] = df_table_i['SQL Data Type'].apply(lambda x: make_python_datatypes(x))
                    df_table_i = df_table_i.rename(columns = {'Column Name' : 'columnName'})
                    

                    print(f'file: {update_file["file_path"].split("/")[-1]}, rows: {len(data_file_split)}')
                    
                    # split records and prepare to process
                    proc_num = len(client.scheduler_info()['workers'])

                    print('Split processing of raw data to dataframes')
                    
                    # using delayed for most everything but bag to distribute data
                    list_bag = bag.from_sequence(data_file_split, npartitions = proc_num)
                    list_delayed = list_bag.to_delayed()
    
                    dictionary_delayed = [dask.delayed(partial(process_record, table_name = table_name, col_table = df_table_i))(x) for x in list_delayed]
                    df_futures = [dask.delayed(partial(process_df_part, df_table_i = df_table_i))(x) for x in zip(dictionary_delayed,list(range(len(dictionary_delayed))))]
                    df_list = dask.compute(*df_futures)
                    
                    del data_file_split
                    gc.collect()

                    print('Combine to single dataframe')
                    df_all = pd.concat(df_list)
                    
    
                    df_all.drop_duplicates(inplace = True)
                    print(f"Saving {table_name}-{file_part} to load location")
                    if hive_type == 'split':
                        df_all['year_partition'] = table_year
                        
                    df_all.to_parquet(path = out_file_path, index = False, compression = 'snappy')
    
    
                    
                #%% Post results
                    
                    
                    # CODE TO DELETE AND POST NEW DATA TO HADOOP
                    # data processed away from the hadoop cluster then pushed into hadoop
                    # impala commands dynamically created based on response from terminal
                    user = 'USERNAME'
                    edge_node = 'EDGENODE_ADDRESS'
                    table_path = 'DB_NAME'
                    db = 'DB_NAME'
                    edge_db_location = f'/tmp/{table_path}/load/'
                    impala_node = 'IMPALA_NODE_ADDRESS:21000'
                    parquet_db_location = f'/hdfs/{table_path}/warehouse'
                    csv_db_location = f'/hdfs/{table_path}/warehouse'
                    temp_location = f'/hdfs/{table_path}/warehouse/temp'
                    print("Loading data to the edge node")
                    
                    # clear data from edge node location
                    result1 = subprocess.getstatusoutput(f"ssh {user}@{edge_node} rm -rf {edge_db_location}")
                    # push data to edge node location
                    result2 = subprocess.getstatusoutput(f"scp -rp {path_hiveload} {user}@{edge_node}:{edge_db_location}")
                    print("Starting Impala uploads...")
    
                    if result2[0] == 0:
                        list_file_load = os.listdir(path_hiveload)
                    for f in list_file_load:
                        
                        if os.stat(os.path.join(path_hiveload,f)).st_size < 10:
                            os.remove(os.path.join(path_hiveload,f))
                            print(f"Removed empty file {f}")
                            continue
                        
                        save_type = f.split('.')[-1]
                        
                        print(f"Pushing updates to Impala for {f}")
                        if hive_type == 'single':
                            table = table_name
                            db_location = csv_db_location
                            put_location = f"{db_location}/{str.lower(table)}"
                            make_folder_statement = f"hdfs dfs -mkdir {db_location}/{str.lower(table)}; "
                            make_folder_year_statement = '\n'
                        elif hive_type == 'split':
                            table = table_name
                            db_location = csv_db_location
                            put_location = f"{db_location}/{str.lower(table)}/year_partition\={table_year}"
                            make_folder_statement = f"hdfs dfs -mkdir -p {db_location}/{str.lower(table)};"
                            make_folder_year_statement = f"hdfs dfs -mkdir -p {db_location}/{str.lower(table)}/year_partition\={table_year};"
                        
                        try:
                            sql_create_statement = sql_create_fn(table.lower(),
                                                                 df_all.dtypes, 
                                                                 df_table_i[['PythonColType','columnName']], 
                                                                 db_location,
                                                                 hive_type, 
                                                                 save_type)
                            
                            # create sql statement
                            result3a = subprocess.getstatusoutput(fr"""
                                                           ssh {user}@{edge_node} "impala-shell 
                                                           --impalad={impala_node} 
                                                           --ssl
                                                           --database={table_path}
                                                           --query=\"{sql_create_statement}\""
                                                           """.replace('\n',''))
                       
                            
                            if result3a[0] != 0:
                                print(f"SQL create statement failed for {f}!")
                        
                        except:
                            print("File exists from a previous run. Cannot send sql create statement")
                            
                        # push data from edge node to hdfs
                        result3b = subprocess.getstatusoutput(f'''ssh {user}@{edge_node} "
                                                              hdfs dfs -put {edge_db_location}{f} {temp_location}"
                                                              '''.replace('\n',''))
                        if result3b[0] == 0:
                            print(f"Data pushed successfully for {f}")
                            
                            
                        if hive_type =='split':
                            # drop partitions
                            result4 = subprocess.getstatusoutput(f"""
                                                       ssh {user}@{edge_node} "impala-shell 
                                                       --impalad={impala_node}
                                                       --ssl
                                                       --database={db}
                                                       --query='alter table {table} drop if exists partition (year_partition<=222222)'"
                                                       """.replace('\n',''))
                        
                        # make folders
                        # move data from temp location to table location, delete temp location
                        result3c = subprocess.getstatusoutput(f'''
                                                             ssh {user}@{edge_node} "
                                                             {make_folder_statement}
                                                             {make_folder_year_statement}
                                                             hdfs dfs -cp -f {temp_location}/{f} {put_location};
                                                             hdfs dfs -rm {temp_location}/{f};
                                                             "
                                                             '''.replace('\n',''))
                        if result3c[0] == 0:
                            print(f"Data processed successfully for {f}")
                            # invalidate metadata
                            result4 = subprocess.getstatusoutput(f"""
                                                       ssh {user}@{edge_node} "impala-shell 
                                                       --impalad={impala_node}
                                                       --ssl
                                                       --database={db}
                                                       --query='invalidate metadata {table}'"
                                                       """.replace('\n',''))
                            
                            # set table to external
                            result4b = subprocess.getstatusoutput(fr"""
                                                       ssh {user}@{edge_node} "impala-shell 
                                                       --impalad={impala_node}
                                                       --ssl
                                                       --database={db}
                                                       --query=\"ALTER TABLE {table} SET TBLPROPERTIES('EXTERNAL'='TRUE');\""
                                                       """.replace('\n',''))
                            
                            # recover partitions
                            result5 = subprocess.getstatusoutput(f"""
                                                   ssh {user}@{edge_node} "impala-shell 
                                                   --impalad={impala_node}
                                                   --ssl
                                                   --database={db}
                                                   --query='alter table {table} recover partitions'"
                                                   """.replace('\n',''))
                            print(f"Metadata updated successfully for {table}")
                            if os.path.isdir(os.path.join(path_hiveold,f)) == True:
                                shutil.rmtree(os.path.join(path_hiveold,f))
                            shutil.move(os.path.join(path_hiveload,f), os.path.join(path_hiveold,f))
                        else:
                            print(f"ERROR----{f} didn't post to hadoop properly! {result3c[1]}")
                
                    time_end = datetime.now()
                    print(f'Program time {time_end - time_start}')
                    del df_all
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_message = traceback.format_exc()
        error_message = error_message.replace('"',"'")
        subject = 'Panjiva Load Issue'
        text = 'Panjiva section hit an issue. Please investigate further!\n' + str(sys.exc_info()[0]) + " - " + str(sys.exc_info()[1]) + '\n' + error_message + " " + str(datetime.now())[:-7] + "\n"
        recipient = script_maintainer
        if mail_bot(recipient, subject, text) == True:
            pass
        else:
            print("EMAIL FAILED!")   