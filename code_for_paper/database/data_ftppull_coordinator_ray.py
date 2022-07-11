#!/python3
# -*- coding: utf-8 -*-
"""
Script to download files from ftp server and collect in local file path
This file is for coordinating the file downloads
The worker processes run from the data_ftppull_worker.py script

"Bill of Lading Data in International Trade Research with an 
Application to the Covid-19 Pandemic" (Flaaen et al.)
"""

#%% Libraries
import os
import sys
import time
import netrc
import pysftp
import shutil
import logging
import traceback
import ray
from datetime import datetime
from ray.util.queue import Queue

def mail_bot(recipient, subject, text):
    return_code = os.system('echo "' + text + '" | mutt -s "' + subject + '" ' + recipient)
    if return_code != 0:
        print('mail failed')
        return False
    else:
        print('mail sent!')
        return True

#%% Setting up path to load worker script
function_path = './'
os.chdir(function_path)
from data_ftppull_worker import ftppull_worker

#%% Setting maintainers to receive alerts when errors occur
maintainers = 'EMAIL1,EMAIL2,EMAIL3'

#%%
if __name__ == '__main__':
    
    try:
    
        PROGRAM_NAME = 'panjiva_data_download'
        log_path = './'
        
        ####################### TIME SETUP
        startup_time = datetime.now()
        startup_time_str = str(startup_time)[:-7].replace('-','').replace(' ','-').replace(':','')
        
        ####### LOGGING SETUP
        log_file_path = os.path.join(log_path,f'{PROGRAM_NAME}-{startup_time_str}.log')
        logging.basicConfig(filename=log_file_path, 
                            format='%(asctime)s.%(msecs)03d:%(levelname)s:%(message)s', 
                            level=logging.INFO,datefmt='%Y-%m-%d,%H:%M:%S')
        
        ###### Launching Ray cluster
        logging.info('Launching ray cluster')
        num_processes = 10
        ray.init(num_cpus=num_processes)
        
        
        ###### Getting authentication details
        
        logging.info('Setting up ftp connection and getting directories')
        log_in = netrc.netrc()
        # netrc file should look like the following, 
        # filling in the capitalized words with appropriate entries: 
            # machine URL 
            # username USERNAME 
            # password PASSWORD
            
        username,_,password = log_in.authenticators('[[S&P FTP URL HERE]]')
        
        
        ftp_connection = pysftp.Connection(host = '[[S&P FTP URL HERE]]', 
                                           username = username, 
                                           password = password)
        
        # navigate to products directory and find panjiva products
        ftp_connection.chdir('/Products')
        dir_products = ftp_connection.listdir()
        dir_panjiva_all_original = [x for x in dir_products if x.find('Panjiva') != -1]
        
        # US im and MX im/ex now have multi-years, so drop extra products
        dir_panjiva_all = [x for x in dir_panjiva_all_original if (x.find('USImport') == -1 or x.endswith(f'USImportDates')) or (x.find('USImport') != -1 and x.find('To') != -1)]
        dir_panjiva_all = [x for x in dir_panjiva_all if (x.find('MXImport') == -1 or x.endswith(f'MXImportDates')) or (x.find('MXImport') != -1 and x.find('To') != -1)]
        dir_panjiva_all = [x for x in dir_panjiva_all if (x.find('MXExport') == -1 or x.endswith(f'MXExportDates')) or (x.find('MXExport') != -1 and x.find('To') != -1)]
        
        # eliminate the following for import/export individual years since there are grouped years instead
        for country in ['EC','CO','CL','BR']:    
            dir_panjiva_all = [x for x in dir_panjiva_all if (x.endswith(f'{country}Import') or x.endswith(f'{country}ImportDates')) or x.find(f'{country}Import') == -1]
            dir_panjiva_all = [x for x in dir_panjiva_all if (x.endswith(f'{country}Export') or x.endswith(f'{country}ExportDates')) or x.find(f'{country}Export') == -1]

        # for when panjiva upgrades the ftp setup and we need to delete old folders
        #        for folder_i in os.listdir('[[FTP LOCAL SAVE PATH]]'):
        #            if folder_i not in dir_panjiva_all:
        #                shutil.rmtree(os.path.join('[[FTP LOCAL SAVE PATH]]',folder_i))
        #                
        
        # reserve order to give priority to US products
        dir_panjiva_all.reverse()
        
        # find all files to consider
        files_to_consider = []
        for dir_i in dir_panjiva_all:
            files_to_consider += [f'/Products/{dir_i}/{x}' for x in ftp_connection.listdir(dir_i)]
        num_files = len(files_to_consider)
        logging.info(f'Running on {num_files} files')        
        
        ######### Ray execution
        logging.info(f'Launching {num_processes} workers')
        processes = [ftppull_worker.remote() for i in range(num_processes)]
        
        # setting up queue for workers
        logging.info(f'Setting up queue')
        queue_to_do = Queue(maxsize=num_files)
        queue_done = Queue(maxsize=num_files)
        _ = [queue_to_do.put(i) for i in files_to_consider]
        logging.info(f'Queue size = {queue_to_do.size()}')
        
        # launching workers to pull from queue in FIFO fashion
        logging.info(f'Directing workers to the queue')
        [p_i.worker_process.remote(queue_to_do, queue_done) for p_i in processes]
        
        # jobs completely asynchronously, so check in on queue size to determine
        # if work is complete. Only wait 3 hours before exiting
        logging.info(f'Waiting for completed files to be done')
        iterations = 0
        while queue_done.size() < num_files and iterations <= 180:
            time.sleep(60)
            iterations += 1
        
        # raise error for maintainers if not all files extracted properly
        if queue_to_do.size() > 0 or queue_done.size() != num_files:
            logging.error("Cluster did not complete jobs properly")
        
        # shutdown cluster and release resources
        logging.info(f'Job complete, shutting down')
        ray.shutdown()

    # send error message out
    except KeyboardInterrupt:
        pass
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_message = traceback.format_exc()
        error_message = error_message.replace('"',"'")
        logging.error(error_message)
        
        subject = 'Panjiva FTP Issue'
        text = 'There has been an error. Please investigate further!\n' + str(sys.exc_info()[0]) + " - " + str(sys.exc_info()[1]) + " " + str(datetime.now())[:-7] + "\n" + error_message
        recipient = maintainers
        if mail_bot(recipient, subject, text) == True:
            pass
        else:
            logging.error("EMAIL FAILED!")