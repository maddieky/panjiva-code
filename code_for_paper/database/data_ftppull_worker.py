#!python3
# -*- coding: utf-8 -*-
"""
This script manages the remote Ray workers to process a queue
of paths to pull data from the FTP server

"""
#%% Libraries
import sqlite3
import os
import sys
from datetime import datetime, timedelta, date
import pysftp
import platform
import netrc
import shutil
import pandas as pd
import ray
import logging
import traceback

@ray.remote
class ftppull_worker:
    

    def __init__(self):

        self.path_ftp = './ftp'
        self.ftp_connect()
    
    def ftp_connect(self):
        log_in = netrc.netrc()
        log_in.authenticators('[[S&P URL HERE]]')
        username,_,password = log_in.authenticators('[[S&P URL HERE]]')
        
        # linux command: sftp username@edx.standardandpoors.com
        self.ftp_connection = pysftp.Connection(host = '[[S&P URL HERE]]', 
                                           username = username, 
                                           password = password)
        
        
        # email recipients for errors
        self.maintainers = 'email1,email2,email3'
    
    def mail_bot(self, recipient, subject, text):
        return_code = os.system('echo "' + text + '" | mutt -s "' + subject + '" ' + recipient)
        if return_code != 0:
            print('mail failed')
            return False
        else:
            print('mail sent!')
            return True
    
    def ftp_downloader(self, ftp_file_location, file_i, path_ftp_section = ""):
        """
        Set up download of file if file was updated and is not present
        in the local file location

        Parameters
        ----------
        ftp_file_location : str
            file name.
        file_i : file details
            stat details of the file.
        path_ftp_section : str, optional
            folder string path for the local store. The default is "".
            
        Raises
        ------
        RuntimeError
            If file download fails.

        Returns
        -------
        None.

        """
        if not os.path.exists(path_ftp_section):
            os.mkdir(path_ftp_section)
        
        download_dummy = False
                
        # get file name
        file_i_name = ftp_file_location.split('/')[-1]
        
        if file_i_name.find('-') != -1:
            subfolder = file_i_name[-22:-14]
        else:
            subfolder = file_i_name [-18:-10]
        
        if not os.path.exists(path_ftp_section + '/' + subfolder):
            try:
                os.mkdir(path_ftp_section + '/' + subfolder)
            except FileExistsError:
                pass
        
        # build final file path for local save
        file_i_savepath = os.path.join(path_ftp_section + '/' + subfolder, file_i_name )
        
        # get modified time and size from file details
        file_i_st_mtime = file_i.st_mtime
        file_i_st_size = file_i.st_size
           
        ###### DETERMINE IF WE HAVE SAME FILE OR NEED TO DOWNLOAD
        if os.path.exists(file_i_savepath):
            # check if the file has the same modified time and size as the ftp server
            if os.path.getmtime(file_i_savepath) >= file_i_st_mtime and os.path.getsize(file_i_savepath) == file_i_st_size:
                download_dummy = False
            else:
                download_dummy = True
        else:
            if datetime.fromtimestamp(file_i_st_mtime) > datetime.now() - timedelta(days = 6):
                download_dummy = True
            else:
                download_dummy = False
        
        print(f'Download?: {download_dummy}, file: {file_i_name}, {datetime.fromtimestamp(file_i_st_mtime)}')
        # if not then download the file from the ftp server
        if download_dummy == True:
            print(f'saving {file_i_name} to {path_ftp_section + "/" + subfolder}')
            try:
                self.ftp_connection.get(ftp_file_location, 
                                   file_i_savepath, preserve_mtime=True)
            except OSError:
                pass
            except Exception as error:
                raise RuntimeError(f'ftp_location: {ftp_file_location}') from error
                
        
    def old_file_remover(self, path_ftp_section):
        """
        Delete files older than 12 days old across the file tree

        Parameters
        ----------
        path_ftp_section : str
            valid file path that contains all the files

        Returns
        -------
        None.

        """
        for r, d, f in os.walk(os.path.join(path_ftp_section)):
            if r.split('/')[-1] < str(date.today() - timedelta(days = 12)).replace('-',''):
                try:
                    shutil.rmtree(r)
                except FileNotFoundError:
                    pass
                except OSError:
                    pass
     
    def worker_process(self, queue_to_do, queue_done):
        """
        Primary process for the class that interacts with the Ray queue

        Parameters
        ----------
        queue_to_do : Ray Queue
            The list of files to try download
        queue_done : TYPE
            The list of files that were downloaded

        Raises
        ------
        RuntimeError
            If FTP connection fails

        Returns
        -------
        None.

        """
        try:
            
            # connect to a central tracking sqlite database
            conn = sqlite3.connect('./tracking.db')
        
            # loop until the queue is empty
            while not queue_to_do.empty():
                
                # grab a file path from the queue
                # Ray makes sure that only one worker gets to the file
                panjiva_file = queue_to_do.get()            
                
                # time tracking
                time_start = datetime.now()
                
                # setting up save path for the product
                path_ftp_section = self.path_ftp + '/' + panjiva_file.split('/')[2]
                
                # remove old files
                self.old_file_remover(path_ftp_section)
                
                # try to determine file details
                try:
                    file_details = self.ftp_connection.lstat(panjiva_file)
                except OSError:
                    self.ftp_connect()
                except Exception as error:
                    raise RuntimeError(f'ftp_location: {panjiva_file}') from error
                
                print(f'***SECTION*** - {panjiva_file}')
                
                # execute download method on ftp file to the save location
                self.ftp_downloader(panjiva_file, file_details, path_ftp_section)
                
                time_end = datetime.now()
                
                # time tracking in db
                try:
                    df = pd.DataFrame({'product' : ['test'],'time' : [time_end - time_start]})
                    df['time'] = df.time.dt.seconds
                    df.to_sql('quota_applied', index = False, con = conn, if_exists = 'append')
                except sqlite3.OperationalError:
                    try:
                        df.to_sql('quota_applied', index = False, con = conn, if_exists = 'append')
                    except:
                        pass
                    
                
                # add file to done queue
                queue_done.put(panjiva_file)
                
            
            # close ftp connection
            self.ftp_connection.close()
            
        # send exception emails
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_message = traceback.format_exc()
            error_message = error_message.replace('"',"'")
            logging.error(error_message)
            
            subject = 'Panjiva FTP Issue'
            text = f'There has been an error on file {panjiva_file}. Please investigate further!\n' + str(sys.exc_info()[0]) + " - " + str(sys.exc_info()[1]) + " " + str(datetime.now())[:-7] + "\n" + error_message
            recipient = self.maintainers
            if self.mail_bot(recipient, subject, text) == True:
                pass
            else:
                logging.error("EMAIL FAILED!")


  