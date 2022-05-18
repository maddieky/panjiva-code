##-----------connection string--------
conn_hstrade <- dbConnect(odbc::odbc(),
                  .connection_string = paste("Driver=Impala",
                                             "OTHERSETTINGS",
<<<<<<< HEAD
                                             sep = ";")) 
=======
                                             sep = ";")) 
>>>>>>> 70c1f3f3024ae3dc968713690bfee7e7ea919c54
