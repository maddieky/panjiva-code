##-----------connection string--------
conn_panjiva <- dbConnect(odbc::odbc(),
                  .connection_string = paste("Driver=Impala",
                                             "OTHERSETTINGS",
                                             sep = ";")) 


quarter_date = function(df) {
  w = df %>%
  mutate(year = (sql("cast(year(arrivaldate) as string)")), 
         qtr = (sql("cast(quarter(arrivaldate) as string)"))) %>%
  mutate(quarter = paste0(year,"/0",qtr)) %>%
  group_by(quarter) 
  return(w)
}

year_date = function(df) {
  w = df %>%
    mutate(year = sql("year(arrivaldate)"))  %>%
    group_by(year) 
  return(w)
}

