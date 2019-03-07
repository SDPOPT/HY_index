library(tidyverse)
library(Rblpapi)
library(RSQLite)
library(DBI)
library(readxl)

blpConnect()

data <- function() {
  
  db <- dbConnect(SQLite(), "hy-property.sqlite")
  ID(db)
  Info(db)
  hist_data(db)

}

ID <- function(db){

  ID <- as_tibble(bsrch(paste("FI:", "HY_INDEX", sep = ""))) %>%
    mutate(ID = as.character(id)) %>% select(ID)
  
  ID <- rbind(ID, dbGetQuery(db, 'SELECT * FROM ID')) %>%
  filter(duplicated(ID) == FALSE)
  
  dbWriteTable(db, "ID", ID, overwrite = TRUE)
  
}

Info <- function(db){
  
  ID <- dbGetQuery(db, 'SELECT * FROM ID')
  fields <- dbGetQuery(db, 'SELECT * FROM fields')
  
  Info <- bdp(ID$ID, fields$flds) %>%
    mutate(ID = ID$ID,
           MATURITY = as.character(MATURITY),
           ISSUE_DT = as.character(ISSUE_DT),
           CALLED_DT = as.character(CALLED_DT),
           FIRST_CALL_DT_ISSUANCE = as.character(FIRST_CALL_DT_ISSUANCE)) %>%
    group_by(MATURITY) %>%
    arrange(desc(AMT_OUTSTANDING)) %>%
    ungroup()
  
  Info <- rbind(Info, dbGetQuery(db, 'SELECT * FROM Info')) %>%
    filter(duplicated(ID) == FALSE)
  
  dbWriteTable(db, "Info", Info, overwrite = TRUE)
}

hist_data <- function(db) {
ID <- dbGetQuery(db, 'SELECT ID, PRICING_SOURCE, 
                 ISSUE_DT, MATURITY, CALLED_DT FROM Info') %>%
  mutate(ticker1 = paste(ID, "@BGN", sep = ""),
         issue_date = as.Date(ISSUE_DT),
         maturity = as.Date(ifelse(is.na(CALLED_DT) == TRUE, MATURITY, CALLED_DT)),
         ticker2 = paste(ID, "@", PRICING_SOURCE, sep = "")) %>%
  select(ID, ticker1, ticker2, issue_date, maturity)

key <- c("PX_MID", "PX_BID", "PX_ASK",
         "Z_SPRD_MID", "DUR_ADJ_OAS_BID",
         "YLD_YTM_MID", "YLD_YTM_BID", "YLD_YTM_ASK")

date01 <- (dbGetQuery(sovdb, 'SELECT date FROM hist_data') %>%
  filter(duplicated(date) == FALSE) %>%
  mutate(date = as.Date(date)) %>%
  arrange(desc(date)))$date[1] +  1
date02 <- Sys.Date() - 1

if(date01 > date02) {return()} 

ID <- ID %>%
  filter(maturity >= date01,
         issue_date <= date02)

data <- getdata(ID$ticker1, key, date01, date02) %>%
  mutate(ticker1 = ID) %>%
  select(-ID) %>%
  left_join(ID) %>%
  filter(date >= issue_date) %>%
  filter(date <= maturity)

ID_new <- data %>% 
  filter(is.na(PX_MID) == TRUE) %>%
  filter(duplicated(ID) == FALSE) %>%
  select(ID, ticker1, ticker2, issue_date, maturity)
  
data_new <- getdata(ID_new$ticker2, key, date01, date02)

if(nrow(data_new) > 0){
data_new <- data_new %>% 
  mutate(ticker2 = ID) %>%
  select(-ID) %>%
  left_join(ID_new) %>%
  filter(date >= issue_date) %>%
  filter(date <= maturity) %>%
  select(-ticker1, -ticker2, -issue_date, -maturity)}

data <- data %>% 
  select(-ticker1, -ticker2, -issue_date, -maturity)

data <- data %>% filter((ID %in% ID_new$ID) == FALSE) %>% rbind(data_new) %>%
  mutate(date = as.character(date))

dbWriteTable(db, "hist_data", data, append = TRUE)

}

getdata <- function(ticker, key , date01, date02) {
  
  opt <- structure(c("PREVIOUS_VALUE", 
                     "ALL_CALENDAR_DAYS"),
                   names = c("nonTradingDayFillMethod", 
                             "nonTradingDayFillOption"))
  
  data0 <- bdh(ticker, key, as.Date(date01), 
               as.Date(date02), options = opt)
  
  data1 <- mapply(function(x, y) {y <- y %>% mutate(ID = x) },
                  names(data0), data0, 
                  USE.NAMES = FALSE, SIMPLIFY = FALSE)
  
  data2 <- as.tibble(do.call("bind_rows", data1))
  
  return(data2)
}
