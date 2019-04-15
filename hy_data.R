library(tidyverse)
library(Rblpapi)
library(RSQLite)
library(DBI)
library(readxl)

blpConnect()

data <- function() {
  
  db <- dbConnect(SQLite(), "HY_index.sqlite")
  ID(db)
  Info(db)
  hist_data(db)

}

ticker <- function(db){
  
  ticker <- paste( bds("BEUCTRUU Index", "INDX_MEMBERS")[[1]], "Corp", sep = " ")
  ID_pool <- bdp(ticker, c("TICKER", "CNTRY_OF_RISK",
                           "RTG_SP", "RTG_MOODY", "RTG_FITCH")) %>% 
    mutate(ID = ticker,
           ticker = TICKER,
           country = CNTRY_OF_RISK,
           SP = clean(RTG_SP),
           MOODY = clean(RTG_MOODY),
           FITCH = clean(RTG_FITCH)) %>%
    filter(country == "CN") %>%
    select(ticker, SP, MOODY, FITCH) %>%
    gather(`MOODY`, `SP`, `FITCH`,
           key = "agent", value = "RTG") %>%
    left_join(read_xlsx("credit_mapping.xlsx", col_names = TRUE)) %>%
    group_by(ticker) %>%
    summarise(max = max(Credit, na.rm = TRUE),
              min = min(Credit, na.rm = TRUE)) %>%
    left_join(read_xlsx("pool.xlsx", col_names = TRUE))
  
}




ID <- function(db){

  ID <- as_tibble(bsrch(paste("FI:", "test_3", sep = ""))) %>%
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
           FIRST_CALL_DT_ISSUANCE = as.character(FIRST_CALL_DT_ISSUANCE),
           MATURITY = if_else(is.na(CALLED_DT) == TRUE, MATURITY, CALLED_DT),
           MATURITY = if_else(is.na(MATURITY) == TRUE, 
                              FIRST_CALL_DT_ISSUANCE, MATURITY))
    
  Info <- rbind(Info, dbGetQuery(db, 'SELECT * FROM Info')) %>%
    filter(duplicated(ID) == FALSE)
  
  dbWriteTable(db, "Info", Info, overwrite = TRUE)
  
}

hist_data <- function(db) {
  
ID <- dbGetQuery(db, 'SELECT ID, PRICING_SOURCE, 
                 ISSUE_DT, MATURITY FROM Info') %>%
  mutate(ticker1 = paste(ID, "@BGN", sep = ""),
         issue_date = as.Date(ISSUE_DT),
         maturity = as.Date(MATURITY),
         ticker2 = paste(ID, "@BVAL", sep = "")) %>%
  select(ID, ticker1, ticker2, issue_date, maturity)

key <- c("PX_MID", "PX_BID", "PX_ASK",
         "Z_SPRD_MID", "DUR_ADJ_OAS_BID", "YLD_YTM_MID")

date01 <- (dbGetQuery(sovdb, 'SELECT date FROM hist_data') %>%
  filter(duplicated(date) == FALSE) %>%
  mutate(date = as.Date(date)) %>%
  arrange(desc(date)))$date[1] +  1
date02 <- Sys.Date() - 1

if(date01 > date02) {return()} 

ID <- ID %>%
  filter(maturity >= date01 + 180,
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
  select(-ticker1, -ticker2, -issue_date, -maturity) %>%
  filter(is.na(PX_MID) == FALSE)}

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
  
  data2 <- as_tibble(do.call("bind_rows", data1))
  
  return(data2)
}

clean <- function(RTG) {
  
  RTG = gsub("(\\*\\+|\\*|\\*-|\\(|\\)|u|-u|NR|P| |e|WR|WD)", "", RTG)
  
}
