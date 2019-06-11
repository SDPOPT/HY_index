library(tidyverse)
library(Rblpapi)
library(RSQLite)
library(DBI)
library(readxl)
library(writexl)
library(WindR)
library(RSQLite)

blpConnect()
w.start(showmenu = FALSE)

ticker <- function(){
  
  db <- dbConnect(SQLite(), "HY_index.sqlite")
  issuer_china <- dbReadTable(db, "issuer_china")
  issuer_nonchina <- dbReadTable(db, "issuer_nonchina")
  credit_mapping <- dbReadTable(db, "credit_mapping")
  mapping_credit <- dbReadTable(db, "mapping_credit")
  
  
  ticker <- paste( bds("BEUCTRUU Index", "INDX_MEMBERS")[[1]], "Corp", sep = " ")
  ticker_pool <- bdp(ticker, c("TICKER", "CNTRY_OF_RISK",
                           "RTG_SP", "RTG_MOODY", "RTG_FITCH", 
                           "BASEL_III_DESIGNATION", "PAYMENT_RANK")) %>%
    mutate(ID = ticker,
           ticker = TICKER,
           country = CNTRY_OF_RISK,
           SP = clean(RTG_SP),
           MOODY = clean(RTG_MOODY),
           FITCH = clean(RTG_FITCH)) %>%
    filter(BASEL_III_DESIGNATION == "") %>%
    filter(grepl("Subordinated", PAYMENT_RANK) == FALSE) %>%
    select(ID, ticker, SP, MOODY, FITCH, country) %>%
    gather(`MOODY`, `SP`, `FITCH`,
           key = "agent", value = "RTG") %>%
    left_join(credit_mapping) %>%
    group_by(ticker, country) %>%
    summarise(max = max(Credit, na.rm = TRUE),
              min = min(Credit, na.rm = TRUE)) %>%
    ungroup()
  
  china_issuer <- ticker_pool %>%
    filter(country  == "CN") %>%
    left_join(issuer_china) %>%
    filter(is.na(sector) == TRUE)
  
  issuer_nonchina <- ticker_pool %>%
    filter(country  != "CN") %>%
    filter(is.infinite(max) == FALSE) %>%
    filter(max < 21) %>%
    filter(min > 14) %>%
    mutate(sector = "NON CHINA") %>%
    select(ticker, sector)
  
  
  write_xlsx(rbind(issuer_china, issuer_nonchina), "ticker_pool.xlsx")
  
}

onshore_rating <- function() {
  
  db <- dbConnect(SQLite(), "HY_index.sqlite")
  onshore_credit_mapping <- dbGetQuery(db, "SELECT * FROM onshore_credit_mapping")
  
  ID <- bsrch("FI:test_4") %>%
    mutate(ID = as.character(id)) %>%
    select(ID) %>%
    mutate(ticker = bdp(ID, "TICKER")$TICKER,
           equity_ticker = bdp(ID, "BB_DEFAULT_RISK_OBLIGOR_TICKER")$
             BB_DEFAULT_RISK_OBLIGOR_TICKER) %>%
    select(ticker, equity_ticker) %>%
    filter(duplicated(equity_ticker) == FALSE)
  
  onshore <- bdp(ID$equity_ticker, c("RTG_DAGONG_LT_LOCAL_CRNCY_ISSUER",
                           "RTG_CHENGXIN_LC_ISSUER",
                           "RTG_LIANHE_LT_LC_ISSUER",
                           "RTG_SBCR_LT_LC_ISSUER",
                           "RTG_CCRC_ISSUER")) %>%
    mutate(equity_ticker = ID$equity_ticker,
           DAGONG = clean(RTG_DAGONG_LT_LOCAL_CRNCY_ISSUER),
           CHENGXIN = clean(RTG_CHENGXIN_LC_ISSUER),
           LIANHE = clean(RTG_LIANHE_LT_LC_ISSUER),
           SBCR = clean(RTG_SBCR_LT_LC_ISSUER),
           CCRC = clean(RTG_CCRC_ISSUER)) %>%
    select(equity_ticker, DAGONG, LIANHE, CHENGXIN, SBCR, CCRC) %>%
    gather(`DAGONG`, `LIANHE`, `CHENGXIN`, `SBCR`, `CCRC`,
           key = "agent", value = "RTG") %>%
    left_join(onshore_credit_mapping) %>%
    group_by(equity_ticker) %>%
    summarise(credit_max = max(Credit, na.rm = TRUE),
              credit_min = min(Credit, na.rm = TRUE)) %>%
    ungroup() %>%
    left_join(ID) %>%
    select(-equity_ticker)
  

  ID1 <- bsrch("FI:test_5") %>% 
    mutate(ID = as.character(id)) %>%
    select(ID) %>%
    mutate(ticker = bdp(ID, "TICKER")$TICKER,
           ID = bdp(ID, "ID_SHORT_CODE")$ID_SHORT_CODE)
  
  
  onshore1 <- w.wss(paste(unique(ID2$ID), collapse = ","), 'latestissurercreditrating')[[2]] %>%
    select(ID = CODE, RTG = LATESTISSURERCREDITRATING) %>%
    filter(RTG != "NaN") %>%
    left_join(ID2) %>% 
    left_join(onshore_credit_mapping) %>%
    group_by(ticker) %>%
    summarise(credit_max = max(Credit, na.rm = TRUE),
              credit_min = min(Credit, na.rm = TRUE)) %>%
    ungroup()
  
  onshore1 <- onshore %>%
    filter(is.infinite(credit_min) == TRUE) %>%
    select(ticker) %>%
    left_join(onshore1)
  
  onshore <- onshore %>%
    filter(is.infinite(credit_min) != TRUE) %>%
    rbind(onshore1) %>%  
    mutate(Credit = credit_max) %>%
    left_join(onshore_credit_mapping) %>%
    select(ticker, RTG)
  
  write_xlsx(onshore, "onshore_rating.xlsx")

}

clean <- function(RTG) {
  
  RTG = gsub("(\\*\\+|\\*|\\*-|\\(|\\)|u|-u|NR|P| |e|WR|WD|\\-1|pi)", "", RTG)
  
}
