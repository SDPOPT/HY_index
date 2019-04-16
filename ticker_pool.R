library(tidyverse)
library(Rblpapi)
library(RSQLite)
library(DBI)
library(readxl)
library(writexl)

blpConnect()


ticker <- function(){
  
  db <- dbConnect(SQLite(), "HY_index.sqlite")
  
  pool_ticker <- read_xlsx("ticker_pool.xlsx", col_names = TRUE)
  dbWriteTable(db, "pool_ticker", pool_ticker, overwrite = TRUE)
  credit_mapping <- dbGetQuery(db, "SELECT * FROM credit_mapping")
  pool_ticker <- pool_ticker %>%
    select(ticker, sector)
  
  
  ticker <- paste( bds("BEUCTRUU Index", "INDX_MEMBERS")[[1]], "Corp", sep = " ")
  ticker_pool <- bdp(ticker, c("TICKER", "CNTRY_OF_RISK",
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
    left_join(credit_mapping) %>%
    group_by(ticker) %>%
    summarise(max = max(Credit, na.rm = TRUE),
              min = min(Credit, na.rm = TRUE)) %>%
    ungroup()
  
  rated <- ticker_pool %>%
    filter(is.infinite(max) == FALSE) %>%
    left_join(pool_ticker) %>%
    mutate(sector = ifelse(min >= 21, "IG", sector)) %>%
    mutate(sector = ifelse(min <= 14, "Black", sector))
  
  unrated <- ticker_pool %>%
    filter(is.infinite(max) == TRUE) %>%
    left_join(pool_ticker)
  
  ticker_pool <- rbind(rated, unrated)
  
  dbWriteTable(db, "ticker_pool", ticker_pool, overwrite = TRUE)
  write_xlsx(ticker_pool, "ticker_pool.xlsx")
  
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
  
  onshore <- bdp(equity, c("RTG_DAGONG_LT_LOCAL_CRNCY_ISSUER",
                           "RTG_CHENGXIN_LC_ISSUER",
                           "RTG_LIANHE_LT_LC_ISSUER",
                           "RTG_SBCR_LT_LC_ISSUER",
                           "RTG_CCRC_ISSUER")) %>%
    mutate(equity_ticker = equity,
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
    left_join(ID)
  
write_xlsx(onshore, "onshore_rating.xlsx")

  
}

clean <- function(RTG) {
  
  RTG = gsub("(\\*\\+|\\*|\\*-|\\(|\\)|u|-u|NR|P| |e|WR|WD|\\-1|pi)", "", RTG)
  
}
