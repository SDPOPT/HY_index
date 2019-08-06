library(tidyverse)
library(RQuantLib)
library(RSQLite)
library(readxl)
library(xlsx)
library(dygraphs)
library(DT)
library(xts)
library(formattable)
library(Rblpapi)
blpConnect()

weight <- function(data, limit) {
  
  data <- data %>%
    filter(fraction <= 2)
  
  weight <- data %>%
    ungroup() %>%
    group_by(date) %>%
    mutate(Cap = price * amount / 100) %>%
    select(date, ticker, Cap) %>%
    mutate(weight0 = Cap / sum(Cap)) %>%
    ungroup() %>%
    group_by(date, ticker) %>%
    summarise(Cap = sum(Cap),
              weight = sum(weight0)) %>%
    arrange(desc(weight)) %>%
    ungroup()
  
  while(max(weight$weight) > limit){
    weight <-  weight %>% 
      group_by(date) %>%
      mutate(weight0 = pmin(weight, limit),
             weight1 = weight - weight0,
             weight2 = sum(weight1),
             weight3 = ifelse(weight0 == limit, 0, 1)) %>%
      ungroup() %>% 
      group_by(date, weight3) %>%
      mutate(weight4 = ifelse(weight0 == limit, weight0, 
                              Cap / sum(Cap) * weight2 + weight0)) %>%
      ungroup() %>%
      select(date, ticker, Cap, weight = weight4)
  }
  
  return(weight)
}

data <- function() {

db <- dbConnect(SQLite(), "HY_index.sqlite")
issuer_china <- dbReadTable(db, "issuer_china")
issuer_nonchina <- dbReadTable(db, "issuer_nonchina")
issuer <- rbind(issuer_china, issuer_nonchina)
credit_mapping <- dbReadTable(db, "credit_mapping")
mapping_credti <- dbReadTable(db, "mapping_credit")
onshore_credit_mapping <- dbReadTable(db, "onshore_credit_mapping")
onshore_rating <- dbReadTable(db, "onshore_rating")


info <- dbGetQuery(db, "SELECT * FROM Info") %>%
  mutate(maturity = as.Date(MATURITY)) %>%
  select(ID, ticker = TICKER, name = SECURITY_SHORT_DES, 
         coupon = CPN, amount = AMT_OUTSTANDING)

data <- bdp(info$ID, c("MATURITY", "YAS_BOND_YLD", "YAS_BOND_PX")) %>%
  mutate(ID = info$ID,
         maturity = MATURITY,
         yield = YAS_BOND_YLD,
         price = YAS_BOND_PX) %>%
  select(ID, price, yield, maturity)

info <- info %>%
  left_join(data) %>%
  filter(yield >= 4) %>%
  left_join()

data <- dbGetQuery(db, "SELECT * FROM hist_data") %>%
  mutate(date = as.Date(date)) %>%
  select(date, ID, price = PX_MID) %>%
  left_join(info) %>%
  filter(maturity > date) %>%
  mutate(busday = isBusinessDay("HongKong", date)) %>%
  filter(busday == "TRUE") %>%
  mutate(monthend = endOfMonth(calendar = "HongKong", dates = date))
}

monthend<- function(data) {

monthend <- data %>% 
  group_by(date, ID) %>%
  filter(date == monthend) %>%
  mutate(fraction = yearFraction(date, maturity, 6)) %>%
  ungroup() 

}

monthbegin <- function(data) {

monthbegin <- data %>%
  mutate(monthbegin = monthend) %>%
  select(monthbegin, ID, price0 = price)

}

monthend_weight <- function(data) {
  
monthend_weight <- data %>% 
  left_join(weight(data, 0.05)) %>%
  mutate(bond_weight = ifelse(fraction > 2, 0,
           (price * amount / 100) / Cap * weight)) %>%
  select(date, ID, weight = bond_weight)

}

index_return <- function() {

return_index <- data() %>%
  left_join(monthend_weight()) %>%
  group_by(ID) %>%
  arrange(-desc(date)) %>%
  mutate(weight = lag(weight),
         count = ifelse(is.na(weight) == TRUE, 0, 1),
         count = cumsum(count)) %>%
  filter(count > 0) %>%
  mutate(weight = na.locf(weight)) %>%
  ungroup() %>% select(-count) %>%
  mutate(monthbegin = endOfMonth(calendar = "HongKong", dates = monthend - 32)) %>%
  left_join(monthbegin()) %>%
  ungroup() %>%
  group_by(ID) %>%
  mutate(date0 = rep(monthbegin[1], times = NROW(date))) %>%
  group_by(ID, date) %>%
  mutate(fraction = yearFraction(date0, date, 6),
         interest = coupon * fraction,
         value0 = ifelse(date == monthend, price + interest, NA)) %>%
  ungroup() %>%
  group_by(ID) %>%
  arrange(-desc(date)) %>%
  mutate(value0 = c(price0[1], value0[1 : (NROW(value0) - 1)]),
         value0 = na.locf(value0),
         return = (price + interest) / value0) %>%
  ungroup() %>%
  group_by(monthend, date) %>%
  summarise(return = sum(weight * return)) %>%
  ungroup()
  
monthend_return <- return_index %>%
  filter(date == monthend) %>%
  mutate(return0 = cumprod(return)) %>%
  select(date, return0)
  
return_index <- return_index %>%
  left_join(monthend_return) %>%
  mutate(return0 = c(1, return0[1 : (NROW(return0) - 1)]),
         return0 = na.locf(return0)) %>%
  mutate(index = return * return0)

}

weight_old <- function(data, limit) {
  
  weight <- data %>%
    mutate(Cap = price * amount / 100) %>%
    mutate(weight0 = Cap / sum(Cap)) %>%
    group_by(ticker) %>%
    summarise(Cap = sum(Cap),
              weight = sum(weight0)) %>%
    arrange(desc(weight)) %>%
    ungroup()
  
  while(max(weight$weight) > limit){
    weight <-  weight %>% 
      mutate(weight0 = pmin(weight, limit),
             weight1 = weight - weight0,
             weight2 = sum(weight1),
             weight3 = ifelse(weight0 == limit, 0, 1)) %>%
      ungroup() %>% 
      group_by(weight3) %>%
      mutate(weight4 = ifelse(weight0 == limit, weight0, 
                              Cap / sum(Cap) * weight2 + weight0)) %>%
      ungroup() %>%
      select(ticker, Cap, weight = weight4)
  }
  
  return(weight)
}

port <- function() {
  
db <- dbConnect(SQLite(), "HY_index.sqlite")

property <- monthend(data()) %>%
  left_join(read_xlsx("ticker_pool.xlsx")) %>%
  filter(sector == "PROPERTY")

other <- monthend(data()) %>%
  left_join(read_xlsx("ticker_pool.xlsx")) %>%
  filter(sector != "PROPERTY")

weight_property <- monthend_weight(property) %>% 
  filter(date == as.Date("2019/3/29")) %>%
  filter(weight > 0)

weight_other <- monthend_weight(other) %>% 
  filter(date == as.Date("2019/3/29")) %>%
  filter(weight > 0)

port_weight <- rbind(weight_property, weight_other) %>%
  mutate(weight = weight * 0.5)

portfolio <- bdp(port_weight$ID, c("SECURITY_SHORT_DES", "TICKER", 
                              "MATURITY", "YAS_BOND_PX",
                              "YAS_BOND_YLD", "YAS_MOD_DUR")) %>%
  mutate(ID = port_weight$ID) %>%
  mutate(name = SECURITY_SHORT_DES,
         ticker = TICKER,
         price = digits(YAS_BOND_PX, 2),
         yield = percent(YAS_BOND_YLD / 100, digits = 2),
         duration = digits(YAS_MOD_DUR, 1)) %>% 
  left_join(port_weight) %>%
  mutate(weight = percent(weight, digits = 2)) %>%
  left_join(dbGetQuery(db, "SELECT * FROM onshore_rating")) %>%
  mutate(onshore_rating = RTG) %>%
  select(ID, ticker, name, weight, price, yield, duration, onshore_rating) %>%
  left_join(read_xlsx("ticker_pool.xlsx")) %>%
  mutate(Credit = as.numeric(min)) %>%
  left_join(dbGetQuery(db, "SELECT * FROM mapping_credit")) %>%
  mutate(rating = ifelse(is.infinite(Credit) == TRUE, "unrated", RTG)) %>%
  select(ID, ticker, name, weight, sector, price, yield, duration, rating, onshore_rating)

rating <- portfolio %>% 
  group_by(rating) %>% 
  summarise(weight = sum(weight))

issuer <- portfolio %>%
  group_by(ticker) %>%
  summarise(weight = sum(weight))

sector <- portfolio %>%
  group_by(sector) %>%
  summarise(weight = sum(weight))

summary <- portfolio %>%
  summarise(yield = sum(yield * weight),
            duration = sum(duration * weight),
            rating = sum(credit * weight))

onshore_rating <- portfolio %>% 
  group_by(RTG) %>% 
  summarise(weight = sum(weight))

}

