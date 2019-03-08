library(tidyverse)
library(RQuantLib)
library(RSQLite)
library(readxl)
library(xlsx)
library(dygraphs)
library(DT)
library(xts)
library(formattable)

db <- dbConnect(SQLite(), "HY_index.sqlite")

info <- dbGetQuery(db, "SELECT * FROM Info") %>%
  mutate(maturity = as.Date(MATURITY)) %>%
  select(ID, ticker = TICKER, name = SECURITY_SHORT_DES, 
         coupon = CPN, maturity, amount = AMT_ISSUED)

data <- dbGetQuery(db, "SELECT * FROM hist_data") %>%
  mutate(date = as.Date(date)) %>%
  select(date, ID, price = PX_MID) %>%
  left_join(info) %>%
  filter(maturity > date) %>%
  mutate(busday = isBusinessDay("HongKong", date)) %>%
  filter(busday == "TRUE") %>%
  mutate(monthend = endOfMonth(calendar = "HongKong", dates = date))

monthend <- data %>% 
  group_by(date, ID) %>%
  filter(date == monthend) %>%
  mutate(fraction = yearFraction(date, maturity, 6)) %>%
  ungroup()

issuer_weight <- weight(monthend)

monthbegin <- monthend %>%
  mutate(monthbegin = monthend) %>%
  select(monthbegin, ID, price0 = price)

monthend <- monthend %>% 
  left_join(issuer_weight) %>%
  mutate(bond_weight = ifelse(fraction <= 0.5, 0,
                              (price * amount / 100) / Cap) * weight) %>%
  select(date, ID, weight = bond_weight)


return_index <- data %>%
  left_join(monthend) %>%
  group_by(ID) %>%
  arrange(-desc(date)) %>%
  mutate(weight = lag(weight),
         count = ifelse(is.na(weight) == TRUE, 0, 1),
         count = cumsum(count)) %>%
  filter(count > 0) %>%
  mutate(weight = na.locf(weight)) %>%
  ungroup() %>% select(-count) %>%
  mutate(monthbegin = endOfMonth(calendar = "HongKong", dates = monthend - 32)) %>%
  left_join(monthbegin) %>%
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
  

monthend <- return_index %>%
  filter(date == monthend) %>%
  mutate(return0 = cumprod(return)) %>%
  select(date, return0)
  
return_index <- return_index %>%
  left_join(monthend) %>%
  mutate(return0 = c(1, return0[1 : (NROW(return0) - 1)]),
         return0 = na.locf(return0)) %>%
  mutate(index = return * return0)


weight <- function(data) {
  
  weight <- data %>%
    ungroup() %>%
    group_by(date) %>%
    mutate(Cap = price * amount / 100,
           Cap = ifelse(fraction <= 0.5, 0, Cap)) %>%
    select(date, ticker, Cap) %>%
    mutate(weight0 = Cap / sum(Cap)) %>%
    ungroup() %>%
    group_by(date, ticker) %>%
    summarise(Cap = sum(Cap),
              weight = sum(weight0)) %>%
    arrange(desc(weight)) %>%
    ungroup()
  
  while(max(weight$weight) > 0.02){
    weight <-  weight %>% 
      group_by(date) %>%
      mutate(weight0 = pmin(weight, 0.02),
             weight1 = weight - weight0,
             weight2 = sum(weight1),
             weight3 = ifelse(weight0 == 0.02, 0, 1)) %>%
      ungroup() %>% 
      group_by(date, weight3) %>%
      mutate(weight4 = ifelse(weight0 == 0.02, weight0, 
                              Cap / sum(Cap) * weight2 + weight0)) %>%
      ungroup() %>%
      select(date, ticker, Cap, weight = weight4)
  }
  
  return(weight)
}

