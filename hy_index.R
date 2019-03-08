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

test <- monthend %>% 
  left_join(issuer_weight) %>%
  mutate(bond_weight = ifelse(fraction <= 0.5, 0,
                              (price * amount / 100) / Cap) * weight)
  


index_return <- function(db, date1, date2) {

date1 <- as.Date(date1)
date2 <- as.Date(date2)

data <- dbGetQuery(db, sprintf("SELECT date, ID, PX_MID 
                                   FROM hist_data 
                                   WHERE date(date) 
                                   BETWEEN date('%s') 
                                   AND date('%s')", 
                                  date1 - 1, date2)) %>%
  mutate(date = as.Date(date)) %>%
  filter(is.na(PX_MID) == FALSE)

data1 <- data %>% 
  filter(date >= date1) %>% 
  mutate(price1 = PX_MID) %>%
  select(-PX_MID)

data0 <- data %>%
  filter(date < date2) %>%
  mutate(date = date + 1,
         price0 = PX_MID) %>%
  select(-PX_MID)

data <- data0 %>% 
  left_join(data1) %>%
  filter(is.na(price1) == FALSE)

bond <- bond_pool(db) %>% select(ID, Coupon, Issued)

data <- data %>% left_join(bond) %>% 
  group_by(ID) %>%
  mutate(date0 = date[1],
         date1 = date[length(date)]) %>%
  ungroup()

trade <- as.tibble(c(sort(unique(data$date0)),
                     sort(unique(data$date1))))
names(trade) <- "date"
trade <- trade %>% 
  filter(duplicated(date) == FALSE) %>%
  filter(date != date2) %>%
  arrange(-desc(date)) %>% 
  mutate(trade = 1, tradeID = c(1 : length(trade)))

data <- data %>% 
  left_join(trade) %>% 
  arrange(-desc(date)) %>%
  group_by(ID) %>%
  mutate(value = na.locf(Issued * price0 * trade),
         tradeID = na.locf(tradeID)) %>%
  select(-date0, -date1, -trade) %>% ungroup()

return <- data %>%
  mutate(interest = Coupon / 365) %>%
  group_by(ID, tradeID) %>%
  arrange(-desc(date)) %>%
  mutate(price_return = price1 / price0[1], 
         interest_return = interest / price0[1]) %>% ungroup() %>%
  group_by(date) %>%
  mutate(weight = value / sum(value)) %>%
  summarise(price_return = sum(price_return * weight),
            interest_return = sum(interest_return * weight)) %>%
  ungroup() %>% left_join(trade) %>%
  mutate(tradeID = na.locf(tradeID)) %>%
  group_by(tradeID) %>%
  mutate(interest_return = cumsum(interest_return),
         total_return = price_return + interest_return) %>%
  ungroup() %>%
  mutate(trade = c(trade[2 : length(trade)], NA),
         price = cumprod(ifelse(is.na(trade) == TRUE, 1, price_return)),
         total = cumprod(ifelse(is.na(trade) == TRUE, 1, total_return))) %>%
  mutate(price = na.locf(c(1, price[1 : length(price) - 1])),
         total = na.locf(c(1, total[1 : length(total) - 1]))) %>%
  mutate(price_return = price_return * price,
         total_return = total_return * total) %>%
  select(date, price_return, total_return)

  return(return)
}


index_return_plot <- function(return) {
price_return <- xts(x = return$price_return * 100, 
                    order.by = return$date)
total_return <- xts(x = return$total_return * 100,
                    order.by = return$date)

result <- cbind(price_return, total_return)
names(result) <- c("price return", "total return")

dygraph(result, main = "China US HY Index", 
        group = "result") %>% dyRangeSelector()

}

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

