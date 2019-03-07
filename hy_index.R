library(tidyverse)
library(RSQLite)
library(readxl)
library(xlsx)
library(dygraphs)
library(DT)
library(xts)
library(formattable)

db <- dbConnect(SQLite(), "hy-property.sqlite")

bond_pool <- function(db) {
  
Bond <- dbGetQuery(db, 'SELECT * FROM Info') %>%
  mutate(`Moody's` = gsub("(\\*-|\\(|\\)|u|NR|P| |e|WR)", "", RTG_MOODY),
         `S&P` = gsub("(\\*-|u|NR| )", "", RTG_SP),
         Amount = AMT_OUTSTANDING / 1000000,
         Issued = AMT_ISSUED / 1000000,
         Maturity = as.Date(MATURITY),
         `1st Call Date` = as.Date(FIRST_CALL_DT_ISSUANCE),
         `Called Date` = as.Date(CALLED_DT)) %>%
  select(ID, Name = SECURITY_SHORT_DES,
         Ticker = TICKER, Coupon = CPN,
         Maturity,  `1st Call Date`, `Called Date`,
         Issued, Amount, `S&P`, `Moody's`) %>%
  arrange(-desc(Ticker), -desc(Maturity))
 
return(Bond)
}

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

index_spread <- function(db, date1, date2) {
  
  date1 <- as.Date(date1)
  date2 <- as.Date(date2)
  
  data <- dbGetQuery(db, sprintf("SELECT date, ID, Z_SPRD_MID 
                                    FROM hist_data 
                                    WHERE date(date) 
                                    BETWEEN date('%s') 
                                    AND date('%s')", 
                                    date1 - 1, date2)) %>%
    mutate(date = as.Date(date)) %>%
    filter(is.na(Z_SPRD_MID) == FALSE)

  bond <- bond_pool(db) %>% select(ID, Issued)
  
  spread <- data %>%
    left_join(bond) %>%
    group_by(date) %>%
    mutate(weight = Issued / sum(Issued)) %>%
    summarise(spread = sum(Z_SPRD_MID * weight))
  
  return(spread)
  
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

index_spread_plot <- function(data) {
  
  Spread <- xts(x = data$spread,
                order.by = data$date)
  names(Spread) <- "Spread"
  
  dygraph(Spread, main = "China HY Property Spread") %>% dyRangeSelector()
}

index_perform <- function(db, date1, date2) {

date1 <- as.Date(date1)
date2 <- as.Date(date2)

Bond <- bond_pool(db) %>%
  filter(Maturity >= date2)

data1 <- dbGetQuery(db, sprintf("SELECT date, ID, PX_MID, Z_SPRD_MID, DUR_ADJ_OAS_BID, YLD_YTM_MID 
                                   FROM hist_data 
                                   WHERE date = '%s'", date2)) %>% 
  mutate(Price = PX_MID,
         Spread = Z_SPRD_MID,
         Yield = YLD_YTM_MID,
         Duration = DUR_ADJ_OAS_BID) %>%
  select(ID, Price, Spread, Yield, Duration)

data0 <- dbGetQuery(sovdb, sprintf("SELECT date, ID, PX_MID, Z_SPRD_MID 
                                  FROM hist_data 
                                  WHERE date = '%s'", date1)) %>%
  mutate(price = PX_MID,
         spread = Z_SPRD_MID) %>%
  select(ID, price, spread)


period <- as.numeric(date2 - date1) + 1

Bond <- Bond %>% left_join(data1) %>%
  left_join(data0) %>%
  mutate(`Price%` = (Price / price - 1) * 100,
         `Spread bps` = Spread - spread,
         `Total%` = (Price / price - 1) * 100 + Coupon / price / 365 * period * 100) %>%
  filter(is.na(`Price%` ) == FALSE) %>%
  select(Name, `S&P`, `Moody's`, Price, Yield, Spread, Duration, `Price%`, `Total%`, `Spread bps`) %>%
  mutate(Price = round(Price, digits = 3),
         Yield = round(Yield, digits = 3),
         Spread = round(Spread, digits = 0),
         Duration = round(Duration, digits = 1),
         `Price%` = round(`Price%`, digits = 2),
         `Total%` = round(`Total%`, digits = 2),
         `Spread bps` = round(`Spread bps`, digits = 0))

return(Bond)

}
