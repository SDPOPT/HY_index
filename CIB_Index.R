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


data <- function() {

db <- dbConnect(SQLite(), "HY_index.sqlite")
issuer_china <- dbReadTable(db, "issuer_china")
issuer_nonchina <- dbReadTable(db, "issuer_nonchina")
issuer <- rbind(issuer_china, issuer_nonchina)
credit_mapping <- dbReadTable(db, "credit_mapping")
mapping_credit <- dbReadTable(db, "mapping_credit")
onshore_credit_mapping <- dbReadTable(db, "onshore_credit_mapping")
onshore_rating <- dbReadTable(db, "onshore_rating")

info <- dbGetQuery(db, "SELECT * FROM Info") %>%
  mutate(maturity = as.Date(MATURITY)) %>%
  select(ID, ticker = TICKER, name = SECURITY_SHORT_DES, 
         coupon = CPN, amount = AMT_OUTSTANDING)

data <- bdp(info$ID, c("MATURITY", "YAS_BOND_YLD", "YAS_BOND_PX", "YAS_MOD_DUR")) %>%
  mutate(ID = info$ID,
         maturity = MATURITY,
         yield = YAS_BOND_YLD,
         price = YAS_BOND_PX,
         duration = YAS_MOD_DUR) %>%
  select(ID, price, yield, maturity, duration)

rating <- bdp(info$ID, c("TICKER", "RTG_SP_NO_WATCH", 
                         "RTG_MOODY_NO_WATCH", "RTG_FITCH_NO_WATCH")) %>%
  mutate(ID = info$ID,
         ticker = TICKER,
         SP = clean(RTG_SP_NO_WATCH),
         MOODY = clean(RTG_MOODY_NO_WATCH),
         FITCH = clean(RTG_FITCH_NO_WATCH)) %>%
  select(ID, ticker, SP, MOODY, FITCH) %>%
  gather(`MOODY`, `SP`, `FITCH`,
         key = "agent", value = "RTG") %>%
  left_join(credit_mapping) %>%
  group_by(ticker) %>%
  summarise(Credit = na.fill(round(mean(Credit, na.rm = TRUE), digits = 0), 15)) %>%
  ungroup() %>%
  left_join(mapping_credit)

onshore_rating <- onshore_rating %>%
  left_join(onshore_credit_mapping) %>%
  mutate(RTG_onshore = RTG,
         Credit_onshore = Credit) %>% 
  select(ticker, RTG_onshore, Credit_onshore)
  

info <- info %>%
  left_join(data) %>%
  filter(yield >= 4) %>%
  left_join(issuer) %>%
  left_join(rating) %>%
  left_join(onshore_rating) %>%
  mutate(RTG_onshore = na.fill(RTG_onshore, "unrated"),
         Credit_onshore = na.fill(Credit_onshore, 0))


return(info)

}

weight <- function(data, limit) {
  
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

port_weight <- function(port) {

property <- port %>%
  filter(sector == "PROPERTY")

nonchina <- port %>%
  filter(sector == "NON CHINA")

other <- port %>%
  filter(sector != "PROPERTY") %>%
  filter(sector != "NON CHINA")

property <- weight(property, 0.02 / 0.5) %>%
  mutate(weight = weight * 0.5)

nonchina <- weight(nonchina, 0.02 / 0.15) %>%
  mutate(weight = weight * 0.15)

other <- weight(other, 0.02 / 0.35) %>%
  mutate(weight = weight * 0.35)

port_weight <- rbind(property, nonchina, other)

return(port_weight)

}

port_report <- function() {
  
  db <- dbConnect(SQLite(), "HY_index.sqlite")
  
  port <- data() %>%
    filter(ticker != "CHIWIN") %>%
    filter(ticker != "ROADKG")
  
  weight_port <- port_weight(port)
  
  port <- port %>%
    filter(sector != "PROPERTY") %>%
    left_join(weight_port) %>%
    group_by(ticker) %>%
    filter(yield == max(yield, na.rm = TRUE)) %>%
    mutate(weight = round(weight * 200, digits = 1) / 100) %>%
    select(-Cap) %>%
    ungroup()
  
  issuer <- port %>%
    group_by(sector, ticker) %>%
    summarise(weight = sum(weight))
  
  rating <- port %>%
    group_by(RTG, Credit) %>%
    summarise(weight = sum(weight))
  
  onshore_rating <- port %>%
    group_by(RTG_onshore, Credit_onshore) %>%
    summarise(weight = sum(weight))
  
  sector <- port %>%
    group_by(sector) %>%
    summarise(weight = sum(weight))
  
  port_summary <- port %>%
    summarise(yield = sum(weight * yield, na.rm = TRUE),
              duration = sum(weight * duration, na.rm = TRUE),
              Credit = round(sum(weight * Credit, na.rm = TRUE), digits = 0)) %>%
    left_join(dbReadTable(db, "mapping_credit"))
  
  
  write.xlsx(port, "CIB_Index.xlsx", sheetName = "port")
  write.xlsx(port_summary, "CIB_Index.xlsx", sheetName = "summary", append = TRUE)
  
  
}

clean <- function(RTG) {
  
  RTG = gsub("(\\*\\+|\\*|\\*-|\\(|\\)|u|-u|NR|P| |e|WR|WD|\\-1|pi)", "", RTG)
  
}


