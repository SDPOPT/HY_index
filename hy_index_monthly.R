library(tidyverse)
library(RSQLite)
library(readxl)
library(xlsx)
library(dygraphs)
library(DT)
library(xts)
library(formattable)
library(Rblpapi)

blpConnect()

list <- bds("BEUCTRUU Index", "INDX_MEMBERS") %>%
  mutate(id = paste(`Member Ticker and Exchange Code`, "Corp", sep = " ")) %>%
  select(id) %>%
  mutate(country = bdp(id, "CNTRY_OF_RISK")$CNTRY_OF_RISK) %>%
  filter(country == "CN") %>%
  select(id)

credit_mapping <- read_xlsx("credit_mapping.xlsx")
key <- read_xlsx("key.xlsx", col_names = TRUE)
issuer <- read_xlsx("Issuers.xlsx", col_names = TRUE)

pool <- bdp(list$id, key$Key) %>% 
  mutate(id = list$id) %>%
  left_join(issuer) %>%
  select(id, TICKER, SECTOR, RTG_SP, RTG_MOODY, RTG_FITCH, BB_COMPOSITE) %>%
  mutate(MOODY = gsub("(\\*-|\\(|\\)|u|NR|P| |e|WR)", "", RTG_MOODY),
         SP = gsub("(\\*|\\*-|u|NR| )", "", RTG_SP),
         FITCH = gsub("(\\*|\\*-|u|NR|WD| )", "", RTG_FITCH),
         BBG = gsub("(\\*|\\*-|u|NR|WD| )", "", BB_COMPOSITE)) %>%
  gather(`MOODY`, `SP`, `FITCH`, `BBG`,
         key = "agent", value = "RTG") %>%
  left_join(credit_mapping) %>%
  group_by(id, TICKER, SECTOR) %>%
  summarise(credit = round(mean(credit, na.rm = TRUE),
                           digits = 0)) %>%
  mutate(credit = na.fill(credit, 0)) %>%
  ungroup() %>%
  group_by(TICKER, SECTOR) %>%
  summarise(credit = max(credit, na.rm = TRUE)) %>%
  mutate(SECTOR = ifelse(credit >= 21, "IG", SECTOR)) %>%
  
  pool <- pool %>% ungroup()

write.xlsx(pool, "pool.xlsx")

issuer <- read_xlsx("pool.xlsx")

ticker <- bsrch("FI:test_3") %>% mutate(id = as.character(id))

bond <- bdp(ticker$id, key$Key) %>%
  mutate(id = ticker$id) %>%
  left_join(issuer)

weight1 <- function(bond) {

weight <- bond %>%
  mutate(Cap = YAS_BOND_PX * AMT_OUTSTANDING / 100) %>%
  filter(is.na(Cap) != TRUE) %>%
  select(id, TICKER, Cap, SECTOR) %>%
  mutate(weight0 = Cap / sum(Cap)) %>%
  group_by(SECTOR, TICKER) %>%
  summarise(Cap = sum(Cap),
            weight = sum(weight0)) %>%
  arrange(desc(weight)) %>%
  ungroup()

while(max(weight$weight) > 0.02){
  weight <-  weight %>% 
    mutate(weight0 = pmin(weight, 0.02),
           weight1 = weight - weight0,
           weight2 = sum(weight1),
           weight3 = ifelse(weight0 == 0.02, 0, 1)) %>%
    group_by(weight3) %>%
    mutate(weight4 = ifelse(weight0 == 0.02, weight0, 
                            Cap / sum(Cap) * weight2 + weight0)) %>%
    ungroup() %>%
    select(SECTOR, TICKER, Cap, weight = weight4)
}
  
weight <- weight %>% select(SECTOR, TICKER,
                            total_Cap = Cap,
                            total_weight = weight)
  
}


bond1 <- bond %>% left_join(weight1(bond)) %>%
  mutate(weight = YAS_BOND_PX * AMT_OUTSTANDING / 
           100 / total_Cap * total_weight) %>%
  left_join(read_xlsx("mapping_credit.xlsx", col_names = TRUE)) %>%
  select(TICKER, CPN, MATURITY, 
         SECTOR, RTG, weight, 
         YAS_BOND_PX, YAS_BOND_YLD, YAS_MOD_DUR,
         total_Cap, total_weight,
         INDUSTRY_GROUP, id)

write.xlsx(bond1, "bond1.xlsx")
