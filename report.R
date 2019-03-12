source("hy_index.R")

weight_chg <- info %>%
  left_join(monthend_weight) %>%
  select(-coupon, -maturity) %>%
  filter(is.na(date) == FALSE) %>%
  spread(key = date, value = weight)

weight_chg <- na.fill(weight_chg, 0)

data <- dbGetQuery(db, "SELECT * FROM hist_data") %>%
  mutate(date = as.Date(date)) %>%
  select(ID, date, price = PX_MID, yield = YLD_YTM_MID, 
         spread = Z_SPRD_MID, duration = DUR_ADJ_OAS_BID)

index_comp <- monthend_weight %>%
  left_join(info) %>%
  left_join(data)

index_stat1 <- index_comp %>%
  group_by(date) %>%
  summarise(yield = percent(sum(yield * weight, na.rm = TRUE) / 100),
            spread = round(sum(spread * weight, na.rm = TRUE), digits = 0),
            duration = round(sum(duration * weight, na.rm = TRUE), digits = 2))

index_stat2 <- index_comp %>%
  group_by(date, ticker) %>%
  summarise(weight = percent(sum(weight))) %>%
  spread(key = date, value = weight)

index_stat2 <- na.fill(index_stat2, "")

index_stat3 <- index_comp %>%
  left_join(pool) %>%
  group_by(date, sector) %>%
  summarise(weight = percent(sum(weight, na.rm = TRUE))) %>%
  spread(key = date, value = weight)

index_stat <- createWorkbook() 
sheet1 <- createSheet(index_stat, sheetName = "Sheet1")  
sheet2 <- createSheet(index_stat, sheetName = "Sheet2")
sheet3 <- createSheet(index_stat, sheetName = "Sheet3")  

addDataFrame(index_stat1, sheet1)
addDataFrame(index_stat2, sheet2)
addDataFrame(index_stat3, sheet3)

saveWorkbook(index_stat, "index_stat.xlsx")

return <- xts(x = return_index$index * 100,
              order.by = return_index$date)
names(return) <- "index value"
dygraph(return, main = "China USD HY index") %>% dyRangeSelector()
