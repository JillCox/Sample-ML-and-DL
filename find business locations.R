
pacman::p_load(tidyverse, odbc, googleway, googlesheets, sqldf)

api_key <- 'AIzaSyBUKxjAyTZ1RhU7ZKlBaK9q-RlytLQ4GUU'


#which google sheets do you have access to?
gs_ls()

# get the google sheet that we want
BusinessList <- gs_title("New List Try")

# which sheets exist in the spredsheet
gs_ws_ls(BusinessList)

#download the sheet you want
BusinessList <- gs_read(ss=BusinessList, ws = "Sheet1")

# convert to a data frame
BusinessList <- as.data.frame(BusinessList)



#cycle through each state, each company
for (row in 1:nrow(BusinessList)) { 
  Company <- BusinessList[row, "Company"]
  State <- BusinessList[row, "State"]

# find locations within a state - this gives twenty locations 
home_depot <- google_places(search_string = paste(Company, State), key = api_key)

homedepot <- data.frame('formatted_address' = home_depot$results[1], 'name' = home_depot$results[5], 'place_id' =home_depot$results[8], 
                        'lat' = home_depot$results$geometry$location[1], 'long' = home_depot$results$geometry$location[2])

# get next twenty locations. 
home_depot_next <- google_places(search_string = paste(Company, State), page_token = home_depot$next_page_token, key = api_key)
homedepotnext <- data.frame('formatted_address' = home_depot$results[1], 'name' = home_depot$results[5], 'place_id' =home_depot$results[8], 
                            'lat' = home_depot$results$geometry$location[1], 'long' = home_depot$results$geometry$location[2])
# homedepotnext <- homedepotnext[, c("formatted_address", "name")]
# homedepotnext <- homedepotnext %>% select(formatted_address,name)

#rbind the two together
home_depot_try <- rbind(homedepot, homedepotnext)

#40
#home_depot_next3 <- google_places(search_string = paste(Company, State), page_token = home_depot_next2$next_page_token, key = api_key)
#homedepotnext3 <- data.frame('formatted_address' = home_depot_next3$results[1], 'name' = home_depot_next3$results[5])
# homedepotnext3 <- homedepotnext3[, c("formatted_address", "name")]
# homedepotnext3 <- homedepotnext3 %>% select(formatted_address,name)

#home_depot_try <- rbind(home_depot_try, homedepotnext3)

message(paste('Finished Row', row))
#insert into table


con_warehouse <- dbConnect(odbc(),
                           Driver = "SQL Server",
                           Server = "freightwaves.ctaqnedkuefm.us-east-2.rds.amazonaws.com",
                           Database = "Warehouse",
                           UID = "fwdbmain",
                           PWD = "7AC?Ls9_z3W#@XrR",
                           Port = 1433)

home_depot_try <- sqldf("SELECT DISTINCT * FROM home_depot_try")
dbWriteTable(con_warehouse, "Google_Businesses", home_depot_try, append = TRUE,row.names = FALSE)
dbDisconnect(con_warehouse)
}




