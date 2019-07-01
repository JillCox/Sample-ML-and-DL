if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, geosphere, sqldf, odbc, dplyr)

con_staging <- dbConnect(odbc(),
                         Driver = "SQL Server",
                         Server = "freightwaves.ctaqnedkuefm.us-east-2.rds.amazonaws.com", 
                         Database = "Staging",
                         UID = "fwdbmain",
                         PWD = "7AC?Ls9_z3W#@XrR",
                         Port = 1433)

Fleetcomplete <- dbGetQuery(con_staging," SELECT FleetID, PersonID, TruckID, Date, Distance, Cycle, Duration, DutyStatus, OccurredAt, endedat, Lat, Long, zip, zip3
                            FROM dbo.FleetComplete
                            WHERE Date >= DATEADD(day, -7, GETDATE()) and Date <= GETDATE()
                            and PersonID is not NULL
                            and Cycle != 'Canada 70hr/7day'
                            and Distance is not NULL")

#make a separate column for time of occurance
Fleetcomplete$time <- as.character(Fleetcomplete$OccurredAt)

#substring
Fleetcomplete$time <- substring(Fleetcomplete$time, 12, 16)

# make a second duty status to categorize (ON and DR) and (OF and SL)
Fleetcomplete$DutyStatus2 <- ifelse((Fleetcomplete$DutyStatus %in% c('OF', 'SL')), 'OFF', 'Active')

#remove unwanted columns
Fleetcomplete$OccurredAt <- NULL
Fleetcomplete$endedat <- NULL

#binary for driving
Fleetcomplete$DR <- ifelse(Fleetcomplete$DutyStatus == 'DR', 1, 0)

#change column name
names(Fleetcomplete)[names(Fleetcomplete) == 'PersonID'] <- 'PERSONID'

#Partition by with a function
DRcount <- aggregate(Fleetcomplete$DR, by=list(PERSONID = Fleetcomplete$PERSONID, Datetime = Fleetcomplete$Date), FUN=sum)

# join DR count back in
names(DRcount)[names(DRcount) == 'Datetime'] <- 'Date'
Fleetcomplete <- merge(Fleetcomplete, DRcount, by= c("PERSONID", "Date"))
names(Fleetcomplete)[names(Fleetcomplete) == 'x'] <- 'timesdriving'

#separate out duration times for driving to eventually get average
Fleetcomplete$DurDR <- ifelse(Fleetcomplete$DR == 1, Fleetcomplete$Duration, NA)

#remove NAs to be able to average
Avgcalc <- Fleetcomplete[Fleetcomplete$DurDR != 'NA', ]

#Partition by with a function
Avgcalc <- aggregate(Avgcalc$DurDR, by= list(Category = Avgcalc$PERSONID, Date = Avgcalc$Date), FUN= mean)

#change column name
names(Avgcalc)[names(Avgcalc) == 'Category'] <- 'PERSONID'
names(Avgcalc)[names(Avgcalc) == 'x'] <- 'DriveTimeperDay'
#join avg Duration back in
subset <- merge(Fleetcomplete, Avgcalc, by= c('PERSONID', 'Date'))

# week of year column
Avgcalc$week <- week(Avgcalc$Date)

# Partition by with a function, avg driving by week
Avgweek <- aggregate(Avgcalc$DriveTimeperDay, by= list(Avgcalc$PERSONID, Avgcalc$week), FUN= sum)


#easier view
names(DRcount)[names(DRcount) == 'x'] <- 'Timesdriving'
choice <- merge(Avgcalc, DRcount, by= c('PERSONID', 'Date'))

step <-  Fleetcomplete %>% distinct(PERSONID, Date, Distance)

#merge into choice
choice <- merge(choice, step, by= c('PERSONID', 'Date'))
rm(step)

#add a column to calculate miles per hour
choice$mph <- choice$Distance / ((choice$DriveTimeperDay * choice$Timesdriving)/60)

#find first and last lat and lon
first <- Fleetcomplete %>%
  group_by(PERSONID) %>%
  group_by(Date) %>%
  filter(DutyStatus2 == 'Active') %>%
  ungroup()

#table of first lat and lon
first1 <- first %>%
  group_by(PERSONID, Date) %>%
  slice(1) %>%
  ungroup()

first1 <- first1[,c("PERSONID","Date","Lat","Long", "zip3", "FleetID", "TruckID")]
first1$fLat <- first1$Lat; first1$Lat <- NULL
first1$fLong <- first1$Long; first1$Long <- NULL

#table of last lat and lon
last <- first %>%
  group_by(PERSONID, Date) %>%
  slice(n()) %>%
  ungroup()

last <- last[,c("PERSONID","Date","Lat","Long", "zip3")]
last$lLat <- last$Lat; last$Lat <- NULL
last$lLong <- last$Long; last$Long <- NULL
last$lzip <- last$zip3; last$zip3 <- NULL

df <- left_join(first1,last,by = c("PERSONID", "Date"))
rm(first)
rm(first1)
rm(last)

df$Haversine <- ifelse(!is.na(df$fLong) & !is.na(df$fLat) & !is.na(df$lLong) & !is.na(df$lLat), 
                       (distHaversine(cbind(df$fLong, df$fLat), cbind(df$lLong, df$lLat)))* 3.28084, NA )

#remove scientific notation
options(scipen=999)
# make the distance read in miles
df$Haversine <- df$Haversine /5280

#############################################################
## Home Region Calculation
#############################################################

choice$PERSONID <- as.character(choice$PERSONID)
Fleetcomplete$PERSONID <- as.character(Fleetcomplete$PERSONID)

df <- df[,c("PERSONID", "Date", "Haversine", "lzip","zip3")]
df$fzip <- df$zip3; df$zip3 <- NULL
choice <- merge(choice, df, by = c("PERSONID", "Date"))

# create lead of date
choice2 <- choice %>%
  group_by(PERSONID) %>%
  mutate(leadDate =lead(Date))
rm(choice)

#difference between the dates
choice2$diff <- choice2$Date - choice2$leadDate
choice2$Date <- strptime(choice2$Date, format = "%Y-%m-%d", tz="UTC")
choice2$leadDate <- strptime(choice2$leadDate, format = "%Y-%m-%d", tz="UTC")
choice2$diff <- difftime(as.POSIXct(choice2$Date), as.POSIXct(choice2$leadDate), units = "days")

#lead and lag diff
choice2$diff <- as.numeric(choice2$diff)
choice2$Date <- as.Date(choice2$Date)
choice2$leadDate <- as.Date(choice2$leadDate)

choice2 <- choice2 %>%
  group_by(PERSONID) %>%
  mutate(lagdiff = lag(diff))

# find last 3 digit zip before time off
choice2$end <- ifelse(choice2$diff < -1, choice2$lzip, NA)

#find first 3 digit zip when starting after time off
choice2$start <- ifelse(choice2$lagdiff < -1, choice2$fzip, NA)
choice2$lzip <- as.numeric(choice2$lzip)
choice2$fzip <- as.numeric(choice2$fzip)
choice2$end <- as.numeric(choice2$end)
choice2$start <- as.numeric(choice2$start)

# find mode end and mode start for each PERSONID
Mode <- function(x, na.rm=FALSE) {
  if(na.rm) {
    x = x[!is.na(x)]
  }
  ux <- unique(x)
  return(ux[which.max(tabulate(match(x,ux)))])
}

choice3 <- choice2 %>%
  group_by(PERSONID) %>%
  summarise(modeend = Mode(end, na.rm = TRUE), modestart = Mode(start, na.rm=TRUE), modefzip = Mode(fzip, na.rm = TRUE), modelzip = Mode(lzip, na.rm=TRUE))

#collect home zips for drivers
choice3$modeend <- as.character(choice3$modeend)
choice3$modestart <- as.character(choice3$modestart)
choice3$modefzip <- as.character(choice3$modefzip)
choice3$modelzip <- as.character(choice3$modelzip)

choice4 <- sqldf("SELECT PERSONID, 
                 CASE WHEN modelzip = modefzip or modelzip = modestart or modelzip = modeend
                 THEN modelzip
                 WHEN modefzip = modestart or modefzip = modeend
                 THEN modefzip
                 WHEN modestart = modeend
                 THEN modestart
                 ELSE modeend
                 END as 'homezip'
                 FROM choice3
                 WHERE modelzip != 'NA' and modefzip != 'NA' and modestart != 'NA' or modeend != 'NA'")

choice5 <- merge(choice2, choice4, by= 'PERSONID')
rm(choice2)
rm(choice3)
rm(Avgcalc)
rm(df)
rm(DRcount)
rm(subset)

#############################################################
## Driver Type Calculation
#############################################################
# amount of times not driving and days of vacation 
choice5$timesoff <- ifelse(choice5$diff < -1, '1', '0')
choice5$daysoff <- ifelse(choice5$diff < -1, choice5$diff, '0')

#summarize
choice5 <- choice5[,c("PERSONID", "Date", "week", "DriveTimeperDay", "Distance", "mph", "Haversine", "homezip", "timesoff", "daysoff")]

choice5$homezip <- as.numeric(choice5$homezip)
choice5$timesoff <- as.numeric(choice5$timesoff)
choice5$daysoff <- as.numeric(choice5$daysoff)
choice5$DriveTimeperDay <- as.numeric(choice5$DriveTimeperDay)
choice5$Distance <- as.numeric(choice5$Distance)
choice5$mph <- as.numeric(choice5$mph)
choice5$week <- as.numeric(choice5$week)

Driversummary <- choice5 %>%
  group_by(PERSONID) %>%
  summarise(med_Drivetime_between_stops = median(DriveTimeperDay, na.rm=TRUE), maxdistance = max(Distance, na.rm=TRUE),
            medianDistance = median(Distance, na.rm = TRUE), medianmph = median(mph, na.rm= TRUE),
            maxhaversine = max(Haversine, na.rm=TRUE), medhaversine = median(Haversine, na.rm=TRUE),
            sddistance = sd(Distance, na.rm=TRUE), homezip = unique(homezip), timesoff= sum(timesoff, na.rm=TRUE), 
            daysoff = sum(daysoff, na.rm=TRUE), n=n())

##############################################
# Cluster analysis
##############################################

#run a cluster to get an idea of what to cut on
# prepare the data
choice6 <- Driversummary
choice6 <-na.omit(choice6)
Driversummary2 <- na.omit(Driversummary)
choice6 <- choice6[c("med_Drivetime_between_stops", "maxdistance", "medianDistance" , "medianmph", "maxhaversine", "medhaversine", 
                     "sddistance", "homezip", "timesoff", "daysoff")]
choice6 <- scale(choice6)

#Partitioning
fitKmeans <- kmeans(choice6, 3)

#get cluster means
clusters <- aggregate(choice6, by=list(fitKmeans$cluster), FUN=mean)

#append cluster assignment
Driversummary2 <- data.frame(Driversummary2, fitKmeans$cluster)

names(Driversummary2)[names(Driversummary2) == 'fitKmeans.cluster'] <- 'cluster'

#medians by cluster 
try <- Driversummary2 %>%
  group_by(cluster)%>%
  summarise(med_Drivetime_between_stops = median(med_Drivetime_between_stops, na.rm=TRUE), maxdistance = max(maxdistance, na.rm=TRUE),
            medianDistance = median(medianDistance, na.rm = TRUE), medianmph = median(medianmph, na.rm= TRUE),
            maxhaversine = max(maxhaversine, na.rm=TRUE), medhaversine = median(medhaversine, na.rm=TRUE),
            sddistance = sd(sddistance, na.rm=TRUE), medtimesoff= median(timesoff, na.rm=TRUE), 
            meddaysoff = median(daysoff, na.rm=TRUE), n=n())

try2 <- Driversummary2 %>%
  group_by(cluster)%>%
  summarise(med_Drivetime_between_stops = median(med_Drivetime_between_stops, na.rm=TRUE), maxdistance = max(maxdistance, na.rm=TRUE),
            medianDistance = median(medianDistance, na.rm = TRUE), medianmph = median(medianmph, na.rm= TRUE),
            maxhaversine = max(maxhaversine, na.rm=TRUE), medhaversine = median(medhaversine, na.rm=TRUE),
            sddistance = sd(sddistance, na.rm=TRUE), medtimesoff= median(timesoff, na.rm=TRUE), 
            meddaysoff = median(daysoff, na.rm=TRUE), n=n())

#add column for driver type label
try$drivertype <- ifelse(try$medianDistance < 250, 'Local', try$cluster)
try$drivertype <- ifelse(try$medianDistance < 450 & try$medianDistance >= 250, 'Regional', try$drivertype)
try$drivertype <- ifelse(try$medianDistance >= 450, 'OTR', try$drivertype)

#merge labels into Driversummary2
try3 <- try[,c("drivertype", "cluster")]
Driversummary3 <- merge(Driversummary2, try3, by= 'cluster')

rm(Fleetcomplete)
rm(Avgweek)
rm(choice4)
rm(choice5)
rm(choice6)
rm(Driversummary)
#push table to sql
#connection to warehouse
Driversummary3$Date <- Sys.Date()
con_warehouse <- dbConnect(odbc(),
                           Driver = "SQL Server",
                           Server = "freightwaves.ctaqnedkuefm.us-east-2.rds.amazonaws.com",
                           Database = "Warehouse",
                           UID = "fwdbmain",
                           PWD = "7AC?Ls9_z3W#@XrR",
                           Port = 1433)


dbWriteTable(con_warehouse, "DriverClassification", Driversummary3, append = TRUE,row.names = FALSE)
dbDisconnect(con_warehouse)


