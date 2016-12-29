## this script is to count number of passengers who has boarded/alighted a tram at every station
## updated on 2016-05-06. Worked on the data for 1st Oct 2014 only
## updated on 2016-05-06. worked on ticket with ID. 
## updated on 2016-05-10. identified the service number and the arrival time of the tram which each ticket alighted/boarded
## updated on 2016-05-10. for the data of 2014-10-01, record with ID, there are 4526 records cannot identify their tram 
#### they are records of: 
#### 1) tap in/out from different directions -- 4196 trips 
######## it is possible that passenger tap-out/in from the opposite station--ignore 
#### 2) tap in/out from the same station -- 247 trips
#### 3) tap-out station is nearer than tap-in station -- 83 trips
## updated on 2016-05-12. for the O-D pairs that can assign to a specific tram, there are 27 tap-out records were paired with duplicate
## entree record. removed by keeping the pair with a later entree
## updated on 2016-05-12. all the possible O-D pair by established assuming that the passenger needs to tap-out when they change route. 
## updated on 2016-05-13. extract the leaving time of the tram at the exit station. the next question is, whether we should assign the
### passenger to the earlier tram or the latter tram
## updated on 2016-05-13. assigned the passenger to the earliest tram, the latter tram, and the shortest travel time tram.
### tested on two more conditions to verify the O-D pair
## updated on 2016-05-16. record down the id of the tram record which the passenger alighted.
## updated on 2016-05-25, ignored the direction to build OD pair.
#### 1) in total 59691 pairs
#### 2) 393 tapping-in and out from the same station
#### 3) created all possible O-D pair, and found 7 pairs out of 59691 not feasible.
## updated on 2016-05-27. correct the regular expression to extract the station name only, ignoring the direction part
## updated on 2016-05-27. pay attention to the Dthr_operation and datetime. there could be 1 hour difference
## updated on 2016-06-01. calculate the duration from tapping-in to tapping-out 
## updated on 2016-06-02. v7_1 selected the lastest possible route based on tapping-out time. 
## updated on 2016-06-06. v7_2 added the part to calculate the waiting time. 
## updated on 2016-06-15. v8 part of it is modified to work on cloud to look into get the data to look into tram frequency and tram delay duration
## updated on 2016-06-23. v9 modified to work on cloud to look into ticket validation number on normal days and incident day.
## updated on 2016-06-27. on hold.. need to shift to plot tram delayed duration VS. tram actual leaving time
## updated on 2016-06-30. Continue with ticket validation data
## updated on 2016-06-30. ggplot2 to display ONLY HH:MM on the x axis
## updated on 2016-07-05. PostgreSQL. deleted duplicated records in the database.
## updated on 2016-07-07. v10. removed the counting number of tap-in/out part, and started to build OD pair
## updated on 2016-07-08. v10_1. extend the tram stopping time at the platform from 1 minute to 1.5 minute. 
## updated on 2016-07-11. v10_2. changed the tram stopping time to 1 minutes.
## updated on 2016-07-11. v10_2. select the passengers who cannot find the feasible latest tram and assign them to the earliest possible tram.
## updated on 2016-07-11. v10_2. found some problem in findding the leaving time at the origin station. 
## updated on 2016-07-12. v10_3. drop the assumption that the tram stopping time is 1 minut
## updated on 2016-07-12. v10_3. and continue to debug the finding tram leaving time at the origin station.
## updated on 2016-07-13. v10_4. create a array with all incident date and make the processing automatic
## updated on 2016-07-13. v10_4. the "automatic" still in progress
## updated on 2016-07-22. v10_6. identify the trip_id first in another function and then look for the tram leaving time at the origian station.
## updated on 2016-07-27. v10_7. corrected the part to select the earliest or the latest tram.
### if we can find a feasible tram before the tapout time, we select the latest
### else, we select the earliest feasible tram after the tapout time.
## updated on 2016-07-29. v10_8. commented some codes to run "code profiling".
## updated on 2016-08-02. v10_9. only keep uncessary columns to speed up R code
## updated on 2016-08-02. v10_10. trying to optimize the code
### combine the part to find the lastest/earliest tram with the part finding the leaving time of the tram at the origin station
## modified on 2016-11-14. v10_11. assigned the passenger to the earliest possible tram
## modified on 2016-11-14. used the timestamp which is the time shifted by 3 hours earlier.
## modified on 2016-11-18. corrected the part to look for the tram departure time at the destination station.
## comments: (added on 2016-11-21). column "Date" in tktvalidation_table seems not useful.
## modified on 2016-12-23. all the OD pair has been pre-built and saved in the table odpair in Postgres Database.
### so the part to build OD pair removed (Line 126 to 195). this scirpt only needsw to import OD pairs from database. 


library("data.table")
library("ggplot2")
library("RPostgreSQL")
library("car")
library("gtools")
library("dplyr")
library("lubridate")
library("zoo")
library("plyr")
library("date")
library("kimisc")
require("sqldf")
library("stringi")
library("scales")
library("gsubfn")
library("utility")
library("profvis")
# # library("compare")

rm(list=setdiff(ls(), "old_result"))
com_assigned_result <- old_result[1, ]

##### Establish connection to PoststgreSQL using RPostgreSQL
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="ratp", host="localhost", port=5432, user="yf", password="casaB1anca")# Simple version (localhost as default)
# dbDisconnect(con)
 

#incident_day <- c("2014-04-02", "2014-04-06", "2014-04-23", "2014-04-28", "2014-06-14", "2014-06-17", "2014-08-23", "2014-08-28", "2014-09-06"
#                  ,"2015-02-17", "2014-03-24" )

##############################################################
# to define which day we would like to select the data
# call the function to assign the passengers
targetdates <- c("2014-04-02", "2014-04-06", "2014-04-23", "2014-04-28", "2014-06-17", "2014-09-06", "2015-02-17", "2015-03-10", "2015-03-11",
                 "2015-03-15", "2015-03-24")
# com_assigned_result <- c()
start.time <- Sys.time()
for (numofdays in 1:length(targetdates)){
  selectdate <- targetdates[numofdays]
  selectdate <- as.Date(selectdate)
  assigned_result <- countingPassenger_v10_11(selectdate)
  
  if(nrow(com_assigned_result) < 2){
    com_assigned_result <-assigned_result 
  } else {
    com_assigned_result <- rbind(com_assigned_result, assigned_result)
  }
}

end.time <- Sys.time()
time_taken <- start.time - end.time
##############################################################

countingPassenger_v10_11 <- function(selectdate){
  
  tramstop <- dbSendQuery(con, paste0("select * from tramstop where date = '", selectdate, "'"))
  tramstop <- fetch(tramstop, n=-1)
  tramstop_table <- as.data.table(tramstop)
  remove(tramstop)
  tramstop_table <- subset(tramstop_table, select=-c(mnemoline, sens, nameofroute, gpsprob, deviation, estloctime, notusetime,schedulebeat, 
                                                     notusebeat, realbeat))
  
  # start.time <- Sys.time()
  
  
  ################################### can be removed #########################################################################################
  odpair <- dbSendQuery(con, paste0("select * from odpair where \"date.x\" = '", selectdate, "'"))
  odpair <- fetch(odpair, n=-1)
  trip <- as.data.table(odpair)
  remove(odpair)
  
#### !!!!!!!!!!!!!!!!!!!!!! (stop here)
  
  # Rprof("Rprof_20160719_cp_v10_7", append = TRUE)#strat to profile the code

  ## call the function "tramstatus_v1_1.R"
  
  source("/home/yf/work/Codes/tramstatus_v1_1.R")
  tramstop_table <- tramstatus_v1_1(tramstop_table)
  tramstop_table <- tramstop_table[, stationid2 := stri_extract_first_regex(stationid, "^[^_]+")]
  
  
  ## funtsion to count the entry every 10 minutes ######################
  
  ######################################################################
  ### this part of code can be found in V9 #############################
  
  
  
  ###### function to look into the tram frequency ######################
  
  ######################################################################
  ###################### modification on 2016-06-15 ends here ##########
  
  
  
  ### 1. every day #####
  # for (numday in 1:31){
  t_tramstop <- tramstop_table

  
  ###### start of pairing OD ###############################
  ######### this part has been done in another R file, build_OD_v1.R
  
#####################################################################################################################################################
  ODpair <- read.csv("/home/yf/work/Codes/allroute_OD_V2.csv")
  ODpair <- as.data.table(ODpair)
  colnames(ODpair) <- c("id", "Origin", "Destination", "route")
  trip_OD <- merge(trip, ODpair, by.x=c("station2.x", "station2.y"), by.y=c("Origin", "Destination"), all.x=TRUE, allow.cartesian=TRUE)
  ############   end of pairing OD  ###################################################

  

############ in another function, plot OD popularity ################################
  #####################################################################################
  
  
  ### 2. every station ####
  ### 2.1. for records with ID ####
  setkey(t_tramstop, stationid2)
  unique_station <- unique(t_tramstop)
  unique_station <- unique_station$stationid2
  
  lengthofstation <- length(unique_station)
  for (numstation in 1:lengthofstation){
    numstation
    remove(trip_OD_station)
    remove(t_tramstop_station)
    trip_OD_station <- trip_OD[station2.x == unique_station[numstation], ] #select the passengers who entered this station
    t_tramstop_station <- t_tramstop[stationid2 == unique_station[numstation], ] # select the tram arrived at this station
    
    
    setkey(t_tramstop_station, realtimestamp_shift)
    setkey(trip_OD_station, dthrtimestamp2.x)
    
    ##########################################################################################################################################  
    # find the first tram passing by the OD of eath ticket record after tap-out time
    nrowofODstation <- nrow(trip_OD_station)
    if (nrowofODstation > 0){
      trip_OD_station[, service_no := 0][, id_tram.x := 0][,trip_id := 0]
      for (k in 1:nrowofODstation){ #looping the ticket record one by one
        # find the earliest tram for the same route as the ticket record
        current_route <- trip_OD_station$route[k]
        entry_time <- trip_OD_station$dthrtimestamp2.x[k]
        service <- t_tramstop_station[realtimestamp_shift >= entry_time & route == current_route, ]
        service_n <- service[1, sv]
        arrival_t <- service[1, realtimestamp_shift]
        id_t <- service[1, id]
        trip_OD_station[k, service_no := service_n]
        trip_OD_station[k, arrival_time.x :=arrival_t]
        trip_OD_station[k, id_tram.x := id_t]
        
        # identify the tram at the destination station
        entry_tram <- t_tramstop[id == id_t, ]
        exit_trip_id <- entry_tram$trip_id
        exit_station <- trip_OD_station$station2.y[k]
        
        # to find the same tram that arrived at the desitnation station
        same_tram <- t_tramstop[(trip_id == exit_trip_id & stationid2 == exit_station & route == current_route), ]
        if (nrow(same_tram) > 1) same_tram <- same_tram[1,]
        tram_dest_time <- same_tram$realtimestamp_shift
        
        if (length(tram_dest_time)!=0){
          id_exittram <- same_tram$id
          trip_OD_station[k, arrival_time.y := tram_dest_time][k, id_tram.y := id_exittram][k,trip_id := exit_trip_id]
        }
      }
      
      trip_OD_station <- trip_OD_station[!is.na(service_no), ]
      
      if(numstation == 1){
        oneday_trip_OD_ID_e <- trip_OD_station
      } else {
        oneday_trip_OD_ID_e <- rbind(oneday_trip_OD_ID_e, trip_OD_station)
      }
    }
  } #end of looping one station 
  
  # trip_earliest_tram <- oneday_trip_OD_ID_e[id_tram.x !=0, ]
  trip_earliest_tram <-oneday_trip_OD_ID_e[,.SD[which.min(arrival_time.x)],by=c("id.x")] 
  
  #############################################################################################################################################
  return(trip_earliest_tram)
}# end of the function

  

############### count the number of passengers inside the tram ##################################
## function "time_space_plot_v5.R"
### take oneday_trip_OD_ID_all as input
#################################################################################################

