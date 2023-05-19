library(httr)
library(jsonlite)
library(purrr)
library(tibble)
library(dplyr)
library(tidyr)
library(bigrquery)
library(DBI)
library(data.table)
library(tidyverse)
library(lubridate)

leagues <- read.csv("/home/masteruser/Kuba/soccer/files/leagues.csv")

leagues_avl <- leagues %>% filter(end >= Sys.Date() & coverage_standings==TRUE)

leagues_avl$code <- paste0("league=",leagues_avl$league_id,"&","season=", leagues_avl$year)


leg_av <- leagues_avl$code

leg_av1 <- paste0("https://api-football-v1.p.rapidapi.com/v3/standings","?",leg_av)


league_table <- as_tibble()


for (i in leg_av1){
  
  league_table_response <- VERB("GET", i, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                           content_type('application/octet-stream'))
  
  league_table_json <- fromJSON(content(league_table_response, as="text")) %>% map_if(is.data.frame, list) %>% enframe() %>% as_tibble() 
  
  league_table_df <- league_table_json %>% filter(name=="response") %>% unnest(cols = c(value)) %>% unnest(cols = c(value)) %>% data.table::as.data.table()
  
  league_table_1 <- league_table_df$league.standings[[1]][[1]]
  
  league_table_1$league_id <- league_table_df$league.id
  
  league_table_1$cet_curr_time <- format(Sys.time(),tz="Europe/Rome")
  
  league_table <- bind_rows(league_table,league_table_1)
  
}

 league_table <- as.data.table(league_table)
 
 oldNames <- colnames(league_table)
 
 newNames <- gsub(".","_",oldNames,fixed = TRUE)
 
 league_table <- league_table %>% rename_at(vars(oldNames), ~ newNames)
 
 
 write.csv(league_table, "/home/masteruser/Kuba/soccer/files/standings.csv", row.names = FALSE)
 

bigrquery::bq_auth(path= "/opt/syek/key.json")

billing = "1108793-EMEA-Bee-Fee Limited"
project = "bi-playground-269209"
data_set = "soccer_test"

con = dbConnect(
  bigrquery::bigquery(),
  project = project,
  dataset = data_set,
  billing = billing
)

dbWriteTable(con, "league_table", league_table, overwrite=TRUE)
