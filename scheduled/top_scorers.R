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

leagues_avl <- leagues %>% filter(current_ == TRUE & coverage_fixtures_statistics_fixtures==TRUE)

leagues_avl$code <- paste0("league=",leagues_avl$league_id,"&","season=", leagues_avl$year)


leg_av <- leagues_avl$code

leg_av1 <- paste0("https://api-football-v1.p.rapidapi.com/v3/players/topscorers","?",leg_av)

top_scorers_df <- as_tibble()

for (i in leg_av1){
  
  details_response <- VERB("GET", i, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                           content_type('application/octet-stream'))
  
  details_json <- fromJSON(content(details_response, as="text")) %>% map_if(is.data.frame, list) %>% enframe() %>% as_tibble() 
  
  results_cnt <- details_json %>% filter(name=="results") %>% unnest(cols = c(value))
  
  if(results_cnt$value > 0) {
  
  details_df <- details_json %>% filter(name=="response") %>% unnest(cols = c(value)) %>% unnest(cols = c(value)) %>% unnest(cols = c(statistics))
  
  details_df$cet_curr_time <- format(Sys.time(),tz="Europe/Rome")
  
  top_scorers_df <- bind_rows(top_scorers_df,details_df)
  
  } else {
    NULL
  }
  
}

top_scorers_df <- data.table::as.data.table(top_scorers_df)

top_scorers <- top_scorers_df %>% select(!name & !team.logo & !league.logo & !league.flag & !player.photo)

oldNames <- colnames(top_scorers)

newNames <- gsub(".","_",oldNames,fixed = TRUE)

top_scorers <- top_scorers %>% rename_at(vars(oldNames), ~ newNames)



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

dbWriteTable(con, "top_scorers", top_scorers, overwrite=TRUE)

