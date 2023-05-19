library(httr)
library(jsonlite)
library(purrr)
library(tibble)
library(dplyr)
library(bigrquery)
library(DBI)
library(data.table)
library(tidyr)




#days <- seq(as.Date("2022/06/04"),as.Date("2022/06/05"),"days")

days <- Sys.Date()-1

days1 <- paste0("https://api-football-v1.p.rapidapi.com/v3/fixtures?date=",days)

details_table <- as_tibble()

for (i in days1) {
  
  details_response <- VERB("GET", i , add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 
                                                           'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), content_type('application/octet-stream'))
  
  
  details_json <- fromJSON(content(details_response, as="text")) %>% map_if(is.data.frame, list) %>% enframe() %>% as_tibble() 
  
  details_df <- details_json %>% filter(name=="response") %>% unnest(cols = c(value)) %>% unnest(cols = c(value))
  
  details_df <- data.table::as.data.table(details_df)
  
  details_df$cet_curr_time <- format(Sys.time(),tz="Europe/Rome")
  
  details_table <- bind_rows(details_table,details_df)
  
}




oldNames <- colnames(details_table)

newNames <- gsub(".","_",oldNames,fixed = TRUE)

game_schedule <- details_table %>% rename_at(vars(oldNames), ~ newNames) %>% select(-name,-league_logo,-league_flag,-teams_home_logo,-teams_away_logo)


#game_schedule$last_update <- format(Sys.time(),tz="Europe/Rome")



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

dbWriteTable(con, "game_schedule_daily", game_schedule, append=TRUE)