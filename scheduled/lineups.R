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


#get only the games from te leagues where stats are available
projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- ("select *
from `bi-playground-269209.soccer_test.game_schedule` a
inner join (select * from `bi-playground-269209.soccer_test.details_leagues` where current_ is true and coverage_fixtures_lineups is true) b
on a.league_id = b.league_id")

fixtures_df <- bq_table_download(bq_project_query(projectid,sql))

time_now <- Sys.time()

date_check <- fixtures_df %>% 
  select(fixture_id, league_country, league_name, home_team,away_team, date, status) %>% 
  mutate(curr_time=Sys.time()) %>%
  mutate(time_diff=as.numeric(difftime(date,curr_time,units = "hours"))) %>% 
  filter(time_diff <= 0.1 & time_diff > -0.15)

date_check$cet_curr_time <- format(Sys.time(),tz="Europe/Rome")

date_check$code <- paste0("fixture=",date_check$fixture_id)

fx_av <- date_check$code

fx_av1 <- paste0("https://api-football-v1.p.rapidapi.com/v3/fixtures/lineups?",fx_av)


#lineups_table <- as_tibble()

lineups <- as_tibble()
coaches <- as_tibble()


for (i in fx_av1){
  
  lineups_response <- VERB("GET", i, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                        content_type('application/octet-stream'))
  
  lineups_json <- jsonlite::fromJSON(content(lineups_response, as="text")) %>% purrr::map_if(is.data.frame, list) %>% tibble::enframe() %>% tibble::as_tibble() 
  
  results_cnt <- lineups_json %>% filter(name=="results") %>% unnest(cols = c(value))
  
  if(results_cnt$value > 0) {   
    
    fix_id <- lineups_json %>% filter(name=="parameters") %>% unnest(cols = c(value))
    
    fix_id <- fix_id$value$fixture
    
    lineups_startXI <- lineups_json %>% filter(name=="response") %>% 
      unnest(cols = c(value)) %>% unnest(cols = c(value)) %>% unnest(cols = c(startXI)) %>% select(-name,-substitutes) %>% mutate(type="startXI")
    
    lineups_substitutes <- lineups_json %>% filter(name=="response") %>% 
      unnest(cols = c(value)) %>% unnest(cols = c(value)) %>% unnest(cols = c(substitutes)) %>% select(-name,-startXI) %>% mutate(type="substitutes")
    
    lineups_teams <- bind_rows(lineups_startXI, lineups_substitutes)
    
    lineups_df <- data.table::as.data.table(lineups_teams) 
    
    lineups_df <- lineups_df %>% mutate(fixture_id=fix_id)
    
    lineups_coaches <- lineups_df %>% select(fixture_id, coach.id, coach.name, formation, team.name, team.id) %>% distinct()
    
    lineups_df <- lineups_df %>% select(-coach.id, -coach.name, -formation)
    
    lineups <- bind_rows(lineups,lineups_df) 
    
    if(nrow(lineups_coaches) > 0){
      coaches <- bind_rows(coaches,lineups_coaches)
    } else {
      NULL
    }
    
  } else {
    NULL
  }
  
}



lineups <- lineups %>% select(!team.logo & !starts_with("team.colors") & !coach.photo)

lineups$cet_curr_time <- format(Sys.time(),tz="Europe/Rome")

oldNames <- colnames(lineups)

newNames <- gsub(".","_",oldNames,fixed = TRUE)

lineups <- lineups %>% rename_at(vars(oldNames), ~ newNames)



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

dbWriteTable(con, "lineups", lineups, append=TRUE)

