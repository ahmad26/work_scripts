
library(tidyr)
library(httr)
#library(data.tree)
library(jsonlite)
library(dplyr)
library(tibble)
library(purrr)
library(lubridate)
library(data.table)
library(bigrquery)
library(DBI)



#get only the games from te leagues where stats are available
projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- ("select a.* from 
(select distinct fixture_id, league_id, fixture_date  from `bi-playground-269209.soccer_test.game_schedule_daily` 
where fixture_status_short = 'FT' and date(fixture_date) >= current_date()-3) a
inner join (select league_id 
from `bi-playground-269209.soccer_test.details_leagues` 
where `end` >= current_date() and coverage_fixtures_events is true) c
on a.league_id = c.league_id
left join (select distinct fixture_id from `bi-playground-269209.soccer_test.events_daily`) b
on a.fixture_id = b.fixture_id
where b.fixture_id is null")

fixtures <- bq_table_download(bq_project_query(projectid,sql))

fixture_ids <- fixtures$fixture_id




events_url <- "https://api-football-v1.p.rapidapi.com/v3/fixtures/events"

events_df <- as_tibble()


for (i in fixture_ids){
  
  tryCatch({
  
  #fixtures_query <- list(fixture = "816427")
  fixtures_response <- VERB("GET", events_url, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 
                                                             'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                            query = list(fixture = i), content_type('application/octet-stream'))
  
  
  fixtures_json <- fromJSON(content(fixtures_response, as="text")) %>% map_if(is.data.frame, list) %>% enframe() %>% as_tibble() 
  
  fixtures_df <- fixtures_json %>% filter(name=="response") %>% unnest(cols = c(value)) %>% unnest(cols = c(value)) %>% data.table::as.data.table()
  
  fixtures_df <- fixtures_df %>% mutate(fixture_id= i)
  
  events_df <- bind_rows(events_df,fixtures_df)
  
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
  
}


oldNames <- colnames(events_df)

newNames <- gsub(".","_",oldNames,fixed = TRUE)

events_df <- events_df %>% rename_at(vars(oldNames), ~ newNames) %>% select(-team_logo, -name, -value)

events_df$cet_curr_time <- format(Sys.time(),tz="Europe/Rome")


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

dbWriteTable(con, "events_daily", events_df, append=TRUE)