
library(httr)
library(jsonlite)
library(purrr)
library(tibble)
library(dplyr)
library(bigrquery)
library(DBI)
library(data.table)
library(tidyr)


details_url <- "https://api-football-v1.p.rapidapi.com/v3/fixtures"

dates <- seq(as.Date('2011-01-01'),as.Date('2011-01-31'),by = 1)

details_query <- list(
  date= Sys.Date()
)

details_response <- VERB("GET", details_url, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 
                                                         'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                         query = details_query, content_type('application/octet-stream'))

details_json <- fromJSON(content(details_response, as="text")) %>% map_if(is.data.frame, list) %>% enframe() %>% as_tibble() 

details_df <- details_json %>% filter(name=="response") %>% unnest(cols = c(value)) %>% unnest(cols = c(value))

details_df <- data.table::as.data.table(details_df) 

# game_schedule <- details_df %>% select(fixture_id=fixture.id, referee=fixture.referee, timezone=fixture.timezone,
#                                     date=fixture.date, status=fixture.status.long, league_id=league.id,
#                                     round=league.round, home_goals=goals.home, away_goals=goals.away,
#                                     home_goals_ht=score.halftime.home, away_goals_ht=score.halftime.away,
#                                     home_goals_et=score.extratime.home, away_goals_et=score.extratime.away,
#                                     home_goals_pt=score.penalty.home, away_goals_pt=score.penalty.away,
#                                     venue_id=fixture.venue.id, league_name=league.name, league_country=league.country,
#                                     home_team=teams.home.name, away_team=teams.away.name)

oldNames <- colnames(details_df)

newNames <- gsub(".","_",oldNames,fixed = TRUE)

game_schedule <- details_df %>% rename_at(vars(oldNames), ~ newNames) %>% select(-name,-league_logo,-league_flag,-teams_home_logo,-teams_away_logo)


game_schedule$last_update <- format(Sys.time(),tz="Europe/Rome")



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

dbWriteTable(con, "game_schedule", game_schedule, overwrite=TRUE)