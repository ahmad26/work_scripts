library(httr)
library(jsonlite)
library(purrr)
library(tibble)
library(dplyr)
library(bigrquery)
library(DBI)
library(data.table)
library(tidyr)


details_url <- "https://api-football-v1.p.rapidapi.com/v3/leagues"

details_query <- list()

details_response <- httr::VERB("GET", details_url, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 
                                                               'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                               query = details_query, content_type('application/octet-stream'))

details_json <- jsonlite::fromJSON(content(details_response, as="text")) %>% purrr::map_if(is.data.frame, list) %>% enframe() %>% as_tibble() 

details_df <- details_json %>% filter(name=="response") %>% unnest(cols = c(value)) %>% unnest(cols = c(value)) %>% unnest(cols = c(seasons))

details_df <- data.table::as.data.table(details_df)

oldNames <- colnames(details_df)

newNames <- gsub(".","_",oldNames,fixed = TRUE)

details_leagues <- details_df %>% rename_at(vars(oldNames), ~ newNames) %>% rename(current_=current)

#details_leagues <- details_df %>% dplyr::filter(coverage.fixtures.statistics_fixtures==TRUE & current==TRUE) %>% select(league_id=league.id)

details_leagues$last_update <- format(Sys.time(),tz="Europe/Rome")


write.csv(details_leagues, "/home/masteruser/Kuba/soccer/files/leagues.csv", row.names = FALSE)


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

dbWriteTable(con, "details_leagues", details_leagues, overwrite=TRUE)


