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

cet_curr_time <- format(Sys.time(),tz="Europe/Rome")

odds_url <- "https://api-football-v1.p.rapidapi.com/v3/odds/bets"

odds_query <- base::list(fixture="853287")

odds_response <- httr::VERB("GET", "https://api-football-v1.p.rapidapi.com/v3/odds?fixture=721030&bookmaker=16", add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 
                                                                   'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                                  content_type('application/octet-stream'))

odds_json <- jsonlite::fromJSON(content(odds_response, as="text")) %>% purrr::map_if(is.data.frame, list) %>% tibble::enframe() %>% tibble::as_tibble() 

odds_df <- odds_json %>% dplyr::filter(name=="response") %>% 
  tidyr::unnest(value) %>% dplyr::select(value) %>% 
  tidyr::unnest(value) %>% 
  tidyr::unnest(bookmakers) %>% select(-name,-id) %>% 
  tidyr::unnest(bets) %>% filter(name=="Match Winner" | name=="Corners Over Under") %>% 
  tidyr::unnest(values) %>% data.table::as.data.table()

livescore_df$cet_curr_time <- cet_curr_time

livescore_fx_ids <- c(livescore_df$fixture.id)

lsStats <- as_tibble()
lsStats_tr <- as_tibble()
counter <- 0


lsStats_url <- "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"

fromJSON("~/Kuba/soccer/files/bets.json")
odds_json <- jsonlite::fromJSON("~/Kuba/soccer/files/bets.json") %>% purrr::map_if(is.data.frame, list) %>% tibble::enframe() %>% tibble::as_tibble() 

