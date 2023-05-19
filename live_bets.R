

library(httr)
library(jsonlite)
library(purrr)
library(tibble)
library(dplyr)
library(bigrquery)
library(DBI)
library(data.table)
library(tidyr)


live_odds_url <- "https://api-football-v1.p.rapidapi.com/v3/odds/live"

live_odds_query <- list()

live_odds_response <- httr::VERB("GET", live_odds_url, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 
                                                               'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                               query = live_odds_query, content_type('application/octet-stream'))

live_odds_json <- jsonlite::fromJSON(content(live_odds_response, as="text")) %>% purrr::map_if(is.data.frame, list) %>% enframe() %>% as_tibble() 

live_odds_df <- live_odds_json %>% filter(name=="response") %>% select(-name) %>% 
  unnest(value) %>% unnest(value) %>% unnest(odds) %>% 
  unnest(values)
live_odds_df <- data.table::as.data.table(live_odds_df)