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

leagues <- read.csv("~/Kuba/soccer/files/leagues.csv")

leagues_avl <- leagues %>% filter(current_ == TRUE & coverage_fixtures_statistics_fixtures==TRUE)

leagues_avl$code <- paste0("league=",leagues_avl$league_id,"&","season=", leagues_avl$year)
  

leg_av <- leagues_avl$code

#fixtures_url <- "https://api-football-v1.p.rapidapi.com/v3/fixtures"

leg_av1 <- paste0("https://api-football-v1.p.rapidapi.com/v3/fixtures","?",leg_av)


fixtures_df <- as_tibble()


for (i in leg_av1){
  
details_response <- VERB("GET", i, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                        content_type('application/octet-stream'))

details_json <- fromJSON(content(details_response, as="text")) %>% map_if(is.data.frame, list) %>% enframe() %>% as_tibble() 

details_df <- details_json %>% filter(name=="response") %>% unnest(cols = c(value)) %>% unnest(cols = c(value))

details_df$cet_curr_time <- format(Sys.time(),tz="Europe/Rome")

fixtures_df <- bind_rows(fixtures_df,details_df)

}

