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

livescore_url <- "https://api-football-v1.p.rapidapi.com/v3/fixtures"

livescore_query <- base::list(live = "all")

livescore_response <- httr::VERB("GET", livescore_url, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 
                                                                   'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                                 query = livescore_query, content_type('application/octet-stream'))

livescore_json <- jsonlite::fromJSON(content(livescore_response, as="text")) %>% purrr::map_if(is.data.frame, list) %>% tibble::enframe() %>% tibble::as_tibble() 

livescore_df <- livescore_json %>% dplyr::filter(name=="response") %>% 
  tidyr::unnest(cols = c(value)) %>% 
  tidyr::unnest(cols = c(value)) %>% 
  data.table::as.data.table()

livescore_df$cet_curr_time <- cet_curr_time


#get only the games from te leagues where stats are available
projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- ("select distinct league_id from `bi-playground-269209.soccer_test.details_leagues` where coverage_fixtures_statistics_fixtures is TRUE")

details_leagues <- bq_table_download(bq_project_query(projectid,sql))

leagues_allowed <- details_leagues$league_id

livescore_df <- livescore_df %>% filter(league.id %in% leagues_allowed)



livescore_fx_ids <- c(livescore_df$fixture.id)

lsStats <- as_tibble()
lsStats_tr <- as_tibble()
counter <- 0


lsStats_url <- "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"

for (i in livescore_fx_ids){
  
  lsStats_response <- VERB("GET", lsStats_url, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                           query = list(fixture = i), content_type('application/octet-stream'))
  
  lsStats_json <- fromJSON(content(lsStats_response, as="text")) %>% map_if(is.data.frame, list) %>% enframe() %>% as_tibble()
  
  results_cnt <- lsStats_json %>% filter(name=="results") %>% unnest(cols = c(value))
  
  if(results_cnt$value > 0) {   
    
    lsStats_df <- lsStats_json %>% filter(name=="response") %>% unnest(cols = c(value)) %>% unnest(cols = c(value)) %>% unnest(cols = c(statistics))
    
    lsStats_df <- data.table::as.data.table(lsStats_df) %>% rename(team_id = team.id)
    
    lsStats_df <- lsStats_df %>% mutate(fixture_id= i)

    lsStats_df$value <- gsub("%", "", lsStats_df$value)
  
    lsStats_df$value[is.na(lsStats_df$value)] <- 0
    
    lsStats_df$value <- as.character(lsStats_df$value)
    
    lsStats_df$minute <- livescore_df$fixture.status.elapsed[livescore_df$fixture.id== i]
    
    lsStats <- bind_rows(lsStats,lsStats_df)
    
    # transposed data
    lsStats_df$type <- gsub(" ", "_", lsStats_df$type)
    lsStats_df$type <- gsub("%", "prc", lsStats_df$type)

    lsStats_tr_s <- lsStats_df %>% select(team_id,fixture_id,minute,type,value) %>% spread(type,value)
    
    lsStats_tr <- bind_rows(lsStats_tr,lsStats_tr_s)

    lsStats_tr$cet_curr_time <- cet_curr_time
    
  } else {
    NULL
  }
  
 # counter <- counter + 1
}

info <- livescore_df %>% select(fixture_id=fixture.id,fixture_date=fixture.date,fixture_timestamp=fixture.timestamp,league_id=league.id,league_country=league.country,league_name=league.name
,home_team=teams.home.name,away_team=teams.away.name,status_short=fixture.status.short,status_elapsed=fixture.status.elapsed,goals_home=goals.home,goals_away=goals.away
,home_team_id=teams.home.id,away_team_id=teams.away.id,cet_curr_time)

live <- livescore_df %>% select(fixture_id=fixture.id, league_id=league.id ,cet_curr_time)

#track_requests <- as_tibble()

#current_req <- data.frame(timestamp=Sys.time(), req_count=counter)

#track_requests <- bind_rows(track_requests,current_req)


if (!is.null(lsStats_tr$expected_goals)){
  lsStats_tr <- lsStats_tr %>% select(-expected_goals)
} else {
  NULL
}

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
 
 dbWriteTable(con, "system", lsStats_tr, append = TRUE)

 dbWriteTable(con, "info", info, append = TRUE)

 dbWriteTable(con, "live", live, overwrite=TRUE)
