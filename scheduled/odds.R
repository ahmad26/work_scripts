
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

sql <- ("select a.fixture_id
from `bi-playground-269209.soccer_test.game_schedule` a
inner join (select * from `bi-playground-269209.soccer_test.details_leagues` where current_ is true and coverage_odds is true) b
on a.league_id = b.league_id
left join `bi-playground-269209.soccer_test.odds_table` c
on a.fixture_id = c.fixture_id
where c.fixture_id is null")

fixtures_df <- bq_table_download(bq_project_query(projectid,sql))

fixtures_df$code <- paste0("fixture=",fixtures_df$fixture_id)

fx_av <- fixtures_df$code

fx_av1 <- paste0("https://api-football-v1.p.rapidapi.com/v3/odds?bookmaker=4&",fx_av)


odds_table <- as_tibble()


for (i in fx_av1){
  tryCatch({
  
  odds_response <- VERB("GET", i, add_headers('X-RapidAPI-Key' = 'fbf06b29a7msh3c377387ad935c0p135771jsneb00747fe96e', 'X-RapidAPI-Host' = 'api-football-v1.p.rapidapi.com'), 
                                content_type('application/octet-stream'))
  
  odds_json <- jsonlite::fromJSON(content(odds_response, as="text")) %>% purrr::map_if(is.data.frame, list) %>% tibble::enframe() %>% tibble::as_tibble() 
  
  results_cnt <- odds_json %>% filter(name=="results") %>% unnest(cols = c(value))
  
  if(results_cnt$value > 0) {
  
    odds_df <- odds_json %>% dplyr::filter(name=="response") %>% 
      tidyr::unnest(value) %>% dplyr::select(value) %>% 
      tidyr::unnest(value) %>% 
      tidyr::unnest(bookmakers) %>% select(-name,-id) %>% 
      tidyr::unnest(bets) %>% filter(name=="Match Winner")
    
    if(nrow(odds_df) > 0) {
      
      odds_df <- odds_df %>% tidyr::unnest(values) %>% 
        data.table::as.data.table()
      
      odds_df <- odds_df %>% select(league_id=league.id,fixture_id=fixture.id, update,odd_type=name,side=value,odd)
      
      odds_df$cet_curr_time <- format(Sys.time(),tz="Europe/Rome")
      
      odds_table <- bind_rows(odds_table,odds_df)
      
    } else {
      NULL
    }
    
  } else {
    NULL
  }
  
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
  
}

#write.csv(odds_table, "~/Kuba/soccer/files/odds.csv", row.names = FALSE)

if (length(odds_table)>0){
  odds_table1 <- odds_table %>% spread(side,odd) 

#odds_table1 <- odds_table %>% spread(side,odd) 

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

dbWriteTable(con, "odds_table", odds_table1, append=TRUE)

} else {

  print("no fresh odds")
}
