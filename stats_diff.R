
library(dplyr)
library(bigrquery)
library(httr)
library(lubridate)

httr::set_config(httr::config(http_version = 0))

projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- 'select a.*
, b.status_short
, case when a.team_id = b.home_team_id then goals_home else goals_away end as goals
, case when a.team_id = b.home_team_id then b.home_team else b.away_team end as team_name
, b.league_name
, case when a.team_id = b.home_team_id then "H" else "A" end as side
from `bi-playground-269209.soccer_test.system` a
inner join `bi-playground-269209.soccer_test.info` b
on a.fixture_id = b.fixture_id and a.minute=b.status_elapsed and a.cet_curr_time = b.cet_curr_time
inner join `bi-playground-269209.soccer_test.live` c
on b.fixture_id = c.fixture_id'

test_games <- bq_table_download(bq_project_query(projectid,sql))

test_games <- test_games %>% arrange(fixture_id,team_id,cet_curr_time)

cols <- c( "Ball_Possession", "Blocked_Shots", "Corner_Kicks", "Fouls", "Goalkeeper_Saves", "Offsides", "Passes_prc",
           "Passes_accurate", "Red_Cards", "Shots_insidebox", "Shots_off_Goal", "Shots_on_Goal", "Shots_outsidebox", 
           "Total_passes", "Total_Shots", "Yellow_Cards")

test_games[cols] <- lapply(test_games[cols], as.integer)

test_games$is_ht <- ifelse(test_games$status_short=='HT' & test_games$status_short==lag(test_games$status_short),"Y","N")


  tg2 <- test_games %>% filter(is_ht=="N") %>% group_by(fixture_id,team_id) %>% 
    mutate(Blocked_Shots_calc = (as.numeric(Blocked_Shots) - as.numeric(lag(Blocked_Shots))),
           Corner_Kicks_calc = (as.numeric(Corner_Kicks) - as.numeric(lag(Corner_Kicks))),
           Fouls_calc = (as.numeric(Fouls) - as.numeric(lag(Fouls))),
           Goalkeeper_Saves_calc = (as.numeric(Goalkeeper_Saves) - as.numeric(lag(Goalkeeper_Saves))),
           Offsides_calc = (as.numeric(Offsides) - as.numeric(lag(Offsides))),
           Passes_accurate_calc = (as.numeric(Passes_accurate) - as.numeric(lag(Passes_accurate))),
           Red_Cards_calc = (as.numeric(Red_Cards) - as.numeric(lag(Red_Cards))),
           Shots_insidebox_calc = (as.numeric(Shots_insidebox) - as.numeric(lag(Shots_insidebox))),
           Shots_off_Goal_calc = (as.numeric(Shots_off_Goal) - as.numeric(lag(Shots_off_Goal))),
           Shots_on_Goal_calc = (as.numeric(Shots_on_Goal) - as.numeric(lag(Shots_on_Goal))),
           Shots_outsidebox_calc = (as.numeric(Shots_outsidebox) - as.numeric(lag(Shots_outsidebox))),
           Total_passes_calc = (as.numeric(Total_passes) - as.numeric(lag(Total_passes))),
           Total_Shots_calc = (as.numeric(Total_Shots) - as.numeric(lag(Total_Shots))),
           Yellow_Cards_calc = (as.numeric(Yellow_Cards) - as.numeric(lag(Yellow_Cards))),
           ) %>% ungroup(fixture_id,team_id)
  
  tg3 <- tg2 %>% mutate(last_up=max(cet_curr_time)) %>% filter(cet_curr_time==last_up)
