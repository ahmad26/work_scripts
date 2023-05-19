
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

# sql <- ("select a.*, (home/(home+draw+away))*100 as prc_home, (draw/(home+draw+away))*100 as prc_draw, (away/(home+draw+away))*100 as prc_away, home, draw, away, h_tag
# from (select *, ROW_NUMBER() OVER(PARTITION BY fixture_id ORDER BY fixture_id, cet_curr_time desc) AS max_date 
# from `bi-playground-269209.soccer_test.info` qualify max_date = 1) a
# inner join `bi-playground-269209.soccer_test.live` b
# on a.fixture_id = b.fixture_id
# left join `bi-playground-269209.soccer_test.odds_table` c
# on a.fixture_id = c.fixture_id
# left join `bi-playground-269209.soccer_test.league_h_tags` d
# on a.league_id = d.league_id")

sql <- ("select d.fixture_id
, fixture_date
, fixture_timestamp
, d.league_id	
, d.league_country	
, d.league_name
, teams_home_name as home_team
, teams_away_name as away_team
, fixture_status_short as status_short	
, fixture_status_elapsed as status_elapsed
, goals_home
, goals_away
, teams_home_id as home_team_id	
, teams_away_id as away_team_id	
, last_update as cet_curr_time
, (home/(home+draw+away))*100 as prc_home
, (draw/(home+draw+away))*100 as prc_draw
, (away/(home+draw+away))*100 as prc_away
, home
, draw
, away
, 1 as max_date
, h_tag
from `bi-playground-269209.soccer_test.odds_table` a
inner join`bi-playground-269209.soccer_test.game_schedule` d
on a.fixture_id = d.fixture_id
left join `bi-playground-269209.soccer_test.league_h_tags` e
on a.league_id = e.league_id
where fixture_status_short in ('2H','HT','1H') 
        and home/(home+draw+away) is not null
        and d.league_id not in (10, 667, 666)")

matches_df <- bq_table_download(bq_project_query(projectid,sql))

# najpierw info bedzie dopisywane do duzej tabeli, usowane beda duplikaty, a potem z tamtad tweetowane

fav_loosing <- matches_df %>% mutate(who_fav=ifelse(prc_home <= 14.1,"H",ifelse(prc_away <= 17.1,"A","N"))) %>% 
  mutate(fav_lsn=ifelse((who_fav=="H" & as.numeric(goals_home) < as.numeric(goals_away)) | (who_fav=="A" & as.numeric(goals_home) > as.numeric(goals_away)),"Y","N")) %>% 
  filter(fav_lsn=="Y") %>% mutate(message_type="favourite_loosing")

# to update tags
# INSERT INTO `bi-playground-269209.soccer_test.league_h_tags` VALUES('Italy', 'Serie A', 135, '#SerieA')


#write.csv(fav_loosing, "~/Kuba/soccer/files/fav_loosing.csv", row.names = FALSE)

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

dbWriteTable(con, "message_events", fav_loosing, append=TRUE)

