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


projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- ("select detail, type, fixture_id, team_id, time_elapsed, time_extra
, ROW_NUMBER() OVER(PARTITION BY fixture_id ORDER BY time_elapsed, time_extra) as refresh_cnt
, ROW_NUMBER() OVER(PARTITION BY fixture_id ORDER BY time_elapsed desc, time_extra desc) as last_event_1
, case when type = 'Goal' then ROW_NUMBER() OVER(PARTITION BY fixture_id, type ORDER BY time_elapsed, time_extra) end as goal_cnt
, case when type = 'Card' then ROW_NUMBER() OVER(PARTITION BY fixture_id, type ORDER BY time_elapsed, time_extra) end as red_card_cnt
from `bi-playground-269209.soccer_test.events_daily` where fixture_id in
(select distinct fixture_id from `bi-playground-269209.soccer_test.events_daily` where detail = 'Red Card')
and type in ('Card','Goal') and detail != 'Yellow Card'
order by fixture_id, time_elapsed, time_extra")

red_card_games <- bq_table_download(bq_project_query(projectid,sql))

red_card_games$goal_cnt <- ifelse(red_card_games$refresh_cnt==1 & is.na(red_card_games$goal_cnt),0,red_card_games$goal_cnt)

red_card_games2 <- red_card_games %>% group_by(fixture_id) %>% mutate(goals=na.locf(goal_cnt, na.rm = FALSE))

red_card_games2$red_card_cnt <- ifelse(red_card_games$refresh_cnt==1 & is.na(red_card_games$red_card_cnt),0,red_card_games$red_card_cnt)

red_card_games2 <- red_card_games2 %>% group_by(fixture_id) %>% mutate(cards=na.locf(red_card_cnt, na.rm = FALSE))

red_card_games2 <- red_card_games2 %>% group_by(fixture_id) %>% mutate(cards=na.locf(red_card_cnt, na.rm = FALSE))

red_card_games2 <- red_card_games2 %>% group_by(fixture_id) %>% mutate(total_goals=max(goals))
red_card_games2 <- red_card_games2 %>% group_by(fixture_id) %>% mutate(total_red_cards=max(cards))

red_card_games2$goal_after_red <- ifelse(red_card_games2$red_card_cnt==2 & red_card_games2$total_goals > red_card_games2$goals,"Y","N")

red_card_games3 <- red_card_games2 %>% filter(red_card_cnt==2)
test <- red_card_games3 %>% filter(time_elapsed < 70) %>% group_by(goal_after_red) %>% summarise(n())