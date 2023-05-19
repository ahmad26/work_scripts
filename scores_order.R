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
library(rtweet)



projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- ("select *
, case when side = 'H' then goals_side else null end as home_score
, case when side = 'A' then goals_side else null end as away_score
from
(select *, ROW_NUMBER() OVER(PARTITION BY fixture_id, side ORDER BY fixture_id, time_elapsed, time_extra) AS goals_side
from
(select a.*, b.fixture_date, goals_home, goals_away
, case when team_id = teams_home_id then 'H' else 'A' end as side
, ROW_NUMBER() OVER(PARTITION BY a.fixture_id ORDER BY a.fixture_id, time_elapsed, time_extra) AS max_date 
from
(SELECT * FROM `bi-playground-269209.soccer_test.events_daily` where type = 'Goal') a
left join (select fixture_id, fixture_date, teams_away_id, teams_home_id, goals_home, goals_away from `bi-playground-269209.soccer_test.game_schedule_daily`) b
on a.fixture_id = b.fixture_id))
order by fixture_id, time_elapsed, time_extra")

results_df1 <- bq_table_download(bq_project_query(projectid,sql))

results_df1 <- results_df1 %>% arrange(fixture_id, max_date)

results_df1 <- results_df1 %>% mutate(home_score2 = ifelse(max_date == 1 & side == "A", 0, home_score)) %>% group_by(fixture_id) %>% fill(home_score2, .direction = "down")
results_df1 <- results_df1 %>% mutate(away_score2 = ifelse(max_date == 1 & side == "H", 0, away_score)) %>% group_by(fixture_id) %>% fill(away_score2, .direction = "down")
results_df1$score <- paste(results_df1$home_score2,results_df1$away_score2,sep = "-")


resultados <- results_df1 %>% group_by(max_date,score) %>% summarise(cnt=n())

test <- results_df1 %>% filter(max_date == 2 & score=="2-1") %>% select(fixture_id)

test1 <- results_df1 %>% filter(fixture_id %in% test$fixture_id)


resu1 <- results_df1 %>% select(fixture_id,max_date,side) %>% filter(max_date <= 10) %>% spread(max_date,side)

cols <- colnames(resu1)

cols <- cols[-1]

resu1$path <- apply( resu1[ , cols ] , 1 , paste , collapse = "-" )

resu2 <- resu1 %>% group_by(path) %>% summarise(cnt=n())

total <- sum(resu2$cnt)

total_0 <- c(68885)

resu2$prc <- round((resu2$cnt/total)*100,2)

resu2$prc2 <- round((resu2$cnt/total_0)*100,2)

