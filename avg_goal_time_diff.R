

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


#sprawdzic sredni czas pomiedzy pierwszym golem straconym przez faworyta a kolejnymi bramkami

projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- ("select *, case when who_fav = winner or winner = 'D' then 'N' else 'Y' end as fav_loosing from
(select * 
, ROW_NUMBER() OVER(PARTITION BY fixture_id, sum_goals ORDER BY fixture_id, cet_curr_time) AS max_date
, case when goals_home > goals_away then 'H' when goals_home < goals_away then 'A' else 'D' end as winner
from
(select b.*, who_fav, b.goals_home + b.goals_away as sum_goals, c.tot_goals
from
(select distinct fixture_id, who_fav from `bi-playground-269209.soccer_test.message_events`) a
inner join `bi-playground-269209.soccer_test.info` b
on a.fixture_id = b.fixture_id
left join (select *, goals_home + goals_away as tot_goals from
(select * , ROW_NUMBER() OVER(PARTITION BY fixture_id ORDER BY fixture_id, cet_curr_time desc) AS max_date
from `bi-playground-269209.soccer_test.info`
qualify max_date = 1)) c
on a.fixture_id = c.fixture_id)
qualify max_date = 1 and sum_goals !=0)
order by fixture_id, cet_curr_time")

matches_df <- bq_table_download(bq_project_query(projectid,sql))

test <- matches_df %>% filter(fav_loosing=="Y")
