

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


projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- ("select * from
(select fixture_id, player_name, team_name, detail, goals_cnt, games_cnt, striker_rank, prc_all_goals,
case when detail = 'Red Card' then  'striker_out' else 'striker_in' end as action
from `bi-playground-269209.soccer_test.strikers` a
inner join `bi-playground-269209.soccer_test.events` b
on a.player_id = b.player_id
where detail like 'Substitution%' or detail = 'Red Card'

union all

select fixture_id, player_name, team_name, detail, goals_cnt, games_cnt, striker_rank, prc_all_goals, 'striker_out' as action
from `bi-playground-269209.soccer_test.strikers` a
inner join `bi-playground-269209.soccer_test.events` b
on a.player_id = b.assist_id
where detail like 'Substitution%') a
inner join `bi-playground-269209.soccer_test.game_schedule` b
on a.fixture_id = b.fixture_id
")

striker_subs <- bq_table_download(bq_project_query(projectid,sql))

striker_subs_read <- read_csv("/home/masteruser/Kuba/soccer/files/striker_subs.csv")

write.csv(striker_subs, "/home/masteruser/Kuba/soccer/files/striker_subs.csv", row.names = FALSE)
