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

sql <- ("select *, case when is_top_scorer='Y' and team_id = ts_team_id and league_id = ts_league_id then 'ts_matched'
when is_top_scorer='Y' and team_id = ts_team_id and league_id != ts_league_id then 'ts_other_league'
when is_top_scorer='Y' and team_id != ts_team_id and league_id = ts_league_id then 'ts_other_team'
when is_top_scorer='Y' and team_id != ts_team_id and league_id != ts_league_id then 'ts_other_other' end message_type from
(with fixtures as
(select a.fixture_id, home_team_id, away_team_id, status_elapsed, a.league_id from
(select *, ROW_NUMBER() OVER(PARTITION BY fixture_id ORDER BY fixture_id, cet_curr_time desc) AS max_date 
from `bi-playground-269209.soccer_test.info` qualify max_date = 1) a
inner join `bi-playground-269209.soccer_test.live` b
on a.fixture_id = b.fixture_id)
select c.*, a.league_id, case when c.player_id = d.player_id then 'Y' else 'N' end as is_top_scorer, 
d.league_id as ts_league_id, d.team_id as ts_team_id,
d.league_country as ts_teague_country, d.league_name as ts_league_name
from fixtures a
inner join `bi-playground-269209.soccer_test.lineups` c
on a.fixture_id = c.fixture_id
left join `bi-playground-269209.soccer_test.top_scorers` d
on c.player_id = d.player_id)")

lineups_live <- bq_table_download(bq_project_query(projectid,sql))


sql <- ("select * from `bi-playground-269209.soccer_test.top_scorers`")

top_scorers <- bq_table_download(bq_project_query(projectid,sql))


teams_in_game <- lineups_live %>% select(league_id,team_id) %>% distinct(league_id,team_id)


# to check if any player from squad appears between top scorers (even if he is a top scorer in different competition)

all_players_in_game <- unique(lineups_live$player_id)

ts_in_game <- top_scorers %>% filter(player_id %in% all_players_in_game)




ts_in_game <- inner_join(top_scorers,teams_in_game,by=c("league_id"="league_id", tem))
