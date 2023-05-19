
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

sql <- ("select a.*, round(TIMESTAMP_DIFF(fixture_date, prev_fixture_date,HOUR)/24,1) as days_since_last_game
, case when game_place = 'H' then b.home else b.away end as odds
, c.rank
, c.points

from

(with teams as
(select distinct teams_home_id as team_id, league_id from `bi-playground-269209.soccer_test.game_schedule` 
  union all
  select distinct teams_away_id as team_id, league_id from `bi-playground-269209.soccer_test.game_schedule`)
select *
, lag(fixture_date) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date ASC) AS prev_fixture_date
, lag(score_fulltime_team) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date ASC) AS prev_game_scored_g
, lag(score_fulltime_opponent) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date ASC) AS prev_game_lost_g
, ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id, fixture_date desc) -1 AS games_order
, ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id, fixture_date) -1 AS games_order_rev
from
(select a.fixture_id
  , fixture_date
  , score_fulltime_home as score_fulltime_team
  , score_fulltime_away as score_fulltime_opponent
  , score_halftime_home as score_halftime_team
  , score_halftime_away as score_halftime_opponent
  #, score_extratime_home
  #, score_extratime_away
  #, score_penalty_home
  #, score_penalty_away
  , fixture_status_elapsed
  , fixture_status_short
  #, teams_home_winner
  #, teams_away_winner
  , teams_home_id as team_id
  , teams_home_name as team_name
  , teams_away_id as opponent_id
  , teams_away_name as opponent_name
  , a.league_id
  , league_country
  , league_name
  , league_round
  , coach_id
  , coach_name
  , case when score_fulltime_home > score_fulltime_away then 'W' when score_fulltime_home < score_fulltime_away then 'L' else 'D' end as FT_result
  , case when score_halftime_home > score_halftime_away then 'W' when score_halftime_home < score_halftime_away then 'L' else 'D' end as HT_result
  #, lag(fixture_date) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date ASC) AS prev_fixture_date
  , ROW_NUMBER() OVER(PARTITION BY b.team_id ORDER BY b.team_id, fixture_date desc) AS games_order_per_site
  , 'H' as game_place
  , league_level
  , is_man
  , is_senior
  from `bi-playground-269209.soccer_test.game_schedule_daily` a
  inner join teams b 
  on a.teams_home_id = b.team_id
  left join `bi-playground-269209.soccer_test.coaches` c
  on a.teams_home_id = c.team_id and a.fixture_id = c.fixture_id
  left join `bi-playground-269209.soccer_test.league_lvl` d
on a.league_id = d.league_id
  where date(cast(fixture_date as datetime)) >= current_date() -60 and fixture_status_short in ('FT','AET','PEN') and a.league_id not in (10, 667, 666)

union all

select a.fixture_id
  , fixture_date
  , score_fulltime_away as score_fulltime_team
  , score_fulltime_home as score_fulltime_opponent
  , score_halftime_away as score_halftime_team
  , score_halftime_home as score_halftime_opponent
  #, score_extratime_home
  #, score_extratime_away
  #, score_penalty_home
  #, score_penalty_away
  , fixture_status_elapsed
  , fixture_status_short
  #, teams_home_winner
  #, teams_away_winner
  , teams_away_id as team_id
  , teams_away_name as team_name
  , teams_home_id as opponent_id
  , teams_home_name as opponent_name
  , a.league_id
  , league_country
  , league_name
  , league_round
  , coach_id
  , coach_name
  , case when score_fulltime_home < score_fulltime_away then 'W' when score_fulltime_home > score_fulltime_away then 'L' else 'D' end as FT_result
  , case when score_halftime_home < score_halftime_away then 'W' when score_halftime_home > score_halftime_away then 'L' else 'D' end as HT_result
  #, lag(fixture_date) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date ASC) AS prev_fixture_date
  , ROW_NUMBER() OVER(PARTITION BY b.team_id ORDER BY b.team_id, fixture_date desc) AS games_order_site
  , 'A' as game_place
  , league_level
  , is_man
  , is_senior
  from `bi-playground-269209.soccer_test.game_schedule_daily` a
  inner join teams b 
  on a.teams_away_id = b.team_id
  left join `bi-playground-269209.soccer_test.coaches` c
  on a.teams_away_id = c.team_id and a.fixture_id = c.fixture_id
  left join `bi-playground-269209.soccer_test.league_lvl` d
on a.league_id = d.league_id
  where date(cast(fixture_date as datetime)) >= current_date() -60 and fixture_status_short in ('FT','AET','PEN') and a.league_id not in (10, 667, 666)
  
  union all

  select a.fixture_id
  , fixture_date
  , cast(score_fulltime_home as INT64) as score_fulltime_team
  , cast(score_fulltime_away as INT64) as score_fulltime_opponent
  , cast(score_halftime_home as INT64) as score_halftime_team
  , cast(score_halftime_away as INT64) as score_halftime_opponent
  #, score_extratime_home
  #, score_extratime_away
  #, score_penalty_home
  #, score_penalty_away
  , fixture_status_elapsed
  , fixture_status_short
  #, teams_home_winner
  #, teams_away_winner
  , teams_home_id as team_id
  , teams_home_name as team_name
  , teams_away_id as opponent_id
  , teams_away_name as opponent_name
  , a.league_id
  , league_country
  , league_name
  , league_round
  , coach_id
  , coach_name
  , null as FT_result
  , null as HT_result
  #, lag(fixture_date) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date ASC) AS prev_fixture_date
  , null AS games_order_per_site
  , 'H' as game_place
  , league_level
  , is_man
  , is_senior
  from `bi-playground-269209.soccer_test.game_schedule` a
  inner join teams b 
  on a.teams_home_id = b.team_id
  left join `bi-playground-269209.soccer_test.coaches` c
  on a.teams_home_id = c.team_id and a.fixture_id = c.fixture_id
  left join `bi-playground-269209.soccer_test.league_lvl` d
on a.league_id = d.league_id
  where a.league_id not in (10, 667, 666)
  
  union all

  select a.fixture_id
  , fixture_date
  , cast(score_fulltime_away as INT64) as score_fulltime_team
  , cast(score_fulltime_home as INT64) as score_fulltime_opponent
  , cast(score_halftime_away as INT64) as score_halftime_team
  , cast(score_halftime_home as INT64) as score_halftime_opponent
  #, score_extratime_home
  #, score_extratime_away
  #, score_penalty_home
  #, score_penalty_away
  , fixture_status_elapsed
  , fixture_status_short
  #, teams_home_winner
  #, teams_away_winner
  , teams_away_id as team_id
  , teams_away_name as team_name
  , teams_home_id as opponent_id
  , teams_home_name as opponent_name
  , a.league_id
  , league_country
  , league_name
  , league_round
  , coach_id
  , coach_name
  , null as FT_result
  , null as HT_result
  #, lag(fixture_date) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date ASC) AS prev_fixture_date
  , null as games_order_site
  , 'A' as game_place
  , league_level
  , is_man
  , is_senior
  from `bi-playground-269209.soccer_test.game_schedule` a
  inner join teams b 
  on a.teams_away_id = b.team_id
  left join `bi-playground-269209.soccer_test.coaches` c
  on a.teams_away_id = c.team_id and a.fixture_id = c.fixture_id
 left join `bi-playground-269209.soccer_test.league_lvl` d
on a.league_id = d.league_id
  where a.league_id not in (10, 667, 666)
  ))a

left join `bi-playground-269209.soccer_test.odds_table` b
on a.fixture_id = b.fixture_id
left join `bi-playground-269209.soccer_test.league_table` c
on a.league_id = c.league_id and a.team_id = c.team_id
where a.league_id not in (10, 667, 666)
order by team_id, fixture_date")

matches_df <- bq_table_download(bq_project_query(projectid,sql)) 

#temp remove cases with multiple duplicated rows
matches_df <- matches_df %>% filter(games_order <= 10)

matches_df <- matches_df %>% 
  dplyr::group_by(team_id) %>%
  #dplyr::mutate(res_lag1=lag(FT_result,n=1), res_lag2=lag(FT_result,n=2), res_lag3=lag(FT_result,n=3)) %>% 
  dplyr::mutate(res_streak=paste0(lag(FT_result,n=1),lag(FT_result,n=2),lag(FT_result,n=3))) %>% 
  dplyr::mutate(pts=if_else(FT_result=="W",3,if_else(FT_result=="D",1,0))) %>% 
  dplyr::mutate(sum_pts=(lag(pts,n=1)+lag(pts,n=2)+lag(pts,n=3)))
  #filter(games_order_rev!=0 & games_order_rev !=1 & games_order_rev !=2)


last_game <- matches_df %>% filter(games_order==1) %>% 
  select(team_id,days_since_last_game,goals_scored_lg=score_fulltime_team,goal_lost_lg=score_fulltime_opponent)


matches_df1 <- matches_df %>% select(team_id,games_order,FT_result) %>% spread(games_order,FT_result) %>% select(-`0`)

matches_df1 <- left_join(matches_df1,last_game,by=c("team_id" = "team_id"))

sql <- ("select a.*, league_level, is_man, is_senior, rank as league_rank, points as league_points from
(select teams_home_id as team_id, fixture_id, league_id, league_country, league_name, teams_home_name as team_name from `bi-playground-269209.soccer_test.game_schedule` 
  union all
  select teams_away_id as team_id, fixture_id, league_id, league_country, league_name, teams_away_name as team_name from `bi-playground-269209.soccer_test.game_schedule`) a
left join `bi-playground-269209.soccer_test.league_lvl` b
on a.league_id = b.league_id
left join `bi-playground-269209.soccer_test.league_table` c
on a.league_id = c.league_id and a.team_id = c.team_id
where a.league_id not in (10, 667, 666)")

matches_df2 <- bq_table_download(bq_project_query(projectid,sql))

matches_df3 <- left_join(matches_df2,matches_df1,by=c("team_id" = "team_id")) %>% arrange(fixture_id)

