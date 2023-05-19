


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
, b.Home
, b.Draw
, b.Away
from

(with teams as
(select distinct team_id from
  (select distinct teams_home_id as team_id from `bi-playground-269209.soccer_test.game_schedule_daily` 
  union all
  select distinct teams_away_id as team_id from `bi-playground-269209.soccer_test.game_schedule_daily`))
select a.*
, lag(fixture_date) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date ASC) AS prev_fixture_date
, lag(score_fulltime_team) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date ASC) AS prev_game_scored_g
, lag(score_fulltime_opponent) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date ASC) AS prev_game_lost_g
, ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id, fixture_date desc) -1 AS games_order
, ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id, fixture_date) -1 AS games_order_rev
, b.rank as fifa_rank
, b.confederation
, c.is_senior
, c.is_man
, c.league_level
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
  from `bi-playground-269209.soccer_test.game_schedule_daily` a
  inner join teams b 
  on a.teams_home_id = b.team_id
  left join `bi-playground-269209.soccer_test.coaches` c
  on a.teams_home_id = c.team_id and a.fixture_id = c.fixture_id
  where date(cast(fixture_date as datetime)) >= current_date() -100 and fixture_status_short in ('FT','AET','PEN') and a.league_id not in (10, 667, 666)

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
  from `bi-playground-269209.soccer_test.game_schedule_daily` a
  inner join teams b 
  on a.teams_away_id = b.team_id
  left join `bi-playground-269209.soccer_test.coaches` c
  on a.teams_away_id = c.team_id and a.fixture_id = c.fixture_id
  where date(cast(fixture_date as datetime)) >= current_date() -100 and fixture_status_short in ('FT','AET','PEN') and a.league_id not in (10, 667, 666)
  ) a
  left join (select * from `bi-playground-269209.soccer_test.fifa_ranking` where rank_date = '2022-12-22') b
  on a.league_country = b.country_full

 left join (select a.league_id, is_senior, is_man, case when b.league_level is null then a.league_type else cast(league_level as string) end as league_level
from `bi-playground-269209.soccer_test.details_leagues` a
left join `bi-playground-269209.soccer_test.league_lvl` b
on a.league_id = b.league_id
where cast(`end` as date) >= current_date-21) c

on a.league_id = c.league_id

  ) a

  left join `bi-playground-269209.soccer_test.odds_table` b
on a.fixture_id = b.fixture_id
        
  order by team_id, fixture_date")

rsr_df <- bq_table_download(bq_project_query(projectid,sql))





# wynik na sume pkt z 3 ostatnich meczy

rsr_df <- rsr_df %>% 
  dplyr::group_by(team_id) %>%
  #dplyr::mutate(res_lag1=lag(FT_result,n=1), res_lag2=lag(FT_result,n=2), res_lag3=lag(FT_result,n=3)) %>% 
  dplyr::mutate(res_streak=paste0(lag(FT_result,n=1),lag(FT_result,n=2),lag(FT_result,n=3))) %>% 
  dplyr::mutate(pts=if_else(FT_result=="W",3,if_else(FT_result=="D",1,0))) %>% 
  dplyr::mutate(sum_pts=(lag(pts,n=1)+lag(pts,n=2)+lag(pts,n=3))) %>% ungroup(team_id)

rsr_df$home_round <- round(rsr_df$Home,1)


rsr_df1 <- rsr_df %>% group_by(game_place,sum_pts, FT_result) %>% dplyr::summarise(by_res=n())
rsr_df2 <- rsr_df %>% group_by(game_place, sum_pts) %>% dplyr::summarise(by_pts=n())
rsr_df3 <- left_join(rsr_df1,rsr_df2, by=c("game_place"="game_place", "sum_pts"="sum_pts")) %>% dplyr::mutate(ratio=by_res/by_pts)


# wygrana gospodarza na roznice pkt pomiedzy gospodarzem i gosciem z 3 ostatnich meczy

rsr_dfH <- rsr_df %>% filter(game_place=="H" & !is.na(sum_pts)) %>% select(fixture_id,home_result=FT_result,Hsp=sum_pts)
rsr_dfA <- rsr_df %>% filter(game_place=="A" & !is.na(sum_pts)) %>% select(fixture_id,Asp=sum_pts)
rsr_dfHA <- left_join(rsr_dfH,rsr_dfA,by=c("fixture_id"="fixture_id")) %>% mutate(pts_diff=Hsp-Asp)

rsr_dfHA1 <- rsr_dfHA %>% group_by(pts_diff,home_result) %>% dplyr::summarise(sp_res=n())
rsr_dfHA2 <- rsr_dfHA %>% group_by(pts_diff) %>% dplyr::summarise(sp=n())
rsr_dfHA3 <- left_join(rsr_dfHA1,rsr_dfHA2,by=c("pts_diff"="pts_diff")) %>% dplyr::mutate(ratio=sp_res/sp)

#,opp_scored=if_else(score_fulltime_opponent>0,"Y","N")


test_hg <- rsr_df %>% filter(game_place=="H",score_halftime_team==0) %>% mutate(over_1_5=if_else((score_fulltime_team+score_fulltime_opponent)>1,"Y","N"),opp_scored=if_else(score_halftime_opponent==1,"Y","N")) %>% 
  group_by(home_round,opp_scored,over_1_5) %>% summarise(abc=n()) 
test_hg1 <- rsr_df %>% filter(game_place=="H",score_halftime_team==0) %>% mutate(over_1_5=if_else((score_fulltime_team+score_fulltime_opponent)>1,"Y","N"),opp_scored=if_else(score_halftime_opponent==1,"Y","N")) %>% group_by(home_round,opp_scored) %>% summarise(def=n())
test_hg2 <- left_join(test_hg,test_hg1,by=c("home_round"="home_round","opp_scored"="opp_scored")) %>% dplyr::mutate(ratio=abc/def)


test_hg <- rsr_df %>% filter(game_place=="H",score_halftime_team==0) %>% mutate(over_1_5=if_else((score_fulltime_team+score_fulltime_opponent)>1,"Y","N"),opp_scored=if_else(score_halftime_opponent==1,"Y","N")) %>% 
  group_by(opp_scored,over_1_5) %>% summarise(abc=n()) 
test_hg1 <- rsr_df %>% filter(game_place=="H",score_halftime_team==0) %>% mutate(over_1_5=if_else((score_fulltime_team+score_fulltime_opponent)>1,"Y","N"),opp_scored=if_else(score_halftime_opponent==1,"Y","N")) %>% 
  group_by(opp_scored) %>% summarise(def=n())
test_hg2 <- left_join(test_hg,test_hg1,by=c("opp_scored"="opp_scored")) %>% dplyr::mutate(ratio=abc/def)

