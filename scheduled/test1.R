
library(dplyr)
library(bigrquery)
library(httr)
library(lubridate)

httr::set_config(httr::config(http_version = 0))

projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- 'select a.*
, case when a.team_id = c.home_team_id then c.goals_home else c.goals_away end as goals
, case when a.team_id = c.home_team_id then c.home_team else c.away_team end as team_name
, c.league_name
, case when a.team_id = c.home_team_id then "H" else "A" end as side
from `bi-playground-269209.soccer_test.system` a
inner join 
(select a.* from `bi-playground-269209.soccer_test.info` a
inner join
(select fixture_id, max(cet_curr_time) as last_update from `bi-playground-269209.soccer_test.info`
where date_diff(cast(cast(fixture_date as timestamp) as datetime), current_datetime(), MINUTE) > -95
group by fixture_id) b
on a.fixture_id = b.fixture_id and a.cet_curr_time = b.last_update
where goals_away = 1 and goals_home = 0) c
on a.fixture_id = c.fixture_id and a.cet_curr_time = c.cet_curr_time'

gamesInfo <- bq_table_download(bq_project_query(projectid,sql))

gamesInfo <- gamesInfo %>% select(team_id,fixture_id,minute,Blocked_Shots,Corner_Kicks,Red_Cards,Shots_on_Goal,Total_Shots,cet_curr_time,goals,team_name,league_name,side)

system1 <- gamesInfo %>% filter(side=="H" & Shots_on_Goal >= 3)







# select a.* 
#   from `bi-playground-269209.soccer_test.info` a
# inner join 
# (select fixture_id, max(cet_curr_time) as last_update from `bi-playground-269209.soccer_test.info` group by fixture_id) b
# on a.fixture_id = b.fixture_id and a.cet_curr_time = b.last_update 
# where date_diff(cast(cast(cet_curr_time as timestamp) as datetime), current_datetime(), MINUTE) > -95
# order by cet_curr_time desc
# 
# select a.*
#   , case when a.team_id = c.home_team_id then c.goals_home else c.goals_away end as goals
# from `bi-playground-269209.soccer_test.system` a
# inner join 
# (select fixture_id, max(cet_curr_time) as last_update from `bi-playground-269209.soccer_test.info` group by fixture_id) b
# on a.fixture_id = b.fixture_id and a.cet_curr_time = b.last_update
# left join `bi-playground-269209.soccer_test.info` c
# on a.fixture_id = b.fixture_id and a.cet_curr_time = c.cet_curr_time
# where date_diff(cast(cast(a.cet_curr_time as timestamp) as datetime), current_datetime(), MINUTE) > -95