library(twitteR)
#library(openssl)
#library(httpuv)
library(httr)
library(lubridate)
library(purrr)
library(tibble)
library(dplyr)
library(tidyr)
library(bigrquery)
library(DBI)
library(data.table)
library(tidyverse)




setup_twitter_oauth(consumer_key <- "IKBoSS3NLcGPqwlxfEa13zOI8",
                    consumer_secret <- "2MSBTWpp1vIZRVU0giJcQAEyHxrHmseWkHgNRiGuo67N6gxvIz",
                    access_token <- "1359905978704134144-U5b7jvt6MS1F9ui939Y7yolrLRy6Rm",
                    access_secret <- "OyW9xwtb1kUG6b4vsjEur7KtJpvsAkCdnaJGdVLGuBVxc")

origop <- options("httr_oauth_cache")
options(httr_oauth_cache = TRUE)



#tweet_info <- fixtures_df %>% 
#  filter(as.Date(as_datetime(fixture.timestamp))==Sys.Date()) %>% 
#  select(league.name,teams.home.name,teams.away.name) %>% 
#  mutate(game=paste0(teams.home.name," - ",teams.away.name))

#text <- paste0("Today games:"," ",paste(tweet_info$game,collapse = ", "))

#loosing favourite

projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

# sql <- ("select *, 
# case when h_tag is not null then concat(home_team,' vs ',away_team, ' ' ,goals_home,' - ', goals_away, ' ', h_tag) 
# else concat(home_team,' vs ',away_team, ' ' ,goals_home,' - ', goals_away, ' ') end as game_name 
# from `bi-playground-269209.soccer_test.message_events` a
# inner join `bi-playground-269209.soccer_test.live` b
# on a.fixture_id = b.fixture_id")

sql <- ("select *, concat(a.goals_home,'-',a.goals_away) as score_a, concat(b.goals_home,'-',b.goals_away) as score_b,
case when h_tag is not null and a.league_country = 'World' then concat(home_team,' vs ',away_team, ', ', status_elapsed, 'min, ' ,a.goals_home,' - ', a.goals_away,' ', h_tag)
when h_tag is not null and a.league_country != 'World' then concat(home_team,' vs ',away_team, ', ', status_elapsed, 'min, ' ,a.goals_home,' - ', a.goals_away,', ', a.league_country,' ', h_tag)
when h_tag is null and a.league_country != 'World' then concat(home_team,' vs ',away_team, ', ', status_elapsed, 'min, ' ,a.goals_home,' - ', a.goals_away,', ', a.league_country, ', ', a.league_name)
else concat(home_team,' vs ',away_team, ', ', status_elapsed, 'min, ' ,a.goals_home,' - ', a.goals_away, ', ', a.league_name) end as game_name 
from `bi-playground-269209.soccer_test.message_events` a
inner join `bi-playground-269209.soccer_test.game_schedule` b
on a.fixture_id = b.fixture_id
where fixture_status_short in ('2H','HT','1H') 
and concat(a.goals_home,'-',a.goals_away) = concat(b.goals_home,'-',b.goals_away)
and a.goals_home + a.goals_away = 1")

matches_df <- bq_table_download(bq_project_query(projectid,sql))

matches_df <- matches_df %>% arrange(fixture_id,cet_curr_time)

#matches_df$ok_to_pub <- ifelse(lag(matches_df$game_name)==matches_df$game_name,0,1)

matches_df$ok_to_pub <- ifelse(lag(matches_df$fixture_id)==matches_df$fixture_id,0,1)

matches_df$ok_to_pub <- ifelse(is.na(matches_df$ok_to_pub),1,matches_df$ok_to_pub)

text_data <- matches_df %>% arrange(fixture_id,desc(cet_curr_time)) %>% 
  group_by(fixture_id) %>% 
  mutate(n_row=row_number()) %>% ungroup(fixture_id) %>% 
  filter(ok_to_pub==1 & n_row==1)

data_string <- text_data$game_name

for (i in data_string){

if(length(i) > 0) {
  text <- paste0(paste("high chance for next #goal: ",i, "#gamblingTwitter #inplay"))
  
  tw <- twitteR::updateStatus(text)
  
} else {
  NULL
}

}


## other tags

#bettingtips
#FreePicks
#freetips
#sportspicks
#sportsbetting
#betting
#bet
#bettingsports
#freepicks
#SportsGambling
#sportsbettingtwitter
#sportsbet
#tipster
#footballbetting
#bettingtips
#bettingsport


