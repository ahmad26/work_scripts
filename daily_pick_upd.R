
library(httr)
#library(jsonlite)
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
library(ggplot2)



projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- ("select a.fixture_date
, a.fixture_id
, a.cet_curr_time
, a.status_short
, a.who_fav
, a.home
, a.draw
, a.away
, a.goals_home
, a.goals_away
, a.league_name
, a.league_country
, continent
, sub_region
, c.league_type
, a.home_team
, a.away_team
, a.status_elapsed
, league_level
, is_man
, is_senior
, score_fulltime_home
, score_fulltime_away
, score_halftime_home
, score_halftime_away
, score_halftime_home + score_halftime_away as total_goals_ht
, score_fulltime_home + score_fulltime_away as total_goals_ft
, case when score_fulltime_home > score_fulltime_away then 'H' when score_fulltime_home < score_fulltime_away then 'A' else 'D' end as who_won
, case when score_fulltime_home != 0 and score_fulltime_away != 0 then 'Y' else 'N' end as bts
from
(select *, ROW_NUMBER() OVER(PARTITION BY fixture_id ORDER BY fixture_id, cet_curr_time) AS first_upd
from `bi-playground-269209.soccer_test.message_events`
qualify first_upd = 1) a
inner join `bi-playground-269209.soccer_test.game_schedule_daily` b
on a.fixture_id = b.fixture_id
left join (select distinct league_id, league_type, country_code from `bi-playground-269209.soccer_test.details_leagues`) c
on a.league_id = c.league_id
left join `bi-playground-269209.soccer_test.league_lvl` h
on a.league_id = h.league_id
left join `bi-playground-269209.soccer_test.country_to_continent` i
on c.country_code = i.code_2
where a.goals_home + a.goals_away = 1
#and (a.goals_home + a.goals_away) = (score_fulltime_home + score_fulltime_away)
and status_short in ('1H','HT','2H')
and date(a.fixture_date) >= current_date()-1")

results_df <- bq_table_download(bq_project_query(projectid,sql))

results_df <- results_df %>% mutate(fav_stat=if_else(who_fav == who_won,"Y",if_else(who_won=="D",who_won,"N"))) %>% filter(!is.na(total_goals_ft) & total_goals_ft > 0)

results_df <- results_df %>% mutate(over1_5=if_else(total_goals_ft>1,"Y","N"), 
                                    under1_5=if_else(total_goals_ft==1,"Y","N"), 
                                    over2_5=if_else(total_goals_ft>2,"Y","N"),
                                    bts=if_else(bts=="Y","Y","N")
                                    ) 
  


#set twitter message

tgp <- length(unique(results_df$fixture_id))

under1_5 <- length(results_df$total_goals_ft[results_df$total_goals_ft==1])
over1_5 <- length(results_df$total_goals_ft[results_df$total_goals_ft>1])
over2_5 <- length(results_df$total_goals_ft[results_df$total_goals_ft>2])
fav_won <- length(results_df$fav_stat[results_df$fav_stat=="Y"])
fav_lost <- length(results_df$fav_stat[results_df$fav_stat=="N"])
fav_draw <- length(results_df$fav_stat[results_df$fav_stat=="D"])
bts <- length(results_df$bts[results_df$bts=="Y"])



text <- paste0("Summary from ",Sys.Date()-1, ", total games picked: ", tgp,
               ", games under 1.5: ",under1_5, ", games over 1.5: ", over1_5,
               ", games over 2.5: ", over2_5, ", favourite team wins: ", fav_won,
               ", underdog wins: ", fav_lost, ", draws: ", fav_draw, ", both teams scored: ", bts, 
               " #gamblingTwitter #inplay")

test <- results_df %>% mutate(result_class=if_else(total_goals_ft == 1,"A",if_else(total_goals_ft == 2,"B",if_else(total_goals_ft >= 2,"C", "D")))) %>%
  group_by(league_level, continent, status_short, who_fav, result_class) %>%
  dplyr::summarise(cnt = n()) %>%
  ungroup() %>%
  spread(result_class,cnt) %>%
  mutate(all_games=(replace_na(A,0)+replace_na(B,0)+replace_na(C,0))) %>%
  mutate(prc_1_5=(replace_na(B,0)+replace_na(C,0))/all_games) %>% 
  mutate(prc_2_5=(replace_na(C,0))/all_games)


daily_stats <- results_df %>% mutate(status2=ifelse(status_short=='2H',"2_half","1_half")) %>% 
  mutate(date=as.Date(floor_date(fixture_date,unit="month"))) %>% 
  mutate(under1_5=ifelse(total_goals_ft==1,"Y","N")) %>%
  mutate(over1_5=ifelse(total_goals_ft>1,"Y","N")) %>% 
  mutate(over2_5=ifelse(total_goals_ft>2,"Y","N")) %>% 
  group_by(date,status2,over1_5) %>% 
  dplyr::summarise(under1_5_cnt=n())

#daily_stats$newdate <- strptime(as.character(daily_stats$date), "%Y-%m-%d")


d <- ggplot(daily_stats, aes(fill=over1_5, x=date, y=under1_5_cnt)) + 
  geom_col(position="stack") + scale_x_date(date_breaks = "1 month") + 
  theme(axis.text.x=element_text(angle = -90, hjust = 0), axis.title.y = element_blank()) + 
  labs(x = "month",y = "over / under 1.5 goals") + 
  #geom_text(aes(label=under1_5_cnt),vjust = -0.2) + 
  facet_wrap(~status2)

d

ggplot(results_df , aes(fill=over1_5, x=over1_5)) + geom_bar(position="stack") + geom_text(aes(label=over1_5))

e <- ggplot(results_df, aes(x=under1_5_cnt)) + 
  geom_col(position="stack") + scale_x_date(date_breaks = "1 month") + 
  # theme(axis.text.x=element_text(angle = -90, hjust = 0), axis.title.y = element_blank()) + 
  # labs(x = "month",y = "over / under 1.5 goals") + 
  #geom_text(aes(label=under1_5_cnt),vjust = -0.2) + 
  facet_wrap(~status2)

e

library(plyr)

success_by_min1 <- results_df %>% mutate(min_round=round_any(status_elapsed, 5, f = ceiling)) %>% group_by(who_fav,min_round,total_goals_ft) %>% dplyr::summarise(by_goals=n())

success_by_min2 <- results_df %>% mutate(min_round=round_any(status_elapsed, 5, f = ceiling)) %>% group_by(who_fav,min_round) %>% dplyr::summarise(by_min=n())
success_by_min <- left_join(success_by_min1,success_by_min2,by=c("who_fav"="who_fav", "min_round"="min_round")) %>% mutate(ratio=by_goals/by_min) %>% filter(total_goals_ft==1)
#success_by_min3 <- success_by_min %>% mutate(min_round=round_any(status_elapsed, 5, f = ceiling))

ggplot(success_by_min , aes(x=min_round, y=by_min)) + geom_bar(position="dodge", stat="identity")


#post tweet

auth_setup_default()

appname <- "post_app2021"
consumer_key <- "IKBoSS3NLcGPqwlxfEa13zOI8"
consumer_secret <- "2MSBTWpp1vIZRVU0giJcQAEyHxrHmseWkHgNRiGuo67N6gxvIz"
access_token <- "1359905978704134144-U5b7jvt6MS1F9ui939Y7yolrLRy6Rm"
access_secret <- "OyW9xwtb1kUG6b4vsjEur7KtJpvsAkCdnaJGdVLGuBVxc"


twitter_token <- create_token(
  app = appname,
  consumer_key,
  consumer_secret,
  access_token,
  access_secret)


post_tweet(
  status = text)
