

library(rtweet)
library(dplyr)
library(bigrquery)
library(DBI)
library(tidyr)
library(purrr)


## get followers

#auth_setup_default()

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

followers <- rtweet::get_followers(user = 'investgambling1', parse = TRUE)

users <- lookup_users(followers$user_id)
users <- users %>% select(user_id,screen_name,source,lang)
users$user_id <- as.character(users$user_id)
users$cet_curr_time <- format(Sys.Date(),tz="Europe/Rome")


#followers <- rtweet::get_followers(user = 'investgambling1', parse = FALSE)

#print(str(followers))

# followers1 <- followers[[1]][["ids"]]
# 
# users1 <- rtweet::lookup_users(followers1, parse = FALSE)
# 
# users2 <- tibble(users1[[1]]) %>% select(-entities,-status,-withheld_in_countries)
# 
# users3 <- users2 %>% unnest(cols = c(value))
#  
# print(head(users1))

#users1 <- users1 %>% dplyr::select(id,id_str,name,screen_name,source,lang)

# users1$user_id <- base::as.character(users1$user_id)
# 
# users1$cet_curr_time <- base::format(Sys.Date(),tz="Europe/Rome")


## check with saved info


projectid <- "bi-playground-269209"

bq_auth(path= "/opt/syek/key.json")

sql <- ("select cast(user_id as string) as user_id, screen_name, status, cast(follow_date as string) as follow_date, lang, source from `bi-playground-269209.soccer_test.followers`")

followers_df <- bq_table_download(bq_project_query(projectid,sql))


# compare both

comp <- full_join(users,followers_df,by=c("user_id"="user_id"))

comp <- comp %>% mutate(status=if_else(is.na(screen_name.y),"New",if_else(is.na(screen_name.x),"Lost","Current")))

new <- comp %>% filter(status=="New") %>% select(user_id,screen_name=screen_name.x,source=source.x,lang=lang.x,follow_date=cet_curr_time,status)
current <- comp %>% filter(status=="Current") %>% select(user_id,screen_name=screen_name.x,source=source.x,lang=lang.x,follow_date,status)
lost <- comp %>% filter(status=="Lost") %>% select(user_id,screen_name=screen_name.y,source=source.y,lang=lang.y,follow_date,status)

all_followers <- bind_rows(new,current,lost)


bigrquery::bq_auth(path= "/opt/syek/key.json")

billing = "1108793-EMEA-Bee-Fee Limited"
project = "bi-playground-269209"
data_set = "soccer_test"

con = dbConnect(
  bigrquery::bigquery(),
  project = project,
  dataset = data_set,
  billing = billing
)

dbWriteTable(con, "followers", all_followers, overwrite=TRUE)