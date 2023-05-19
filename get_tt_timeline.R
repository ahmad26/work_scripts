
library(rtweet)

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


tweets <- get_my_timeline(n = 100)
