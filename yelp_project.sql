-- creating table for s3 yelp data
CREATE TABLE yelp_reviews (review_text VARIANT);

-- Loading yelp data from S3
COPY INTO yelp_reviews
FROM 's3://mysnowflakebucket-v2/mysqlfolder/'
CREDENTIALS = (
  AWS_KEY_ID = '*****'
  AWS_SECRET_KEY = '*********'
)
FILE_FORMAT = (TYPE = JSON);


-- ingesting data using the "load data table" option on snowflake 
-- Lets create the user info table
create table user_info(user_id variant);

create or replace table table_user_info as
Select user_id:name::string as user_name
,user_id:user_id::string as user_id
,user_id:friends::string as friends_id
from user_info;



-- Sentiment analysis using textblob
CREATE OR REPLACE FUNCTION analyze_sentiment(text STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('textblob') 
HANDLER = 'sentiment_analyzer'
AS $$
from textblob import TextBlob
def sentiment_analyzer(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'Neutral'
    else:
        return 'Negative'
$$;

-- select * from yelp_reviews limit 5;

-- Turn JSON into table
create or replace table table_yelp_reviews as
Select review_text:business_id::string as business_id
,review_text:date::date as review_date
,review_text:stars::number as review_stars
,review_text:text::string as review_text
,review_text:review_id::string as review_id
,review_text:user_id::string as user_id
,analyze_sentiment(review_text) as sentiments
from yelp_reviews;

-- select * from table_yelp_reviews limit 10;

-- cleaning up and standardizing the sentiments column
update table_yelp_reviews
set sentiments = lower(trim(sentiments));

-- removing rows where business_id is null
--check how many rows will be deleted
Select count(*) 
from table_yelp_reviews
where business_id IS NULL;

Delete from table_yelp_reviews
where business_id is null;


-- company with the highest number of 5 star reviews
select 
    business_id, 
    count(review_stars) as five_stars_count
from table_yelp_reviews
Where review_stars = 5
group by business_id

-- users who gave 5 star reviews
select table_yelp_reviews.business_id as business_id
, table_yelp_reviews.user_id as user_id
, table_user_info.user_name as user_name
, table_yelp_reviews.review_stars as review_stars
from table_yelp_reviews
join table_user_info
ON table_yelp_reviews.user_id = table_user_info.user_id
where review_stars = 5

-- top 5 businesses by average rating
select business_id
, avg(review_stars) as avg_rating
, count(*) as total_reviews
from table_yelp_reviews
group by business_id
order by avg_rating desc, total_reviews desc
limit 10000;


-- Sentiments distribution per business
select 
    business_id,
    sum(case when sentiments = 'positive'  then 1 else 0 end) as positive_count,
    sum(case when sentiments = 'negative' then 1 else 0 end) as negative_count,
    sum(case when sentiments = 'neutral' then 1 else 0 end) as neutral_count
from table_yelp_reviews
group by business_id
order by positive_count desc;


-- which businesses are improving or declining in ratings yearly?
select business_id,
    year(review_date) as review_year,
    round(avg(review_stars), 2) as avg_monthly_rating,
    count(*) as monthly_reviews
from table_yelp_reviews
group by business_id,year(review_date)
order by business_id, review_year;


-- Most influential reviewers (users with the largest friend networks)
select 
    user_id, 
    user_name,
    case
        when friends_id is null or friends_id = '' then 0
        else length(friends_id) - length(replace(friends_id, ',', '')) + 1
    end as friend_count
from table_user_info
order by friend_count desc
limit 10000;


-- most active reviewers
select 
    yr.user_id,
    ui.user_name,
    count(*) as review_count
from table_yelp_reviews yr
join table_user_info ui
    on yr.user_id=ui.user_id
group by yr.user_id, ui.user_name
order by review_count desc
limit 1000;

-- consistency of experience
select 
    business_id,
    round(avg(review_stars), 2) as avg_rating,
    stddev(review_stars) as rating_variability,
    count(*) as review_count
from table_yelp_reviews
group by business_id
having count(*) > 20
order by rating_variability desc;

-- Early warning signals for businesses based on rising negative sentiment by year 
Select business_id,
       year(review_date) as review_year,
       sum(case when sentiments='negative' then 1 else 0 end) as negative_reviews,
       count(*) as total_reviews,
       round(100.0 * sum(case when sentiments='negative' then 1 else 0 end) / count(*), 2) as pct_negative
from table_yelp_reviews
group by business_id, year(review_date)
having count(*) > 10
order by pct_negative desc;




