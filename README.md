### Case Study: Yelp Review Analytics with Python, AWS S3 & Snowflake
This project showcases how unstructured text can be transformed into actionable intelligence that benefits both businesses and consumers.

### Problem

Yelp reviews are a goldmine for understanding customer experiences, business performance, and user influence. However, the raw data is unstructured (JSON format) and scattered, making it difficult to extract actionable insights such as:
- Which businesses consistently delight or disappoint customers?
- Who are the most influential or active reviewers?
- How do sentiment trends signal business health over time?
The challenge was to design an end-to-end pipeline that could:
- Ingest, clean, and structure raw Yelp JSON data.
- Perform sentiment analysis at scale.
- Generate insights on businesses, users, and temporal trends.

### Insights

- **Consistency matters**: Some businesses had high ratings but large variability, suggesting inconsistent experiences.
- **Early warning signals**: Businesses with increasing negative sentiment over time showed clear decline patterns.
- **Reviewer power**: A small group of active, influential reviewers shaped much of the Yelp ecosystem.
- **Customer happiness clusters**: Businesses with consistently high positive sentiment had stronger long-term performance.


### Impact

- For businesses: This pipeline provides an automated way to detect declining performance early, track sentiment shifts, and benchmark against competitors.
- For platforms: Enhances the ability to identify key influencers and maintain data quality by filtering inactive users.
- For analysts: Demonstrates the power of combining Snowflake’s scalability with Python UDFs for advanced NLP at query time


### Analysis

Key steps implemented in Snowflake + Python UDFs:

Data Preparation & Cleaning
- Loaded JSON reviews from S3 into Snowflake.
- Extracted structured fields (business_id, stars, user_id, text, review_date).
- Standardized sentiment labels (positive, neutral, negative).
- Removed rows with null business_id.

Sentiment Analysis
- Built a Python UDF in Snowflake using TextBlob.
- Classified each review as Positive, Neutral, or Negative.

Business Insights
- Identified top businesses by 5-star reviews and average rating.
- Measured sentiment distribution per business.
- Tracked yearly trends in ratings to spot improving/declining businesses.
- Evaluated consistency of experience using rating variability (standard deviation).
- Built early warning signals for businesses with rising negative sentiment.

User Insights
- Ranked most active reviewers (by review count).
- Ranked most influential reviewers (by friend network size).
- Linked high-rated reviews to specific users.

### Data
- Source: Yelp Open Dataset (JSON reviews + user info).
- Volume: Millions of reviews containing business IDs, review text, ratings, and user connections.
- Challenge: Large single JSON files needed to be broken into manageable chunks for ingestion.

### Pipeline
- Python → Split Yelp JSON files into smaller chunks.
- AWS S3 → Stored processed review files for scalable access.
- Snowflake Ingestion → Used COPY INTO to load JSON data from S3 into Snowflake tables.
- Transformation → Parsed JSON into structured tables (table_yelp_reviews, table_user_info).
- Sentiment Analysis → Built a Python UDF in Snowflake (using TextBlob) to classify reviews as positive, neutral, or negative.
