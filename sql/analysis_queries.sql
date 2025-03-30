-- This SQL script contains various queries to analyze stock prices and sentiment scores.
-- 1. Get the average stock price and sentiment scores for each stock
-- 2. Get the average sentiment scores for each stock
-- 3. Get the average sentiment scores for each stock by date
-- 4. Get the average sentiment scores for each stock by date and ticker
-- 5. Get the average sentiment scores for each stock by date and ticker, and join with stock prices
-- 6. Get the all-time most mentioned tickers
-- Average stock price and sentiment scores for each stock
SELECT 
  s.ticker,
  AVG(s.price) AS avg_price,
  AVG(ss.sentiment_score) AS avg_sentiment_score
FROM stock_prices s
JOIN sentiment_scores ss ON s.ticker = ss.ticker
GROUP BY s.ticker
ORDER BY s.ticker;


-- All-time most mentioned tickers
SELECT 
  ticker,
  COUNT(*) AS total_mentions
FROM sentiment_scores
GROUP BY ticker
ORDER BY total_mentions DESC



-- Weekly top-mentioned tickers
SELECT 
  ticker,
  STRFTIME('%Y-%W', date) AS week,
  COUNT(*) AS mentions_per_week
FROM sentiment_scores
GROUP BY week, ticker
ORDER BY week DESC, mentions_per_week DESC;


-- Average sentiment scores for each stock
SELECT 
  ticker,
  DATE(date) AS date,
  ROUND(AVG(polarity), 3) AS avg_sentiment,
  COUNT(*) AS total_mentions
FROM sentiment_scores
GROUP BY ticker, DATE(created_utc)
ORDER BY date DESC;


-- Average sentiment scores for each stock by date
SELECT
  s.ticker,
  DATE(s.date) AS date,
  ROUND(AVG(s.polarity), 3) AS avg_sentiment,
  COUNT(*) AS total_mentions,
  sp.open,
  sp.close,
  sp.volume
FROM sentiment_scores s
JOIN stock_prices sp
  ON s.ticker = sp.ticker AND DATE(s.date) = DATE(sp.date)
GROUP BY s.ticker, DATE(s.date)
ORDER BY date DESC;
