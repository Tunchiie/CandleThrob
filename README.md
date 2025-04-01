# CandleThrob – Market Sentiment Signal Analysis

---

##  Data Structure & Preprocessing

- Raw Reddit data is collected per stock using PRAW (title + text + timestamp)
- Sentiment is scored using `TextBlob` (polarity & subjectivity)
- Market data includes: `close price`, `volume`, `daily return`
- Calculated columns:
  - `avg_sentiment`, `total_mentions`, `signal_strength = avg_sentiment * total_mentions`
  - `daily_return = (price_t - price_t-1) / price_t-1`

Each row represents a **(date, ticker)** pair.

---

## Background and Overview

Platforms like Reddit have evolved into real-time trading sentiment hubs that influence stock trajectory. Posts and comment sentiment can preemptively signal market behavior — yet most trading strategies ignore this soft data and focus on stock patterns. This project analyzes Reddit sentiment for selected stocks, combines it with historical market data, and generates signal scores, visual alerts, and behavioral patterns.

It calculates:
- **Sentiment Polarity & Subjectivity**
- **Daily Return & Volume**
- **Custom Signal Strength**
- **Spike alerts**  
And visualizes all of this across multiple dashboards and Python plots.

---

## Executive Summary

CandleThrob found that sentiment spikes from Reddit posts **occasionally coincide with price volatility** or shifts in trading volume — but the correlation is relatively weak overall. However, tracking **sudden polarity spikes** proved useful for **alert-based monitoring**.

![Sentiment Spike - AAPL](https://github.com/Tunchiie/CandleThrob/blob/14a63037bf7fee2d3a5f9f25671ed98cbb70659e/images/AAPL%20Sentiment%20Spike.png)

--- 
##  Evaluation Metrics

- **Sentiment Polarity** → Range: [-1, 1]
- **Subjectivity** → Range: [0, 1]
- **Daily Return** → Price performance metric
- **Volume** → Market interest
- **Signal Strength** → Custom momentum score
- **Spike Detection** → Based on deviation from rolling average

---

##  Insights

### 1.  Sentiment Spike Detection  
- CandleThrob successfully detected sharp deviations in sentiment polarity, identifying potential “signal triggers” across all five tickers.

  NVDA had the most frequent and volatile sentiment swings, indicating it’s a highly reactive or polarizing stock in Reddit communities.

  ![Sentiment Spike - NVDA](https://github.com/Tunchiie/CandleThrob/blob/14a63037bf7fee2d3a5f9f25671ed98cbb70659e/images/NVDA%20Sentiment%20spike.png)

- AAPL and GOOGL both showed clean upward spikes in polarity that corresponded with large clusters of posts. These spikes often preceded small price bumps.
  
  ![Sentiment Spike - AAPL](https://github.com/Tunchiie/CandleThrob/blob/14a63037bf7fee2d3a5f9f25671ed98cbb70659e/images/AAPL%20Sentiment%20Spike.png)

  ![Sentiment Spike - GOOGL](https://github.com/Tunchiie/CandleThrob/blob/14a63037bf7fee2d3a5f9f25671ed98cbb70659e/images/GOOGL%20Sentiment%20spike.png)

- MSFT and AMZN had fewer and more muted spikes, suggesting less Reddit attention or more neutral conversation.

  ![Sentiment Spike - AMZN](https://github.com/Tunchiie/CandleThrob/blob/14a63037bf7fee2d3a5f9f25671ed98cbb70659e/images/AMZN%20Sentiment%20Spike.png)

  ![Sentiment Spike - MSFT](https://github.com/Tunchiie/CandleThrob/blob/14a63037bf7fee2d3a5f9f25671ed98cbb70659e/images/MSFT%20sentiment%20spike.png)

---

### 2. Weak Correlation Across Metrics

The correlation matrix revealed:

 -  Slight negative correlation (-0.12) between sentiment polarity and daily returns

 -  No meaningful relationship between subjectivity and any financial metric

 -  Very weak correlation between sentiment and volume

This confirms that sentiment alone is not a linear predictor of return — reinforcing the need for combined indicators like signal strength.

![Correlation Matrix](https://github.com/Tunchiie/CandleThrob/blob/14a63037bf7fee2d3a5f9f25671ed98cbb70659e/images/Correlation%20Matrix.png)
---

### 3. Most Returns Cluster Around Neutral Sentiment
The scatter plot of sentiment polarity vs daily return showed:

 - Most Reddit-driven sentiment hovered between -0.2 and 0.2 polarity

 - Returns clustered around zero, with no obvious pattern favoring positive or negative sentiment

This reinforces the idea that Reddit chatter is frequent, but not always market-moving — especially without large volume or external catalysts.

![Polarity vs Return](https://github.com/Tunchiie/CandleThrob/blob/117b51f392d2ae24685ec24ab8166e1316844b01/images/Polarity%20vs%20Daily%20Return%20by%20ticker.png)

- Most return events occur between polarity scores of -0.2 and 0.2
- No clear clustering in positive or negative zones

---

### 4. Volume Doesn't Always Match Sentiment Hype

When comparing sentiment data to actual price and volume movements:

 - NVDA showed price climbs with low or negative sentiment

 - AAPL’s volume increased steadily, regardless of whether sentiment was rising or falling

This suggests that Reddit sentiment does not reliably explain volume surges or price trends, and could benefit from being combined with technical signals or macro triggers.

![Price and Volume](https://github.com/Tunchiie/CandleThrob/blob/117b51f392d2ae24685ec24ab8166e1316844b01/images/Price%20and%20Volume%20over%20Time.png)

---

### 5. Sentiment Distribution Varies by Stock

The stacked bar chart comparing sentiment types (positive/neutral/negative) across tickers showed:

 - NVDA had the most positive sentiment days and few negatives

 - GOOGL had a high count of neutral mentions, indicating balanced discussions

 - AAPL, MSFT, and AMZN saw more even distributions, making them less predictable from sentiment alone

This breakdown is useful for assessing signal reliability — stocks with highly polarized or positive sentiment might be more reactive to buzz.

![Sentiment Breakdown](https://github.com/Tunchiie/CandleThrob/blob/14a63037bf7fee2d3a5f9f25671ed98cbb70659e/images/Sentiment%20Breakdown%20by%20stock.png)

---

### 6.  Sentiment Polarity Over Time

Plotting all ticker sentiments over time shows that:

 - Reddit’s attention rotates, with different stocks peaking in polarity at different moments

 - NVDA and GOOGL showed more volatility in sentiment polarity over time

 - MSFT and AMZN remained relatively neutral

This confirms the non-stationary nature of retail sentiment, and supports building ticker-specific thresholds rather than using one-size-fits-all logic.

![Sentiment Over Time](https://github.com/Tunchiie/CandleThrob/blob/14a63037bf7fee2d3a5f9f25671ed98cbb70659e/images/Sentiment%20polarity%20over%20time.png)

---

## Recommendations

- **Signal strength alerts** (polarity × mentions) can help surface early retail activity
- Combine this with price/volume breakouts for stronger indicators
- Use as a **filtering layer**, not a sole trading signal

---

##  Caveats & Assumptions

- Reddit volume ≠ actual market volume
- Sentiment model (TextBlob) may misinterpret sarcasm or slang
- Signal strength isn't normalized across tickers with different audience sizes

---

## Recommendations
- Use VADER or fine-tuned BERT models for sentiment
- Backtest signal spikes against next-day returns
- Build web interface with alerts
- Introduce real-time tracking & update cycles
- Dynamic threshold control for spike alerts
- Smoother signal strength (EMA or Bollinger Bands)
- Weight sentiment by upvotes/karma
- Add volatility as a secondary signal layer
