SELECT 
    s.stock,
    s.date,
    s.close,
    s.volume,
    a.polarity,
    a.subjectivity
FROM stock_prices s
JOIN (
    SELECT stock, date, 
           AVG(polarity) AS polarity,
           AVG(subjectivity) AS subjectivity
    FROM sentiment_scores
    GROUP BY stock, date
) a
ON s.stock = a.stock AND s.date = a.date
ORDER BY s.stock, s.date;
