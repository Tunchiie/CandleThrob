DROP TABLE IF EXISTS stock_prices;


CREATE TABLE IF NOT EXISTS stock_prices (
    stock TEXT,
    date TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL
    volume INTEGER
);

DROP TABLE IF EXISTS sentiment_scores;


CREATE TABLE IF NOT EXISTS sentiment_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock TEXT,
    date TEXT,
    source_type TEXT,
    content TEXT,
    polarity REAL,
    subjectivity REAL
);