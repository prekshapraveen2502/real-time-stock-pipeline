-- Example analytics queries over the Gold star schema
-- (fact_daily_prices joined to dim_ticker and dim_date).

-- Daily summary with company names and date attributes.
SELECT t.symbol, t.company_name, d.full_date, f.day_high, f.day_low, f.avg_close, f.total_volume
FROM fact_daily_prices f
JOIN dim_ticker t ON f.ticker_key = t.ticker_key
JOIN dim_date d ON f.date_key = d.date_key
ORDER BY t.symbol, d.full_date;

-- Company with the highest average closing price.
SELECT t.company_name, f.avg_close
FROM fact_daily_prices f
JOIN dim_ticker t ON f.ticker_key = t.ticker_key
ORDER BY f.avg_close DESC
LIMIT 1;

-- Total traded volume by day of week.
SELECT d.day_of_week, SUM(f.total_volume) AS total_volume
FROM fact_daily_prices f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.day_of_week
ORDER BY total_volume DESC;

-- Daily price range (high minus low) per company.
SELECT t.symbol, d.full_date, f.day_high - f.day_low AS price_range
FROM fact_daily_prices f
JOIN dim_ticker t ON f.ticker_key = t.ticker_key
JOIN dim_date d ON f.date_key = d.date_key
ORDER BY price_range DESC;

-- Current ticker directory (SCD Type 2: only the live version of each ticker).
SELECT symbol, company_name, currency, effective_from
FROM dim_ticker
WHERE is_current
ORDER BY symbol;
