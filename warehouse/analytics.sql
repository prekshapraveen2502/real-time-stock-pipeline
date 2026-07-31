-- Example analytics queries over the Gold daily summary table.

-- Daily summary for every company.
SELECT symbol, date, day_high, day_low, avg_close, total_volume
FROM daily_summary
ORDER BY symbol;

-- Highest average closing price.
SELECT symbol, avg_close
FROM daily_summary
ORDER BY avg_close DESC
LIMIT 1;

-- Most actively traded company by volume.
SELECT symbol, total_volume
FROM daily_summary
ORDER BY total_volume DESC
LIMIT 1;

-- How much each stock moved during the day (high minus low).
SELECT symbol, date, day_high - day_low AS price_range
FROM daily_summary
ORDER BY price_range DESC;
