CREATE TABLE fact_returns_daily (
	date DATETIME, 
	"Ticker" TEXT, 
	"Return" FLOAT
);
CREATE TABLE dim_ticker_metrics (
	"Ticker" TEXT, 
	days BIGINT, 
	mean_return FLOAT, 
	vol FLOAT, 
	max_drawdown FLOAT, 
	mean_return_annual FLOAT, 
	vol_annual FLOAT
);
