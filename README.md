# short_term_sentiment_indicator
This script is to build short term sentiment indicator to gaudge the sentiment.

# Short-Term Sentiment Breadth (Option A)

Option A definition:
- A stock is positive on date T if Close[T] > Close[T-N]
- Breadth% = (#positive / #available) * 100
- A stock is "available" only if both Close[T] and Close[T-N] exist (handles IPO/missing data)

## Input CSV format
Wide file:
- First column: Date (or first column is treated as date)
- Remaining columns: symbols (one column per stock)
- Values: daily close prices

Example:
Date,RELIANCE,TCS,HDFCBANK
2024-01-01,2520.5,3781.0,1650.2

## Install
pip install -r requirements.txt

## Run
python src/compute_sentiment_breadth.py --universe nifty500 --close_csv data/nifty500/close.csv

### Common variants
- 10D:  --lookback 10
- 20D:  --lookback 20
- 30D:  --lookback 30

### Historical window (e.g. 2017 onwards)
python src/compute_sentiment_breadth.py \
  --universe nifty500 \
  --close_csv data/nifty500/close.csv \
  --lookback 20 \
  --start 2017-01-01 \
  --min_coverage 0.60
