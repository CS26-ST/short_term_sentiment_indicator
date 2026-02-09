import pandas as pd
import matplotlib.pyplot as plt
import os

# ==============================
# CONFIG
# ==============================
LOOKBACK_DAYS = 20
MIN_COVERAGE = 0.80

DATA_PATH = "data/nifty500/close.csv"
OUTPUT_CSV = "output/nifty500_sentiment_breadth_20d.csv"
OUTPUT_IMG = "images/nifty500_sentiment_breadth_20d.png"


def main():
    os.makedirs("output", exist_ok=True)
    os.makedirs("images", exist_ok=True)

    df = pd.read_csv(DATA_PATH)
    date_col = "Date" if "Date" in df.columns else df.columns[0]

    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).set_index(date_col)

    df = df.apply(pd.to_numeric, errors="coerce")

    shifted = df.shift(LOOKBACK_DAYS)

    valid = df.notna() & shifted.notna()
    available = valid.sum(axis=1)
    positive = ((df > shifted) & valid).sum(axis=1)

    total_stocks = df.shape[1]
    coverage = available / total_stocks

    breadth = (positive / available) * 100

    result = pd.DataFrame({
        "breadth_pct": breadth,
        "positive_stocks": positive,
        "available_stocks": available,
        "coverage": coverage
    })

    result = result[result["coverage"] >= MIN_COVERAGE]
    result = result.dropna()

    result.to_csv(OUTPUT_CSV)

    # Plot
    plt.figure(figsize=(13, 6))
    plt.plot(result.index, result["breadth_pct"], linewidth=1.5)
    plt.ylim(0, 100)
    plt.grid(alpha=0.3)
    plt.title("Nifty 500 Short-Term Sentiment Breadth (Option A – 20D)")
    plt.ylabel("Breadth (%)")
    plt.xlabel("Date")
    plt.tight_layout()
    plt.savefig(OUTPUT_IMG, dpi=150)
    plt.close()


if __name__ == "__main__":
    main()
