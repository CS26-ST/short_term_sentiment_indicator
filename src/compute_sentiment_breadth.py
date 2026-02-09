import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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

    # ------------------------------
    # Load data
    # ------------------------------
    df = pd.read_csv(DATA_PATH)

    date_col = "Date" if "Date" in df.columns else df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    df = df.sort_values(date_col).set_index(date_col)

    # ensure numeric
    df = df.apply(pd.to_numeric, errors="coerce")

    # ------------------------------
    # Option A logic
    # ------------------------------
    shifted = df.shift(LOOKBACK_DAYS)

    valid_mask = df.notna() & shifted.notna()
    available = valid_mask.sum(axis=1)
    positive = ((df > shifted) & valid_mask).sum(axis=1)

    total_stocks = df.shape[1]
    coverage = available / total_stocks

    breadth_pct = (positive / available) * 100

    result = pd.DataFrame({
        "breadth_pct": breadth_pct,
        "positive_stocks": positive,
        "available_stocks": available,
        "coverage": coverage,
        "total_stocks": total_stocks
    })

    result = result[result["coverage"] >= MIN_COVERAGE]
    result = result.dropna(subset=["breadth_pct"])

    # ------------------------------
    # Save CSV
    # ------------------------------
    result.to_csv(OUTPUT_CSV, index_label="Date")

    # ------------------------------
    # Plot (FORCED Month–Year axis)
    # ------------------------------
    result.index = pd.to_datetime(result.index)

    fig, ax = plt.subplots(figsize=(14, 6))

    ax.plot(
        result.index,
        result["breadth_pct"],
        linewidth=1.6
    )

    ax.set_ylim(0, 100)
    ax.set_ylabel("Breadth (%)")
    ax.set_xlabel("Date")
    ax.set_title("Nifty 500 Short-Term Sentiment Breadth (Option A – 20D)")

    # 🔥 Force Month + Year ticks (mentor-style)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))

    ax.xaxis.set_minor_locator(mdates.MonthLocator())

    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate(rotation=45)

    plt.tight_layout()
    plt.savefig(OUTPUT_IMG, dpi=150)
    plt.close()


if __name__ == "__main__":
    main()
