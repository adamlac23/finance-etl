from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

def plot_equity_curves(prices_pln: pd.DataFrame, out_png: Path):
    out_png = Path(out_png)
    out_png.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(11, 5))
    has_any = False
    for col in prices_pln.columns:
        s = prices_pln[col].dropna()
        if s.empty:
            continue
        ax.plot(s.index, s.values, label=col)
        has_any = True

    ax.set_title("Equity Curves (in base currency)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    ax.grid(True, linestyle="--", alpha=0.4)
    if has_any:
        ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=160)
    plt.close(fig)
    return out_png