from .utils import load_cfg
from .extract import extract_all
from .transform import to_base_currency, compute_returns
from .load import save_outputs
from .viz import plot_equity_curves
from pathlib import Path

def run():
    print("Extract…")
    data = extract_all()   

    print("Transform…")
    cfg = load_cfg()
    base = cfg["project"]["base_currency"] 
    prices_base = to_base_currency(
        prices=data["stocks"],
        fx=data["fx"],
        base=base
    )
    daily_rets, agg = compute_returns(prices_base)

    print("Load…")
    save_outputs(daily_rets, agg)  

    print("Visualize…")
    fig_dir = Path(cfg["paths"]["figures_dir"])
    fig_dir.mkdir(parents=True, exist_ok=True)
    out_png = fig_dir / "equity_curves.png"
    plot_equity_curves(prices_base, out_png)
    print("Done.")

if __name__ == "__main__":
    run()