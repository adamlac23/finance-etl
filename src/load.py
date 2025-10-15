from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from .utils import load_cfg, ensure_dir

def load_to_sqlite(daily: pd.DataFrame, agg: pd.DataFrame):
    cfg = load_cfg()
    db_path = Path(cfg["paths"]["warehouse_db"])
    ensure_dir(db_path.parent)
    engine = create_engine(f"sqlite:///{db_path}")
    daily.to_sql("fact_returns_daily", engine, if_exists="replace", index=False)
    agg.to_sql("dim_ticker_metrics", engine, if_exists="replace", index=False)

def persist_files(daily: pd.DataFrame, agg: pd.DataFrame):
    cfg = load_cfg()
    proc = Path(cfg["paths"]["processed_dir"])
    ensure_dir(proc)
    daily.to_parquet(proc / "returns_daily.parquet", index=False)
    agg.to_csv(proc / "ticker_metrics.csv", index=False)

def save_outputs(daily: pd.DataFrame, agg: pd.DataFrame):
    """Zapisz do plików i do SQLite."""
    persist_files(daily, agg)
    load_to_sqlite(daily, agg)