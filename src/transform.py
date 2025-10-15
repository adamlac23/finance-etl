import pandas as pd

def to_base_currency(prices: pd.DataFrame,
                     fx: pd.DataFrame,
                     base: str,
                     prices_ccy: str = "USD",
                     ecb_base: str = "EUR") -> pd.DataFrame:
    """
    Konwertuje ceny (domyślnie w USD) do waluty bazowej `base` używając kursów ECB (EUR->XXX).
    Zwraca wide DataFrame: index=date, columns=tickery (ceny w walucie bazowej).
    """
    if prices.empty:
        raise ValueError("prices is empty in to_base_currency()")

    fx = fx.copy()
    fx.columns = [c.strip().lower() for c in fx.columns]
    if not {"date", "currency", "rate_vs_eur"} <= set(fx.columns):
        raise ValueError("FX dataframe must have columns: date, currency, rate_vs_eur")
    fx["date"] = pd.to_datetime(fx["date"])

    if prices_ccy.upper() == base.upper():
        out = prices.copy()
        out.index = pd.to_datetime(out.index)
        out.index.name = "date"
        return out

    eur_to_base = (fx.loc[fx["currency"].str.upper() == base.upper(), ["date", "rate_vs_eur"]]
                     .rename(columns={"rate_vs_eur": "eur_to_base"}))
    eur_to_prices = (fx.loc[fx["currency"].str.upper() == prices_ccy.upper(), ["date", "rate_vs_eur"]]
                       .rename(columns={"rate_vs_eur": "eur_to_prices"}))
    rates = pd.merge(eur_to_base, eur_to_prices, on="date", how="inner").sort_values("date")
    if rates.empty:
        raise ValueError("No overlap between FX series for base and prices currency.")
    rates["price_to_base"] = rates["eur_to_base"] / rates["eur_to_prices"]
    rates = rates.set_index("date").ffill()

    out = prices.copy()
    out.index = pd.to_datetime(out.index)
    out = out.sort_index().resample("D").last()
    out = out.join(rates[["price_to_base"]], how="left").ffill()
    converted = out[out.columns.difference(["price_to_base"])].multiply(out["price_to_base"], axis=0)
    converted.index.name = "date"
    return converted

def compute_returns(prices_base: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Wejście: wide DataFrame (index=date, columns=tickery) z cenami w walucie bazowej.
    Zwraca:
      - daily_rets (long): columns = [date, Ticker, Return]
      - agg (per Ticker): dni, średnia, vol, max drawdown, annualizacje
    """
    prices = prices_base.sort_index()
    rets_wide = prices.pct_change().dropna(how="all")
    daily = (rets_wide
             .reset_index()
             .melt(id_vars="date", var_name="Ticker", value_name="Return")
             .dropna())

    agg = daily.groupby("Ticker").agg(
        days=("Return", "count"),
        mean_return=("Return", "mean"),
        vol=("Return", "std"),
    ).reset_index()

    def max_dd(x):
        cum = (1 + x).cumprod()
        peak = cum.cummax()
        return (cum / peak - 1).min()

    mdd = daily.groupby("Ticker")["Return"].apply(max_dd).rename("max_drawdown")
    agg = agg.merge(mdd, on="Ticker", how="left")
    agg["mean_return_annual"] = agg["mean_return"] * 252.0
    agg["vol_annual"] = agg["vol"] * (252.0 ** 0.5)

    return daily, agg