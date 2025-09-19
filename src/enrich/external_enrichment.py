"""
External data enrichment utilities
- Public holidays via Nager.Date
- FX rates via Frankfurter
"""

from __future__ import annotations
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd
import requests


class ExternalEnrichment:
    def __init__(self, holiday_country_code: str = 'US', fx_target_currency: str = 'USD', timeout: int = 20):
        self.holiday_country_code = holiday_country_code
        self.fx_target_currency = fx_target_currency
        self.timeout = timeout
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)

    # -------------------- Holidays --------------------
    def _fetch_holidays(self, years: List[int]) -> pd.DataFrame:
        """Fetch public holidays for given years from Nager.Date API."""
        rows: List[Dict[str, Any]] = []
        for y in sorted(set([y for y in years if pd.notnull(y)])):
            url = f"https://date.nager.at/api/v3/PublicHolidays/{int(y)}/{self.holiday_country_code}"
            try:
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                data = resp.json()
                for item in data:
                    rows.append({
                        'date': item.get('date'),  # YYYY-MM-DD
                        'localName': item.get('localName'),
                        'name': item.get('name'),
                        'countryCode': item.get('countryCode'),
                        'types': ','.join(item.get('types', [])) if isinstance(item.get('types'), list) else item.get('types'),
                    })
            except Exception as e:
                self.logger.warning(f"Holiday fetch failed for {y}: {e}")
        return pd.DataFrame(rows)

    def enrich_with_holidays(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'transaction_date' not in df.columns:
            return df
        df_out = df.copy()
        dt = pd.to_datetime(df_out['transaction_date'], errors='coerce')
        years = dt.dt.year.dropna().astype(int).unique().tolist()
        h = self._fetch_holidays(years)
        if h.empty:
            df_out['is_public_holiday'] = False
            return df_out
        h['date'] = pd.to_datetime(h['date'], errors='coerce')
        h = h.dropna(subset=['date'])
        h['date_str'] = h['date'].dt.strftime('%Y-%m-%d')
        df_out['is_public_holiday'] = df_out['transaction_date'].isin(h['date_str'])
        return df_out

    # -------------------- FX --------------------
    def _fetch_fx_rates(self, dates: List[str], from_currencies: List[str]) -> pd.DataFrame:
        """Fetch FX rates from Frankfurter API (base=EUR). We'll convert via EUR if needed."""
        rows: List[Dict[str, Any]] = []
        unique_dates = sorted(set([d for d in dates if isinstance(d, str) and len(d) == 10]))
        unique_curs = sorted(set([c for c in from_currencies if isinstance(c, str) and c]))
        if not unique_dates or not unique_curs:
            return pd.DataFrame()
        for d in unique_dates:
            try:
                url = f"https://api.frankfurter.app/{d}?from=EUR"
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                payload = resp.json()
                rates = payload.get('rates', {})
                rates['EUR'] = 1.0
                rows.append({'date': d, **rates})
            except Exception as e:
                self.logger.warning(f"FX fetch failed for {d}: {e}")
        return pd.DataFrame(rows)

    def enrich_with_fx(self, df: pd.DataFrame, target_currency: Optional[str] = None) -> pd.DataFrame:
        if 'transaction_date' not in df.columns or 'currency' not in df.columns:
            return df
        target = (target_currency or self.fx_target_currency or 'USD').upper()
        df_out = df.copy()
        # Gather needed dates and currencies
        dates = df_out['transaction_date'].dropna().astype(str).tolist()
        from_curs = df_out['currency'].dropna().astype(str).str.upper().tolist()
        fx = self._fetch_fx_rates(dates, from_curs)
        if fx.empty:
            return df_out
        # Melt wide rates into tidy for easy lookup
        fx_melt = fx.melt(id_vars=['date'], var_name='cur', value_name='rate_per_EUR')
        # Join by date
        df_out = df_out.merge(fx_melt, left_on=['transaction_date'], right_on=['date'], how='left')
        # Compute conversion factor: amount_in_target = amount_in_source * (EUR->target)/(EUR->source)
        # For that we need EUR->target on same date
        eur_to_target = fx_melt[fx_melt['cur'] == target][['date', 'rate_per_EUR']].rename(columns={'rate_per_EUR': 'eur_to_target'})
        df_out = df_out.merge(eur_to_target, left_on='transaction_date', right_on='date', how='left', suffixes=('', '_t'))
        # Source currency EUR->source
        src_cur_rate = fx_melt.rename(columns={'cur': 'src_cur', 'rate_per_EUR': 'eur_to_src'})
        df_out = df_out.merge(src_cur_rate[['date', 'src_cur', 'eur_to_src']], left_on=['transaction_date','currency'], right_on=['date','src_cur'], how='left')
        # Avoid division by zero
        df_out['fx_factor'] = (df_out['eur_to_target'] / df_out['eur_to_src']).replace([pd.NA, float('inf')], pd.NA)
        for col in ['net_transaction_amount', 'credit_amount', 'debit_amount']:
            if col in df_out.columns:
                df_out[f'{col}_{target}'] = (df_out[col] * df_out['fx_factor']).astype(float)
        # Cleanup helper columns
        drop_cols = [c for c in ['rate_per_EUR','eur_to_target','eur_to_src','src_cur','date','date_t'] if c in df_out.columns]
        df_out = df_out.drop(columns=drop_cols)
        return df_out

    def enrich(self, df: pd.DataFrame, enable_holidays: bool = True, enable_fx: bool = False, fx_target_currency: str = 'USD') -> pd.DataFrame:
        out = df
        if enable_holidays:
            out = self.enrich_with_holidays(out)
        if enable_fx:
            out = self.enrich_with_fx(out, target_currency=fx_target_currency)
        return out
