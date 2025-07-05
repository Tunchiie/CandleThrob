import pytest
import pandas as pd
from ingestion.fetch_data import DataIngestion
from ingestion.enrich_data import EnrichMacros

"""
    The code contains multiple test functions for enriching and transforming macroeconomic data using
    the EnrichMacros class.
    It includes tests for macroeconomic data enrichment, lagging, rolling means, percentage changes,
    z-scores, and handling empty DataFrames.
"""

@pytest.fixture
def macros():
    print("Setting up macroeconomic data for testing...")
    data = DataIngestion()
    data.fetch_fred_data()
    return data.macro_df

def test_enrich_macros(sample_macros):
    print("Testing macroeconomic data enrichment...")
    macros = EnrichMacros(sample_macros)
    macros.transform_macro_data()

    assert isinstance(macros.macro_df, pd.DataFrame)
    assert not macros.macro_df.empty
    assert "GDP" in macros.macro_df.columns
    assert "UNRATE" in macros.macro_df.columns

    macros.lag_macro_data(cols=["GDP", "UNRATE"], lag_periods=[30, 60, 90])
    assert "GDP_lagged_30" in macros.transformed_df.columns
    assert "UNRATE_lagged_30" in macros.transformed_df.columns
    assert "UNRATE_z_score" in macros.transformed_df.columns
    assert "GDP_pct_change" in macros.transformed_df.columns
    assert "FEDFUNDS_rolling_30" in macros.transformed_df.columns
    assert "INDPRO_rolling_90" in macros.transformed_df.columns
    assert not macros.transformed_df["GDP_lagged_30"].isnull().all()
    assert not macros.transformed_df["UNRATE_lagged_30"].isnull().all()
    assert not macros.transformed_df["UNRATE_z_score"].isnull().all()
    assert not macros.transformed_df["GDP_pct_change"].isnull().all()
    assert not macros.transformed_df["FEDFUNDS_rolling_30"].isnull().all()
    assert not macros.transformed_df["INDPRO_rolling_90"].isnull().all()


def test_macro_data_empty(sample_macros):
    print("Testing macroeconomic data with empty DataFrame...")
    macros = EnrichMacros(sample_macros)
    macros.macro_df = pd.DataFrame()
    with pytest.raises(
        ValueError, match="Macro DataFrame is empty. Cannot transform empty DataFrame."
    ):
        macros.transform_macro_data()


def test_macro_data_initialization(sample_macros):
    print("Testing macroeconomic data initialization...")
    macros = EnrichMacros(sample_macros)
    assert macros.macro_df is not None
    assert isinstance(macros.macro_df, pd.DataFrame)
    assert macros.transformed_df is None
    assert macros.start_date is not None
    assert macros.end_date is not None


def test_macro_data_fetch(sample_macros):
    print("Testing macroeconomic data fetch...")
    macros = EnrichMacros(sample_macros)
    assert not macros.macro_df.empty
    assert isinstance(macros.macro_df, pd.DataFrame)
    assert "GDP" in macros.macro_df.columns
    assert "UNRATE" in macros.macro_df.columns
    assert "CPIAUCSL" in macros.macro_df.columns
    assert "FEDFUNDS" in macros.macro_df.columns


def test_macro_data_lag(sample_macros):
    print("Testing macroeconomic data lagging...")
    macros = EnrichMacros(sample_macros)
    macros.lag_macro_data(cols=["GDP", "UNRATE"], lag_periods=[30, 60, 90])

    assert "GDP_lagged_30" in macros.transformed_df.columns
    assert "UNRATE_lagged_30" in macros.transformed_df.columns
    assert not macros.transformed_df["GDP_lagged_30"].isnull().all()
    assert not macros.transformed_df["UNRATE_lagged_30"].isnull().all()


def test_macro_data_rolling(sample_macros):
    print("Testing macroeconomic data rolling mean...")
    macros = EnrichMacros(sample_macros)
    macros.rolling_macro_data(cols=["FEDFUNDS", "INDPRO"], window=[30, 90])

    assert "FEDFUNDS_rolling_30" in macros.transformed_df.columns
    assert "INDPRO_rolling_90" in macros.transformed_df.columns
    assert not macros.transformed_df["FEDFUNDS_rolling_30"].isnull().all()
    assert not macros.transformed_df["INDPRO_rolling_90"].isnull().all()


def test_macro_data_pct_change(sample_macros):
    print("Testing macroeconomic data percentage change...")
    macros = EnrichMacros(sample_macros)
    macros.pct_change_macro_data(cols=["GDP", "UMCSENT"], periods=[90])

    assert "GDP_pct_change" in macros.transformed_df.columns
    assert not macros.transformed_df["GDP_pct_change"].isnull().all()
    assert "UMCSENT_pct_change" in macros.transformed_df.columns
    assert not macros.transformed_df["UMCSENT_pct_change"].isnull().all()


def test_macro_data_z_score(sample_macros):
    print("Testing macroeconomic data z-score...")
    macros = EnrichMacros(sample_macros)
    macros.z_score_macro_data(cols=["GDP", "UNRATE"])
    assert "GDP_z_score" in macros.transformed_df.columns
    assert not macros.transformed_df["GDP_z_score"].isnull().all()
    assert "UNRATE_z_score" in macros.transformed_df.columns
    assert not macros.transformed_df["UNRATE_z_score"].isnull().all()
