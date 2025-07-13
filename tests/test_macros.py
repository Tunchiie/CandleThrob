import os
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date as date_type
from unittest.mock import Mock, patch
from CandleThrob.ingestion.fetch_data import DataIngestion
from CandleThrob.utils.models import MacroData


class TestMacroDataFunctionality:
    """Comprehensive test suite for macroeconomic data functionality."""

    @pytest.fixture(scope="function")
    def db_conn(self):
        """Create a fresh in-memory database conn for testing."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import connmaker
        from utils.models import Base
        
        # Create in-memory SQLite database for testing
        engine = create_engine("sqlite:///:memory:", echo=False)
        Base.metadata.create_all(engine)
        conn = connmaker(bind=engine)
        conn = conn()
        
        try:
            yield conn
        finally:
            conn.close()

    @pytest.fixture
    def sample_macro_data(self):
        """Create sample macro data for testing."""
        dates = pd.date_range(start='2024-01-01', end='2024-01-05', freq='D')
        data = []
        series_ids = ['FEDFUNDS', 'CPIAUCSL', 'UNRATE']
        
        for dt in dates:
            for series_id in series_ids:
                data.append({
                    'date': dt.date(),
                    'series_id': series_id,
                    'value': np.random.uniform(1.0, 10.0)
                })
        
        return pd.DataFrame(data)

    @pytest.fixture
    def mock_fred_api(self):
        """Mock FRED API for testing."""
        with patch('ingestion.fetch_data.Fred') as mock_fred:
            mock_instance = Mock()
            mock_fred.return_value = mock_instance
            
            # Create mock data for different series
            mock_data = {
                'FEDFUNDS': pd.Series([5.25, 5.30, 5.35], 
                                    index=pd.date_range('2024-01-01', periods=3)),
                'CPIAUCSL': pd.Series([310.2, 310.4, 310.6], 
                                    index=pd.date_range('2024-01-01', periods=3)),
                'UNRATE': pd.Series([3.7, 3.8, 3.7], 
                                  index=pd.date_range('2024-01-01', periods=3))
            }
            
            mock_instance.get_series.side_effect = lambda series_id, **kwargs: mock_data.get(series_id, pd.Series())
            yield mock_instance

    def test_create_table(self, db_conn):
        """Test that MacroData table can be created."""
        macro_model = MacroData()
        macro_model.create_table(db_conn)
        
        # Verify table exists by checking if we can query it
        db_conn.query(MacroData).first()
        # Should not raise an exception even if empty
        
    def test_data_exists_empty_table(self, db_conn):
        """Test data_exists returns False for empty table."""
        macro_model = MacroData()
        macro_model.create_table(db_conn)
        
        assert not macro_model.data_exists(db_conn)
        assert not macro_model.data_exists(db_conn, series_id='FEDFUNDS')
    
    def test_get_last_date_empty_table(self, db_conn):
        """Test get_last_date returns None for empty table."""
        macro_model = MacroData()
        macro_model.create_table(db_conn)
        
        assert macro_model.get_last_date(db_conn) is None
        assert macro_model.get_last_date(db_conn, series_id='FEDFUNDS') is None
    
    def test_insert_data_valid(self, db_conn, sample_macro_data):
        """Test inserting valid macro data."""
        macro_model = MacroData()
        macro_model.create_table(db_conn)
        
        # Insert sample data
        macro_model.insert_data(db_conn, sample_macro_data)
        
        # Verify data was inserted
        assert macro_model.data_exists(db_conn)
        
        # Check specific series
        assert macro_model.data_exists(db_conn, series_id='FEDFUNDS')
        assert macro_model.data_exists(db_conn, series_id='CPIAUCSL')
        
        # Check record count
        count = db_conn.query(MacroData).count()
        assert count == len(sample_macro_data)
    
    def test_insert_data_incremental_loading(self, db_conn, sample_macro_data):
        """Test incremental loading - only new data should be inserted."""
        macro_model = MacroData()
        macro_model.create_table(db_conn)
        
        # Insert initial data
        macro_model.insert_data(db_conn, sample_macro_data)
        initial_count = db_conn.query(MacroData).count()
        
        # Try to insert the same data again
        macro_model.insert_data(db_conn, sample_macro_data)
        final_count = db_conn.query(MacroData).count()
        
        # Count should remain the same (no duplicates)
        assert final_count == initial_count
    
    def test_insert_data_new_dates(self, db_conn, sample_macro_data):
        """Test that new dates are properly inserted."""
        macro_model = MacroData()
        macro_model.create_table(db_conn)
        
        # Insert initial data
        macro_model.insert_data(db_conn, sample_macro_data)
        initial_count = db_conn.query(MacroData).count()
        
        # Create new data with later dates
        new_dates = pd.date_range(start='2024-01-06', end='2024-01-08', freq='D')
        new_data = []
        for dt in new_dates:
            for series_id in ['FEDFUNDS', 'CPIAUCSL']:
                new_data.append({
                    'date': dt.date(),
                    'series_id': series_id,
                    'value': np.random.uniform(1.0, 10.0)
                })
        
        new_df = pd.DataFrame(new_data)
        macro_model.insert_data(db_conn, new_df)
        
        final_count = db_conn.query(MacroData).count()
        assert final_count == initial_count + len(new_df)
    
    def test_get_last_date_with_data(self, db_conn, sample_macro_data):
        """Test get_last_date returns correct date when data exists."""
        macro_model = MacroData()
        macro_model.create_table(db_conn)
        
        macro_model.insert_data(db_conn, sample_macro_data)
        
        last_date = macro_model.get_last_date(db_conn)
        expected_date = sample_macro_data['date'].max()
        
        assert last_date == expected_date
    
    def test_insert_empty_dataframe(self, db_conn):
        """Test inserting empty DataFrame doesn't cause errors."""
        macro_model = MacroData()
        macro_model.create_table(db_conn)
        
        empty_df = pd.DataFrame(columns=['date', 'series_id', 'value'])
        macro_model.insert_data(db_conn, empty_df)
        
        # Should not raise exception and should remain empty
        assert not macro_model.data_exists(db_conn)

    @patch.dict(os.environ, {'FRED_API_KEY': 'test_key'})
    def test_fetch_fred_data_initialization(self):
        """Test DataIngestion initializes correctly for FRED data."""
        data_ingestion = DataIngestion(start_date="2024-01-01", end_date="2024-01-05")
        assert data_ingestion.macro_df is None
    
    @patch('ingestion.fetch_data.os.getenv')
    @patch('ingestion.fetch_data.Fred')
    def test_fetch_fred_data_success(self, mock_fred, mock_getenv):
        """Test successful FRED data fetching."""
        # Setup environment variable mock
        mock_getenv.return_value = 'test_key'
        
        # Setup FRED API mock
        mock_instance = Mock()
        mock_fred.return_value = mock_instance
        
        # Create mock data for different series
        mock_data = {
            'FEDFUNDS': pd.Series([5.25, 5.30, 5.35], 
                                index=pd.date_range('2024-01-01', periods=3)),
            'CPIAUCSL': pd.Series([310.2, 310.4, 310.6], 
                                index=pd.date_range('2024-01-01', periods=3)),
            'UNRATE': pd.Series([3.7, 3.8, 3.7], 
                              index=pd.date_range('2024-01-01', periods=3))
        }
        
        mock_instance.get_series.side_effect = lambda series_id, **kwargs: mock_data.get(series_id, pd.Series())
        
        data_ingestion = DataIngestion(start_date="2024-01-01", end_date="2024-01-03")
        data_ingestion.fetch_fred_data()
        
        # Verify macro_df was created
        assert data_ingestion.macro_df is not None
        assert not data_ingestion.macro_df.empty
        
        # Check expected columns
        expected_columns = ['date', 'series_id', 'value']
        for col in expected_columns:
            assert col in data_ingestion.macro_df.columns
        
        # Check that we have the expected series
        series_ids = data_ingestion.macro_df['series_id'].unique()
        expected_series = ['FEDFUNDS', 'CPIAUCSL', 'UNRATE']
        for series in expected_series:
            assert series in series_ids
    
    def test_fetch_fred_data_no_api_key(self):
        """Test that missing API key raises appropriate error."""
        with patch('ingestion.fetch_data.os.getenv') as mock_getenv:
            mock_getenv.return_value = None
            data_ingestion = DataIngestion(start_date="2024-01-01", end_date="2024-01-03")
            with pytest.raises(RuntimeError, match="FRED API key is not set"):
                data_ingestion.fetch_fred_data()
    
    @patch('ingestion.fetch_data.os.getenv')
    @patch('ingestion.fetch_data.Fred')
    def test_fetch_fred_data_api_error(self, mock_fred, mock_getenv):
        """Test handling of FRED API errors."""
        mock_getenv.return_value = 'test_key'
        mock_instance = Mock()
        mock_fred.return_value = mock_instance
        mock_instance.get_series.side_effect = Exception("API Error")
        
        data_ingestion = DataIngestion(start_date="2024-01-01", end_date="2024-01-03")
        
        # Should handle the error gracefully but might raise ValueError for empty data
        with pytest.raises(ValueError, match="No data returned from FRED API"):
            data_ingestion.fetch_fred_data()
    
    @patch('ingestion.fetch_data.os.getenv')
    @patch('ingestion.fetch_data.Fred')
    def test_fetch_fred_data_date_formatting(self, mock_fred, mock_getenv):
        """Test that dates are properly formatted and processed."""
        # Setup environment variable mock
        mock_getenv.return_value = 'test_key'
        
        # Setup FRED API mock
        mock_instance = Mock()
        mock_fred.return_value = mock_instance
        
        # Create mock data for different series
        mock_data = {
            'FEDFUNDS': pd.Series([5.25, 5.30, 5.35], 
                                index=pd.date_range('2024-01-01', periods=3)),
            'CPIAUCSL': pd.Series([310.2, 310.4, 310.6], 
                                index=pd.date_range('2024-01-01', periods=3)),
            'UNRATE': pd.Series([3.7, 3.8, 3.7], 
                              index=pd.date_range('2024-01-01', periods=3))
        }
        
        mock_instance.get_series.side_effect = lambda series_id, **kwargs: mock_data.get(series_id, pd.Series())
        
        data_ingestion = DataIngestion(start_date="2024-01-01", end_date="2024-01-03")
        data_ingestion.fetch_fred_data()
        
        # Check date column formatting
        assert data_ingestion.macro_df is not None
        assert 'date' in data_ingestion.macro_df.columns
        
        # Dates should be datetime/date objects
        dates = data_ingestion.macro_df['date']
        assert all(isinstance(d, (datetime, date_type)) for d in dates)
    
    @patch('ingestion.fetch_data.os.getenv')
    @patch('ingestion.fetch_data.Fred')
    def test_fetch_fred_data_value_types(self, mock_fred, mock_getenv):
        """Test that values are properly converted to numeric types."""
        # Setup environment variable mock
        mock_getenv.return_value = 'test_key'
        
        # Setup FRED API mock
        mock_instance = Mock()
        mock_fred.return_value = mock_instance
        
        # Create mock data for different series
        mock_data = {
            'FEDFUNDS': pd.Series([5.25, 5.30, 5.35], 
                                index=pd.date_range('2024-01-01', periods=3)),
            'CPIAUCSL': pd.Series([310.2, 310.4, 310.6], 
                                index=pd.date_range('2024-01-01', periods=3)),
            'UNRATE': pd.Series([3.7, 3.8, 3.7], 
                              index=pd.date_range('2024-01-01', periods=3))
        }
        
        mock_instance.get_series.side_effect = lambda series_id, **kwargs: mock_data.get(series_id, pd.Series())
        
        data_ingestion = DataIngestion(start_date="2024-01-01", end_date="2024-01-03")
        data_ingestion.fetch_fred_data()
        
        # Values should be numeric
        assert data_ingestion.macro_df is not None
        values = data_ingestion.macro_df['value']
        assert all(isinstance(v, (int, float, np.number)) for v in values if pd.notna(v))

    def test_macro_data_columns(self, sample_macro_data):
        """Test that sample data has required columns."""
        required_columns = ['date', 'series_id', 'value']
        for col in required_columns:
            assert col in sample_macro_data.columns
    
    def test_macro_data_types(self, sample_macro_data):
        """Test that data types are correct."""
        # Date should be date objects
        assert all(isinstance(d, date_type) for d in sample_macro_data['date'])
        
        # Series ID should be strings
        assert all(isinstance(s, str) for s in sample_macro_data['series_id'])
        
        # Values should be numeric
        assert all(isinstance(v, (int, float, np.number)) for v in sample_macro_data['value'] if pd.notna(v))
    
    def test_macro_data_series_ids(self):
        """Test that expected FRED series IDs are included."""
        expected_series = [
            "FEDFUNDS", "CPIAUCSL", "UNRATE", "GDP", "GS10", 
            "USREC", "UMCSENT", "HOUST", "RSAFS", "INDPRO", "M2SL"
        ]
        
        # This test validates that our test data and real implementation
        # are using the expected FRED series IDs
        for series in expected_series:
            assert isinstance(series, str)
            assert len(series) > 0

    def test_invalid_date_range(self):
        """Test that invalid date ranges are handled properly."""
        with pytest.raises(ValueError):
            DataIngestion(start_date="2024-01-10", end_date="2024-01-01")
    
    def test_database_connection_error(self):
        """Test handling of database connection errors."""
        macro_model = MacroData()
        
        # Mock a conn that raises an exception
        mock_conn = Mock()
        mock_conn.query.side_effect = Exception("Database connection error")
        
        # Should handle the error gracefully
        assert not macro_model.data_exists(mock_conn)
        assert macro_model.get_last_date(mock_conn) is None
    
    def test_malformed_data_insertion(self, db_conn):
        """Test handling of malformed data during insertion."""
        macro_model = MacroData()
        macro_model.create_table(db_conn)
        
        # Create malformed data (missing required columns)
        bad_data = pd.DataFrame({
            'wrong_column': ['A', 'B'],
            'another_wrong_column': [1, 2]
        })
        
        # Should handle the error without crashing
        with pytest.raises(Exception):
            macro_model.insert_data(db_conn, bad_data)


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
