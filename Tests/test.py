"""
Test suite for Reader, Validator, Cleaner, and Loader classes

Project Structure:
    project_root/
    ├── src/
    │   ├── Reader.py
    │   ├── Validator.py
    │   ├── Cleaner.py
    │   └── Loader.py
    └── tests/
        └── test_suite.py

To run tests with coverage (from project root):
    pip install pytest pytest-cov pytest-mock requests-mock
    pytest tests/ --cov=src --cov-report=html --cov-report=term-missing

This will generate:
    - Terminal coverage report
    - HTML report in htmlcov/index.html
"""

import pytest
import pandas as pd
import yaml
import tempfile
import os
import sys
from unittest.mock import Mock, patch, MagicMock
import requests

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


# Sample YAML config matching your sources.yml structure
SAMPLE_CONFIG = """
defaults:
  db_url: postgresql+psycopg2://postgres:test@localhost:5432/testdb
  batch_size: 5000
  on_conflict: upsert

sources:
  - name: tax_csv
    type: csv
    path: test_data.csv
    target_table: tax_table
    pk: [objectid]
    schema:
      objectid: int
      zip_code: int
      num_props: int
      balance: float
    rules:
      - rule: "zip_code >= 19019"
      - rule: "zip_code <= 19160"

  - name: lead_api
    type: api_json
    path: https://example.com/api/data
    target_table: lead_table
    pk: [id]
    schema:
      id: int
      zip_code: int
      num_screen: int
      num_bll_5plus: float
      perc_5plus: float
    rules:
      - rule: "zip_code >= 19019"
      - rule: "zip_code <= 19160"
"""


@pytest.fixture
def config_dict():
    """Return config as dictionary."""
    return yaml.safe_load(SAMPLE_CONFIG)


@pytest.fixture
def config_file():
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write(SAMPLE_CONFIG)
        config_path = f.name
    
    yield config_path
    
    # Cleanup
    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def sample_csv_file():
    """Create a temporary CSV file for testing."""
    csv_content = """objectid,zip_code,num_props,balance
1,19020,10,1500.50
2,19100,20,2500.75
3,19150,30,3500.25
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_content)
        csv_path = f.name
    
    yield csv_path
    
    # Cleanup
    if os.path.exists(csv_path):
        os.unlink(csv_path)


@pytest.fixture
def sample_valid_df():
    """Create valid sample DataFrame."""
    return pd.DataFrame({
        'objectid': [1, 2, 3],
        'zip_code': [19020, 19100, 19150],
        'num_props': [10, 20, 30],
        'balance': [1500.50, 2500.75, 3500.25]
    })


@pytest.fixture
def sample_invalid_df():
    """Create invalid sample DataFrame (violates rules)."""
    return pd.DataFrame({
        'objectid': [1, 2, 3, 4],
        'zip_code': [19020, 18000, 19150, 20000],  # 18000 and 20000 out of range
        'num_props': [10, 20, 30, 40],
        'balance': [1500.50, 2500.75, 3500.25, 4500.00]
    })


@pytest.fixture
def sample_lead_df():
    """Create sample lead data DataFrame."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'zip_code': [19020, 19100, 19150],
        'num_screen': [100, 150, 200],
        'num_bll_5plus': [5.0, None, 10.0],
        'perc_5plus': [5.0, None, 5.0]
    })


# ===== Reader Tests =====

class TestReader:
    """Tests for the Reader class."""
    
    def test_reader_initialization(self, config_dict):
        """Test that reader initializes correctly."""
        from src.Reader import reader
        
        r = reader(config_dict)
        assert r is not None
        assert 'tax_csv' in r.sources
        assert 'lead_api' in r.sources
    
    def test_read_csv(self, config_dict, sample_csv_file):
        """Test reading CSV file."""
        from src.Reader import reader
        
        # Update config to point to our test CSV
        config_dict['sources'][0]['path'] = sample_csv_file
        
        r = reader(config_dict)
        df = r.read('tax_csv')
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'objectid' in df.columns
        assert 'zip_code' in df.columns
    
    def test_csv_reader_method(self, config_dict, sample_csv_file):
        """Test csvReader method directly."""
        from src.Reader import reader
        
        r = reader(config_dict)
        df = r.csvReader(sample_csv_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
    
    @patch('requests.get')
    def test_api_reader_success(self, mock_get, config_dict):
        """Test API reader with successful response."""
        from src.Reader import reader
        
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'rows': [
                {'id': 1, 'zip_code': 19020, 'num_screen': 100},
                {'id': 2, 'zip_code': 19100, 'num_screen': 150}
            ]
        }
        mock_get.return_value = mock_response
        
        r = reader(config_dict)
        df = r.apiReader('https://example.com/api/data')
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'id' in df.columns
    
    @patch('requests.get')
    def test_api_reader_failure(self, mock_get, config_dict):
        """Test API reader with failed response."""
        from src.Reader import reader
        
        # Mock failed API response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        r = reader(config_dict)
        
        with pytest.raises(requests.exceptions.HTTPError):
            r.apiReader('https://example.com/api/data')
    
    def test_read_invalid_source(self, config_dict):
        """Test reading from non-existent source."""
        from src.Reader import reader
        
        r = reader(config_dict)
        
        with pytest.raises(ValueError, match="not found in config"):
            r.read('nonexistent_source')


# ===== Validator Tests =====

class TestValidator:
    """Tests for the Validator class."""
    
    def test_validator_initialization(self, config_dict):
        """Test that validator initializes correctly."""
        from src.Validator import validator
        
        v = validator(config_dict)
        assert v is not None
        assert 'tax_csv' in v.sources
    
    def test_validate_valid_data(self, config_dict, sample_valid_df):
        """Test validation of valid data."""
        from src.Validator import validator
        
        v = validator(config_dict)
        valid, invalid_schema, invalid_rules = v.validate(sample_valid_df, 'tax_csv')
        
        assert len(valid) == 3
        assert len(invalid_schema) == 0
        assert len(invalid_rules) == 0
    
    def test_validate_invalid_rules(self, config_dict, sample_invalid_df):
        """Test validation catches rule violations."""
        from src.Validator import validator
        
        v = validator(config_dict)
        valid, invalid_schema, invalid_rules = v.validate(sample_invalid_df, 'tax_csv')
        
        # Should reject 2 rows (zip codes 18000 and 20000)
        assert len(valid) == 2
        assert len(invalid_rules) == 2
    
    def test_validate_schema_null_pk(self, config_dict):
        """Test validation catches null primary keys."""
        from src.Validator import validator
        
        df_with_null_pk = pd.DataFrame({
            'objectid': [1, None, 3],
            'zip_code': [19020, 19100, 19150],
            'num_props': [10, 20, 30],
            'balance': [1500.50, 2500.75, 3500.25]
        })
        
        v = validator(config_dict)
        valid, invalid_schema, invalid_rules = v.validate(df_with_null_pk, 'tax_csv')
        
        # Should reject row with null objectid
        assert len(valid) == 2
        assert len(invalid_schema) == 1
    
    def test_type_conversion(self, config_dict):
        """Test that validator converts types correctly."""
        from src.Validator import validator
        
        df_needing_conversion = pd.DataFrame({
            'objectid': ['1', '2', '3'],  # Strings
            'zip_code': ['19020', '19100', '19150'],  # Strings
            'num_props': ['10', '20', '30'],  # Strings
            'balance': ['1500.50', '2500.75', '3500.25']  # Strings
        })
        
        v = validator(config_dict)
        valid, invalid_schema, invalid_rules = v.validate(df_needing_conversion, 'tax_csv')
        
        # Should successfully convert all
        assert len(valid) == 3
        assert valid['objectid'].dtype in ['int64', 'Int64']
        assert valid['zip_code'].dtype in ['int64', 'Int64']
    
    def test_list_sources(self, config_dict):
        """Test listing sources."""
        from src.Validator import validator
        
        v = validator(config_dict)
        sources = v.list_sources()
        
        assert 'tax_csv' in sources
        assert 'lead_api' in sources
    
    def test_get_source_config(self, config_dict):
        """Test getting source configuration."""
        from src.Validator import validator
        
        v = validator(config_dict)
        config = v.get_source_config('tax_csv')
        
        assert config['name'] == 'tax_csv'
        assert 'schema' in config
        assert 'rules' in config
    
    def test_validate_nonexistent_source(self, config_dict, sample_valid_df):
        """Test validation with non-existent source."""
        from src.Validator import validator
        
        v = validator(config_dict)
        
        with pytest.raises(ValueError, match="not found in config"):
            v.validate(sample_valid_df, 'nonexistent_source')


# ===== Cleaner Tests =====

class TestCleaner:
    """Tests for the Cleaner class."""
    
    def test_cleaner_initialization(self, config_dict):
        """Test that cleaner initializes correctly."""
        from src.Cleaner import cleaner
        
        c = cleaner(config_dict)
        assert c is not None
        assert 'tax_csv' in c.sources
    
    def test_clean_removes_duplicates(self, config_dict):
        """Test that cleaner removes duplicate rows."""
        from src.Cleaner import cleaner
        
        df_with_dupes = pd.DataFrame({
            'id': [1, 2, 2, 3],
            'zip_code': [19020, 19100, 19100, 19150],
            'num_screen': [100, 150, 150, 200],
            'num_bll_5plus': [5.0, 10.0, 10.0, 15.0],
            'perc_5plus': [5.0, 6.7, 6.7, 7.5]
        })
        
        c = cleaner(config_dict)
        c.clean(df_with_dupes)
        
        # Should have 3 rows after removing duplicate
        assert len(df_with_dupes) == 3
    
    def test_clean_fills_num_bll_5plus(self, config_dict, sample_lead_df):
        """Test that cleaner fills null num_bll_5plus values."""
        from src.Cleaner import cleaner
        
        c = cleaner(config_dict)
        c.clean(sample_lead_df)
        
        # Check that null was filled with 2
        assert sample_lead_df['num_bll_5plus'].isna().sum() == 0
        assert sample_lead_df.loc[1, 'num_bll_5plus'] == 2
    
    def test_clean_calculates_perc_5plus(self, config_dict, sample_lead_df):
        """Test that cleaner calculates perc_5plus from num_bll_5plus/num_screen."""
        from src.Cleaner import cleaner
        
        c = cleaner(config_dict)
        c.clean(sample_lead_df)
        
        # Check that perc_5plus was calculated correctly for row 1
        expected_perc = (2 / 150) * 100
        assert abs(sample_lead_df.loc[1, 'perc_5plus'] - expected_perc) < 0.01
    
    def test_clean_preserves_existing_values(self, config_dict, sample_lead_df):
        """Test that cleaner doesn't overwrite valid existing values."""
        from src.Cleaner import cleaner
        
        original_perc = sample_lead_df.loc[0, 'perc_5plus']
        
        c = cleaner(config_dict)
        c.clean(sample_lead_df)
        
        # Should preserve original non-null values
        assert sample_lead_df.loc[0, 'perc_5plus'] == original_perc


# ===== Loader Tests =====

class TestLoader:
    """Tests for the Loader class."""
    
    def test_loader_initialization(self, config_dict):
        """Test that loader initializes correctly."""
        from src.Loader import loader
        
        l = loader(config_dict)
        assert l is not None
        assert l.db_url == config_dict['defaults']['db_url']
    
    @patch('sqlalchemy.create_engine')
    def test_load_success(self, mock_create_engine, config_dict, sample_valid_df):
        """Test successful data loading."""
        from src.Loader import loader
        
        # Mock the engine and to_sql
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        l = loader(config_dict)
        
        # Capture print output
        with patch('builtins.print') as mock_print:
            l.load(sample_valid_df, 'test_table')
            mock_print.assert_called_with("DataFrame successfully written to PostgreSQL.")
    
    @patch('sqlalchemy.create_engine')
    def test_load_failure(self, mock_create_engine, config_dict, sample_valid_df):
        """Test data loading with error."""
        from src.Loader import loader
        
        # Mock the engine to raise an exception
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Make to_sql raise an exception
        with patch.object(pd.DataFrame, 'to_sql', side_effect=Exception("DB Error")):
            l = loader(config_dict)
            
            with patch('builtins.print') as mock_print:
                l.load(sample_valid_df, 'test_table')
                # Check that error message was printed
                call_args = str(mock_print.call_args)
                assert "Error writing DataFrame" in call_args
    
    @patch('sqlalchemy.create_engine')
    def test_load_uses_correct_parameters(self, mock_create_engine, config_dict, sample_valid_df):
        """Test that load uses correct parameters."""
        from src.Loader import loader
        
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        l = loader(config_dict)
        
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            l.load(sample_valid_df, 'test_table')
            
            # Verify to_sql was called with correct parameters
            mock_to_sql.assert_called_once()
            args, kwargs = mock_to_sql.call_args
            assert args[0] == 'test_table'
            assert kwargs['con'] == mock_engine
            assert kwargs['if_exists'] == 'replace'
            assert kwargs['index'] == False


# ===== Integration Tests =====

class TestIntegration:
    """Integration tests for complete ETL pipeline."""
    
    @patch('requests.get')
    def test_full_pipeline_csv(self, mock_get, config_dict, sample_csv_file):
        """Test complete pipeline: read -> validate -> clean -> load (CSV)."""
        from src.Reader import reader
        from src.Validator import validator
        from src.Cleaner import cleaner
        from src.Loader import loader
        
        # Update config
        config_dict['sources'][0]['path'] = sample_csv_file
        
        # Read
        r = reader(config_dict)
        df = r.read('tax_csv')
        assert len(df) > 0
        
        # Validate
        v = validator(config_dict)
        valid_df, invalid_schema, invalid_rules = v.validate(df, 'tax_csv')
        assert len(valid_df) > 0
        
        # Load (mocked)
        with patch('sqlalchemy.create_engine'):
            with patch.object(pd.DataFrame, 'to_sql'):
                l = loader(config_dict)
                l.load(valid_df, 'test_table')
    
    @patch('requests.get')
    @patch('sqlalchemy.create_engine')
    def test_full_pipeline_api(self, mock_engine, mock_get, config_dict):
        """Test complete pipeline: read -> validate -> clean -> load (API)."""
        from src.Reader import reader
        from src.Validator import validator
        from src.Cleaner import cleaner
        from src.Loader import loader
        
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'rows': [
                {'id': 1, 'zip_code': 19020, 'num_screen': 100, 'num_bll_5plus': 5.0, 'perc_5plus': 5.0},
                {'id': 2, 'zip_code': 19100, 'num_screen': 150, 'num_bll_5plus': None, 'perc_5plus': None}
            ]
        }
        mock_get.return_value = mock_response
        
        # Read
        r = reader(config_dict)
        df = r.read('lead_api')
        assert len(df) == 2
        
        # Validate
        v = validator(config_dict)
        valid_df, invalid_schema, invalid_rules = v.validate(df, 'lead_api')
        
        # Clean
        c = cleaner(config_dict)
        c.clean(valid_df)
        
        # Check that cleaning worked
        assert valid_df['num_bll_5plus'].isna().sum() == 0
        
        # Load (mocked)
        with patch.object(pd.DataFrame, 'to_sql'):
            l = loader(config_dict)
            l.load(valid_df, 'lead_table')


# ===== Parametrized Tests =====

class TestParametrized:
    """Parametrized tests for various scenarios."""
    
    @pytest.mark.parametrize("zip_code,expected_valid", [
        (19019, True),   # Lower bound
        (19160, True),   # Upper bound
        (19100, True),   # Mid range
        (19018, False),  # Just below
        (19161, False),  # Just above
        (10000, False),  # Far below
        (99999, False),  # Far above
    ])
    def test_zip_code_validation(self, config_dict, zip_code, expected_valid):
        """Test zip code validation with various values."""
        from src.Validator import validator
        
        df = pd.DataFrame({
            'objectid': [1],
            'zip_code': [zip_code],
            'num_props': [10],
            'balance': [1500.50]
        })
        
        v = validator(config_dict)
        valid, invalid_schema, invalid_rules = v.validate(df, 'tax_csv')
        
        if expected_valid:
            assert len(valid) == 1
            assert len(invalid_rules) == 0
        else:
            assert len(valid) == 0
            assert len(invalid_rules) == 1
    
    @pytest.mark.parametrize("value,expected_type", [
        ('123', 'int'),
        ('123.45', 'float'),
        ('true', 'bool'),
        ('hello', 'str'),
    ])
    def test_type_conversions(self, config_dict, value, expected_type):
        """Test various type conversions."""
        from src.Validator import validator
        
        v = validator(config_dict)
        df = pd.DataFrame({'test_col': [value]})
        
        v._try_convert_column(df, 'test_col', expected_type)
        
        if expected_type == 'int':
            assert df['test_col'].dtype in ['int64', 'Int64']
        elif expected_type == 'float':
            assert df['test_col'].dtype in ['float64', 'float32', 'float16']
        elif expected_type == 'str':
            assert df['test_col'].dtype in ['object', 'string']


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--cov=.", "--cov-report=term-missing", "--cov-report=html"])