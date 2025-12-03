"""
Basic test to verify pytest setup works.
Save this as: tests/test_basic.py

Run with: pytest tests/test_basic.py -v
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def test_imports():
    """Test that all modules can be imported."""
    try:
        from Reader import reader
        from Validator import validator
        from Cleaner import cleaner
        from Loader import loader
        assert True
    except ImportError as e:
        assert False, f"Import failed: {e}"

def test_simple_math():
    """Simple test to verify pytest is working."""
    assert 1 + 1 == 2
    assert 2 * 3 == 6

def test_reader_can_instantiate():
    """Test that reader class can be instantiated."""
    from Reader import reader
    import yaml
    
    config = {'sources': []}
    r = reader(config)
    assert r is not None

def test_validator_can_instantiate():
    """Test that validator class can be instantiated."""
    from Validator import validator
    import yaml
    
    config = {'sources': []}
    v = validator(config)
    assert v is not None

def test_cleaner_can_instantiate():
    """Test that cleaner class can be instantiated."""
    from Cleaner import cleaner
    import yaml
    
    config = {'sources': []}
    c = cleaner(config)
    assert c is not None

def test_loader_can_instantiate():
    """Test that loader class can be instantiated."""
    from Loader import loader
    import yaml
    
    config = {'defaults': {'db_url': 'test'}}
    l = loader(config)
    assert l is not None