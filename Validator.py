import pandas as pd
import yaml
from typing import Dict, List, Any


class validator:
    """Validates pandas DataFrames against YAML configuration rules."""
    
    def __init__(self, cfg: yaml):
        """      
        Args:
            config_path: Path to the YAML configuration file
        """

        self.config = cfg
        self.defaults = self.config.get('defaults', {})
        self.sources = {src['name']: src for src in self.config.get('sources', [])}
    
    def validate(self, df: pd.DataFrame, source_name: str) -> tuple:
        """
        Validate a DataFrame against the rules for a named source.
        
        Args:
            df: The pandas DataFrame to validate
            source_name: Name of the source in the YAML config
        """
        
        if source_name not in self.sources:
            raise ValueError(f"Source '{source_name}' not found in config")
        
        source_config = self.sources[source_name]

        valid = pd.DataFrame()
        invalid = pd.DataFrame()
        
        valid,invalid = self._validate_rules(df, source_config.get('rules', []))
        
        
        return valid, invalid
    
    
    def _validate_rules(self, df: pd.DataFrame, rules: List[Dict[str, str]]) -> tuple:
        """
        Validate that all rows in DataFrame satisfy the rules.
        """
        invalid = pd.DataFrame()
        valid = pd.DataFrame()
        
        for rule_spec in rules:
            passed = df.query(rule_spec['rule'])
            valid = pd.concat([valid,passed])
        
        invalid = df.drop(valid.index)
        
        return valid,invalid
    
    
    def get_source_config(self, source_name: str) -> Dict[str, Any]:
        """Get the configuration for a specific source."""
        if source_name not in self.sources:
            raise ValueError(f"Source '{source_name}' not found in config")
        return self.sources[source_name]
    
    def list_sources(self) -> List[str]:
        """List all available source names."""
        return list(self.sources.keys())