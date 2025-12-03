#Validator
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
        
        validS,invalidS = self._validate_schema(df, source_config.get('schema', []), source_config['pk'])
        valid,invalidR = self._validate_rules(validS, source_config.get('rules', []))

        #invalid = pd.concat([invalidS,invalidR])
        
        
        return valid, invalidS, invalidR
    
    
    def _validate_rules(self, df: pd.DataFrame, rules: List[Dict[str, str]]) -> tuple:
        """
        Validate that all rows in DataFrame satisfy the rules.
        """
        invalid = pd.DataFrame()
        valid = df
        

        for rule_spec in rules:
            valid = valid.query(rule_spec['rule'])
        
        invalid = df.drop(valid.index)
        
        return valid,invalid
    

    def _validate_schema(self, df: pd.DataFrame, schema: List[Dict[str, str]], pk: List[str]) -> tuple:
        """
        Validate that all rows in DataFrame satisfy the schema.
        """
        invalid = pd.DataFrame()


        # Check and attempt to convert data types
        for col, expected_type in schema.items():
            if col not in df.columns:
                continue
            
            type_valid = self._check_column_type(df[col], expected_type)
            if not type_valid:
                # Try to convert to the expected type
                self._try_convert_column(df, col, expected_type)


        for key in pk:
            drop = df.dropna(subset=key)
            invalid = pd.concat([invalid,df.drop(drop.index)])

        df.drop(invalid.index, inplace=True)

        return df,invalid


    def _check_column_type(self, series: pd.Series, expected_type: str) -> bool:
        """Check if a series matches the expected type."""
        type_map = {
            'int': ['int64', 'int32', 'int16', 'int8'],
            'float': ['float64', 'float32', 'float16'],
            'bool': ['bool'],
            'str': ['object', 'string']
        }
        
        expected_dtypes = type_map.get(expected_type, [expected_type])
        return str(series.dtype) in expected_dtypes


    def _try_convert_column(self, df: pd.DataFrame, col: str, expected_type: str) -> tuple:
        """Attempt to convert a column to the expected type."""
        original_series = df[col].copy()
        
        try:
            if expected_type == 'int':
                # Try to convert to int, coercing errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                
            elif expected_type == 'float':
                # Try to convert to float, coercing errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            elif expected_type == 'bool':
                # Try to convert to bool
                df[col] = df[col].astype(bool)
                
            elif expected_type == 'str':
                # Convert to string
                df[col] = df[col].astype(str)
            
        except Exception as e:
            # If conversion fails, revert to original
            df[col] = original_series

        


    def get_source_config(self, source_name: str) -> Dict[str, Any]:
        """Get the configuration for a specific source."""
        if source_name not in self.sources:
            raise ValueError(f"Source '{source_name}' not found in config")
        return self.sources[source_name]
    
    def list_sources(self) -> List[str]:
        """List all available source names."""
        return list(self.sources.keys())