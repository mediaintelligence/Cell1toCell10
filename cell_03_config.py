# Cell 03: Configuration Management
# This cell handles configuration loading and management

import os
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class Config:
    """Configuration management class."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config.yaml"
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        try:
            if self.config_path.endswith('.yaml') or self.config_path.endswith('.yml'):
                with open(self.config_path, 'r') as f:
                    return yaml.safe_load(f)
            elif self.config_path.endswith('.json'):
                with open(self.config_path, 'r') as f:
                    return json.load(f)
            else:
                logger.warning(f"Unsupported config file format: {self.config_path}")
                return {}
        except FileNotFoundError:
            logger.warning(f"Config file not found: {self.config_path}")
            return self._get_default_config()
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "app": {
                "name": "Cell1toCell10",
                "version": "1.0.0",
                "debug": False
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
            "api": {
                "timeout": 30,
                "retries": 3
            },
            "database": {
                "type": "sqlite",
                "path": "data/app.db"
            },
            "models": {
                "default": "gpt-3.5-turbo",
                "temperature": 0.7,
                "max_tokens": 1000
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save(self) -> None:
        """Save configuration to file."""
        try:
            if self.config_path.endswith('.yaml') or self.config_path.endswith('.yml'):
                with open(self.config_path, 'w') as f:
                    yaml.dump(self.config, f, default_flow_style=False)
            elif self.config_path.endswith('.json'):
                with open(self.config_path, 'w') as f:
                    json.dump(self.config, f, indent=2)
            logger.info(f"Configuration saved to {self.config_path}")
        except Exception as e:
            logger.error(f"Error saving config: {e}")

# Environment variables configuration
def load_env_config():
    """Load configuration from environment variables."""
    config = {}
    
    # API Keys
    config['anthropic_api_key'] = os.getenv('ANTHROPIC_API_KEY')
    config['openai_api_key'] = os.getenv('OPENAI_API_KEY')
    config['gemini_api_key'] = os.getenv('GEMINI_API_KEY')
    
    # Database
    config['db_host'] = os.getenv('DB_HOST', 'localhost')
    config['db_port'] = os.getenv('DB_PORT', '5432')
    config['db_name'] = os.getenv('DB_NAME', 'app')
    config['db_user'] = os.getenv('DB_USER', 'postgres')
    config['db_password'] = os.getenv('DB_PASSWORD', '')
    
    # App settings
    config['debug'] = os.getenv('DEBUG', 'False').lower() == 'true'
    config['log_level'] = os.getenv('LOG_LEVEL', 'INFO')
    
    return config

# Global configuration instance
config = Config()
env_config = load_env_config()

if __name__ == "__main__":
    logger.info("Configuration loaded successfully")
    logger.info(f"App name: {config.get('app.name')}")
    logger.info(f"Debug mode: {config.get('app.debug')}") 