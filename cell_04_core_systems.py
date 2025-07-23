# Cell 04: Core Systems
# This cell contains core system components and utilities

import os
import sys
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

logger = logging.getLogger(__name__)

class CoreSystem:
    """Core system management class."""
    
    def __init__(self):
        self.components = {}
        self.initialized = False
    
    def register_component(self, name: str, component: Any) -> None:
        """Register a system component."""
        self.components[name] = component
        logger.info(f"Registered component: {name}")
    
    def get_component(self, name: str) -> Optional[Any]:
        """Get a registered component."""
        return self.components.get(name)
    
    def initialize(self) -> None:
        """Initialize the core system."""
        logger.info("Initializing core system...")
        self.initialized = True
        logger.info("Core system initialized")

class FileManager:
    """File management utilities."""
    
    @staticmethod
    def ensure_directory(path: str) -> None:
        """Ensure a directory exists."""
        Path(path).mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def read_file(path: str) -> str:
        """Read a file and return its contents."""
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    
    @staticmethod
    def write_file(path: str, content: str) -> None:
        """Write content to a file."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
    
    @staticmethod
    def list_files(directory: str, pattern: str = "*") -> List[str]:
        """List files in a directory matching a pattern."""
        return [str(f) for f in Path(directory).glob(pattern)]

class DataProcessor:
    """Data processing utilities."""
    
    @staticmethod
    def validate_data(data: Any) -> bool:
        """Validate data structure."""
        if data is None:
            return False
        return True
    
    @staticmethod
    def clean_data(data: str) -> str:
        """Clean and normalize data."""
        if not data:
            return ""
        return data.strip()
    
    @staticmethod
    def format_data(data: Any, format_type: str = "json") -> str:
        """Format data for output."""
        import json
        if format_type == "json":
            return json.dumps(data, indent=2)
        return str(data)

class CacheManager:
    """Simple cache management."""
    
    def __init__(self):
        self.cache = {}
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set a cache value."""
        self.cache[key] = {
            'value': value,
            'ttl': ttl,
            'timestamp': asyncio.get_event_loop().time() if ttl else None
        }
    
    def get(self, key: str) -> Optional[Any]:
        """Get a cache value."""
        if key not in self.cache:
            return None
        
        item = self.cache[key]
        if item['ttl'] and asyncio.get_event_loop().time() - item['timestamp'] > item['ttl']:
            del self.cache[key]
            return None
        
        return item['value']
    
    def clear(self) -> None:
        """Clear all cache."""
        self.cache.clear()

# Global instances
core_system = CoreSystem()
file_manager = FileManager()
data_processor = DataProcessor()
cache_manager = CacheManager()

if __name__ == "__main__":
    core_system.initialize()
    logger.info("Core systems loaded successfully") 