cp combined_cell6.py combined_cell6.txt
#Code_cell6.0- Configs
import logging
import asyncio
import os
import json
import time
import hashlib
import uuid
import traceback
import statistics
import psutil
import nest_asyncio
nest_asyncio.apply()

import re
import base64
import csv
import chardet
import pandas as pd
import numpy as np
import networkx as nx
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Type, Set
from collections import defaultdict, Counter, deque
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from cachetools import TTLCache
from google.cloud import bigquery, storage
from google.api_core import exceptions, retry
from google.oauth2 import service_account
import aiohttp
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import traceback
import sys
# ----------------------------------------------------------------------
# Setup logging at module level
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()



# ----------------------------------------------------------------------
# Custom Exceptions
# ----------------------------------------------------------------------
class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass

class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass

class ResourceError(Exception):
    """Custom exception for resource-related errors"""
    pass
class CommunicationError(Exception):
    """Exception raised for communication-related errors."""
    pass

class InitializationError(Exception):
    """Exception raised for initialization failures."""
    pass

class ConfigurationError(Exception):
    """Exception raised for configuration-related errors."""
    pass

# ----------------------------------------------------------------------
# Common Dataclasses & Configurations
# ----------------------------------------------------------------------
@dataclass
class Neo4jConfig:
    """Neo4j database configuration"""
    uri: str = "neo4j+s://6fdaa9bb.databases.neo4j.io"
    user: str = field(default_factory=lambda: os.getenv("NEO4J_USER", "neo4j"))
    password: str = field(default_factory=lambda: os.getenv("NEO4J_PASSWORD", ""))
    max_connection_lifetime: int = 3600
    connection_timeout: int = 30
    max_retry_time: int = 30

@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    bigquery_project_id: str = field(default_factory=lambda: os.getenv("BIGQUERY_PROJECT_ID", ""))
    bigquery_project_id: str = field(default_factory=lambda: os.getenv("BIGQUERY_PROJECT_ID", ""))
    bigquery_dataset_id: str = field(default_factory=lambda: os.getenv("BIGQUERY_DATASET_ID", ""))
    storage_bucket: str = field(default_factory=lambda: os.getenv("STORAGE_BUCKET", ""))



@dataclass
class AgentConfig:
    """Configuration for individual agents"""
    name: str
    type: str
    model_config: 'ModelConfig'
    layer_id: int
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.name:
            raise ValueError("Agent name is required")
        if not self.type:
            raise ValueError("Agent type is required")
        if not isinstance(self.layer_id, int) or self.layer_id < 0:
            raise ValueError("Invalid layer ID")

@dataclass
class REWOOConfig:
    """REWOO system configuration"""
    enabled: bool = True
    max_planning_steps: int = 5
    evidence_threshold: float = 0.8
    context_window: int = 4096
    cache_size: int = 1000
    planning_temperature: float = 0.7
    max_retries: int = 3
    timeout: int = 30
    metadata: Dict[str, Any] = field(default_factory=dict)



@dataclass
class SystemConfig:
    """
    System-wide configuration that can serve as 'EnhancedConfig' as well.
    You can extend this with any additional fields needed.
    """
    evidence_store_config: Dict[str, Any] = field(default_factory=lambda: {
        'storage_type': 'memory',
        'cache_size': 1000,
        'ttl': 3600,
        'validation_rules': {
            'required_fields': ['content', 'source', 'timestamp'],
            'max_size': 1024 * 1024  # 1MB
        }
    })
    communication_config: Dict[str, Any] = field(default_factory=lambda: {
        'max_retries': 3,
        'timeout': 30,
        'batch_size': 100
    })
    rewoo_config: Dict[str, Any] = field(default_factory=lambda: {
        'enabled': True,
        'max_planning_steps': 5,
        'evidence_threshold': 0.8,
        'context_window': 4096,
        'planning_temperature': 0.7
    })
    layer_configs: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    agent_configs: List['AgentConfig'] = field(default_factory=list)
    database_config: DatabaseConfig = field(default_factory=DatabaseConfig)
    
    
# Custom Exceptions
class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass

class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass

class ResourceError(Exception):
    """Custom exception for resource-related errors"""
    pass

# Dataclasses and Configurations


@dataclass
class BossAgentConfig:
    """Configuration for boss agent"""
    model_name: str = "claude-3-5-sonnet@20240620"
    api_key: Optional[str] = None
    max_tokens: int = 4000
    temperature: float = 0.7
    top_p: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary"""
        return {
            'model_name': self.model_name,
            'api_key': self.api_key,
            'max_tokens': self.max_tokens,
            'temperature': self.temperature,
            'top_p': self.top_p,
            'metadata': self.metadata
        }


@dataclass
class Neo4jConfig:
    """Neo4j database configuration"""
    uri: str = "neo4j+s://6fdaa9bb.databases.neo4j.io"
    user: str = field(default_factory=lambda: os.getenv("NEO4J_USER", "neo4j"))
    password: str = field(default_factory=lambda: os.getenv("NEO4J_PASSWORD", ""))
    max_connection_lifetime: int = 3600
    connection_timeout: int = 30
    max_retry_time: int = 30

@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    bigquery_project_id: str = field(default_factory=lambda: os.getenv("BIGQUERY_PROJECT_ID", ""))
    bigquery_dataset_id: str = field(default_factory=lambda: os.getenv("BIGQUERY_DATASET_ID", ""))
    storage_bucket: str = field(default_factory=lambda: os.getenv("STORAGE_BUCKET", ""))

@dataclass
class WorkerConfig:
    """Enhanced worker configuration with tool support"""
    name: str
    type: str = "default"
    batch_size: int = 1000
    timeout: int = 3600
    max_retries: int = 3
    tool_set: Dict[str, Any] = field(default_factory=dict)
    cache_size: int = 1000
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate configuration after initialization"""
        self._validate_config()

    def _validate_config(self):
        """Validate worker configuration"""
        if not self.name:
            raise ValueError("Worker name is required")
        if self.batch_size < 1:
            raise ValueError("Batch size must be positive")
        if self.timeout < 0:
            raise ValueError("Timeout must be non-negative")
        if self.max_retries < 0:
            raise ValueError("Max retries must be non-negative")
        if not isinstance(self.tool_set, dict):
            raise TypeError("Tool set must be a dictionary")


from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class StorageConfig:
    """Storage configuration with validation"""
    
    type: str = "memory"  # memory, disk, or distributed
    compression: bool = False
    backup_enabled: bool = False
    path: str = "evidence_store"
    max_size: int = 1024 * 1024 * 100  # 100MB default
    retention_days: int = 30
    validation_rules: Dict[str, Any] = field(
        default_factory=lambda: {
            'allowed_types': ['memory', 'disk', 'distributed'],
            'min_size': 1024 * 1024,  # 1MB
            'max_size': 1024 * 1024 * 1024  # 1GB
        }
    )

    def __post_init__(self):
        """Validate configuration after initialization"""
        self._validate_config()

    def _validate_config(self):
        """
        Validate the storage configuration.
        
        Raises:
            ValueError: If any validation check fails
        """
        # Validate storage type
        if self.type not in self.validation_rules['allowed_types']:
            raise ValueError(
                f"Invalid storage type: {self.type}. "
                f"Allowed types: {self.validation_rules['allowed_types']}"
            )

        # Validate max_size
        if not (self.validation_rules['min_size'] <= self.max_size <= self.validation_rules['max_size']):
            raise ValueError(
                f"Invalid max_size: {self.max_size}. "
                f"Must be between {self.validation_rules['min_size']} and "
                f"{self.validation_rules['max_size']} bytes"
            )

        # Validate retention_days
        if self.retention_days <= 0:
            raise ValueError(
                f"Invalid retention_days: {self.retention_days}. "
                "Must be greater than 0"
            )

        # Validate path if storage type is disk
        if self.type == "disk" and not self.path:
            raise ValueError("Path must be specified for disk storage type")



# Import necessary modules
import logging
import asyncio
import sys
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime
from cachetools import TTLCache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass

from dataclasses import dataclass, field
from typing import Dict, Any, Type
import logging

class EnhancedValidator:
    """Enhanced validator with complete implementation"""
    def __init__(self):
        self._initialize_required_types()
        self.logger = logging.getLogger(__name__)

    def _initialize_required_types(self):
        """Initialize required types for validation"""
        self._required_types = {
            'storage': {
                'type': str,
                'compression': bool,
                'backup_enabled': bool,
                'path': str
            },
            'caching': {
                'enabled': bool,
                'max_size': int,
                'ttl': int,
                'strategy': str
            },
            'indexing': {
                'enabled': bool,
                'fields': list,
                'auto_update': bool
            },
            'validation': {
                'required_fields': list,
                'max_size': int,
                'strict_mode': bool
            }
        }

    def validate_storage_config(self, config: Dict[str, Any]) -> bool:
        """Validate storage configuration"""
        return self._validate_config(config, self._required_types['storage'])

    def validate_caching_config(self, config: Dict[str, Any]) -> bool:
        """Validate caching configuration"""
        return self._validate_config(config, self._required_types['caching'])

    def validate_indexing_config(self, config: Dict[str, Any]) -> bool:
        """Validate indexing configuration"""
        return self._validate_config(config, self._required_types['indexing'])

    def validate_validation_config(self, config: Dict[str, Any]) -> bool:
        """Validate validation configuration"""
        return self._validate_config(config, self._required_types['validation'])

    def _validate_config(self, config: Dict[str, Any], required_types: Dict[str, Type]) -> bool:
        """Validate configuration against required types"""
        try:
            if not isinstance(config, dict):
                self.logger.error("Config must be a dictionary")
                return False
                
            # Check all required fields exist
            if not all(field in config for field in required_types):
                self.logger.error(f"Missing required fields: {set(required_types.keys()) - set(config.keys())}")
                return False
                
            # Check all field types match
            for field, field_type in required_types.items():
                if not isinstance(config[field], field_type):
                    self.logger.error(f"Invalid type for {field}: expected {field_type}, got {type(config[field])}")
                    return False
                    
            return True
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            return False

    def validate_all_configs(self, configs: Dict[str, Dict[str, Any]]) -> Dict[str, bool]:
        """Validate all configurations"""
        try:
            validation_results = {}
            for config_name, config in configs.items():
                validator_method = getattr(self, f"validate_{config_name}_config", None)
                if validator_method is None:
                    self.logger.error(f"No validator method for {config_name}")
                    validation_results[config_name] = False
                else:
                    validation_results[config_name] = validator_method(config)
            return validation_results
        except Exception as e:
            self.logger.error(f"Validation of all configs failed: {str(e)}")
            return {name: False for name in configs}

@dataclass
class EvidenceStoreConfig:
    """Evidence store configuration with strict validation"""
    storage: Dict[str, Any] = field(default_factory=lambda: {
        'type': 'memory',
        'compression': False,
        'backup_enabled': False,
        'path': 'evidence_store'
    })

    caching: Dict[str, Any] = field(default_factory=lambda: {
        'enabled': True,
        'max_size': 1000,
        'ttl': 3600,
        'strategy': 'lru'
    })

    indexing: Dict[str, Any] = field(default_factory=lambda: {
        'enabled': True,
        'fields': ['type', 'source', 'timestamp'],
        'auto_update': True
    })

    validation: Dict[str, Any] = field(default_factory=lambda: {
        'required_fields': ['content', 'source', 'timestamp'],
        'max_size': 1024 * 1024,
        'strict_mode': True
    })

    def __post_init__(self):
        """Initialize and validate configuration"""
        self.validator = EnhancedValidator()
        self._validate()

    def _validate(self):
        """Validate configuration against defined fields"""
        validation_results = self.validator.validate_all_configs({
            'storage': self.storage,
            'caching': self.caching,
            'indexing': self.indexing,
            'validation': self.validation
        })

        failed = [k for k, v in validation_results.items() if not v]
        if failed:
            details = {k: getattr(self, k) for k in failed}
            raise ConfigurationError(
                f"Invalid configuration sections: {failed}\n"
                f"Details: {details}"
            )

    @classmethod
    def create_default(cls) -> 'EvidenceStoreConfig':
        """Create configuration with defaults"""
        return cls()
@dataclass(frozen=True)
class ConfigFields:
    """Configuration field definitions"""
    STORAGE = {
        'type': str,
        'compression': bool,
        'backup_enabled': bool,
        'path': str
    }
    
    CACHING = {
        'enabled': bool,
        'max_size': int,
        'ttl': int,
        'strategy': str
    }
    
    INDEXING = {
        'enabled': bool,
        'fields': list,
        'auto_update': bool
    }
    
    VALIDATION = {
        'required_fields': list,
        'max_size': int,
        'strict_mode': bool
    }



@dataclass
class ProfileDefinition:
    """
    Configuration profile settings (renamed to avoid conflict with the Enum)
    """
    name: str
    validation_mode: str = "lenient"
    required_components: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowConfig:
    """
    Configuration for workflow execution
    """
    steps: List[Dict[str, Any]] = field(default_factory=list)
    resource_requirements: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


    async def initialize(self) -> None:
        """
        Initialize workflow configuration
        """
        try:
            # Initialize validation rules
            await self._initialize_validation_rules()

            # Initialize error handlers
            await self._initialize_error_handlers()

            # Initialize resource tracking
            await self._initialize_resource_tracking()

            self.initialized = True
            self.logger.info("Workflow configuration initialized successfully")
        except Exception as e:
            self.logger.error(f"Workflow configuration initialization failed: {e}")
            raise

    async def _initialize_validation_rules(self) -> None:
        """Initialize validation rules"""
        try:
            self.validation_rules = {
                'step': self._validate_step,
                'resource': self._validate_resource,
                'metadata': self._validate_metadata,
                'retry': self._validate_retry_policy,
                'dependency': self._validate_dependency
            }
            self.logger.info("Validation rules initialized")
        except Exception as e:
            self.logger.error(f"Validation rules initialization failed: {e}")
            raise

    async def _initialize_error_handlers(self) -> None:
        """Initialize error handlers"""
        try:
            self.error_handlers = {
                'step_failure': self._handle_step_failure,
                'resource_error': self._handle_resource_error,
                'validation_error': self._handle_validation_error,
                'timeout': self._handle_timeout
            }
            self.logger.info("Error handlers initialized")
        except Exception as e:
            self.logger.error(f"Error handlers initialization failed: {e}")
            raise

    async def _initialize_resource_tracking(self) -> None:
        """Initialize resource tracking"""
        try:
            self.resource_tracking = {
                'allocated': defaultdict(float),
                'available': defaultdict(float),
                'limits': defaultdict(float)
            }
            self.logger.info("Resource tracking initialized")
        except Exception as e:
            self.logger.error(f"Resource tracking initialization failed: {e}")
            raise

    async def _validate_step(self, step: Dict[str, Any]) -> bool:
        """Validate workflow step configuration"""
        try:
            # Check required fields
            required_fields = ['id', 'type', 'action', 'requirements']
            if not all(field in step for field in required_fields):
                self.logger.error(f"Missing required fields in step: {required_fields}")
                return False

            # Validate step ID
            if not isinstance(step['id'], str) or not step['id']:
                self.logger.error("Invalid step ID")
                return False

            # Validate step type
            valid_types = ['processing', 'analysis', 'transformation', 'validation']
            if step['type'] not in valid_types:
                self.logger.error(f"Invalid step type: {step['type']}")
                return False

            # Validate action
            if not isinstance(step['action'], str) or not step['action']:
                self.logger.error("Invalid step action")
                return False

            # Validate requirements
            if not isinstance(step['requirements'], dict):
                self.logger.error("Invalid step requirements format")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Step validation failed: {e}")
            return False

    async def _validate_resource(self, resource: Dict[str, Any]) -> bool:
        """Validate resource configuration"""
        try:
            # Check required fields
            required_fields = ['type', 'amount', 'unit']
            if not all(field in resource for field in required_fields):
                self.logger.error(f"Missing required fields in resource: {required_fields}")
                return False

            # Validate resource type
            valid_types = ['cpu', 'memory', 'storage', 'network']
            if resource['type'] not in valid_types:
                self.logger.error(f"Invalid resource type: {resource['type']}")
                return False

            # Validate amount
            if not isinstance(resource['amount'], (int, float)) or resource['amount'] <= 0:
                self.logger.error("Invalid resource amount")
                return False

            # Validate unit
            valid_units = {
                'cpu': ['cores', 'threads'],
                'memory': ['MB', 'GB'],
                'storage': ['MB', 'GB', 'TB'],
                'network': ['Mbps', 'Gbps']
            }
            if resource['unit'] not in valid_units[resource['type']]:
                self.logger.error(f"Invalid unit for resource type {resource['type']}")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Resource validation failed: {e}")
            return False

    async def _validate_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Validate workflow metadata"""
        try:
            # Check required fields
            required_fields = ['name', 'version', 'description']
            if not all(field in metadata for field in required_fields):
                self.logger.error(f"Missing required fields in metadata: {required_fields}")
                return False

            # Validate name
            if not isinstance(metadata['name'], str) or not metadata['name']:
                self.logger.error("Invalid metadata name")
                return False

            # Validate version
            if not isinstance(metadata['version'], str) or not metadata['version']:
                self.logger.error("Invalid metadata version")
                return False

            # Validate description
            if not isinstance(metadata['description'], str):
                self.logger.error("Invalid metadata description")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Metadata validation failed: {e}")
            return False

    async def _validate_retry_policy(self, policy: Dict[str, Any]) -> bool:
        """Validate retry policy configuration"""
        try:
            # Check required fields
            required_fields = ['max_attempts', 'backoff_factor', 'max_delay']
            if not all(field in policy for field in required_fields):
                self.logger.error(f"Missing required fields in retry policy: {required_fields}")
                return False

            # Validate max_attempts
            if not isinstance(policy['max_attempts'], int) or policy['max_attempts'] <= 0:
                self.logger.error("Invalid max_attempts value")
                return False

            # Validate backoff_factor
            if not isinstance(policy['backoff_factor'], (int, float)) or policy['backoff_factor'] <= 0:
                self.logger.error("Invalid backoff_factor value")
                return False

            # Validate max_delay
            if not isinstance(policy['max_delay'], int) or policy['max_delay'] <= 0:
                self.logger.error("Invalid max_delay value")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Retry policy validation failed: {e}")
            return False

    async def _validate_dependency(self, dependency: Dict[str, Any]) -> bool:
        """Validate workflow step dependency"""
        try:
            # Check required fields
            required_fields = ['step_id', 'type', 'condition']
            if not all(field in dependency for field in required_fields):
                self.logger.error(f"Missing required fields in dependency: {required_fields}")
                return False

            # Validate step_id
            if not isinstance(dependency['step_id'], str) or not dependency['step_id']:
                self.logger.error("Invalid dependency step_id")
                return False

            # Validate dependency type
            valid_types = ['success', 'failure', 'completion']
            if dependency['type'] not in valid_types:
                self.logger.error(f"Invalid dependency type: {dependency['type']}")
                return False

            # Validate condition
            if not isinstance(dependency['condition'], str) or not dependency['condition']:
                self.logger.error("Invalid dependency condition")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Dependency validation failed: {e}")
            return False

    async def _handle_step_failure(self, step_id: str, error: Exception) -> None:
        """Handle step execution failure"""
        self.logger.error(f"Step {step_id} failed: {str(error)}")
        # Implement step failure handling logic

    async def _handle_resource_error(self, resource_type: str, error: Exception) -> None:
        """Handle resource allocation error"""
        self.logger.error(f"Resource error for {resource_type}: {str(error)}")
        # Implement resource error handling logic

    async def _handle_validation_error(self, validation_type: str, error: Exception) -> None:
        """Handle validation error"""
        self.logger.error(f"Validation error for {validation_type}: {str(error)}")
        # Implement validation error handling logic

    async def _handle_timeout(self, step_id: str) -> None:
        """Handle step execution timeout"""
        self.logger.error(f"Step {step_id} timed out")
        # Implement timeout handling logic

    def add_step(self, step_config: Dict[str, Any]) -> None:
        """Add a workflow step with validation"""
        if not isinstance(step_config, dict):
            raise ValueError("Step configuration must be a dictionary")
        self.steps.append(step_config)

    def set_resource_requirements(self, requirements: Dict[str, Any]) -> None:
        """Set and validate resource requirements"""
        if not all(isinstance(v, (int, float)) for v in requirements.values()):
            raise ValueError("Resource requirements must be numeric")
        self.resource_requirements = requirements

    def get_config(self) -> Dict[str, Any]:
        """Get complete workflow configuration"""
        return {
            'steps': self.steps,
            'resource_requirements': self.resource_requirements,
            'metadata': self.metadata,
            'retry_policy': getattr(self, 'retry_policy', None)
        }


async def create_agent(agent_name, layer_id, config):
    """
    Example placeholder for agent creation
    """
    agent_config = config.get_agent_config(f"{agent_name}_{layer_id}")
    if agent_config:
        agent_class = config._get_agent_class(agent_config.type)
        agent = agent_class()
        await agent.initialize()
        return agent
    return None




@dataclass
class EnhancedConfig:
    def __init__(self, configuration_profile: str = "minimal", validation_mode: str = "lenient"):
        # Initialize validator first
        self.validator = EnhancedValidator()
        
        # Required base attributes
        self.storage: Dict[str, Any] = {
            'type': 'memory',
            'compression': False,
            'backup_enabled': False,
            'path': 'evidence_store'
        }
        
        self.caching: Dict[str, Any] = {
            'enabled': True,
            'max_size': 1000,
            'ttl': 3600,
            'strategy': 'lru'
        }
        
        self.indexing: Dict[str, Any] = {
            'enabled': True,
            'fields': ['type', 'source', 'timestamp'],
            'auto_update': True
        }
        
        self.validation: Dict[str, Any] = {
            'required_fields': ['content', 'source', 'timestamp'],
            'max_size': 1024 * 1024,
            'strict_mode': True
        }
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self._initializing = False
        self.initialized = False
        
        # Validate configuration
        self.validate_config()

    def validate_config(self) -> None:
        try:
            # Check required sections
            required_sections = {'storage', 'caching', 'indexing', 'validation'}
            if not all(hasattr(self, section) for section in required_sections):
                missing = required_sections - set(self.__dict__.keys())
                raise ConfigurationError(f"Missing sections: {missing}")

            # Validate each section using validator
            if not self.validator.validate_storage_config(self.storage):
                raise ConfigurationError("Invalid storage configuration")
                
            if not self.validator.validate_cache_config(self.caching):
                raise ConfigurationError("Invalid cache configuration")
                
            if not self.validator.validate_indexing_config(self.indexing):
                raise ConfigurationError("Invalid indexing configuration")
                
            if not self.validator.validate_validation_config(self.validation):
                raise ConfigurationError("Invalid validation configuration")
                
            self.logger.info("Configuration validation successful")
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            raise ConfigurationError(f"Configuration validation failed: {str(e)}")
            
    def ensure_defaults(self) -> None:
        """Ensure all default values are present"""
        default_configs = {
            'storage': {'type': 'memory', 'compression': False, 'backup_enabled': False, 'path': 'evidence_store'},
            'caching': {'enabled': True, 'max_size': 1000, 'ttl': 3600, 'strategy': 'lru'},
            'indexing': {'enabled': True, 'fields': ['type', 'source', 'timestamp'], 'auto_update': True},
            'validation': {'required_fields': ['content', 'source', 'timestamp'], 'max_size': 1024 * 1024, 'strict_mode': True}
        }

        for section, defaults in default_configs.items():
            if not hasattr(self, section):
                setattr(self, section, defaults)

class SecureConfigManager:
    """Enhanced configuration manager with proper core and component initialization"""
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.initialized = False
        self._initializing = False
        self.config_paths = {
            'env_file': '.env',
            'service_account': 'service-account.json',
            'backup_config': 'config.json'
        }
        self.required_env_vars = [
            'VERTEX_AI_PROJECT_ID',
            'VERTEX_AI_LOCATION',
            'VERTEX_AI_CREDENTIALS_PATH'
        ]
        self.required_sections = {
            'storage': {'type', 'path'},
            'validation': {'rules', 'threshold'},
            'indexing': {'enabled', 'fields'}
        }
        
        self.initialized = False
        # Core attributes
        self.config = {}
        self.metrics = defaultdict(Counter)
        self.cache = None
        
        # Core component attributes
        self.evidence_store = None
        self.communication_system = None
        self.planning_system = None

    async def initialize(self) -> None:
        """Initialize both core and core components with proper sequencing"""
        if self._initializing or self.initialized:
            return
            
        self._initializing = True
        try:
            # First initialize core
            await self._initialize_core()
            
            # Then initialize components
            await self._initialize_core_components()
            
            self.initialized = True
            self.logger.info("Configuration manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Configuration manager initialization failed: {e}")
            await self.cleanup()
            raise
        finally:
            self._initializing = False

    async def _initialize_core(self) -> None:
        """Initialize basic core infrastructure"""
        try:
            # Initialize basic configuration
            self.config = {}
            
            # Initialize metrics tracking
            self.metrics = defaultdict(Counter)
            
            # Initialize cache
            self.cache = TTLCache(maxsize=1000, ttl=3600)
            
            # Initialize validation rules
            self.validation_rules = {
                'required_fields': ['model_name', 'max_tokens', 'temperature'],
                'value_ranges': {
                    'max_tokens': (1, 32768),
                    'temperature': (0.0, 2.0)
                }
            }
            
            self.logger.info("Core initialized successfully")
        except Exception as e:
            self.logger.error(f"Core initialization failed: {e}")
            raise

    async def _initialize_core_components(self) -> None:
        """Initialize functional core components"""
        try:
            # Initialize evidence store
            self.evidence_store = EvidenceStore(self.config.get('evidence_store_config', {}))
            await self.evidence_store.initialize()
            
            # Initialize planning system
            self.planning_system = PlanningSystem(self.config)
            await self.planning_system.initialize()
            
            # Initialize communication system
            self.communication_system = CommunicationSystem()
            await self.communication_system.initialize()
            
            self.logger.info("Core components initialized successfully")
        except Exception as e:
            self.logger.error(f"Core component initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup both core and core components"""
        try:
            # First cleanup components
            await self._cleanup_components()
            
            # Then cleanup core
            await self._cleanup_core()
            
            self.initialized = False
            self._initializing = False
            
            self.logger.info("Configuration manager cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Configuration manager cleanup failed: {e}")
            raise

    async def _cleanup_core(self) -> None:
        """Cleanup core infrastructure"""
        try:
            self.config.clear()
            self.metrics.clear()
            if self.cache:
                self.cache.clear()
            self.logger.info("Core cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Core cleanup failed: {e}")
            raise

    async def _cleanup_components(self) -> None:
        """Cleanup core components"""
        try:
            if self.evidence_store:
                await self.evidence_store.cleanup()
            
            if self.planning_system:
                await self.planning_system.cleanup()
                
            if self.communication_system:
                await self.communication_system.cleanup()
                
            self.logger.info("Core components cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Component cleanup failed: {e}")
            raise

    
    
    def _verify_dependencies(self) -> None:
        """Verify required dependencies"""
        try:
            import dotenv
            self.logger.info("Required dependencies verified")
        except ImportError:
            self.logger.error("python-dotenv not installed")
            raise ImportError("Please install python-dotenv: pip install python-dotenv")

    async def _load_environment(self) -> None:
        """Load environment configuration"""
        try:
            from dotenv import load_dotenv
            
            # Check for .env file
            env_path = self._find_config_file('.env')
            if env_path:
                load_dotenv(env_path)
                self.logger.info(f"Loaded environment from {env_path}")
            else:
                self.logger.warning("No .env file found, checking environment variables")
            
            # Verify required variables
            missing_vars = [var for var in self.required_env_vars
                          if not os.getenv(var)]
            if missing_vars:
                raise ConfigurationError(f"Missing required environment variables: {missing_vars}")
                
        except Exception as e:
            self.logger.error(f"Environment loading failed: {e}")
            raise

    async def _load_service_account(self) -> None:
        """Load service account credentials"""
        try:
            creds_path = os.getenv('VERTEX_AI_CREDENTIALS_PATH')
            if not creds_path:
                raise ConfigurationError("VERTEX_AI_CREDENTIALS_PATH not set")
                
            if not os.path.exists(creds_path):
                raise ConfigurationError(f"Service account file not found: {creds_path}")
                
            # Load and verify credentials
            with open(creds_path) as f:
                creds = json.load(f)
                
            required_fields = ['project_id', 'private_key', 'client_email']
            if not all(field in creds for field in required_fields):
                raise ConfigurationError("Invalid service account configuration")
                
            self.logger.info("Service account credentials loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Service account loading failed: {e}")
            raise

    def _find_config_file(self, filename: str) -> Optional[str]:
        """Find configuration file in possible locations"""
        search_paths = [
            os.getcwd(),
            os.path.join(os.getcwd(), 'config'),
            os.path.dirname(os.getcwd())
        ]
        
        for path in search_paths:
            file_path = os.path.join(path, filename)
            if os.path.exists(file_path):
                return file_path
        
        return None
     
    async def _load_core_config(self) -> Dict[str, Any]:
        """Load core configuration with security checks"""
        try:
            config = {
                'security': {
                    'encryption_enabled': True,
                    'key_rotation': 30,  # days
                    'min_key_length': 2048
                },
                'storage': {
                    'type': 'encrypted',
                    'backup_enabled': True,
                    'compression': True
                },
                'validation': {
                    'strict_mode': True,
                    'validate_types': True,
                    'validate_values': True
                }
            }

            # Validate configuration
            if not await self._validate_core_config(config):
                raise ConfigurationError("Invalid core configuration")

            self.logger.info("Core configuration loaded successfully")
            return config

        except Exception as e:
            self.logger.error(f"Core configuration loading failed: {e}")
            raise
    async def validate_config(self, config: Dict[str, Any]) -> bool:
        try:
            for section, required_fields in self.required_sections.items():
                if section not in config:
                    raise ConfigurationError(f"Missing section: {section}")
                if not all(field in config[section] for field in required_fields):
                    raise ConfigurationError(f"Missing required fields in {section}")
            return True
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
    async def _validate_core_config(self, config: Dict[str, Any]) -> bool:
        """Validate core configuration"""
        try:
            required_sections = {'security', 'storage', 'validation'}
            if not all(section in config for section in required_sections):
                return False

            # Validate security settings
            security = config.get('security', {})
            if not all(key in security for key in ['encryption_enabled', 'key_rotation', 'min_key_length']):
                return False

            return True

        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
            
    async def _can_initialize_vertex(self, config: Dict[str, Any]) -> bool:
        """Check if Vertex AI can be initialized"""
        try:
            required_fields = ['project_id', 'location']
            if not all(config.get(field) for field in required_fields):
                return False

            # Check credentials
            if config.get('credentials_path'):
                if not os.path.exists(config['credentials_path']):
                    self.logger.warning("Credentials file not found")
                    return False
            elif not config.get('credentials'):
                self.logger.warning("No credentials provided")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Vertex AI initialization check failed: {e}")
            return False

    async def _setup_vertex_client(self, config: Dict[str, Any]) -> None:
        """Setup Vertex AI client"""
        try:
            if config.get('credentials_path'):
                credentials = service_account.Credentials.from_service_account_file(
                    config['credentials_path']
                )
            else:
                credentials = service_account.Credentials.from_service_account_info(
                    config['credentials']
                )

            vertexai.init(
                project=config['project_id'],
                location=config['location'],
                credentials=credentials
            )
            
            self.logger.info("Vertex AI client initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Vertex AI client setup failed: {e}")
            raise

    def get_vertex_ai_config(self) -> Dict[str, Any]:
        """Get Vertex AI configuration"""
        if not hasattr(self, 'config'):
            raise RuntimeError("Configuration not initialized")
            
        return {
            'project_id': os.getenv('VERTEX_AI_PROJECT_ID'),
            'location': os.getenv('VERTEX_AI_LOCATION', 'us-central1'),
            'credentials_path': os.getenv('VERTEX_AI_CREDENTIALS_PATH'),
            'credentials': self.config.get('vertex_ai_credentials')
        }
    async def _initialize_vertex_config(self) -> Dict[str, Any]:
        """Initialize Vertex AI configuration with fallbacks"""
        try:
            vertex_config = {
                'project_id': os.getenv('VERTEX_AI_PROJECT_ID'),
                'location': os.getenv('VERTEX_AI_LOCATION', 'us-central1'),
                'api_endpoint': os.getenv('VERTEX_AI_ENDPOINT'),
                'credentials_path': os.getenv('VERTEX_AI_CREDENTIALS_PATH')
            }

            # Validate Vertex AI configuration
            if not await self._validate_vertex_config(vertex_config):
                self.logger.warning("Invalid Vertex AI configuration, using defaults")
                vertex_config = await self._get_default_vertex_config()

            # Initialize Vertex AI client if possible
            if await self._can_initialize_vertex(vertex_config):
                await self._setup_vertex_client(vertex_config)

            return vertex_config

        except Exception as e:
            self.logger.error(f"Vertex AI configuration failed: {e}")
            return await self._get_default_vertex_config()

    async def _validate_vertex_config(self, config: Dict[str, Any]) -> bool:
        """Validate Vertex AI configuration"""
        required_fields = ['project_id', 'location']
        return all(field in config and config[field] for field in required_fields)

    async def _get_default_vertex_config(self) -> Dict[str, Any]:
        """Get default Vertex AI configuration"""
        return {
            'project_id': None,
            'location': 'us-central1',
            'api_endpoint': None,
            'credentials_path': None,
            'enabled': False
        }

# =================
# ModelConfig
# =================

class ModelConfig:
    """
    Enhanced model configuration with dictionary-like access
    """
    def __init__(self,
                 model_name: str,
                 api_key: Optional[str] = None,
                 layer_id: Optional[int] = None,
                 version: str = "latest",
                 max_tokens: int = 4000,
                 temperature: float = 0.7,
                 top_p: float = 1.0,
                 context_window: int = 8192,
                 pool_size: int = 1,
                 metadata: Dict[str, Any] = None
                 ):
        self.model_name = model_name
        self.api_key = api_key
        self.layer_id = layer_id
        self.version = version
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.top_p = top_p
        self.context_window = context_window
        self.pool_size = pool_size
        self.metadata = metadata or {}

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'model_name': self.model_name,
            'api_key': self.api_key,
            'layer_id': self.layer_id,
            'version': self.version,
            'max_tokens': self.max_tokens,
            'temperature': self.temperature,
            'top_p': self.top_p,
            'context_window': self.context_window,
            'pool_size': self.pool_size,
            'metadata': self.metadata
        }


# =================
# LayerConfig
# =================

@dataclass
class LayerConfig:
    """Enhanced layer configuration with comprehensive settings"""
    layer_id: int
    agents: List[str] = field(default_factory=list)
    model_name: str = ""
    pool_size: int = 1
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    parallel_config: Dict[str, Any] = field(default_factory=lambda: {
        'max_workers': 3,
        'batch_size': 1000,
        'timeout': 3600
    })
    evidence_config: Dict[str, Any] = field(default_factory=lambda: {
        'version_control': True,
        'cache_size': 1000,
        'validation_rules': {}
    })

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary format"""
        return {
            'layer_id': self.layer_id,
            'agents': self.agents,
            'model_name': self.model_name,
            'pool_size': self.pool_size,
            'enabled': self.enabled,
            'metadata': self.metadata,
            'parallel_config': self.parallel_config,
            'evidence_config': self.evidence_config
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LayerConfig':
        """Create config from dictionary"""
        return cls(**data)
# =================
# APISettings
# =================

class APISettings:
    """
    API-specific settings configuration
    """
    def __init__(self,
                 vertex_ai_project_id: str,
                 vertex_ai_location: str,
                 vertex_ai_credentials: Optional[Dict[str, Any]] = None,
                 openai_api_key: Optional[str] = None,
                 anthropic_api_key: Optional[str] = None,
                 mistral_api_key: Optional[str] = None,
                 vertex_ai_credentials_path: Optional[str] = None,
                 configuration_profile: str = "minimal",
                 validation_mode: str = "lenient",
                 ):
        self.vertex_ai_project_id = vertex_ai_project_id
        self.vertex_ai_location = vertex_ai_location
        self.vertex_ai_credentials = vertex_ai_credentials
        self.openai_api_key = openai_api_key
        self.anthropic_api_key = anthropic_api_key
        self.mistral_api_key = mistral_api_key
        self.vertex_ai_credentials_path = vertex_ai_credentials_path
        self.validation_mode = validation_mode

        if self.vertex_ai_credentials_path and not self.vertex_ai_credentials:
            try:
                import json
                with open(self.vertex_ai_credentials_path) as f:
                    self.vertex_ai_credentials = json.load(f)
            except Exception as e:
                logging.warning(f"Failed to load vertex_ai_credentials: {e}")
                self.vertex_ai_credentials = None
@dataclass
class CommunicationConfig:
    """Configuration for communication system"""
    max_retries: int = 3
    timeout: int = 30
    batch_size: int = 100
    enabled: bool = True
    channels: Dict[str, Any] = field(default_factory=lambda: {
        'system': {'enabled': True, 'buffer_size': 1000},
        'task': {'enabled': True, 'buffer_size': 1000},
        'result': {'enabled': True, 'buffer_size': 1000},
        'error': {'enabled': True, 'buffer_size': 1000}
    })
    metrics: Dict[str, Any] = field(default_factory=dict)
    validation_rules: Dict[str, Any] = field(default_factory=lambda: {
        'message_size_limit': 1024 * 1024,  # 1MB
        'required_fields': ['sender', 'receiver', 'content']
    })

# First, define all configuration classes
@dataclass
class ArtifactManagerConfig:
    """Configuration for artifact management"""
    storage_path: str = "artifacts"
    max_size: int = 1024 * 1024 * 100  # 100MB
    compression_enabled: bool = True
    backup_enabled: bool = True
    validation_rules: Dict[str, Any] = field(default_factory=lambda: {
        'max_artifact_size': 1024 * 1024 * 10,  # 10MB
        'allowed_types': ['data', 'model', 'metric', 'report'],
        'required_metadata': ['creator', 'timestamp', 'type']
    })
    cache_config: Dict[str, Any] = field(default_factory=lambda: {
        'enabled': True,
        'max_size': 1000,
        'ttl': 3600
    })

    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.storage_path:
            raise ValueError("storage_path cannot be empty")
        if self.max_size <= 0:
            raise ValueError("max_size must be positive")

@dataclass
class CheckpointManagerConfig:
    """Configuration for checkpoint management"""
    checkpoint_dir: str = "checkpoints"
    max_checkpoints: int = 10
    auto_cleanup: bool = True
    compression_enabled: bool = True
    validation_enabled: bool = True
    backup_enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.checkpoint_dir:
            raise ValueError("checkpoint_dir cannot be empty")
        if self.max_checkpoints <= 0:
            raise ValueError("max_checkpoints must be positive")




# ------------------------------------------------------------------
# The EnhancedConfig Class (Fully Unified & Corrected)
# ------------------------------------------------------------------

class EnhancedConfig:
    """Enhanced configuration management system"""
    
    def __init__(self, configuration_profile: str = "minimal", validation_mode: str = "lenient"):
        """Initialize the configuration system
        
        Args:
            configuration_profile (str): Profile determining feature set
            validation_mode (str): Validation strictness level
        """
        self.configuration_profile = configuration_profile
        self.validation_mode = validation_mode
        self.logger = logging.getLogger(self.__class__.__name__)
        self._initializing = True
        self._load_default_configs()
        # Initialize validator first
        self.validator = EnhancedValidator()
        self._setup_basic_config()
        self._initializing = False
        self.initialized = True
        
        


        # Initialize configuration containers
        self.model_configs = {}
        self.layer_configs = {}
        self.agent_configs = []
        
        # Core attributes
        self.config_manager = None
        self.metrics = defaultdict(Counter)
        self.cache = None
        
        # Core components
        
        self.evidence_store = None
        self.planning_system = None
        self.communication_system = None
        self.rewoo_system = None
    
        # Initialize basic containers
        self.metrics = defaultdict(Counter)
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        
                # Core components
        # Initialize configuration manager
        self.config_manager = ConfigurationManager()
        
        # Initialize evidence store config
        self.evidence_store_config = EvidenceStoreConfig()
        # Communication configuration and system
        self.communication_config = CommunicationConfig()
        
        # Initialize async components
        asyncio.create_task(self._init_basic_components())
        
        

        
       
        self._initialization_state = defaultdict(bool)
        
        # Core managers
        self.artifact_manager = None
        self.checkpoint_manager = None
        self.metrics_tracker = None
        self.agent_factory = None
        self.moa_system = None
        self.knowledge_graph = None
        self.resource_manager = None
        self.validation_system = None
        self.llm_manager = None
        self.error_handler = None
        self.feedback_loop = None

        
    
        # Basic containers
        self.model_configs: Dict[str, ModelConfig] = {}
        self.layer_configs: Dict[int, LayerConfig] = {}
        self.agent_configs: List[Dict[str, Any]] = []
        
        # Config dictionaries
        self.communication_config: Dict[str, Any] = {}
        self.rewoo_config: Dict[str, Any] = {}
        self.worker_configs: Dict[str, Any] = {}
        self.validation_config: Dict[str, Any] = {}
        self.pipeline_config: Dict[str, Any] = {}
        
        # Placeholders for references to worker/pipeline objects
        self._workers: Dict[str, EnhancedWorker] = {}
        self._pipelines: Dict[str, DataPipelineOptimizer] = {}
        
        # API Settings
        self.api_settings: Optional[APISettings] = None
        
        # System components
        self.communication_system: Optional[CommunicationSystem] = None
        self.evidence_store: Optional[EvidenceStore] = None
        self.boss_agent: Optional[BossAgent] = None
        self.planning_system: Optional[PlanningSystem] = None
        self.rewoo_system: Optional[REWOOSystem] = None
        self.artifact_manager: Optional[EnhancedArtifactManager] = None
        self.checkpoint_manager: Optional[CheckpointManager] = None
        self.world_observer: Optional[WorldObserver] = None
        
        
        
        # Extended components
        self.validation_system: Optional[ValidationSystem] = None
        self.llm_manager: Optional[UnifiedLLMManager] = None
        self.error_handler: Optional[ErrorHandler] = None
        self.feedback_loop: Optional[FeedbackLoop] = None
        self.resource_manager: Optional[UnifiedResourceManager] = None
        self.knowledge_graph: Optional[EnhancedKnowledgeGraph] = None
        self.agent_coordinator: Optional[AgentCoordinator] = None
        self.metrics_tracker: Optional[MetricsTracker] = None
        self.agent_factory: Optional[AgentFactory] = None
        self.moa_system: Optional[EnhancedMoASystem] = None
        self.data_pipeline_optimizer: Optional[DataPipelineOptimizer] = None
        
        # Track agents by layer
        self.layer_agents: Dict[int, List[BossAgent]] = defaultdict(list)
        
        # Component status
        self._component_status: Dict[str, bool] = defaultdict(bool)
        
        # Global state tracking
        self.state = {
            'initialization': defaultdict(bool),
            'runtime': {
                'active': False,
                'error_state': False,
                'last_error': None
            },
            'components': {},
            'metrics': {
                'initialization_time': None,
                'error_count': 0,
                'last_operation': None
            }
        }
        
        # Core configuration attributes
        self.storage = {
            'type': 'memory',
            'compression': False,
            'backup_enabled': False,
            'path': 'evidence_store'
        }
        
        self.caching = {
            'enabled': True,
            'max_size': 1000,
            'ttl': 3600,
            'strategy': 'lru'
        }
        
        self.indexing = {
            'enabled': True,
            'fields': ['type', 'source', 'timestamp'],
            'auto_update': True
        }
        
        self.validation = {
            'required_fields': ['content', 'source', 'timestamp'],
            'max_size': 1024 * 1024,
            'strict_mode': True
        }
        
     # Initialize state
        self._initializing = True
        self.initialized = False
        
        # Initialize validator first
        self.validator = EnhancedValidator()
        
        try:
            # Load and validate configurations
            self._load_default_configs()
            self.validate_config()
            self._setup_basic_config()
            
            # Mark initialization complete
            self._initializing = False
            self.initialized = True
            
        except Exception as e:
            self.logger.error(f"Initialization failed: {str(e)}")
            self._cleanup_partial_initialization()
            raise ConfigurationError(f"Configuration initialization failed: {str(e)}")


    def _load_default_configs(self) -> None:
        """Load default configurations"""
        try:
            # Initialize core configurations
            self.storage = {
                'type': 'memory',
                'compression': False,
                'backup_enabled': False,
                'path': 'evidence_store'
            }
            
            self.caching = {
                'enabled': True,
                'max_size': 1000,
                'ttl': 3600,
                'strategy': 'lru'
            }
            
            self.indexing = {
                'enabled': True,
                'fields': ['type', 'source', 'timestamp'],
                'auto_update': True
            }
            
            self.validation = {
                'required_fields': ['content', 'source', 'timestamp'],
                'max_size': 1024 * 1024,
                'strict_mode': True
            }
            
            # Initialize evidence store config
            self.evidence_store_config = EvidenceStoreConfig(
                storage=self.storage,
                caching=self.caching,
                indexing=self.indexing,
                validation=self.validation
            )
            
            self.logger.info("Default configurations loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to load default configurations: {e}")
            raise ConfigurationError(f"Failed to load default configurations: {str(e)}")

    def validate_config(self) -> None:
        """Validate configuration"""
        try:
            validation_results = self.validator.validate_all_configs({
                'storage': self.storage,
                'caching': self.caching,
                'indexing': self.indexing,
                'validation': self.validation
            })
            
            failed = [k for k, v in validation_results.items() if not v]
            if failed:
                raise ConfigurationError(f"Invalid configuration sections: {failed}")
                
            self.logger.info("Configuration validation successful")
            
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            raise ConfigurationError(f"Configuration validation failed: {str(e)}")

    def _setup_basic_config(self) -> None:
        """Setup basic configuration components"""
        try:
            # Initialize metrics tracking
            self.metrics = defaultdict(Counter)
            
            # Initialize caching
            self.cache = TTLCache(maxsize=1000, ttl=3600)
            
            # Create default evidence store config
            if not hasattr(self, 'evidence_store_config'):
                self.evidence_store_config = EvidenceStoreConfig.create_default()
                
            self.logger.info("Basic configuration setup completed")
            
        except Exception as e:
            self.logger.error(f"Basic configuration setup failed: {e}")
            raise ConfigurationError(f"Failed to setup basic configuration: {str(e)}")

    def _cleanup_partial_initialization(self) -> None:
        """Cleanup partial initialization state"""
        try:
            if hasattr(self, 'evidence_store') and self.evidence_store:
                self.evidence_store.cleanup()
            
            self.initialized = False
            self._initializing = False
            
            self.logger.info("Partial initialization cleaned up")
            
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")
# --------------------------------------
# PUBLIC INITIALIZATION
# --------------------------------------
    async def initialize(self) -> None:
        """Initialize configuration with proper lifecycle management"""
        if self._initializing or self.initialized:
            return
        
        self._initializing = True
        try:
            # Phase 1: Core Infrastructure
            self.config_manager = SecureConfigManager()
            await self.config_manager.initialize()
        
            # Validate core configuration
            self._validate_configuration()
        
            # Phase 2: Essential Services
            await self._initialize_core()
            await self._initialize_core_components()
        
            # Initialize evidence store with validated config
            if not hasattr(self, 'evidence_store'):
                self.evidence_store = EvidenceStore(self.evidence_store_config)
            await self.evidence_store.initialize()
            
        
            await self._initialize_artifact_manager()
            await self._initialize_checkpoint_manager()
        
            # Phase 4: Communication & Coordination
            self.communication_system = CommunicationSystem()
            await self.communication_system.initialize()
            await self._initialize_communication()
            
            # Initialize evidence store
            self.evidence_store = EvidenceStore(self)
            await self.evidence_store.initialize()
            
            
            
            # Initialize REWOO system
            self.rewoo_system = REWOOSystem(self)
            await self.rewoo_system.initialize()
            # Phase 5: AI & Processing Systems
            self.planning_system = PlanningSystem(self.config)
            await self.planning_system.initialize()
        
        
            vertex_config = self.config_manager.get_vertex_ai_config()
            await self._init_basic_components()
        
            # Phase 6: Monitoring & Management
            await self._initialize_metrics_tracker()
            await self._initialize_error_handler()
            await self._initialize_resource_manager()
        
            # Phase 7: Agent Systems
            await self._initialize_agent_factory()
            await self._initialize_llm_manager()
            await self._initialize_moa_system()
        
            # Phase 8: Knowledge & Validation
            await self._initialize_knowledge_graph()
            await self._initialize_validation_system()
        
            # Phase 9: Feedback & Learning
            await self._initialize_feedback_loop()
        
            self.initialized = True
            self.logger.info("Configuration initialized successfully")
        except Exception as e:
            self.logger.error(f"Configuration initialization failed: {e}")
            await self._cleanup_partial_initialization()
            raise
        finally:
            self._initializing = False

    async def _cleanup_partial_initialization(self) -> None:
        if hasattr(self, 'evidence_store') and self.evidence_store:
            await self.evidence_store.cleanup()
        self.initialized = False
        self._initializing = False

    
    
    

    async def _load_env_file(self) -> None:
        """Asynchronous environment file loading"""
        try:
            env_loaded = False
            for env_path in self._get_env_paths():
                if os.path.exists(env_path):
                    await self._load_env_from_path(env_path)
                    env_loaded = True
                    break
            
            if not env_loaded:
                await self._load_default_config()
        except Exception as e:
            self.logger.error(f"Environment loading failed: {e}")
            await self._load_default_config()
    def _get_env_paths(self) -> List[str]:
        """Get potential environment file paths"""
        return [
            os.path.join(os.getcwd(), '.env'),
            os.path.join(os.getcwd(), 'config', '.env'),
            os.path.join(os.path.dirname(os.getcwd()), '.env')
        ]

    

    async def _load_env_file(self) -> None:
        """Enhanced environment file loading with fallbacks"""
        try:
            env_loaded = False
            for env_path in self._get_env_paths():
                if os.path.exists(env_path):
                    load_dotenv(env_path)
                    env_loaded = True
                    self.logger.info(f"Loaded environment from {env_path}")
                    break
            
            if not env_loaded:
                self.logger.warning("No .env file found, using defaults")
                self._load_default_config()
        except Exception as e:
            self.logger.error(f"Environment loading failed: {e}")
            self._load_default_config()
    
    
    async def _init_basic_components(self) -> None:
        """Asynchronous initialization of basic components"""
        try:
            # Initialize components
            self.model_configs = {}
            self.layer_configs = {}
            self.agent_configs = []
            
            # Load environment
            await self._load_env_file()
            
            self.logger.info("Basic components initialized successfully")
        except Exception as e:
            self.logger.error(f"Basic initialization failed: {e}")
            raise

# --------------------------------------
# CORE INITIALIZATION
# --------------------------------------
    async def _initialize_core(self) -> None:
        """Initialize core infrastructure"""
        try:
            # Initialize configuration manager
            self.config_manager = SecureConfigManager()
            await self.config_manager.initialize()
            
            # Initialize metrics tracking
            self.metrics = defaultdict(Counter)
            
            # Initialize cache
            self.cache = TTLCache(maxsize=1000, ttl=3600)
            
            self.logger.info("Core initialized successfully")
        except Exception as e:
            self.logger.error(f"Core initialization failed: {e}")
            raise

    async def _initialize_core_components(self) -> None:
        """Initialize core components"""
        try:
            # Initialize evidence store
            self.evidence_store = EvidenceStore(self.config_manager.config.get('evidence_store_config', {}))
            await self.evidence_store.initialize()
            
            # Initialize planning system
            self.planning_system = PlanningSystem(self.config_manager.config)
            await self.planning_system.initialize()
            
            # Initialize communication system
            self.communication_system = CommunicationSystem()
            await self.communication_system.initialize()
            
            # Initialize REWOO system
            self.rewoo_system = REWOOSystem(self)
            await self.rewoo_system.initialize()
            
            self.logger.info("Core components initialized successfully")
        except Exception as e:
            self.logger.error(f"Core component initialization failed: {e}")
            raise
    async def _init_basic_components(self) -> None:
        """Initialize basic components"""
        try:
            # Initialize basic state tracking
            self.state = {
                'initialization': defaultdict(bool),
                'runtime': {
                    'active': False,
                    'error_state': False,
                    'last_error': None
                },
                'components': {},
                'metrics': {
                    'initialization_time': None,
                    'error_count': 0,
                    'last_operation': None
                }
            }
            
            self.logger.info("Basic components initialized successfully")
        except Exception as e:
            self.logger.error(f"Basic component initialization failed: {e}")
            raise

            
            
    
    async def _initialize_components(self) -> None:
        """Initialize system components"""
        try:
            # Initialize basic state tracking
            self.state = {
                'initialization': defaultdict(bool),
                'runtime': {
                    'active': False,
                    'error_state': False,
                    'last_error': None
                },
                'components': {},
                'metrics': {
                    'initialization_time': None,
                    'error_count': 0,
                    'last_operation': None
                }
            }
            
            self.logger.info("Components initialized successfully")
        except Exception as e:
            self.logger.error(f"Component initialization failed: {e}")
            raise

    async def _initialize_api_settings(self) -> None:
        """Initialize API settings with proper error handling"""
        try:
            if not self.config_manager:
                raise ConfigurationError("Configuration manager not initialized")
                
            vertex_config = self.config_manager.get_vertex_ai_config()
            
            self.api_settings = APISettings(
                vertex_ai_project_id=vertex_config.get('project_id', ''),
                vertex_ai_location=vertex_config.get('location', 'us-central1'),
                vertex_ai_credentials=vertex_config.get('credentials'),
                validation_mode=self.validation_mode
            )
            
            self._initialization_state['api_settings'] = True
            self.logger.info("API settings initialized successfully")
            
        except Exception as e:
            self.logger.error(f"API settings initialization failed: {e}")
            raise

    async def _initialize_model_configs(self) -> None:
        """Initialize model configurations with proper error handling."""
        try:
            if not self.api_settings:
                await self._initialize_api_settings()
            
            # Example model configurations:
            self.model_configs = {
                'BossAgent': ModelConfig(
                    model_name="claude-3-5-sonnet@20240620",
                    api_key=os.getenv("ANTHROPIC_API_KEY"),
                    layer_id=0,
                    pool_size=1,
                    metadata={
                        "role": "coordinator",
                        "capabilities": ["planning", "delegation", "synthesis"],
                        "type": "boss",
                        "client": "anthropic",
                        "model_family": "claude-3"
                    }
                ),
                'Layer1Agent': ModelConfig(
                    model_name="o1-2024-12-17",
                    api_key=os.getenv("OPENAI_API_KEY"),
                    layer_id=1,
                    pool_size=1,
                    context_window=200000,
                    max_tokens=100000,
                    metadata={
                        "role": "processor",
                        "capabilities": ["data_analysis", "transformation"],
                        "type": "worker",
                        "client": "together",
                        "model_family": "o1"
                    }
                ),
                'Layer2Agent': ModelConfig(
                    model_name="gemini-2.0-flash-exp",
                    api_key=None,  # We'll rely on vertex_ai_credentials
                    layer_id=2,
                    pool_size=1,
                    context_window=65536,
                    metadata={
                        "role": "reasoner",
                        "capabilities": ["complex_reasoning", "decision_making"],
                        "type": "worker",
                        "client": "vertex",
                        "model_family": "gemini"
                    }
                ),
                'Layer3Agent': ModelConfig(
                    model_name="mistral-large-2",
                    api_key=os.getenv("MISTRAL_API_KEY"),
                    layer_id=3,
                    pool_size=1,
                    context_window=32768,
                    metadata={
                        "role": "validator",
                        "capabilities": ["verification", "quality_assurance"],
                        "type": "worker",
                        "client": "mistral",
                        "model_family": "mistral"
                    }
                ),
                'Layer4Agent': ModelConfig(
                    model_name="o1-mini-2024-09-12",
                    api_key=os.getenv("OPENAI_API_KEY"),
                    layer_id=4,
                    pool_size=1,
                    context_window=128000,
                    max_tokens=65536,
                    metadata={
                        "role": "synthesizer",
                        "capabilities": ["integration", "summarization"],
                        "type": "worker",
                        "client": "together",
                        "model_family": "o1-mini"
                    }
                ),
            }
            # Validate all configurations
            for name, config in self.model_configs.items():
                if not await self._validate_model_config(name, config):
                    raise ConfigurationError(f"Invalid configuration for {name}")

            self.logger.info(f"Initialized {len(self.model_configs)} model configurations")
            self._initialization_state['model_configs'] = True

        
            
            # If using Vertex AI, initialize it:
            if self.api_settings.vertex_ai_project_id and self.api_settings.vertex_ai_location and vertexai is not None:
                vertexai.init(
                    project=self.api_settings.vertex_ai_project_id,
                    location=self.api_settings.vertex_ai_location
                )
                # For example, define a gemini model placeholder
                self.gemini_model = GenerativeModel("gemini-2.0-flash-exp")
            
            self._initialization_state['model_configs'] = True
            self.logger.info("Model configurations initialized successfully")
            self.logger.info(f"Initialized {len(self.model_configs)} model configurations")
        except Exception as e:
            self.logger.error(f"Model configuration initialization failed: {e}", exc_info=True)
            raise

    async def _initialize_layer_configs(self) -> None:
        """Initialize layer configurations"""
        try:
            self.layer_configs = {
                1: LayerConfig(
                    layer_id=1,
                    agents=['Layer1Agent'],
                    model_name="o1-2024-12-17",
                    metadata={'type': 'processor'}
                ),
                2: LayerConfig(
                    layer_id=2,
                    agents=['Layer2Agent'],
                    model_name="gemini-2.0-flash-exp",
                    metadata={'type': 'analyzer'}
                ),
                3: LayerConfig(
                    layer_id=3,
                    agents=['Layer3Agent'],
                    model_name="mistral-large-2",
                    metadata={'type': 'validator'}
                ),
                4: LayerConfig(
                    layer_id=4,
                    agents=['Layer4Agent'],
                    model_name="o1-mini-2024-09-12",
                    metadata={'type': 'synthesizer'}
                )
            }
            self.logger.info("Layer configurations initialized successfully")
        except Exception as e:
            self.logger.error(f"Layer configuration initialization failed: {e}")
            raise

    # --------------------------------------
    # ADDITIONAL CONFIG INIT
    # --------------------------------------
    
    # Update EnhancedConfig
    async def _initialize_configs(self) -> None:
        try:
            self.model_configs = await self._load_model_configs()
            self.layer_configs = await self._load_layer_configs()
            self.communication_config = await self._load_communication_config()
            self.evidence_store_config = await self._load_evidence_store_config()
        
            
    
            # Communication config
            self.communication_config = {
                'max_retries': 3,
                'timeout': 30,
                'batch_size': 100
            }
            # Evidence store config
            self.evidence_store_config = {
                'storage': {
                    'type': 'memory',
                    'compression': False,
                    'backup_enabled': False
                },
                'caching': {
                    'enabled': True,
                    'max_size': 1000,
                    'ttl': 3600
                },
                'indexing': {
                    'enabled': True,
                    'fields': ['type', 'source', 'timestamp']
                },
                'validation': {
                    'required_fields': ['content', 'source', 'timestamp'],
                    'max_size': 1024 * 1024
                }
            }
            # Pipeline config
            self.pipeline_config = {
                'batch_size': 1000,
                'timeout': 3600,
                'max_retries': 3
            }
            # Planning system config
            self.planning_system_config = {
                'max_planning_steps': 10,
                'planning_timeout': 300,
                'max_replanning_attempts': 3,
                'validation_threshold': 0.8
            }
            # Worker config placeholder
            self.worker_configs = {
                'default_worker': {
                    'name': 'default_worker',
                    'tool_set': {},
                    'cache_size': 1000,
                    'timeout': 3600,
                    'max_retries': 3,
                    'batch_size': 100
                }
            }
            self.logger.info("Additional configurations initialized successfully")
        except Exception as e:
            logger.error(f"Configuration initialization failed: {e}")
            raise
    # --------------------------------------
    # MAIN COMPONENTS INITIALIZATION
    # --------------------------------------
    # Add all other initialization methods...
    async def _initialize_metrics_tracker(self) -> None:
        """Initialize metrics tracking system"""
        try:
            self.metrics_tracker = MetricsTracker()
            await self.metrics_tracker.initialize()
            self.logger.info("Metrics tracker initialized successfully")
        except Exception as e:
            self.logger.error(f"Metrics tracker initialization failed: {e}")
            raise

    async def _initialize_error_handler(self) -> None:
        """Initialize error handling system"""
        try:
            self.error_handler = ErrorHandler()
            await self.error_handler.initialize()
            self.logger.info("Error handler initialized successfully")
        except Exception as e:
            self.logger.error(f"Error handler initialization failed: {e}")
            raise

    async def _initialize_resource_manager(self) -> None:
        """Initialize resource management system"""
        try:
            self.resource_manager = UnifiedResourceManager()
            await self.resource_manager.initialize()
            self.logger.info("Resource manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Resource manager initialization failed: {e}")
            raise

    # Continue with other initialization methods...
    
    async def _initialize_artifact_manager(self) -> None:
        """Initialize artifact manager"""
        try:
            if not hasattr(self, 'artifact_manager_config'):
                self.artifact_manager_config = ArtifactManagerConfig()
            
            # Create storage directory if it doesn't exist
            os.makedirs(self.artifact_manager_config.storage_path, exist_ok=True)
            
            self.logger.info("Artifact manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Artifact manager initialization failed: {e}")
            raise

    async def _initialize_checkpoint_manager(self) -> None:
        """Initialize checkpoint manager"""
        try:
            if not hasattr(self, 'checkpoint_manager_config'):
                self.checkpoint_manager_config = CheckpointManagerConfig()
            
            # Create checkpoint directory if it doesn't exist
            os.makedirs(self.checkpoint_manager_config.checkpoint_dir, exist_ok=True)
            
            self.logger.info("Checkpoint manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Checkpoint manager initialization failed: {e}")
            raise

    async def _initialize_communication(self) -> None:
        """Initialize communication system with proper configuration"""
        try:
            if not hasattr(self, 'communication_config'):
                self.communication_config = CommunicationConfig()
            
            self.communication = LayerCommunication(self)
            await self.communication.initialize()
            
            # Setup default channels
            default_channels = ['system', 'task', 'result', 'error']
            for channel in default_channels:
                await self.communication.create_channel(channel)
            
            # Setup layer channels
            for layer_id in range(1, 5):
                await self.communication.create_channel(f"layer_{layer_id}")
            
            self.logger.info("Communication system initialized successfully")
        except Exception as e:
            self.logger.error(f"Communication system initialization failed: {e}")
            raise

    async def _initialize_communication_system(self) -> None:
        """Initialize communication system with proper configuration"""
        try:
            if not hasattr(self, 'communication_config'):
                self.communication_config = CommunicationConfig()
            
            self.communication = LayerCommunication(self)
            await self.communication.initialize()
            
            # Setup default channels
            default_channels = ['system', 'task', 'result', 'error']
            for channel in default_channels:
                await self.communication.create_channel(channel)
            
            # Setup layer channels
            for layer_id in range(1, 5):
                await self.communication.create_channel(f"layer_{layer_id}")
            
            self.logger.info("Communication system initialized successfully")
        except Exception as e:
            self.logger.error(f"Communication system initialization failed: {e}")
            raise
    async def _initialize_evidence_store(self) -> None:
        """Initialize evidence store with proper validation"""
        try:
            self.evidence_store = EnhancedEvidenceStore(self.evidence_store_config)
            await self.evidence_store.initialize()
            self.logger.info("Evidence store initialized successfully")
        except Exception as e:
            self.logger.error(f"Evidence store initialization failed: {e}")
            raise
    
    async def _initialize_layer_communication(self) -> None:
        """Initialize layer communication"""
        try:
            self.communication = LayerCommunication(self)
            await self.communication.initialize()
            
            # Setup layer channels
            for layer_id in range(1, 5):
                await self.communication.create_channel(f"layer_{layer_id}")
                
            self.logger.info("Layer communication initialized successfully")
        except Exception as e:
            self.logger.error(f"Layer communication initialization failed: {e}")
            raise

    

    async def _initialize_planning_system(self) -> None:
        """Initialize planning system with the config dictionary."""
        if 'PlanningSystem' in globals():
            self.planning_system = PlanningSystem(self)
            self.planning_system.max_planning_steps =  self.planning_system_config.get('max_planning_steps', 10)
            self.planning_system.planning_timeout =     self.planning_system_config.get('planning_timeout', 300)
            self.planning_system.max_replanning_attempts = self.planning_system_config.get('max_replanning_attempts', 3)
            self.planning_system.validation_threshold = self.planning_system_config.get('validation_threshold', 0.8)
            
            await self.planning_system.initialize()
            self._initialization_state['planning'] = True
            self.logger.info("Planning system initialized")
        else:
            self.logger.warning("PlanningSystem class not found, skipping initialization.")
    # Additional helper methods...
    async def _initialize_rewoo(self) -> None:
        """Initialize REWOO system"""
        try:
            self.rewoo_system = REWOOSystem(self)
            await self.rewoo_system.initialize()
            self.logger.info("REWOO system initialized successfully")
        except Exception as e:
            self.logger.error(f"REWOO system initialization failed: {e}")
            raise
    async def _initialize_validation_rules(self) -> None:
        """Initialize validation rules with proper error handling"""
        try:
            self.validation_rules = {
                'storage': self._validate_storage_config,
                'indexing': self._validate_indexing_config,
                'validation': self._validate_validation_config
            }
            self.logger.info("Validation rules initialized")
        except Exception as e:
            self.logger.error(f"Validation rules initialization failed: {e}")
            raise


    async def _initialize_boss_agent(self) -> None:
        """Initialize the boss agent (layer 0)."""
        if 'BossAgent' in globals():
            boss_config = self.model_configs.get('BossAgent')
            if not boss_config:
                raise ConfigurationError("No BossAgent config found")
            self.boss_agent = BossAgent(
                name="MainBoss",
                model_info=boss_config.to_dict(),
                config=self
            )
            await self.boss_agent.initialize()
            self._initialization_state['boss'] = True
            self.logger.info("Boss agent initialized")
        else:
            self.logger.warning("BossAgent class not found, skipping initialization.")
    async def initialize_moa_config() -> EnhancedConfig:
        try:
            config = EnhancedConfig()
            evidence_store = EvidenceStore(EvidenceStoreConfig.create_default())
            await evidence_store.initialize()
            config.evidence_store = evidence_store
            await config.initialize()
            return config
        except Exception as e:
            logger.error(f"Configuration initialization failed: {e}")
            raise
    # Example of additional pipeline components (stub)
    async def _initialize_pipeline_components(self) -> None:
        """Initialize data pipeline components."""
        if 'DataPipelineOptimizer' in globals():
            try:
                pipeline = DataPipelineOptimizer(self)
                await pipeline.initialize()
                self._pipelines['default'] = pipeline
                self.logger.info("Pipeline components initialized successfully")
            except Exception as e:
                self.logger.error(f"Pipeline initialization failed: {e}", exc_info=True)
                raise
        else:
            self.logger.warning("DataPipelineOptimizer class not found, skipping initialization.")

    async def _initialize_worker_components(self) -> None:
        """Initialize enhanced worker components."""
        if 'EnhancedWorker' in globals():
            try:
                for worker_name, worker_config in self.worker_configs.items():
                    worker = EnhancedWorker(
                        name=worker_config.get('name', 'default_worker'),
                        config=self,
                        tool_set=worker_config.get('tool_set', {})
                    )
                    await worker.initialize()
                    self._workers[worker_name] = worker
                self.logger.info("Worker components initialized successfully")
            except Exception as e:
                self.logger.error(f"Worker initialization failed: {e}", exc_info=True)
                raise
        else:
            self.logger.warning("EnhancedWorker class not found, skipping initialization.")

    async def _initialize_world_observer(self) -> None:
        """Initialize world observer."""
        if 'WorldObserver' in globals():
            try:
                self.world_observer = WorldObserver()
                await self.world_observer.initialize()
                # Example additional configs
                self.world_observer.metrics = {}
                self.world_observer.alert_thresholds = {}
                
                # Subscribe to observation channels
                if self.communication_system:
                    await self.communication_system.subscribe('world_state', 'world_observer')
                
                self._initialization_state['world_observer'] = True
                self.logger.info("World observer initialized successfully")
            except Exception as e:
                self.logger.error(f"World observer initialization failed: {e}", exc_info=True)
                raise
        else:
            self.logger.warning("WorldObserver class not found, skipping initialization.")
    async def _initialize_agent_coordination(self) -> None:
        """Initialize agent coordination mechanisms"""
        try:
            # Set up coordination channels
            coordination_channels = [
                'task_assignment',
                'result_collection',
                'agent_communication',
                'system_updates'
            ]
            
            for channel in coordination_channels:
                if self.communication_system:
                    await self.communication_system.create_channel(channel)
            
            # Set up agent subscriptions
            for layer_id, agents in self.layer_agents.items():
                for agent in agents:
                    if self.communication_system:
                        await self.communication_system.register_agent(
                            agent.name,
                            ['task_assignment', 'agent_communication']
                        )
            
            self.logger.info("Agent coordination initialized successfully")
        except Exception as e:
            self.logger.error(f"Agent coordination initialization failed: {e}")
            raise
    async def _initialize_metrics_tracker(self) -> None:
        """Initialize metrics tracker."""
        try:
            self.metrics_tracker = MetricsTracker()
            await self.metrics_tracker.initialize()
            
            self._initialization_state['metrics_tracker'] = True
            self.logger.info("Metrics tracker initialized successfully")
        except Exception as e:
            self.logger.error(f"Metrics tracker initialization failed: {e}")
            raise

    async def _initialize_agent_factory(self) -> None:
        """Initialize agent factory."""
        try:
            self.agent_factory = AgentFactory(self)
            await self.agent_factory.initialize()
            
            self._initialization_state['agent_factory'] = True
            self.logger.info("Agent factory initialized successfully")
        except Exception as e:
            self.logger.error(f"Agent factory initialization failed: {e}")
            raise

    async def _initialize_moa_system(self) -> None:
        """Initialize MoA system."""
        try:
            self.moa_system = EnhancedMoASystem(self)
            await self.moa_system.initialize()
            
            # Subscribe to MoA channels
            await self.communication_system.subscribe('moa_coordination', 'moa_system')
            
            self._initialization_state['moa_system'] = True
            self.logger.info("MoA system initialized successfully")
        except Exception as e:
            self.logger.error(f"MoA system initialization failed: {e}")
            raise

    async def _initialize_knowledge_graph(self) -> None:
        """Initialize knowledge graph."""
        try:
            self.knowledge_graph = EnhancedKnowledgeGraph(self)
            await self.knowledge_graph.initialize()
            
            self._initialization_state['knowledge_graph'] = True
            self.logger.info("Knowledge graph initialized successfully")
        except Exception as e:
            self.logger.error(f"Knowledge graph initialization failed: {e}")
            raise

    async def _initialize_resource_manager(self) -> None:
        """Initialize resource manager."""
        try:
            self.resource_manager = UnifiedResourceManager()
            await self.resource_manager.initialize()
            
            self._initialization_state['resource_manager'] = True
            self.logger.info("Resource manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Resource manager initialization failed: {e}")
            raise

    async def _initialize_validation_system(self) -> None:
        """Initialize validation system."""
        try:
            self.validation_system = ValidationSystem()
            await self.validation_system.initialize()
            
            self._initialization_state['validation_system'] = True
            self.logger.info("Validation system initialized successfully")
        except Exception as e:
            self.logger.error(f"Validation system initialization failed: {e}")
            raise

    async def _initialize_llm_manager(self) -> None:
        """Initialize LLM manager."""
        try:
            self.llm_manager = UnifiedLLMManager(self)
            await self.llm_manager.initialize()
            
            self._initialization_state['llm_manager'] = True
            self.logger.info("LLM Manager initialized successfully")
        except Exception as e:
            self.logger.error(f"LLM Manager initialization failed: {e}")
            raise

    async def _initialize_error_handler(self) -> None:
        """Initialize error handler."""
        try:
            self.error_handler = ErrorHandler()
            await self.error_handler.initialize()
            
            self._initialization_state['error_handler'] = True
            self.logger.info("Error Handler initialized successfully")
        except Exception as e:
            self.logger.error(f"Error Handler initialization failed: {e}")
            raise

    async def _initialize_feedback_loop(self) -> None:
        """Initialize feedback loop."""
        try:
            self.feedback_loop = FeedbackLoop()
            await self.feedback_loop.initialize()
            
            self._initialization_state['feedback_loop'] = True
            self.logger.info("Feedback loop initialized successfully")
        except Exception as e:
            self.logger.error(f"Feedback loop initialization failed: {e}")
            raise
    # --------------------------------------
    # Agent initialization stubs
    # --------------------------------------
    async def _initialize_agents(self) -> None:
        """Initialize all agent components (example)."""
        try:
            # Initialize agent factory
            if 'AgentFactory' in globals():
                self.agent_factory = AgentFactory(self)
                await self.agent_factory.initialize()
            
            await self._initialize_layer_agents()
            await self._initialize_agent_coordination()
            
            self.logger.info("Agent initialization completed successfully")
        except Exception as e:
            self.logger.error(f"Agent initialization failed: {e}", exc_info=True)
            raise

    async def _initialize_layer_agents(self) -> None:
        """Enhanced layer agent initialization with retry logic"""
        try:
            for layer_id in range(1, 5):
                retry_count = 0
                max_retries = 3
            
                while retry_count < max_retries:
                    try:
                        agent_configs = self._get_layer_configs(layer_id)
                        for config in agent_configs:
                            agent = await self._create_layer_agent(config)
                            if agent:
                                await self._register_agent(agent)
                                self.logger.info(f"Initialized agent for layer {layer_id}")
                        break
                    except Exception as e:
                        retry_count += 1
                        self.logger.warning(f"Layer {layer_id} initialization attempt {retry_count} failed: {e}")
                        if retry_count == max_retries:
                            raise
                        await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                    
            self.logger.info("Layer agents initialized successfully")
        
        except Exception as e:
            self.logger.error(f"Layer agent initialization failed: {e}")
            raise

    async def _create_layer_agent(self, agent_name: str, layer_id: int):
        """Placeholder method that would create an agent instance."""
        # In a real system, you might use the agent_factory or similar
        config = self.model_configs.get(agent_name)
        if not config:
            self.logger.error(f"No model config found for {agent_name}")
            return None
        # For demonstration, re-using BossAgent as a stub
        agent = BossAgent(
            name=agent_name,
            model_info=config.to_dict(),
            config=self
        )
        await agent.initialize()
        return agent

    async def _initialize_agent_coordination(self) -> None:
        """Initialize agent coordination mechanisms."""
        try:
            # Possibly set up channels
            coordination_channels = [
                'task_assignment',
                'result_collection',
                'agent_communication',
                'system_updates'
            ]
            if self.communication_system:
                for channel in coordination_channels:
                    await self.communication_system.create_channel(channel)
            
            # Example: register agents
            for layer_id, agents in self.layer_agents.items():
                for agent in agents:
                    if self.communication_system:
                        await self.communication_system.register_agent(
                            agent.name,
                            ['task_assignment', 'agent_communication']
                        )
            
            self.logger.info("Agent coordination initialized successfully")
        except Exception as e:
            self.logger.error(f"Agent coordination initialization failed: {e}", exc_info=True)
            raise
    
    # --------------------------------------
    # VALIDATION / VERIFICATION
    # --------------------------------------
    
    async def _verify_configuration_sections(self) -> bool:
        """Verify configuration sections with detailed validation"""
        try:
            section_validators = {
                'model_config': self._validate_model_config,
                'layer_config': self._validate_layer_config,
                'communication_config': self._validate_communication_config,
                'evidence_store_config': self._validate_evidence_store_config,
                'validation_config': self._validate_validation_config
            }
        
            validation_results = {}
            for section, validator in section_validators.items():
                try:
                    validation_results[section] = await validator()
                except Exception as e:
                    self.logger.error(f"Validation failed for section {section}: {e}")
                    validation_results[section] = False
                
            # Generate validation report
            self._generate_validation_report(validation_results)
        
            return all(validation_results.values())
        
        except Exception as e:
            self.logger.error(f"Configuration section verification failed: {e}")
            return False
            
            
    async def _verify_initialization(self) -> bool:
        """Verify everything we consider essential is up and running."""
        try:
            # Check for communication
            if not await self._verify_communication():
                return False
            
            # Check if evidence store is ready
            if not await self._verify_evidence_store():
                return False

            # Check boss agent
            if not await self._verify_boss_agent():
                return False

            return True
        except Exception as e:
            self.logger.error(f"Initialization verification failed: {e}")
            return False

    async def _verify_communication(self) -> bool:
        """Verify communication system initialization and functionality."""
        try:
            if not self.communication_system:
                self.logger.error("Communication system not initialized")
                return False
            if not self.communication_system.initialized:
                self.logger.error("Communication system not marked as initialized")
                return False
            
            # Verify required channels exist
            required_channels = {'system', 'coordination', 'task_assignment', 'result_collection'}
            existing_channels = set(self.communication_system.channels.keys())
            # It's okay if they don't all exist, but let's just do a minimal check
            missing_channels = required_channels - existing_channels
            if missing_channels:
                self.logger.warning(f"Missing some channels: {missing_channels}")
            
            # Example channel verification
            if 'system' in self.communication_system.channels:
                channel = self.communication_system.channels['system']
                if channel.empty():
                    # Put & get test
                    test_message = {'test': True}
                    await channel.put(test_message)
                    received_message = await channel.get()
                    if received_message != test_message:
                        self.logger.error("System channel message verification failed")
                        return False
            
            return True
        except Exception as e:
            self.logger.error(f"Communication verification failed: {e}")
            return False

    async def _verify_evidence_store(self) -> bool:
        """Verify evidence store initialization."""
        try:
            if not self.evidence_store:
                self.logger.error("Evidence store not initialized")
                return False
            if not self.evidence_store.initialized:
                self.logger.error("Evidence store not marked as initialized")
                return False
            return True
        except Exception as e:
            self.logger.error(f"Evidence store verification failed: {e}")
            return False

    async def _verify_boss_agent(self) -> bool:
        """Verify boss agent initialization."""
        try:
            if not self.boss_agent:
                self.logger.error("Boss agent not initialized")
                return False
            if not self.boss_agent.initialized:
                self.logger.error("Boss agent not marked as initialized")
                return False
            return True
        except Exception as e:
            self.logger.error(f"Boss agent verification failed: {e}")
            return False

    # --------------------------------------
    # CLEANUP
    # --------------------------------------
    async def cleanup(self) -> None:
        """Cleanup configuration resources"""
        try:
            # First cleanup components
            await self._cleanup_components()
            
            # Then cleanup core
            await self._cleanup_core()
            
            self.initialized = False
            self._initializing = False
            
            self.logger.info("Enhanced configuration cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Configuration cleanup failed: {e}")
            raise

    
            

    async def _cleanup_core(self) -> None:
        """Cleanup core infrastructure"""
        try:
            if self.config_manager:
                await self.config_manager.cleanup()
            
            self.metrics.clear()
            if self.cache:
                self.cache.clear()
                
            self.logger.info("Core cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Core cleanup failed: {e}")
            raise

    async def _cleanup_components(self) -> None:
        """Cleanup core components"""
        try:
            if self.evidence_store:
                await self.evidence_store.cleanup()
            
            if self.planning_system:
                await self.planning_system.cleanup()
                
            if self.communication_system:
                await self.communication_system.cleanup()
                
            if self.rewoo_system:
                await self.rewoo_system.cleanup()
                
            self.logger.info("Core components cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Component cleanup failed: {e}")
            raise
            
    async def _cleanup_partial_initialization(self) -> None:
        """Cleanup partially initialized components"""
        try:
            components_to_cleanup = [
                (self.evidence_store, 'Evidence store'),
                (self.communication_system, 'Communication system'),
                (self.planning_system, 'Planning system'),
                (self.rewoo_system, 'REWOO system'),
                (self.artifact_manager, 'Artifact manager'),
                (self.checkpoint_manager, 'Checkpoint manager'),
                (self.metrics_tracker, 'Metrics tracker'),
                (self.error_handler, 'Error handler'),
                (self.resource_manager, 'Resource manager'),
                (self.agent_factory, 'Agent factory'),
                (self.llm_manager, 'LLM manager'),
                (self.moa_system, 'MOA system'),
                (self.knowledge_graph, 'Knowledge graph'),
                (self.validation_system, 'Validation system'),
                (self.feedback_loop, 'Feedback loop')
            ]
        
            for component, name in components_to_cleanup:
                if component is not None:
                    try:
                        await component.cleanup()
                    except Exception as e:
                        self.logger.error(f"Failed to cleanup {name}: {e}")
                    
            self.initialized = False
            self._initializing = False
            self.logger.info("Partial initialization cleaned up")
        
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")
    
    async def _cleanup_partial_initialization(self) -> None:
        """Cleanup partial initialization state"""
        try:
            if hasattr(self, 'evidence_store') and self.evidence_store:
                await self.evidence_store.cleanup()
            self.initialized = False
            self._initializing = False
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")
    # --------------------------------------
    # GETTERS
    # --------------------------------------
    def get_config_status(self) -> Dict[str, Any]:
        """Get configuration status"""
        return {
            'initialized': self.initialized,
            'initializing': self._initializing,
            'profile': self.configuration_profile,
            'validation_mode': self.validation_mode,
            'components': {
                'evidence_store': hasattr(self, 'evidence_store') and self.evidence_store is not None,
                'validator': hasattr(self, 'validator') and self.validator is not None
            }
        }

    def get_model_config(self, model_name: str) -> Optional[ModelConfig]:
        """Get model configuration by name."""
        return self.model_configs.get(model_name)
        
    def get_layer_config(self, layer_id: int) -> Optional[LayerConfig]:
        """Get layer configuration by ID"""
        return self.layer_configs.get(layer_id)
        
    def get_api_status(self) -> Dict[str, str]:
        """Get status of API configurations."""
        if not self.api_settings:
            return {
                'openai': 'missing',
                'anthropic': 'missing',
                'mistral': 'missing',
                'vertex_ai': 'missing'
            }
        return {
            'openai': 'configured' if self.api_settings.openai_api_key else 'missing',
            'anthropic': 'configured' if self.api_settings.anthropic_api_key else 'missing',
            'mistral': 'configured' if self.api_settings.mistral_api_key else 'missing',
            'vertex_ai': 'configured' if self.api_settings.vertex_ai_project_id else 'missing'
        }
    def get_boss_config(self) -> ModelConfig:
        """Get boss agent configuration"""
        try:
            boss_config = ModelConfig(
                model_name="claude-3-5-sonnet@20240620",
                api_key=os.getenv("ANTHROPIC_API_KEY"),
                layer_id=0,
                version="latest",
                max_tokens=4000,
                temperature=0.7,
                top_p=1.0,
                context_window=8192,
                pool_size=1,
                metadata={
                    "role": "coordinator",
                    "capabilities": ["planning", "delegation", "synthesis"],
                    "type": "boss",
                    "client": "anthropic",
                    "model_family": "claude-3"
                }
            )

            if not self._validate_boss_config(boss_config):
                raise ConfigurationError("Invalid boss agent configuration")

            return boss_config

        except Exception as e:
            self.logger.error(f"Failed to get boss configuration: {e}")
            raise
            
    

    def get_pipeline(self, name: str = 'default') -> Optional[DataPipelineOptimizer]:
        """Get initialized pipeline optimizer."""
        if not self.initialized:
            raise RuntimeError("Configuration not initialized")
        return self._pipelines.get(name)

    def get_worker(self, name: str) -> Optional[EnhancedWorker]:
        """Get initialized worker by name."""
        if not self.initialized:
            raise RuntimeError("Configuration not initialized")
        return self._workers.get(name)
    @property
    def status(self) -> Dict[str, Any]:
        """Get current status of all components"""
        return {
            'initialized': self.initialized,
            'initializing': self._initializing,
            'components': {
                'evidence_store': self.evidence_store.initialized if hasattr(self, 'evidence_store') else False,
                'communication_system': self.communication_system.initialized if hasattr(self, 'communication_system') else False,
                'planning_system': self.planning_system.initialized if hasattr(self, 'planning_system') else False,
                'rewoo_system': self.rewoo_system.initialized if hasattr(self, 'rewoo_system') else False,
                # Add other components...
            }
        }
    # --------------------------------------
    # OPTIONAL VALIDATIONS
    # --------------------------------------
    
            
    def ensure_defaults(self) -> None:
        """Ensure all default values are present"""
        default_configs = {
            'storage': {'type': 'memory', 'compression': False, 'backup_enabled': False, 'path': 'evidence_store'},
            'caching': {'enabled': True, 'max_size': 1000, 'ttl': 3600, 'strategy': 'lru'},
            'indexing': {'enabled': True, 'fields': ['type', 'source', 'timestamp'], 'auto_update': True},
            'validation': {'required_fields': ['content', 'source', 'timestamp'], 'max_size': 1024 * 1024, 'strict_mode': True}
        }

        for section, defaults in default_configs.items():
            if not hasattr(self, section):
                setattr(self, section, defaults)
                self.logger.info(f"Setting default for {section}")
            else:
                self.logger.info(f"Section {section} already exists, skipping default")

    

    @classmethod
    def create_with_defaults(cls) -> 'EvidenceStoreConfig':
        """Create configuration with defaults"""
        try:
            config = cls()
            config._ensure_defaults()
            return config
        except Exception as e:
            logger.error(f"Failed to create configuration with defaults: {e}")
            raise ConfigurationError(f"Configuration creation failed: {str(e)}")
    
    
    def _validate_initialization_state(self) -> bool:
        """Validate initialization state of all components"""
        required_components = [
            'evidence_store',
            'communication_system',
            'planning_system',
            'rewoo_system',
            'metrics_tracker',
            'knowledge_graph'
        ]
    
        return all(
            hasattr(self, component) and
            getattr(self, component) is not None and
            getattr(self, component).initialized
            for component in required_components
        )
    
    def _validate_boss_config(self, config: ModelConfig) -> bool:
        """Validate boss agent configuration"""
        try:
            # Check required fields
            required_fields = ['model_name', 'api_key', 'layer_id', 'metadata']
            if not all(hasattr(config, field) for field in required_fields):
                return False

            # Validate layer ID
            if config.layer_id != 0:
                return False

            # Validate metadata
            required_metadata = ['role', 'capabilities', 'type']
            if not all(key in config.metadata for key in required_metadata):
                return False

            return True

        except Exception as e:
            self.logger.error(f"Boss configuration validation failed: {e}")
            return False
    async def _handle_initialization_error(self, error: Exception) -> None:
        """Handle initialization errors with proper cleanup"""
        try:
            # Record error
            self.logger.error(f"Initialization error: {error}")
            
            # Cleanup any partially initialized components
            await self._cleanup_partial_initialization()
            
            # Reset initialization state
            self.initialized = False
            self._initializing = False
            self._initialization_state.clear()
        except Exception as e:
            self.logger.error(f"Error handler failed: {e}")
            raise

    
            
    def _validate_communication_config(self) -> bool:
        """Optional separate method to validate communication config structure."""
        try:
            if not isinstance(self.communication_config, dict):
                self.logger.error("communication_config must be a dictionary")
                return False
            required_keys = {'max_retries', 'timeout', 'batch_size'}
            if not required_keys.issubset(self.communication_config.keys()):
                self.logger.error("communication_config missing required keys")
                return False
            return True
        except Exception as e:
            self.logger.error(f"Communication config validation failed: {e}")
            return False

    async def _validate_model_config(self, name: str, config: ModelConfig) -> bool:
        """Validate model configuration"""
        try:
            # Check required attributes
            required_attrs = ['model_name', 'layer_id', 'metadata']
            if not all(hasattr(config, attr) for attr in required_attrs):
                return False

            # Validate metadata
            required_metadata = ['role', 'capabilities', 'type']
            if not all(key in config.metadata for key in required_metadata):
                return False

            # Validate layer-specific requirements
            if name == 'BossAgent' and config.layer_id != 0:
                return False

            return True

        except Exception as e:
            self.logger.error(f"Model configuration validation failed for {name}: {e}")
            return False

    
    def _validate_layer_config(self, layer_config: Dict[str, Any]) -> bool:
        """Validate layer configuration dict."""
        required_fields = ['agents', 'metadata']
        return all(field in layer_config for field in required_fields)

    def _validate_configuration(self) -> None:
        """Validate configuration settings with proper initialization check."""
        try:
            # Validate configuration profile
            if self.configuration_profile not in ['minimal', 'full', 'test']:
                raise ConfigurationError(f"Invalid configuration profile: {self.configuration_profile}")
            
            # Validate validation mode
            if self.validation_mode not in ['strict', 'lenient']:
                raise ConfigurationError(f"Invalid validation mode: {self.validation_mode}")
            
            # Validate presence of essential containers
            required_components = ['model_configs', 'layer_configs', 'agent_configs']
            missing_components = []
            for component in required_components:
                if not hasattr(self, component) or getattr(self, component) is None:
                    missing_components.append(component)
            
            if missing_components:
                raise ConfigurationError(f"Missing or uninitialized components: {', '.join(missing_components)}")
            
            self.logger.info("Configuration validation completed successfully")
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            raise ConfigurationError(f"Configuration validation failed: {str(e)}")


    
   

    # ============================
    # END OF FINAL UNIFIED CLASS
    # ============================
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import asyncio
import os
import json
import time
import hashlib
import uuid
import traceback
import statistics
import psutil
import nest_asyncio
nest_asyncio.apply()

import re
import base64
import csv
import chardet
import pandas as pd
import numpy as np
import networkx as nx
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Type, Set
from collections import defaultdict, Counter, deque
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from cachetools import TTLCache
from google.cloud import bigquery, storage
from google.api_core import exceptions, retry
from google.oauth2 import service_account
import aiohttp
from dotenv import load_dotenv
import matplotlib.pyplot as plt

# ----------------------------------------------------------------------
# Setup logging at module level
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()





# Enhanced Artifact Management System

import json
import hashlib
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

@dataclass
class ArtifactMetadata:
    """Enhanced metadata for artifacts"""
    created_at: datetime
    updated_at: datetime
    version: int
    creator: str
    artifact_type: str
    tags: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    validation_status: str = "pending"
    checksum: str = ""
    size_bytes: int = 0
    
# ----------------------------------------------------------------------
# Checkpoint Manager
# ----------------------------------------------------------------------
class EnhancedCheckpointManager:
    """Manages system checkpoints and state persistence"""
    def __init__(self, checkpoint_dir: str = 'checkpoints'):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_file = os.path.join(checkpoint_dir, 'checkpoint.json')
        self.current_state: Dict[str, Any] = {}
        self.initialized = False
    
    async def initialize(self) -> None:
        """Initialize checkpoint manager"""
        try:
            os.makedirs(self.checkpoint_dir, exist_ok=True)
            await self.load_checkpoint()
            self.initialized = True
            self.logger.info("Checkpoint manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Checkpoint manager initialization failed: {e}")
            raise

    async def load_checkpoint(self) -> None:
        """Load checkpoint from file"""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    self.current_state = json.load(f)
                self.logger.info(f"Loaded checkpoint from {self.checkpoint_file}")
            else:
                self.current_state = {
                    'created_at': datetime.now().isoformat(),
                    'last_updated': datetime.now().isoformat(),
                    'checkpoints': {}
                }
                await self.save_checkpoint()
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            raise

    async def save_checkpoint(self) -> None:
        """Save current state to checkpoint file"""
        try:
            self.current_state['last_updated'] = datetime.now().isoformat()
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.current_state, f, indent=2)
            self.logger.info(f"Saved checkpoint to {self.checkpoint_file}")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
            raise

    async def get_state(self, key: str) -> Optional[Any]:
        """Get state value by key"""
        return self.current_state.get('checkpoints', {}).get(key)

    async def set_state(self, key: str, value: Any) -> None:
        """Set state value by key"""
        if 'checkpoints' not in self.current_state:
            self.current_state['checkpoints'] = {}
        self.current_state['checkpoints'][key] = value
        await self.save_checkpoint()

    async def cleanup(self) -> None:
        """Cleanup checkpoint manager resources"""
        try:
            await self.save_checkpoint()
            self.initialized = False
            self.logger.info("Checkpoint manager cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Checkpoint manager cleanup failed: {e}")
            raise

# ----------------------------------------------------------------------
# Artifact Classes & Manager
# ----------------------------------------------------------------------
@dataclass
class ArtifactMetadata:
    """Enhanced metadata for artifacts"""
    created_at: datetime
    updated_at: datetime
    version: int
    creator: str
    artifact_type: str
    tags: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    validation_status: str = "pending"
    checksum: str = ""
    size_bytes: int = 0

class Artifact:
    """Artifact class for storing data and metadata"""
    def __init__(self, artifact_type: str, subagent: str, descriptor: str, content: Any, metadata: Dict[str, Any]):
        self.artifact_type = artifact_type
        self.subagent = subagent
        self.descriptor = descriptor
        self.version = 1
        self.content = content
        self.metadata = metadata
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

    def update(self, content: Any, metadata: Dict[str, Any]) -> None:
        """Update artifact content and metadata"""
        self.content = content
        self.metadata.update(metadata)
        self.version += 1
        self.updated_at = datetime.now()

class ArtifactRegistry:
    """Registry for managing artifacts"""
    def __init__(self):
        self.artifacts: Dict[str, List[Artifact]] = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    async def initialize(self) -> None:
        """Initialize the artifact registry"""
        try:
            self.artifacts.clear()
            # Load any existing artifacts from storage
            await self._load_existing_artifacts()
            self.logger.info("Artifact registry initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize artifact registry: {e}")
            raise

    async def _load_existing_artifacts(self) -> None:
        """Load existing artifacts from storage"""
        try:
            artifact_file = os.path.join(self._get_storage_path(), 'artifacts.json')
            if os.path.exists(artifact_file):
                with open(artifact_file, 'r') as f:
                    stored_artifacts = json.load(f)
                    for artifact_data in stored_artifacts:
                        self._restore_artifact(artifact_data)
        except Exception as e:
            self.logger.warning(f"Failed to load existing artifacts: {e}")

    def _get_storage_path(self) -> str:
        """Get the storage path for artifacts"""
        storage_path = os.path.join(os.getcwd(), 'artifacts')
        os.makedirs(storage_path, exist_ok=True)
        return storage_path

    def _restore_artifact(self, artifact_data: Dict[str, Any]) -> None:
        """Restore artifact from stored data"""
        artifact = Artifact(
            artifact_type=artifact_data['type'],
            subagent=artifact_data['subagent'],
            descriptor=artifact_data['descriptor'],
            content=artifact_data['content'],
            metadata=artifact_data['metadata']
        )
        artifact_id = self._generate_artifact_id(artifact)
        if artifact_id not in self.artifacts:
            self.artifacts[artifact_id] = []
        self.artifacts[artifact_id].append(artifact)

    def create_artifact(self, artifact_type: str, subagent: str, descriptor: str,
                       content: Any, metadata: Dict[str, Any]) -> str:
        """Create a new artifact and return its identifier"""
        artifact = Artifact(artifact_type, subagent, descriptor, content, metadata)
        artifact_id = self._generate_artifact_id(artifact)
        if artifact_id not in self.artifacts:
            self.artifacts[artifact_id] = []
        self.artifacts[artifact_id].append(artifact)
        self.logger.info(f"Created artifact: {artifact_id}")
        return artifact_id

    def _generate_artifact_id(self, artifact: Artifact) -> str:
        """Generate a unique identifier for an artifact"""
        return f"ARTIFACT_{artifact.artifact_type}_{artifact.subagent}_{artifact.descriptor}_v{artifact.version}"

    async def cleanup(self) -> None:
        """Cleanup artifact registry resources"""
        try:
            # Save artifacts to storage
            await self._save_artifacts()
            self.artifacts.clear()
            self.logger.info("Artifact registry cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Failed to cleanup artifact registry: {e}")
            raise

    async def _save_artifacts(self) -> None:
        """Save artifacts to storage"""
        try:
            artifacts_data = []
            for artifact_versions in self.artifacts.values():
                for artifact in artifact_versions:
                    artifacts_data.append({
                        'type': artifact.artifact_type,
                        'subagent': artifact.subagent,
                        'descriptor': artifact.descriptor,
                        'content': artifact.content,
                        'metadata': artifact.metadata,
                        'version': artifact.version,
                        'created_at': artifact.created_at.isoformat(),
                        'updated_at': artifact.updated_at.isoformat()
                    })
            
            storage_path = self._get_storage_path()
            with open(os.path.join(storage_path, 'artifacts.json'), 'w') as f:
                json.dump(artifacts_data, f)
        except Exception as e:
            self.logger.error(f"Failed to save artifacts: {e}")
            raise

# ----------------------------------------------------------------------
# EnhancedArtifactManager
# ----------------------------------------------------------------------
class EnhancedArtifactManager:
    """Enhanced artifact management with improved tracking and validation"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.artifacts: Dict[str, Any] = {}
        self.indices: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
        self.metrics = defaultdict(Counter)
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.initialized = False

    def _initialize_indices(self) -> None:
        """Initialize artifact indices"""
        self.indices = {
            'type': defaultdict(list),
            'creator': defaultdict(list),
            'tag': defaultdict(list),
            'status': defaultdict(list),
            'version': defaultdict(list)
        }

    async def initialize(self) -> None:
        """Initialize artifact manager"""
        try:
            self.artifacts.clear()
            self.indices.clear()
            self.metrics.clear()
            self.cache.clear()
            self._initialize_indices()
            self.initialized = True
            self.logger.info("Artifact manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Artifact manager initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup artifact manager resources"""
        try:
            self.artifacts.clear()
            self.indices.clear()
            self.metrics.clear()
            self.cache.clear()
            self.logger.info("Artifact manager cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Artifact manager cleanup failed: {e}")
            raise
            
    def _calculate_checksum(self, content: Any) -> str:
        """Calculate content checksum"""
        content_str = json.dumps(content, sort_keys=True)
        return hashlib.sha256(content_str.encode()).hexdigest()

    def _calculate_size(self, content: Any) -> int:
        """Calculate content size in bytes"""
        return len(json.dumps(content).encode())

    async def create_artifact(self,
                              content: Any,
                              artifact_type: str,
                              creator: str,
                              tags: Optional[List[str]] = None) -> str:
        """Create new artifact with enhanced validation and tracking"""
        try:
            # Generate artifact ID (simple approach)
            artifact_id = f"{artifact_type.upper()}_{uuid.uuid4().hex}"
            
            metadata = ArtifactMetadata(
                created_at=datetime.now(),
                updated_at=datetime.now(),
                version=1,
                creator=creator,
                artifact_type=artifact_type,
                tags=tags or [],
                checksum=self._calculate_checksum(content),
                size_bytes=self._calculate_size(content)
            )

            artifact = {
                'id': artifact_id,
                'content': content,
                'metadata': metadata,
                'history': []
            }

            # Validate artifact
            await self._validate_artifact(artifact)
            
            # Store artifact
            self.artifacts[artifact_id] = artifact
            
            # Update indices
            self._update_indices(artifact_id, artifact)
            self.metrics['artifacts_created'][artifact_type] += 1
            
            return artifact_id
            
        except Exception as e:
            self.logger.error(f"Artifact creation failed: {e}")
            self.metrics['creation_failures'][artifact_type] += 1
            raise

    async def get_artifact(self, artifact_id: str, version: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Retrieve artifact with optional version control"""
        if artifact_id not in self.artifacts:
            return None

        artifact = self.artifacts[artifact_id]
        if version is None:
            return artifact
        
        # Return historical version if requested
        if version <= len(artifact['history']):
            # Reconstruct from history
            base = artifact['history'][version - 1]
            return {
                'id': artifact_id,
                'content': base['content'],
                'metadata': base['metadata']
            }
        return artifact

    async def update_artifact(self,
                              artifact_id: str,
                              content: Any,
                              metadata_updates: Optional[Dict[str, Any]] = None) -> str:
        """Update artifact with version control and validation"""
        try:
            if artifact_id not in self.artifacts:
                raise ValueError(f"Artifact {artifact_id} not found")

            artifact = self.artifacts[artifact_id]
            
            # Save current state in history
            artifact['history'].append({
                'content': artifact['content'],
                'metadata': asdict(artifact['metadata'])
            })

            # Update content and metadata
            artifact['content'] = content
            if metadata_updates:
                for k, v in metadata_updates.items():
                    if hasattr(artifact['metadata'], k):
                        setattr(artifact['metadata'], k, v)

            # Increment version and timestamps
            artifact['metadata'].version += 1
            artifact['metadata'].updated_at = datetime.now()
            artifact['metadata'].checksum = self._calculate_checksum(content)
            artifact['metadata'].size_bytes = self._calculate_size(content)

            # Validate updated artifact
            await self._validate_artifact(artifact)
            
            # Update indices
            self._update_indices(artifact_id, artifact)
            self.metrics['artifacts_updated'][artifact['metadata'].artifact_type] += 1
            
            return artifact_id

        except Exception as e:
            if 'metadata' in self.artifacts[artifact_id]:
                art_type = self.artifacts[artifact_id]['metadata'].artifact_type
                self.metrics['update_failures'][art_type] += 1
            self.logger.error(f"Artifact update failed: {e}")
            raise

    async def _validate_artifact(self, artifact: Dict[str, Any]) -> None:
        """Validate artifact content and metadata"""
        if artifact['content'] is None:
            artifact['metadata'].validation_status = "invalid"
            raise ValueError("Artifact content cannot be None")
        
        if not artifact['metadata'].artifact_type:
            artifact['metadata'].validation_status = "invalid"
            raise ValueError("Artifact type is required")

        if not artifact['metadata'].creator:
            artifact['metadata'].validation_status = "invalid"
            raise ValueError("Artifact creator is required")

        # If all checks pass
        artifact['metadata'].validation_status = "valid"

    def _update_indices(self, artifact_id: str, artifact: Dict[str, Any]) -> None:
        """Update artifact indices"""
        a_meta = artifact['metadata']
        self.indices['type'][a_meta.artifact_type].append(artifact_id)
        self.indices['creator'][a_meta.creator].append(artifact_id)
        for tag in a_meta.tags:
            self.indices['tag'][tag].append(artifact_id)
        self.indices['status'][a_meta.validation_status].append(artifact_id)
        self.indices['version'][str(a_meta.version)].append(artifact_id)

    def get_metrics(self) -> Dict[str, Any]:
        """Get artifact management metrics"""
        return {
            'artifacts_created': dict(self.metrics['artifacts_created']),
            'artifacts_updated': dict(self.metrics['artifacts_updated']),
            'creation_failures': dict(self.metrics['creation_failures']),
            'update_failures': dict(self.metrics['update_failures']),
            'total_artifacts': len(self.artifacts),
            'artifacts_by_type': {
                type_: len(ids) for type_, ids in self.indices['type'].items()
            },
            'artifacts_by_status': {
                status: len(ids) for status, ids in self.indices['status'].items()
            }
        }

# ----------------------------------------------------------------------
# Evidence & EvidenceStore
# ----------------------------------------------------------------------
from typing import Dict, Any, Optional
from collections import defaultdict
from datetime import datetime
import asyncio
import logging
from cachetools import TTLCache

@dataclass
class Evidence:
    """Evidence structure for tracking and validation"""
    id: str
    content: Any
    source: str
    timestamp: datetime
    evidence_type: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    validation_score: float = 0.0
    context: Dict[str, Any] = field(default_factory=dict)

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

class EnhancedEvidenceStore:
    """Enhanced evidence store with proper configuration handling"""
    def __init__(self, config: EvidenceStoreConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.initialized = False
        self._storage = None
        self._cache = None
        self._index = None
        
        #
        self.evidence_chains = defaultdict(list)
        self.version_control = VersionControl()
        self.metrics = defaultdict(Counter)
        self._lock = asyncio.Lock()
        
    async def initialize(self) -> None:
        """Initialize evidence store with proper validation"""
        try:
            # Initialize storage
            self._storage = await self._initialize_storage()
            
            # Initialize cache if enabled
            if self.config.caching['enabled']:
                self._cache = TTLCache(
                    maxsize=self.config.caching['max_size'],
                    ttl=self.config.caching['ttl']
                )
            
            # Initialize index if enabled
            if self.config.indexing['enabled']:
                self._index = {}
                for field in self.config.indexing['fields']:
                    self._index[field] = defaultdict(list)

            self.initialized = True
            self.logger.info("Evidence store initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Evidence store initialization failed: {e}")
            raise

    async def _initialize_storage(self):
        """Initialize storage backend based on configuration"""
        if self.config.storage.type == "memory":
            return {}
        elif self.config.storage.type == "disk":
            os.makedirs(self.config.storage.path, exist_ok=True)
            return self.config.storage.path
        else:
            raise ConfigurationError(f"Unsupported storage type: {self.config.storage.type}")
        

    def _validate_and_convert_config(self, config: Union[Dict[str, Any], EvidenceStoreConfig]) -> EvidenceStoreConfig:
        """Validate and convert configuration to proper format"""
        if isinstance(config, dict):
            return EvidenceStoreConfig(**config)
        elif isinstance(config, EvidenceStoreConfig):
            return config
        else:
            raise ConfigurationError("Invalid configuration type")

    
    async def _cleanup_partial_initialization(self) -> None:
        """Cleanup partially initialized resources"""
        try:
            self.evidence_cache.clear()
            self.validation_rules.clear()
            self.initialized = False
            self.logger.info("Partial initialization cleaned up")
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")

    def _initialize_indices(self) -> Dict[str, Any]:
        """Initialize index structures"""
        if not self.config.indexing['enabled']:
            return {}
            
        return {
            field: defaultdict(list)
            for field in self.config.indexing['fields']
        }

    def _initialize_cache(self) -> TTLCache:
        """Initialize caching system"""
        if not self.config.caching['enabled']:
            return TTLCache(maxsize=1, ttl=1)  # Dummy cache
            
        return TTLCache(
            maxsize=self.config.caching['max_size'],
            ttl=self.config.caching['ttl']
        )

    def _initialize_metrics(self) -> Dict[str, Counter]:
        """Initialize metrics tracking"""
        if not self.config.metrics['enabled']:
            return {}
            
        return {
            'operations': Counter(),
            'performance': defaultdict(list),
            'errors': Counter()
        }

    def _initialize_validation(self) -> Dict[str, Any]:
        """Initialize validation rules"""
        return {
            'rules': self.config.validation['required_fields'],
            'max_size': self.config.validation['max_size'],
            'strict_mode': self.config.validation['strict_mode']
        }
        
    async def _initialize_validation_rules(self) -> None:
        """Initialize validation rules with proper error handling"""
        try:
            self.validation_rules = {
                'evidence': self._validate_evidence,
                'chain': self._validate_chain,
                'source': self._validate_source
            }
            
            # Initialize evidence cache
            self.evidence_cache = TTLCache(
                maxsize=self.config.cache_size,
                ttl=self.config.cache_ttl
            )
            
            self.logger.info("Validation rules initialized successfully")
        except Exception as e:
            self.logger.error(f"Validation rules initialization failed: {e}")
            raise
    

    async def _initialize_components(self) -> None:
        """Initialize all evidence store components"""
        try:
            # Initialize evidence manager
            if hasattr(self, 'evidence_manager'):
                # Use existing store_evidence method from EnhancedEvidenceManager
                self.store_evidence = self.evidence_manager.store_evidence
            
            # Initialize indices if enabled
            if self.config.indexing['enabled']:
                self._initialize_indices()
            
            # Initialize caching if enabled
            if self.config.caching['enabled']:
                self._initialize_cache()
            
            # Initialize metrics tracking
            if self.config.metrics['enabled']:
                self._initialize_metrics()
                
            self.logger.info("Components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Component initialization failed: {e}")
            raise
    
    def _validate_evidence_store_config(self, config: Dict[str, Any]) -> bool:
        """Validate evidence store configuration"""
        try:
            required_sections = {'storage', 'caching', 'validation', 'indexing'}
            if not all(section in config for section in required_sections):
                missing = required_sections - set(config.keys())
                self.logger.error(f"Missing required sections: {missing}")
                return False

            # Validate storage configuration
            storage_config = config.get('storage', {})
            if not self._validate_storage_config(storage_config):
                return False

            # Validate caching configuration
            cache_config = config.get('caching', {})
            if not self._validate_cache_config(cache_config):
                return False

            return True
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
            
    async def validate_evidence_chain(self, chain: List[Evidence]) -> bool:
        """Validate evidence chain completeness and consistency"""
        try:
            if not chain:
                return False
                
            # Validate chain sequence
            for i in range(len(chain)-1):
                if not await self._validate_chain_link(chain[i], chain[i+1]):
                    return False
                    
            return True
        except Exception as e:
            self.logger.error(f"Chain validation failed: {e}")
            return False
            
    async def _validate_chain_link(self, prev_evidence: Evidence,
                                 next_evidence: Evidence) -> bool:
        """Validate link between evidence items in chain"""
        try:
            # Validate temporal sequence
            if prev_evidence.timestamp >= next_evidence.timestamp:
                return False
                
            # Validate logical connection
            if not self._validate_evidence_connection(prev_evidence, next_evidence):
                return False
                
            return True
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_str = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.logger.error(f"Chain link validation failed:\n{tb_str}")
            return False

    def _validate_evidence_connection(self, prev_evidence: Evidence,
                                    next_evidence: Evidence) -> bool:
        """Validate logical connection between evidence items"""
        try:
            # Implement validation logic
            return True
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_str = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.logger.error(f"Evidence connection validation failed:\n{tb_str}")
            return False
            
    def _validate_storage_config(self, storage_config: Dict[str, Any]) -> bool:
        """Validate storage configuration"""
        required_fields = {'type', 'compression', 'backup_enabled', 'path'}
        return all(field in storage_config for field in required_fields)

    def _validate_cache_config(self, cache_config: Dict[str, Any]) -> bool:
        """Validate cache configuration"""
        required_fields = {'enabled', 'max_size', 'ttl', 'strategy'}
        return all(field in cache_config for field in required_fields)
    def _validate_and_convert_config(self, config: Dict[str, Any]) -> EvidenceStoreConfig:
        """Validate and convert dictionary config to EvidenceStoreConfig"""
        required_sections = {'storage', 'caching', 'indexing', 'validation', 'metrics'}
        
        # Check for required sections
        if not all(section in config for section in required_sections):
            missing = required_sections - set(config.keys())
            self.logger.error(f"Missing required configuration sections: {missing}")
            raise ConfigurationError("Missing required configuration sections")
            
        # Create EvidenceStoreConfig with provided values
        return EvidenceStoreConfig(
            storage=config.get('storage', EvidenceStoreConfig().storage),
            caching=config.get('caching', EvidenceStoreConfig().caching),
            indexing=config.get('indexing', EvidenceStoreConfig().indexing),
            validation=config.get('validation', EvidenceStoreConfig().validation),
            metrics=config.get('metrics', EvidenceStoreConfig().metrics)
        )

    def _verify_configuration(self) -> bool:
        """Verify configuration completeness and validity"""
        try:
            # Verify all sections are present and valid
            sections = ['storage', 'caching', 'indexing', 'validation', 'metrics']
            for section in sections:
                if not hasattr(self.config, section):
                    self.logger.error(f"Missing configuration section: {section}")
                    return False
                    
                if not isinstance(getattr(self.config, section), dict):
                    self.logger.error(f"Invalid configuration section: {section}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration verification failed: {e}")
            return False


    
    async def _cleanup_partial_initialization(self) -> None:
        """Cleanup partially initialized resources"""
        try:
            if hasattr(self, 'evidence'):
                self.evidence.clear()
            if hasattr(self, 'indices'):
                self.indices.clear()
            if hasattr(self, 'cache'):
                self.cache.clear()
            if hasattr(self, 'metrics'):
                self.metrics.clear()
            if hasattr(self, 'evidence_cache'):
                self.evidence_cache.clear()
            if hasattr(self, 'version_control'):
                await self.version_control.cleanup()
            
            self.initialized = False
            self._initializing = False
            self.logger.info("Partial initialization cleaned up")
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")

    
    async def cleanup(self) -> None:
        """Cleanup evidence store resources"""
        try:
            # Clear collections
            if hasattr(self, 'evidence'):
                self.evidence.clear()
            if hasattr(self, 'indices'):
                self.indices.clear()
            if hasattr(self, 'cache'):
                self.cache.clear()
            if hasattr(self, 'metrics'):
                self.metrics.clear()
            if hasattr(self, 'evidence_cache'):
                self.evidence_cache.clear()
            
            self.initialized = False
            self.logger.info("Evidence store cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Evidence store cleanup failed: {e}")
            raise

    
    async def store_evidence(self, chain_id: str, evidence: Dict[str, Any]) -> str:
        evidence_id = str(uuid.uuid4())
        await self.version_control.create_version(chain_id, evidence)
        self.evidence_chains[chain_id].append(evidence_id)
        return evidence_id
        
            # Create evidence record
            evidence_record = {
                'value': value,
                'metadata': metadata,
                'timestamp': datetime.now().isoformat()
            }

            # Store evidence
            async with self._lock:
                self.evidence[key] = evidence_record
                self._update_indices(key, evidence_record)
                self.recent_additions.append(key)
                self.metrics['stored_evidence'] += 1

        except Exception as e:
            self.logger.error(f"Evidence storage failed: {str(e)}")
            raise
    async def store_evidence_chain(self, chain_id: str, evidence: List[Evidence]) -> None:
        """Store evidence chain with validation"""
        try:
            # Validate chain completeness
            if not self._validate_chain(evidence):
                raise ValidationError("Invalid evidence chain")
                
            # Store with graph structure
            await self._store_chain_nodes(chain_id, evidence)
            await self._store_chain_edges(chain_id, evidence)
            
            # Update indices
            await self._update_chain_indices(chain_id, evidence)
        except Exception as e:
            self.logger.error(f"Evidence chain storage failed: {e}")
            raise
    def _validate_evidence(self, value: Any, metadata: Dict[str, Any]) -> bool:
        """Validate evidence content and metadata"""
        try:
            # Check required fields
            required_fields = self.config['validation_rules']['required_fields']
            if not all(field in metadata for field in required_fields):
                return False

            # Check size limit
            content_size = len(str(value).encode('utf-8'))
            if content_size > self.config['validation_rules']['max_size']:
                return False

            return True
        except Exception as e:
            self.logger.error(f"Evidence validation failed: {str(e)}")
            return False

    def _update_indices(self, key: str, evidence_record: Dict[str, Any]) -> None:
        """Update evidence indices"""
        try:
            metadata = evidence_record['metadata']
            
            # Update type index
            if 'type' in metadata:
                self.evidence_index['type'][metadata['type']].append(key)
            
            # Update source index
            if 'source' in metadata:
                self.evidence_index['source'][metadata['source']].append(key)
            
            # Update timestamp index
            self.evidence_index['timestamp'][evidence_record['timestamp']].append(key)
            
        except Exception as e:
            self.logger.error(f"Index update failed: {str(e)}")
            raise

    async def get_validation_status(self) -> Dict[str, Any]:
        """Get current validation status and metrics"""
        try:
            if not self.initialized:
                raise RuntimeError("Evidence store not initialized")

            status = {
                'total_validated': len(self.validation_status),
                'valid_count': sum(1 for v in self.validation_status.values()
                                 if v.get('status') == 'valid'),
                'invalid_count': sum(1 for v in self.validation_status.values()
                                   if v.get('status') == 'invalid'),
                'validation_errors': dict(Counter(
                    v.get('error_type') for v in self.validation_status.values()
                    if v.get('status') == 'invalid'
                )),
                'last_validation': max((v.get('timestamp')
                                      for v in self.validation_status.values()),
                                     default=None),
                'initialized': self.initialized,
                'total_evidence': len(self.evidence),
                'recent_additions': len(self.recent_additions),
                'metrics': self.metrics,
                'indices_status': {
                    index_name: len(index)
                    for index_name, index in self.evidence_index.items()
                }
            }
            return status
        except Exception as e:
            self.logger.error(f"Failed to get validation status: {str(e)}")
            raise

    async def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of available evidence."""
        if not self.initialized:
            raise RuntimeError("Evidence store not initialized")

        try:
            # Use cache if available
            cache_key = 'evidence_summary'
            if cache_key in self.cache:
                self.metrics['cache_hits']['total'] += 1
                return self.cache[cache_key]

            self.metrics['cache_misses']['total'] += 1
            summary = {
                'total_evidence': len(self.evidence),
                'evidence_types': dict(Counter(
                    ev.get('metadata', {}).get('type', 'unknown')
                    for ev in self.evidence.values()
                )),
                'sources': dict(Counter(
                    ev.get('metadata', {}).get('source', 'unknown')
                    for ev in self.evidence.values()
                )),
                'temporal_distribution': self._get_temporal_distribution(),
                'validation_status': await self.get_validation_status(),
                'recent_additions': len(self.recent_additions),
                'metrics': dict(self.metrics),
                'cache_stats': {
                    'size': len(self.cache),
                    'max_size': self.config['cache_size'],
                    'hit_ratio': self.metrics['cache_hits']['total'] /
                                (self.metrics['cache_hits']['total'] +
                                 self.metrics['cache_misses']['total'])
                    if (self.metrics['cache_hits']['total'] +
                        self.metrics['cache_misses']['total']) > 0 else 0
                }
            }

            # Cache the summary
            self.cache[cache_key] = summary
            return summary
        except Exception as e:
            self.logger.error(f"Failed to get summary: {str(e)}")
            raise

    def _get_temporal_distribution(self) -> Dict[str, int]:
        """Get temporal distribution of evidence."""
        try:
            distribution = defaultdict(int)
            for evidence in self.evidence.values():
                timestamp = evidence.get('timestamp')
                if timestamp:
                    date = timestamp.split('T')[0]  # Get date part only
                    distribution[date] += 1
            return dict(distribution)
        except Exception as e:
            self.logger.error(f"Temporal distribution calculation failed: {str(e)}")
            return {}

    

    def _verify_indices(self) -> bool:
        """Verify integrity of evidence indices."""
        try:
            required_indices = {'type', 'source', 'timestamp'}
            if not all(idx in self.evidence_index for idx in required_indices):
                return False

            # Verify index structures
            for index_name, index in self.evidence_index.items():
                if not isinstance(index, defaultdict):
                    return False

            return True
        except Exception as e:
            self.logger.error(f"Index verification failed: {str(e)}")
            return False


class EvidenceVersionControl:
    def __init__(self):
        self.version_history = defaultdict(list)
        self.current_versions = {}
        self.logger = logging.getLogger(__name__)

    async def initialize(self) -> None:
        """Initialize version control system"""
        try:
            self.version_history.clear()
            self.current_versions.clear()
            self.logger.info("Version control initialized successfully")
        except Exception as e:
            self.logger.error(f"Version control initialization failed: {e}")
            raise

    async def store_version(self, evidence_id: str, evidence: Dict[str, Any]) -> None:
        """Store new version of evidence"""
        try:
            version = len(self.version_history[evidence_id])
            self.version_history[evidence_id].append({
                'version': version,
                'evidence': evidence,
                'timestamp': datetime.now().isoformat()
            })
            self.current_versions[evidence_id] = version
        except Exception as e:
            self.logger.error(f"Version storage failed: {e}")
            raise
class EvidenceManager:
    """Enhanced REWOO evidence management"""
    def __init__(self):
        self.evidence_store = {}
        self.version_history = defaultdict(list)
        self.validation_rules = {}
        self.logger = logging.getLogger(__name__)

    async def store_evidence(self, key: str, value: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Store evidence with versioning and validation"""
        try:
            # Validate evidence
            if not self._validate_evidence(value):
                raise ValidationError("Evidence validation failed")
                
            # Store with version
            version = len(self.version_history[key])
            evidence_record = {
                'value': value,
                'metadata': metadata or {},
                'version': version,
                'timestamp': datetime.now().isoformat()
            }
            
            self.evidence_store[key] = evidence_record
            self.version_history[key].append(evidence_record)
            
            self.logger.info(f"Stored evidence {key} version {version}")
            
        except Exception as e:
            self.logger.error(f"Evidence storage failed: {e}")
            raise
    async def create_evidence_chain(self, task_id: str) -> None:
        """Create and track evidence chain for a task"""
        try:
            chain = []
            
            # Layer 1 Evidence (Data Processing)
            csv_evidence = await self.evidence_store.get_evidence(f"csv_processing_{task_id}")
            if csv_evidence:
                chain.append(("#E1", csv_evidence))
            
            # Layer 2 Evidence (Knowledge Graph)
            entity_evidence = await self.evidence_store.get_evidence(f"entity_creation_{task_id}")
            if entity_evidence:
                chain.append(("#E2", entity_evidence))
                
            # Store complete chain
            await self.evidence_store.store_evidence(
                f"chain_{task_id}",
                chain,
                {'type': 'evidence_chain'}
            )
            
        except Exception as e:
            self.logger.error(f"Evidence chain creation failed: {e}")
            raise
            
class CommunicationSystem:
    """Enhanced communication system with proper initialization"""
    def __init__(self):
        self.config = config  # Add config
        self.subscribers = defaultdict(set)
        self.message_history = defaultdict(list)
        self.metrics = defaultdict(Counter)
        self.logger = logging.getLogger(__name__)
        self.initialized = False
        self._lock = asyncio.Lock()
        self.channel_states = defaultdict(dict)
        self.retry_config = RetryConfig(max_attempts=3, backoff_factor=2)
        self.channels = defaultdict(asyncio.Queue)
        self.message_tracker = MessageTracker()

    
    async def initialize(self) -> None:
        """Initialize communication system with proper sequence"""
        try:
            # Initialize core channels
            await self._initialize_core_channels()
            
            # Initialize layer communication
            await self._setup_layer_communication()
            
            self.initialized = True
            self.logger.info("Communication system initialized successfully")
        except Exception as e:
            self.logger.error(f"Communication system initialization failed: {e}")
            raise

    async def _initialize_core_channels(self) -> None:
        """Initialize core communication channels"""
        try:
            core_channels = [
                'system',
                'coordination',
                'task_assignment',
                'result_collection',
                'error_handling'
            ]
            
            for channel in core_channels:
                self.channels[channel] = asyncio.Queue()
                self.channel_states[channel] = {
                    'active': True,
                    'message_count': 0,
                    'last_activity': datetime.now().isoformat()
                }
                
            self.logger.info("Core channels initialized successfully")
        except Exception as e:
            self.logger.error(f"Core channel initialization failed: {e}")
            raise
            
       
    async def _setup_layer_communication(self) -> None:
        """Setup layer communication with config"""
        try:
            num_layers = self.config.num_layers  # Get from config
            for layer_id in range(1, num_layers + 1):
                channel_name = f"layer_{layer_id}"
                self.channels[channel_name] = asyncio.Queue()
                self.channel_states[channel_name] = {
                    'active': True,
                    'message_count': 0,
                    'last_activity': datetime.now().isoformat()
                }
            self.logger.info("Layer communication setup completed")
        except Exception as e:
            self.logger.error(f"Layer communication setup failed: {e}")
            raise

    async def create_channel(self, channel_name: str, buffer_size: int = 1000) -> asyncio.Queue:
        """Create a new communication channel with specified buffer size"""
        try:
            async with self._lock:
                if channel_name not in self.channels:
                    self.channels[channel_name] = asyncio.Queue(maxsize=buffer_size)
                    self.metrics['channels_created'] += 1
                    self.logger.info(f"Channel {channel_name} created successfully")
                return self.channels[channel_name]
        except Exception as e:
            self.logger.error(f"Channel creation failed: {e}")
            raise

    async def send_to_layer(self, layer_id: int, message: Dict[str, Any]) -> None:
        """Send message to specific layer channel"""
        try:
            if layer_id not in self.layer_channels:
                raise ValueError(f"Invalid layer ID: {layer_id}")
                
            channel = self.layer_channels[layer_id]
            await channel.put(message)
            
            # Update metrics
            self.channel_states[f"layer_{layer_id}"]['message_count'] += 1
            self.channel_states[f"layer_{layer_id}"]['last_activity'] = datetime.now().isoformat()
            
        except Exception as e:
            self.logger.error(f"Layer message sending failed: {e}")
            raise

    async def receive_from_layer(self, layer_id: int, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Receive message from specific layer channel"""
        try:
            if layer_id not in self.layer_channels:
                raise ValueError(f"Invalid layer ID: {layer_id}")
                
            channel = self.layer_channels[layer_id]
            try:
                message = await asyncio.wait_for(channel.get(), timeout=timeout)
                return message
            except asyncio.TimeoutError:
                return None
                
        except Exception as e:
            self.logger.error(f"Layer message receiving failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup communication system resources"""
        try:
            # Clear all channels
            for channel in self.channels.values():
                while not channel.empty():
                    try:
                        await channel.get_nowait()
                    except asyncio.QueueEmpty:
                        break

            # Clear layer channels
            self.layer_channels.clear()
            
            # Clear states and metrics
            self.channel_states.clear()
            self.metrics.clear()
            self.message_history.clear()
            
            self.initialized = False
            self.logger.info("Communication system cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Communication system cleanup failed: {e}")
            raise

    

    async def register_agent(self, agent_name: str, channels: List[str]) -> None:
        """Register an agent with specified channels"""
        try:
            async with self._lock:
                self.agent_registrations[agent_name] = {
                    'channels': channels,
                    'registered_at': datetime.now().isoformat(),
                    'status': 'active'
                }
                
                # Subscribe to channels
                for channel in channels:
                    if channel not in self.channels:
                        self.channels[channel] = asyncio.Queue()
                    self.subscribers[channel].add(agent_name)
                
                self.metrics['agent_registrations'] += 1
                self.logger.info(f"Agent {agent_name} registered successfully")
                
        except Exception as e:
            self.logger.error(f"Agent registration failed: {e}")
            raise

    async def create_channel(self, channel_name: str) -> None:
        """Create a new communication channel"""
        try:
            async with self._lock:
                if channel_name not in self.channels:
                    self.channels[channel_name] = asyncio.Queue()
                    self.metrics['channels_created'] += 1
                    self.logger.info(f"Channel {channel_name} created successfully")
        except Exception as e:
            self.logger.error(f"Channel creation failed: {e}")
            raise
    async def send_message_with_retry(self, sender: str, target: str,
                                    message: Dict[str, Any]) -> bool:
        for attempt in range(self.retry_config.max_attempts):
            try:
                await self.channels[target].put({
                    'id': str(uuid.uuid4()),
                    'sender': sender,
                    'content': message,
                    'timestamp': datetime.now().isoformat()
                })
                return True
            except Exception as e:
                await asyncio.sleep(self.retry_config.backoff_factor ** attempt)
        return False
    async def send_message(self, sender: str, target: str, message: Dict[str, Any]) -> None:
        """Send message to target"""
        try:
            if not self.initialized:
                raise RuntimeError("Communication system not initialized")
                
            if target not in self.channels:
                raise ValueError(f"Channel {target} does not exist")
                
            message_data = {
                'id': str(uuid.uuid4()),
                'sender': sender,
                'target': target,
                'content': message,
                'timestamp': datetime.now().isoformat()
            }
            
            await self.channels[target].put(message_data)
            self.message_history[target].append(message_data)
            self.metrics['messages_sent'][target] += 1
            
        except Exception as e:
            self.logger.error(f"Message sending failed: {e}")
            raise

    async def receive_message(self, agent_name: str, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Receive message for agent with timeout"""
        try:
            if not self.initialized:
                raise RuntimeError("Communication system not initialized")
                
            if agent_name not in self.agent_registrations:
                raise ValueError(f"Agent {agent_name} not registered")
                
            # Get agent's subscribed channels
            agent_channels = self.agent_registrations[agent_name]['channels']
            
            # Try to receive from any subscribed channel
            tasks = [
                asyncio.create_task(self._receive_from_channel(channel, timeout))
                for channel in agent_channels
            ]
            
            done, pending = await asyncio.wait(
                tasks,
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
                
            # Get result from completed task if any
            for task in done:
                try:
                    result = await task
                    if result:
                        self.metrics['messages_received'][agent_name] += 1
                        return result
                except Exception as e:
                    self.logger.error(f"Message reception failed: {e}")
                    
            return None
            
        except Exception as e:
            self.logger.error(f"Message reception failed: {e}")
            raise

    async def _receive_from_channel(self, channel: str, timeout: float) -> Optional[Dict[str, Any]]:
        """Receive message from specific channel"""
        try:
            return await asyncio.wait_for(
                self.channels[channel].get(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            return None

    async def verify_channel(self, channel_name: str) -> bool:
        """Verify channel functionality"""
        try:
            if channel_name not in self.channels:
                return False
                
            # Test message
            test_message = {
                'type': 'verification',
                'content': 'test'
            }
            
            # Send test message
            await self.send_message('system', channel_name, test_message)
            
            # Try to receive it
            received = await self._receive_from_channel(channel_name, timeout=1.0)
            
            return received is not None
            
        except Exception as e:
            self.logger.error(f"Channel verification failed: {e}")
            return False

    async def cleanup(self) -> None:
        """Cleanup communication system resources"""
        try:
            # Clear channels
            for channel in self.channels.values():
                while not channel.empty():
                    try:
                        await channel.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                        
            # Clear collections
            self.channels.clear()
            self.subscribers.clear()
            self.message_history.clear()
            self.agent_registrations.clear()
            self.metrics.clear()
            
            # Clear cache
            self.cache.clear()
            
            self.initialized = False
            self.logger.info("Communication system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Communication system cleanup failed: {e}")
            raise
    async def broadcast_to_layer(self, layer_id: int, message: Dict[str, Any]) -> None:
        """Broadcast message to all agents in a layer"""
        try:
            layer_agents = self._get_layer_agents(layer_id)
            tasks = [
                self.send_message(agent.name, message)
                for agent in layer_agents
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"Layer broadcast failed: {e}")
            raise
    async def _initialize_subscribers(self) -> None:
        """Initialize subscriber management system"""
        try:
            self.subscribers = defaultdict(set)
            self.subscription_history = []
            self.metrics['subscriptions'] = Counter()
            
            # Initialize default channels
            channels = ['system', 'coordination', 'task_assignment']
            for channel in channels:
                await self.create_channel(channel)
                
            self.logger.info("Subscriber system initialized successfully")
        except Exception as e:
            self.logger.error(f"Subscriber initialization failed: {e}")
            raise

    async def _verify_initialization(self) -> bool:
        """Verify system initialization"""
        try:
            # Verify channels
            if not self.channels:
                return False
                
            # Verify core components
            if not await self._verify_core_components():
                return False
                
            return True
        except Exception as e:
            self.logger.error(f"Verification failed: {e}")
            return False

   

    

    async def _initialize_channels(self) -> None:
        """Initialize communication channels"""
        try:
            self.channels = {
                'system': asyncio.Queue(),
                'coordination': asyncio.Queue(),
                'task_assignment': asyncio.Queue(),
                'result_collection': asyncio.Queue()
            }
            self.channel_states = {
                channel: {'active': True, 'message_count': 0}
                for channel in self.channels
            }
            self.logger.info("Channels initialized successfully")
        except Exception as e:
            self.logger.error(f"Channel initialization failed: {e}")
            raise

    async def _verify_initialization(self) -> bool:
        """Verify system initialization"""
        try:
            # Verify channels
            if not self.channels:
                return False
                
            # Verify subscribers
            if not hasattr(self, 'subscribers'):
                return False
                
            # Verify core functionality
            test_message = {'test': True}
            test_channel = 'system'
            
            await self.channels[test_channel].put(test_message)
            received = await self.channels[test_channel].get()
            
            return received == test_message
        except Exception as e:
            self.logger.error(f"Verification failed: {e}")
            return False
 
                
    
                
    async def _verify_channel(self, channel_name: str) -> bool:
        """Verify individual channel functionality"""
        try:
            test_message = {
                "type": "verification",
                "timestamp": datetime.now().isoformat()
            }
            
            await self.channels[channel_name].put(test_message)
            received = await asyncio.wait_for(
                self.channels[channel_name].get(),
                timeout=5.0
            )
            
            return received == test_message
            
        except Exception as e:
            self.logger.error(f"Channel verification failed: {channel_name} - {e}")
            return False
            
   
    
   
    async def subscribe(self, channel_name: str, subscriber: str) -> None:
        """
        Subscribe an entity to a communication channel.

        Args:
            channel_name (str): Name of the channel to subscribe to.
            subscriber (str): Name of the subscribing entity.
        """
        async with self._lock:
            if channel_name not in self.channels:
                await self.create_channel(channel_name)

            self.subscribers[channel_name].add(subscriber)
            self.metrics["subscriptions"][channel_name] += 1
            self.logger.info(f"Subscriber {subscriber} added to channel {channel_name}.")
    async def unsubscribe(self, channel_name: str, subscriber: str) -> None:
        """
        Unsubscribe an entity from a communication channel.

        Args:
            channel_name (str): Name of the channel to unsubscribe from.
            subscriber (str): Name of the unsubscribing entity.
        """
        async with self._lock:
            if channel_name in self.subscribers:
                self.subscribers[channel_name].discard(subscriber)
                self.logger.info(
                    f"Subscriber {subscriber} removed from channel {channel_name}."
                )
    
            
     
    
            
    
    async def unregister_agent(self, agent_name: str) -> None:
        """Unregister an agent from the communication system"""
        try:
            async with self._lock:
                if agent_name in self.registered_agents:
                    agent_channels = self.registered_agents[agent_name]['channels']
                    del self.registered_agents[agent_name]
                    for channel in agent_channels:
                        self.subscribers[channel].discard(agent_name)
                    self.logger.info(f"Agent {agent_name} unregistered successfully")
                    self.metrics['agent_unregistrations'] += 1
        except Exception as e:
            self.logger.error(f"Agent unregistration failed for {agent_name}: {e}")
            raise
    

    def get_metrics(self) -> Dict[str, Any]:
        """
        Retrieve system metrics.

        Returns:
            Dict[str, Any]: Metrics related to messages, subscriptions, etc.
        """
        return {
            "messages_sent": dict(self.metrics["messages_sent"]),
            "messages_received": dict(self.metrics["messages_received"]),
            "subscriptions": dict(self.metrics["subscriptions"]),
        }


    
    
    
    async def _verify_system(self) -> bool:
        """Comprehensive system verification"""
        try:
            # Verify required attributes
            required_attrs = ['message_history', 'channels', 'subscribers']
            if not all(hasattr(self, attr) for attr in required_attrs):
                return False

            # Verify channels
            required_channels = {'system', 'coordination', 'task_assignment', 'result_collection'}
            if not all(channel in self.channels for channel in required_channels):
                return False

            # Verify channel functionality
            for channel in required_channels:
                if not await self._verify_channel(channel):
                    return False

            return True

        except Exception as e:
            self.logger.error(f"System verification failed: {e}")
            return False
    
    async def _verify_communication(self) -> bool:
        """
        Verify the communication system integrity by testing channel operations.
        """
        try:
            required_channels = {"system", "coordination", "task_assignment"}
            for channel in required_channels:
                if channel not in self.communication_system.channels:
                    self.logger.error(f"Missing required channel: {channel}")
                    return False

                # Test channel functionality
                test_message = {"test": True}
                await self.communication_system.send_message(channel, "tester", test_message)
                received = await self.communication_system.receive_message(channel, timeout=1.0)

                if received != {"sender": "tester", "content": test_message, "timestamp": received["timestamp"]}:
                    self.logger.error(f"Message verification failed for channel: {channel}")
                    return False

            self.logger.info("Communication system verification passed.")
            return True
        except Exception as e:
            self.logger.error(f"Communication verification error: {e}")
            return False
            
class LayerCommunication:
    def __init__(self, config: EnhancedConfig):
        self.config = config
        self.channels = {}
        self.message_queue = asyncio.Queue()
        self.metrics = defaultdict(Counter)
        
    async def initialize(self) -> None:
        """Initialize communication system"""
        try:
            # Initialize channels
            await self._initialize_channels()
            
            # Initialize metrics tracking
            self.metrics = {
                'messages_sent': Counter(),
                'messages_received': Counter(),
                'errors': Counter()
            }
            
            self.initialized = True
            self.logger.info("Communication system initialized successfully")
        except Exception as e:
            self.logger.error(f"Communication initialization failed: {e}")
            raise
    async def _route_message(self, message: Dict[str, Any]) -> None:
        """Route message to appropriate layer based on content and priority"""
        try:
            layer_id = self._determine_target_layer(message)
            priority = message.get('priority', 1)
            
            await self.send_to_layer(layer_id, message, priority)
            
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_str = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.logger.error(f"Message routing failed:\n{tb_str}")
            raise

    def _determine_target_layer(self, message: Dict[str, Any]) -> int:
        """Determine target layer based on message content"""
        try:
            if 'target_layer' in message:
                return message['target_layer']
            
            message_type = message.get('type', '')
            routing_map = {
                'data_processing': 1,
                'knowledge_graph': 2,
                'analysis': 3,
                'synthesis': 4
            }
            return routing_map.get(message_type, 1)
            
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            tb_str = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.logger.error(f"Target layer determination failed:\n{tb_str}")
            return 1
    async def send_message(self, sender: str, target: str,
                          message: Dict[str, Any]) -> None:
        """Send message with proper error handling"""
        try:
            if not self.initialized:
                raise RuntimeError("Communication system not initialized")
                
            await self.channels[target].put({
                'sender': sender,
                'content': message,
                'timestamp': datetime.now().isoformat()
            })
            
            self.metrics['messages_sent'][target] += 1
        except Exception as e:
            self.logger.error(f"Message sending failed: {e}")
            self.metrics['errors']['send'] += 1
            raise
    async def send_to_layer(self, layer_id: int, message: Dict[str, Any],
                           priority: int = 1) -> None:
        """Send message to specific layer with priority"""
        try:
            if layer_id not in self.layer_channels:
                raise ValueError(f"Invalid layer ID: {layer_id}")
                
            enhanced_message = {
                'content': message,
                'priority': priority,
                'timestamp': datetime.now().isoformat()
            }
            
            await self.layer_channels[layer_id].put(enhanced_message)
            self.metrics[f'layer_{layer_id}_messages'] += 1
            
        except Exception as e:
            self.logger.error(f"Layer message sending failed: {e}")
            raise
class PlanningSystem:
    """Enhanced planning system with proper state management"""
    def __init__(self, config: 'EnhancedConfig'):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._plans = {}  # Initialize plans dictionary
        self.planning_history = []
        self.active_plans = {}
        self._metrics = defaultdict(Counter)  # Initialize metrics properly
        self.initialized = False
        self.evidence_store = None
        self.cache = TTLCache(maxsize=1000, ttl=3600)

    async def initialize(self) -> None:
        """Initialize planning system with proper error handling"""
        try:
            # Initialize evidence store
            self.evidence_store = EvidenceStore(self.config.evidence_store_config)
            await self.evidence_store.initialize()
            
            # Initialize plans storage
            self._plans = {}
            self.planning_history = []
            self.active_plans = {}
            
            # Initialize metrics
            self.metrics = {
                'plans_created': 0,
                'successful_plans': 0,
                'failed_plans': 0,
                'replanning_attempts': 0
            }
            
            self.initialized = True
            self.logger.info("Planning system initialized successfully")
        except Exception as e:
            self.logger.error(f"Planning system initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Enhanced cleanup with proper metrics handling"""
        try:
            if hasattr(self, 'evidence_store') and self.evidence_store:
                await self.evidence_store.cleanup()
            
            # Clear collections
            self._plans.clear()
            self.planning_history.clear()
            self.active_plans.clear()
            self._metrics.clear()  # Clear metrics properly
            if hasattr(self, 'cache'):
                self.cache.clear()
            
            self.initialized = False
            self.logger.info("Planning system cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Planning system cleanup failed: {e}")
            raise


    async def create_plan(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Create execution plan with comprehensive monitoring"""
        if not self.initialized:
            raise RuntimeError("Planning system not initialized")
            
        plan_id = f"plan_{datetime.now().timestamp()}"
        try:
            # Create plan
            plan = await self._create_execution_plan(task)
            
            # Store plan
            self._plans[plan_id] = plan
            self.planning_history.append({
                'plan_id': plan_id,
                'task': task,
                'timestamp': datetime.now().isoformat()
            })
            
            # Update metrics
            self.metrics['plans_created'] += 1
            
            return plan
        except Exception as e:
            self.logger.error(f"Plan creation failed: {e}")
            self.metrics['failed_plans'] += 1
            raise

    async def _create_execution_plan(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Create detailed execution plan"""
        try:
            return {
                'task': task,
                'steps': await self._generate_steps(task),
                'created_at': datetime.now().isoformat(),
                'status': 'created',
                'metadata': {
                    'version': '1.0',
                    'generator': self.__class__.__name__
                }
            }
        except Exception as e:
            self.logger.error(f"Execution plan creation failed: {e}")
            raise

    async def _generate_steps(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate execution steps"""
        # Implement step generation logic
        return []

    async def _decompose_task(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Decompose task into subtasks"""
        try:
            subtasks = []
            
            # Handle string tasks
            if isinstance(task, str):
                subtasks.append({
                    'type': 'simple',
                    'description': task,
                    'requirements': []
                })
                return subtasks
            
            # Handle complex tasks
            if 'type' in task:
                subtask = {
                    'type': task['type'],
                    'description': task.get('description', ''),
                    'requirements': task.get('requirements', [])
                }
                subtasks.append(subtask)
            
            # Add task-specific requirements
            if 'requirements' in task:
                for req in task['requirements']:
                    subtask = {
                        'type': 'requirement',
                        'description': req,
                        'requirements': []
                    }
                    subtasks.append(subtask)
            
            return subtasks
            
        except Exception as e:
            self.logger.error(f"Task decomposition failed: {e}")
            raise

    async def _generate_step(self, subtask: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Generate execution step for subtask"""
        try:
            return {
                'id': str(uuid.uuid4()),
                'type': subtask['type'],
                'description': subtask['description'],
                'requirements': subtask['requirements'],
                'context': context,
                'status': 'pending'
            }
        except Exception as e:
            self.logger.error(f"Step generation failed: {e}")
            raise

    async def _validate_plan(self, plan: Dict[str, Any]) -> bool:
        """Validate generated plan"""
        try:
            if not plan or 'steps' not in plan:
                return False
                
            # Validate required fields
            required_fields = ['task', 'steps', 'metadata']
            if not all(field in plan for field in required_fields):
                return False
                
            # Validate steps
            for step in plan['steps']:
                if not await self._validate_step(step):
                    return False
                    
            return True
            
        except Exception as e:
            self.logger.error(f"Plan validation failed: {e}")
            return False

    async def _validate_step(self, step: Dict[str, Any]) -> bool:
        """Validate execution step"""
        try:
            required_fields = ['id', 'type', 'description']
            return all(field in step for field in required_fields)
        except Exception as e:
            self.logger.error(f"Step validation failed: {e}")
            return False

    async def _validate_evidence(self, evidence: Dict[str, Any]) -> bool:
        """Validate evidence data"""
        try:
            required_fields = ['source', 'content', 'timestamp']
            return all(field in evidence for field in required_fields)
        except Exception as e:
            self.logger.error(f"Evidence validation failed: {e}")
            return False

    async def _validate_task(self, task: Dict[str, Any]) -> bool:
        """Validate task structure"""
        try:
            if isinstance(task, str):
                return bool(task.strip())
                
            required_fields = ['type', 'description']
            return all(field in task for field in required_fields)
        except Exception as e:
            self.logger.error(f"Task validation failed: {e}")
            return False

    async def _replan(self, original_plan: Dict[str, Any], reason: str) -> Dict[str, Any]:
        """Generate new plan when original fails"""
        try:
            self.logger.info(f"Replanning due to: {reason}")
            
            # Create context with original plan information
            context = {
                'original_plan': original_plan,
                'failure_reason': reason,
                'attempt': len(self.planning_history) + 1
            }
            
            # Generate new plan
            new_plan = await self._create_execution_plan(
                original_plan['task'],
                context
            )
            
            # Store planning history
            self.planning_history.append({
                'original_plan': original_plan,
                'new_plan': new_plan,
                'reason': reason,
                'timestamp': datetime.now().isoformat()
            })
            
            return new_plan
            
        except Exception as e:
            self.logger.error(f"Replanning failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup planning system resources"""
        try:
            if hasattr(self, 'evidence_store') and self.evidence_store:
                await self.evidence_store.cleanup()
            
            self._plans.clear()
            self.planning_history.clear()
            self._metrics.clear()
            self.initialized = False
            
            self.logger.info("Planning system cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Planning system cleanup failed: {e}")
            raise

    def get_metrics(self) -> Dict[str, Any]:
        """Get planning system metrics"""
        return {
            'total_plans': len(self.planning_history),
            'successful_plans': self._metrics['successful_plans'],
            'failed_plans': self._metrics['failed_plans'],
            'replan_attempts': self._metrics['replan_attempts']
        }

class WorldObserver:
    """System for observing and tracking world state"""
    def __init__(self):
        self.world_state = defaultdict(dict)
        self.logger = logging.getLogger(__name__)
        self.initialized = False

    async def initialize(self) -> None:
        """Initialize world observer"""
        try:
            self.world_state.clear()
            self.initialized = True
            self.logger.info("World observer initialized successfully")
        except Exception as e:
            self.logger.error(f"World observer initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup world observer resources"""
        try:
            self.world_state.clear()
            self.initialized = False
            self.logger.info("World observer cleaned up successfully")
        except Exception as e:
            self.logger.error(f"World observer cleanup failed: {e}")
            raise

    async def observe(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Observe current world state."""
        if not self.initialized:
            raise RuntimeError("World observer not initialized")
            
        try:
            observation = {
                'timestamp': datetime.now().isoformat(),
                'context': context,
                'world_state': dict(self.world_state)
            }
            return observation
        except Exception as e:
            self.logger.error(f"Observation failed: {str(e)}")
            raise
            
# Add missing imports
import sys
import logging
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime

class REWOOSystem:
    def __init__(self, config: 'EnhancedConfig'):
        self.config = config
        self.logger = logging.getLogger(__name__)
        # Initialize components in constructor
        self.evidence_store = EvidenceStore()
        self.planning_system = PlanningSystem(config)
        self.world_observer = WorldObserver()  # Initialize here
        self.initialized = False
        self._metrics = defaultdict(Counter)  # Add metrics

    async def initialize(self) -> None:
        """Initialize REWOO system with proper component initialization"""
        try:
            # Initialize evidence store
            await self.evidence_store.initialize()
            
            # Initialize planning system
            await self.planning_system.initialize()
            
            # Initialize world observer
            if not self.world_observer:
                self.world_observer = WorldObserver()
            await self.world_observer.initialize()
            
            self.initialized = True
            self.logger.info("REWOO system initialized successfully")
        except Exception as e:
            self.logger.error(f"REWOO system initialization failed: {e}")
            raise
            
    def _initialize_components(self):
        """Initialize core components with proper logging"""
        try:
            # Initialize logger first
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            self.logger = logging.getLogger(self.__class__.__name__)
            
            # Initialize other components
            self.evidence_store = EvidenceStore(self.config.evidence_store_config)
            self.planning_system = PlanningSystem(self.config)
            self.world_observer = WorldObserver()
            
        except Exception as e:
            raise InitializationError(f"Failed to initialize components: {str(e)}")
    
    async def create_plan(self, task: Dict[str, Any]) -> Dict[str, Any]:
        observation = await self._observe_world_state()
        plan = await self.planning_system.create_plan(task, observation)
        await self.evidence_store.store_evidence(
            str(datetime.now().timestamp()),
            plan,
            {'task': task}
        )
        return plan
    
    async def _initialize_components(self) -> None:
        """Initialize core REWOO components"""
        try:
            # Initialize evidence store
            self.evidence_store = EvidenceStore(self.config.evidence_store_config)
            await self.evidence_store.initialize()
            
            # Initialize planning system
            self.planning_system = PlanningSystem(self.config)
            await self.planning_system.initialize()
            
            # Initialize metrics
            self.metrics = {
                'plans_created': Counter(),
                'evidence_collected': Counter(),
                'decisions_made': Counter()
            }
            
            self.logger.info("REWOO components initialized successfully")
        except Exception as e:
            self.logger.error(f"REWOO components initialization failed: {e}")
            raise
    async def create_evidence_chain(self, task_id: str) -> None:
        """Create and track evidence chain for a task"""
        try:
            chain = []
            
            # Layer 1 Evidence (Data Processing)
            csv_evidence = await self.evidence_store.get_evidence(
                f"csv_processing_{task_id}")
            if csv_evidence:
                chain.append(("#E1", csv_evidence))
            
            # Layer 2 Evidence (Knowledge Graph)
            entity_evidence = await self.evidence_store.get_evidence(
                f"entity_creation_{task_id}")
            if entity_evidence:
                chain.append(("#E2", entity_evidence))
                
            # Store complete chain
            await self.evidence_store.store_evidence(
                f"chain_{task_id}",
                chain,
                {'type': 'evidence_chain'}
            )
            
        except Exception as e:
            self.logger.error(f"Evidence chain creation failed: {e}")
            raise
    async def _initialize_evidence_collection(self) -> None:
        """Initialize evidence collection system"""
        try:
            # Clear existing collections
            self.evidence_patterns.clear()
            self.planning_history.clear()
            
            # Initialize evidence collection settings
            self.evidence_collection_config = {
                'max_evidence_age': 3600,  # 1 hour
                'min_confidence': 0.8,
                'max_evidence_items': 1000
            }
            
            # Initialize evidence cache
            self.cache.clear()
            
            self.logger.info("Evidence collection initialized successfully")
        except Exception as e:
            self.logger.error(f"Evidence collection initialization failed: {e}")
            raise

    async def _initialize_planning_system(self) -> None:
        """Initialize planning system"""
        try:
            # Initialize planning configurations
            self.planning_config = {
                'max_steps': 5,
                'temperature': 0.7,
                'max_tokens': 1000,
                'planning_mode': 'strategic'
            }
            
            # Clear planning history
            self.planning_history.clear()
            
            self.logger.info("Planning system initialized successfully")
        except Exception as e:
            self.logger.error(f"Planning system initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup REWOO system resources"""
        try:
            # Cleanup evidence store
            if hasattr(self, 'evidence_store') and self.evidence_store:
                await self.evidence_store.cleanup()
                self.evidence_store = None
            
            # Cleanup planning system
            if hasattr(self, 'planning_system') and self.planning_system:
                await self.planning_system.cleanup()
                self.planning_system = None
            
            # Clear collections
            self.planning_history.clear()
            self.evidence_patterns.clear()
            self.metrics.clear()
            
            # Clear cache
            if hasattr(self, 'cache'):
                self.cache.clear()
            
            self.initialized = False
            self._initializing = False
            
            self.logger.info("REWOO system cleaned up successfully")
        except Exception as e:
            self.logger.error(f"REWOO system cleanup failed: {e}")
            raise

    
    async def _verify_components(self) -> Dict[str, bool]:
        """Verify REWOO components"""
        try:
            components = {
                'evidence_store': self._verify_evidence_store(),
                'planning_system': self._verify_planning_system(),
                'world_observer': self._verify_world_observer()
            }
            
            return {
                'success': all(components.values()),
                'components': components
            }
        except Exception as e:
            self.logger.error(f"Component verification failed: {e}")
            raise

    def _verify_evidence_store(self) -> bool:
        """Verify evidence store initialization"""
        return (
            hasattr(self, 'evidence_store') and
            self.evidence_store is not None and
            hasattr(self.evidence_store, 'initialized') and
            self.evidence_store.initialized
        )
    async def _validate_plan(self, plan: Dict[str, Any]) -> bool:
        """Validate generated plan."""
        try:
            if not plan or 'steps' not in plan:
                return False
                
            # Validate required fields
            required_fields = ['task', 'steps', 'evidence']
            if not all(field in plan for field in required_fields):
                return False
                
            # Validate steps
            for step in plan['steps']:
                if not await self._validate_step(step):
                    return False
                    
            return True
            
        except Exception as e:
            self.logger.error(f"Plan validation failed: {str(e)}")
            return False

    async def _validate_step(self, step: Dict[str, Any]) -> bool:
        """Validate execution step."""
        try:
            required_fields = ['action', 'inputs', 'evidence_requirements']
            return all(field in step for field in required_fields)
        except Exception as e:
            self.logger.error(f"Step validation failed: {str(e)}")
            return False
        
    # In code_cell6_1, update the _initialize_rewoo method:
    async def _initialize_rewoo(self):
        """Initialize REWOO system and related components"""
        try:
            # Pass self as config to REWOOSystem and PlanningSystem
            self.rewoo_system = REWOOSystem(self)
            self.evidence_store = EvidenceStore()
            self.planning_system = PlanningSystem(self)
        
            await self.evidence_store.initialize()
            await self.planning_system.initialize()
            await self.rewoo_system.initialize()

            self.rewoo_config = REWOOConfig(
                enabled=True,
                max_planning_steps=5,
                evidence_threshold=0.8,
                context_window=4096,
                planning_temperature=0.7
            )

            self._initialization_state['rewoo'] = True
            self.logger.info("REWOO system initialized successfully")
        except Exception as e:
            self.logger.error(f"REWOO system initialization failed: {e}")
            raise

    
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task with evidence tracking"""
        task_id = f"task_{datetime.now().timestamp()}"
        
        try:
            # Create evidence chain
            await self.evidence_manager.create_evidence_chain(task_id)
            
            # Process through layers with evidence
            result = await self._process_with_evidence(task_id, task)
            
            # Store final evidence
            await self.evidence_store.store_evidence(
                task_id,
                result,
                {'type': 'task_complete'}
            )
            
            return result
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            raise
    async def replan(self, original_plan: Dict[str, Any], reason: str) -> Dict[str, Any]:
        """Generate new plan when original fails"""
        if not self.initialized:
            raise RuntimeError("REWOO system not initialized")

        try:
            # Create replanning context
            context = {
                'original_plan': original_plan,
                'failure_reason': reason,
                'timestamp': datetime.now().isoformat()
            }
            
            # Get additional evidence
            new_evidence = await self.evidence_store.get_evidence_since(
                original_plan.get('timestamp', '')
            )
            
            # Generate new plan
            new_plan = await self.planning_system.create_plan(
                original_plan['task'],
                context,
                new_evidence
            )
            
            # Store replanning evidence
            await self.evidence_store.store_evidence(
                str(datetime.now().timestamp()),
                {
                    'original_plan': original_plan,
                    'new_plan': new_plan,
                    'reason': reason
                },
                {'type': 'replanning'}
            )
            
            return new_plan
            
        except Exception as e:
            self.logger.error(f"Replanning failed: {e}")
            raise
    

    async def _initialize_validation_rules(self):
        """Initialize validation rules"""
        try:
            self.validation_rules = {
                'task': self._validate_task,
                'plan': self._validate_plan,
                'execution': self._validate_execution,
                'evidence': self._validate_evidence
            }
            self.logger.info("Validation rules initialized")
        except Exception as e:
            self.logger.error(f"Validation rules initialization failed: {e}")
            raise

    async def _initialize_recovery_handlers(self):
        """Initialize recovery handlers"""
        try:
            self.recovery_handlers = {
                'task_failure': self._handle_task_failure,
                'plan_failure': self._handle_plan_failure,
                'evidence_failure': self._handle_evidence_failure
            }
            self.logger.info("Recovery handlers initialized")
        except Exception as e:
            self.logger.error(f"Recovery handlers initialization failed: {e}")
            raise

    
    
    
    async def _analyze_requirements(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze task requirements"""
        try:
            return [{
                'type': 'requirement',
                'description': task.get('description', ''),
                'priority': task.get('priority', 1),
                'dependencies': []
            }]
        except Exception as e:
            self.logger.error(f"Requirements analysis failed: {e}")
            raise

    async def _generate_steps(self, requirements: List[Dict[str, Any]],
                            observation: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate execution steps"""
        try:
            steps = []
            for req in requirements:
                step = {
                    'type': 'step',
                    'description': req['description'],
                    'priority': req['priority'],
                    'dependencies': req['dependencies'],
                    'evidence_required': True
                }
                steps.append(step)
            return steps
        except Exception as e:
            self.logger.error(f"Step generation failed: {e}")
            raise

    async def _plan_evidence_collection(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Plan evidence collection strategy"""
        try:
            return {
                'required_evidence': [],
                'collection_strategy': 'sequential',
                'validation_rules': {}
            }
        except Exception as e:
            self.logger.error(f"Evidence collection planning failed: {e}")
            raise

    
    async def validate_execution(self,
                               plan: Dict[str, Any],
                               result: Dict[str, Any]) -> Dict[str, bool]:
        """Validate execution results against plan"""
        try:
            validation_result = {
                "valid": False,
                "validation_checks": {}
            }
            
            # Perform validation checks
            checks = await self._perform_validation_checks(plan, result)
            validation_result['validation_checks'] = checks
            
            # Calculate overall success
            validation_result['valid'] = all(
                check['passed'] for check in checks.values()
            )
            
            # Store validation evidence
            await self.evidence_store.store_evidence(
                str(datetime.now().timestamp()),
                {
                    'plan': plan,
                    'result': result,
                    'validation': validation_result
                },
                {'type': 'validation'}
            )
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Execution validation failed: {str(e)}")
            raise

    async def _perform_validation_checks(self,
                                      plan: Dict[str, Any],
                                      result: Dict[str, Any]) -> Dict[str, Any]:
        """Perform comprehensive validation checks"""
        checks = {}
        
        try:
            # Check completeness
            checks['completeness'] = {
                'passed': self._check_completeness(plan, result),
                'timestamp': datetime.now().isoformat()
            }
            
            # Check consistency
            checks['consistency'] = {
                'passed': self._check_consistency(plan, result),
                'timestamp': datetime.now().isoformat()
            }
            
            # Check evidence quality
            checks['evidence_quality'] = {
                'passed': await self._check_evidence_quality(result),
                'timestamp': datetime.now().isoformat()
            }
            
            return checks
            
        except Exception as e:
            self.logger.error(f"Validation checks failed: {str(e)}")
            raise

    def _check_completeness(self,
                          plan: Dict[str, Any],
                          result: Dict[str, Any]) -> bool:
        """Check if all planned steps were completed"""
        try:
            planned_steps = set(step['id'] for step in plan.get('steps', []))
            completed_steps = set(step['id'] for step in result.get('steps', []))
            return planned_steps.issubset(completed_steps)
        except Exception as e:
            self.logger.error(f"Completeness check failed: {str(e)}")
            return False

    def _check_consistency(self,
                         plan: Dict[str, Any],
                         result: Dict[str, Any]) -> bool:
        """Check consistency between plan and results"""
        try:
            # Check step ordering
            if not self._check_step_ordering(plan, result):
                return False
            
            # Check output consistency
            if not self._check_output_consistency(plan, result):
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Consistency check failed: {str(e)}")
            return False

    async def _check_evidence_quality(self, result: Dict[str, Any]) -> bool:
        """Check quality of collected evidence"""
        try:
            evidence = result.get('evidence', {})
            
            # Check evidence completeness
            if not self._check_evidence_completeness(evidence):
                return False
            
            # Check evidence freshness
            if not await self._check_evidence_freshness(evidence):
                return False
            
            # Check evidence consistency
            if not self._check_evidence_consistency(evidence):
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Evidence quality check failed: {str(e)}")
            return False

    async def cleanup(self) -> None:
        """Cleanup REWOO system resources"""
        try:
            if self.evidence_store:
                await self.evidence_store.cleanup()
            
            if self.planning_system:
                await self.planning_system.cleanup()
            
            if self.world_observer:
                await self.world_observer.cleanup()
            
            self.initialized = False
            self.logger.info("REWOO system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"REWOO system cleanup failed: {str(e)}")
            raise
    
    async def create_evidence_chain(self, task_id: str) -> None:
        """Create and track evidence chain for task execution"""
        try:
            chain = []
            
            # Layer 1 Evidence (Data Processing)
            csv_evidence = await self.evidence_store.get_evidence(
                f"csv_processing_{task_id}")
            if csv_evidence:
                chain.append(("#E1", csv_evidence))
            
            # Layer 2 Evidence (Knowledge Graph)
            entity_evidence = await self.evidence_store.get_evidence(
                f"entity_creation_{task_id}")
            if entity_evidence:
                chain.append(("#E2", entity_evidence))
                
            # Store complete chain
            await self.evidence_store.store_evidence(
                f"chain_{task_id}",
                chain,
                {'type': 'evidence_chain'}
            )
            
        except Exception as e:
            self.logger.error(f"Evidence chain creation failed: {e}")
            raise
    async def _store_evidence_chain(self, chain_id: str, evidence: List[Evidence]) -> None:
        """Store evidence chain with validation"""
        try:
            # Validate chain completeness
            if not self._validate_chain(evidence):
                raise ValidationError("Invalid evidence chain")
                
            # Store with graph structure
            await self._store_chain_nodes(chain_id, evidence)
            await self._store_chain_edges(chain_id, evidence)
            
            # Update indices
            await self._update_chain_indices(chain_id, evidence)
        except Exception as e:
            self.logger.error(f"Evidence chain storage failed: {e}")
            raise
    def _generate_planning_prompt(self, task: str) -> str:
        """Generate planning prompt for task execution"""
        return f"""
        Task Planning and Evidence Collection
        
        Task Description:
        {task}
        
        Please analyze this task and create a detailed execution plan that includes:
        1. Required evidence and information
        2. Step-by-step execution strategy
        3. Success criteria and validation methods
        4. Potential challenges and mitigation strategies
        
        Consider available tools and capabilities:
        - Data processing and analysis
        - Information retrieval
        - Pattern recognition
        - Knowledge graph operations
        
        Provide your plan in a structured format with clear steps and evidence requirements.
        """

    async def _validate_plan(self, plan: Dict[str, Any]) -> bool:
        """Validate the generated plan"""
        try:
            # Check required plan components
            required_fields = ['steps', 'evidence_requirements', 'success_criteria']
            if not all(field in plan for field in required_fields):
                return False
                
            # Validate each step
            for step in plan['steps']:
                if not self._validate_step(step):
                    return False
                    
            return True
            
        except Exception as e:
            self.logger.error(f"Plan validation failed: {e}")
            return False

    def _validate_step(self, step: Dict[str, Any]) -> bool:
        """Validate a single plan step"""
        required_fields = ['action', 'inputs', 'expected_output']
        return all(field in step for field in required_fields)

    async def cleanup(self) -> None:
        """Cleanup REWOO system resources"""
        try:
            # Cleanup evidence store
            if self.evidence_store:
                await self.evidence_store.cleanup()
                
            # Cleanup planning system
            if self.planning_system:
                await self.planning_system.cleanup()
                
            self.initialized = False
            self.logger.info("REWOO system cleaned up successfully")
        except Exception as e:
            self.logger.error(f"REWOO system cleanup failed: {e}")
            raise

    
            
    async def _observe_world_state(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Observe current world state with proper error handling"""
        try:
            # Get current system state
            current_state = {
                'timestamp': datetime.now().isoformat(),
                'context': context,
                'system_metrics': self._get_system_metrics(),
                'agent_states': self._get_agent_states(),
                'evidence_summary': await self._get_evidence_summary()
            }
        
            # Store observation in evidence store
            await self.evidence_store.store_evidence(
                f"observation_{datetime.now().timestamp()}",
                current_state,
                {'type': 'world_state'}
            )
        
            return current_state
        
        except Exception as e:
            self.logger.error(f"World state observation failed: {e}")
            raise

    async def _get_evidence_summary(self) -> Dict[str, Any]:
        """Get summary of available evidence"""
        try:
            return await self.evidence_store.get_summary()
        except Exception as e:
            self.logger.error(f"Evidence summary retrieval failed: {e}")
            return {}

    def _get_system_metrics(self) -> Dict[str, Any]:
        """Get current system metrics"""
        return {
            'cpu_usage': psutil.cpu_percent(),
            'memory_usage': psutil.virtual_memory().percent,
            'active_tasks': len(self.active_tasks) if hasattr(self, 'active_tasks') else 0
        }

    def _get_agent_states(self) -> Dict[str, Any]:
        """Get current agent states"""
        return {
            agent_name: {'status': 'active' if agent.initialized else 'inactive'}
            for agent_name, agent in self.agents.items()
        } if hasattr(self, 'agents') else {}

    
    
class ParallelProcessor:
    def __init__(self, config: 'EnhancedConfig'):
        self.config = config
        self.worker_pools = defaultdict(list)
        self.task_queue = asyncio.Queue()
        
    async def process_parallel(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process tasks in parallel"""
        results = []
        async with asyncio.TaskGroup() as group:
            for task in tasks:
                task_result = await group.create_task(
                    self._process_single_task(task)
                )
                results.append(task_result)
        return results


class BaseAgent(ABC):
    def __init__(self, name: str, model_info: Dict[str, Any], config: 'EnhancedConfig'):
        self.name = name
        self.model_info = model_info
        self.config = config
        self.initialized = False

    @abstractmethod
    async def initialize(self) -> None:
        pass
            
from typing import Dict, List, Any, Optional, Union, Type
from dataclasses import dataclass, field
import logging
import asyncio
from datetime import datetime
from collections import defaultdict, Counter
from cachetools import TTLCache

# Base Agent Class
class BaseAgent:
    """Base class for all agents with enhanced communication capabilities"""
    def __init__(self, name: str, model_info: Dict[str, Any], config: 'EnhancedConfig'):
        self.name = name
        self.model_info = model_info
        self.config = config
        self.layer_id = model_info.get('layer_id', 0)  # Default to layer 0
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{name}")
        self.communication_system = config.communication_system
        self.initialized = False
        self.metrics = defaultdict(Counter)
        
        
        # Initialize evidence store with configuration
        self.evidence_store = EvidenceStore(config.evidence_store_config)
        
        # Core components
        self.rewoo_system = None
        self.planning_system = None
        
        # State management
        self._initializing = False
        self.state = defaultdict(dict)
        
        # Performance tracking
        self.metrics = defaultdict(Counter)
        self.performance_history = []
        
        # Caching
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        
        
     
    async def initialize(self) -> None:
        """Initialize agent with proper error handling"""
        try:
            await self._initialize_core_components()
            await self._initialize_evidence_store()
            await self._initialize_planning_system()
        
            self.initialized = True
            self.logger.info(f"Agent {self.name} initialized successfully")
        except Exception as e:
            self.logger.error(f"Agent initialization failed: {e}")
            await self.cleanup()
            raise
            
    async def cleanup(self) -> None:
        """Cleanup agent resources"""
        try:
            if self.evidence_store:
                await self.evidence_store.cleanup()
            self.initialized = False
            self.logger.info(f"Agent {self.name} cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Agent cleanup failed: {e}")
            raise
    async def _initialize_core_components(self) -> None:
        """Initialize core agent components"""
        try:
            # Initialize evidence store
            self.evidence_store = EvidenceStore(self.config.evidence_store_config)
            await self.evidence_store.initialize()
            
            # Initialize planning system
            self.planning_system = PlanningSystem(self.config)
            await self.planning_system.initialize()
            
            # Initialize metrics tracking
            self.metrics = {
                'tasks_processed': Counter(),
                'coordination_events': Counter(),
                'recovery_attempts': Counter()
            }
            
            self.logger.info(f"Core components initialized successfully")
        except Exception as e:
            self.logger.error(f"Core component initialization failed: {e}")
            raise
    async def _initialize_evidence_store(self):
        """Initialize evidence store."""
        try:
            evidence_store = EvidenceStore()
            await evidence_store.initialize()
            return evidence_store
        except Exception as e:
            self.logger.error(f"Evidence store initialization failed: {e}")
            raise

    async def _initialize_rewoo_system(self):
        """Initialize REWOO system."""
        try:
            rewoo_system = REWOOSystem(self.config)
            await rewoo_system.initialize()
            return rewoo_system
        except Exception as e:
            self.logger.error(f"REWOO system initialization failed: {e}")
            raise

    async def _initialize_planning_system(self):
        """Initialize planning system."""
        try:
            planning_system = PlanningSystem()
            await planning_system.initialize()
            return planning_system
        except Exception as e:
            self.logger.error(f"Planning system initialization failed: {e}")
            raise

    async def _setup_communication(self) -> None:
        """Setup communication channels."""
        try:
            await self.communication.subscribe(f"agent_{self.name}", self.name)
            await self.communication.subscribe("broadcast", self.name)
        except Exception as e:
            self.logger.error(f"Communication setup failed: {e}")
            raise

    async def _initialize_agent(self) -> None:
        """Initialize agent-specific components. To be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _initialize_agent")

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task with REWOO capabilities and evidence collection."""
        if not self.initialized:
            raise RuntimeError(f"Agent {self.name} not initialized")
            
        task_id = f"task_{datetime.now().timestamp()}"
        
        try:
            # Create execution plan using REWOO
            plan = await self.rewoo_system.create_plan(task)
            
            # Execute plan with evidence collection
            result = await self._execute_plan(plan)
            
            # Store execution evidence
            await self.evidence_store.store_evidence(
                task_id,
                result,
                {'agent': self.name, 'task': task}
            )
            
            # Update metrics
            self._update_metrics(task_id, result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Task processing failed: {str(e)}")
            await self._handle_task_error(task_id, e)
            raise

    async def _execute_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute plan with evidence tracking."""
        try:
            results = []
            for step in plan['steps']:
                # Execute step
                result = await self._execute_step(step)
                
                # Validate step result
                validation = await self._validate_step_result(result)
                
                if not validation['valid']:
                    # Replan if validation fails
                    new_plan = await self.rewoo_system.replan(
                        plan,
                        f"Step validation failed: {validation['reason']}"
                    )
                    return await self._execute_plan(new_plan)
                    
                results.append(result)
                
            return self._aggregate_results(results)
            
        except Exception as e:
            self.logger.error(f"Plan execution failed: {str(e)}")
            raise

    async def cleanup(self) -> None:
        """Cleanup agent resources."""
        try:
            if self.evidence_store:
                await self.evidence_store.cleanup()
            if self.rewoo_system:
                await self.rewoo_system.cleanup()
            if self.planning_system:
                await self.planning_system.cleanup()
                
            self.initialized = False
            self._initializing = False
            
            self.logger.info(f"Agent {self.name} cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Agent cleanup failed: {str(e)}")
            raise
    async def send_to_next_layer(self, message: Dict[str, Any]) -> None:
        """Send message to next layer - base implementation"""
        if not self.initialized:
            raise RuntimeError(f"Agent {self.name} not initialized")
            
        try:
            next_layer = self.layer_id + 1
            await self.communication_system.send_to_layer(next_layer, {
                'sender': self.name,
                'content': message,
                'timestamp': datetime.now().isoformat(),
                'metadata': {
                    'source_layer': self.layer_id,
                    'message_type': 'forward'
                }
            })
            self.metrics['messages_sent']['next_layer'] += 1
        except Exception as e:
            self.logger.error(f"Failed to send message to next layer: {e}")
            raise

    async def receive_from_previous_layer(self) -> Optional[Dict[str, Any]]:
        """Receive message from previous layer - base implementation"""
        if not self.initialized:
            raise RuntimeError(f"Agent {self.name} not initialized")
            
        try:
            message = await self.communication_system.receive_from_layer(self.layer_id - 1)
            if message:
                self.metrics['messages_received']['previous_layer'] += 1
            return message
        except Exception as e:
            self.logger.error(f"Failed to receive message from previous layer: {e}")
            raise
    def _update_metrics(self, task_id: str, result: Dict[str, Any]) -> None:
        """Update agent metrics."""
        try:
            self.metrics['tasks_processed']['total'] += 1
            self.metrics['tasks_processed'][result.get('type', 'unknown')] += 1
            
            if result.get('status') == 'success':
                self.metrics['successful_tasks'] += 1
            else:
                self.metrics['failed_tasks'] += 1
                
            self.task_history.append({
                'task_id': task_id,
                'result': result,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            self.logger.error(f"Metrics update failed: {str(e)}")

    async def _handle_task_error(self, task_id: str, error: Exception) -> None:
        """Handle task processing errors."""
        try:
            await self.evidence_store.store_evidence(
                f"error_{task_id}",
                {
                    'error': str(error),
                    'stack_trace': error.__traceback__,
                    'timestamp': datetime.now().isoformat()
                },
                {'type': 'error', 'agent': self.name}
            )
            self.metrics['task_errors'] += 1
        except Exception as e:
            self.logger.error(f"Error handling failed: {str(e)}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get agent metrics."""
        return {
            'tasks_processed': dict(self.metrics['tasks_processed']),
            'successful_tasks': self.metrics['successful_tasks'],
            'failed_tasks': self.metrics['failed_tasks'],
            'task_errors': self.metrics['task_errors']
        }
    
    
class LayerAgent(BaseAgent):
    """Base class for all layer-specific agents with enhanced capabilities"""
    def __init__(self, name: str, model_info: Dict[str, Any], config: 'EnhancedConfig'):
        super().__init__(name, model_info, config)
        self.layer_capabilities = set()
        self.processing_history = []
        self.performance_metrics = defaultdict(list)
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.initialized = False
        self.name = name
        self.model_info = model_info
        self.config = config
        # Removed self.communication = communication
        self.evidence_store = None
        self.rewoo_system = None
        self.planning_system = None
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{name}")
        self.metrics = defaultdict(Counter)
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self._initializing = False

    async def initialize(self) -> None:
        """Initialize layer agent with proper error handling"""
        try:
            await super().initialize()
            await self._setup_layer_capabilities()
            await self._initialize_processing_components()
            self.initialized = True
            self.logger.info(f"Layer agent {self.name} initialized successfully")
        except Exception as e:
            self.logger.error(f"Layer agent initialization failed: {e}")
            raise

    async def _initialize_processing_components(self) -> None:
        """Initialize processing components"""
        try:
            # Initialize evidence store
            self.evidence_store = EvidenceStore()
            await self.evidence_store.initialize()

            # Initialize planning system with config
            self.planning_system = PlanningSystem(self.config)
            await self.planning_system.initialize()

        except Exception as e:
            self.logger.error(f"Component initialization failed: {e}")
            raise

    async def _setup_layer_capabilities(self) -> None:
        """Setup base capabilities for layer agent"""
        self.layer_capabilities.update([
            'task_processing',
            'evidence_collection',
            'result_validation'
        ])

    

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task with comprehensive error handling and metrics tracking"""
        if not self.initialized:
            raise RuntimeError(f"Layer agent {self.name} not initialized")

        start_time = time.time()
        task_id = f"task_{datetime.now().timestamp()}"

        try:
            # Create execution plan
            plan = await self.create_plan(task)
            
            # Execute plan with evidence collection
            result = await self._execute_plan(plan)
            
            # Validate and store results
            validated_result = await self._validate_result(result)
            await self._store_evidence(task_id, validated_result)
            
            # Update metrics
            self._update_metrics(task_id, time.time() - start_time)
            
            return validated_result
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            await self._handle_processing_error(task_id, e)
            raise

    async def _initialize_evidence_store(self):
        """Initialize evidence store"""
        from .evidence import EvidenceStore
        evidence_store = EvidenceStore()
        await evidence_store.initialize()
        return evidence_store

    async def _initialize_rewoo_system(self):
        """Initialize REWOO system"""
        from .rewoo import REWOOSystem
        rewoo_system = REWOOSystem(self.config)
        await rewoo_system.initialize()
        return rewoo_system

    async def _initialize_planning_system(self):
        """Initialize planning system"""
        from .planning import PlanningSystem
        planning_system = PlanningSystem()
        await planning_system.initialize()
        return planning_system

    async def _initialize_agent(self) -> None:
        """Initialize agent-specific components"""
        raise NotImplementedError("Subclasses must implement _initialize_agent")

    

    async def _execute_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute plan with evidence tracking"""
        try:
            results = []
            for step in plan['steps']:
                # Execute step
                result = await self._execute_step(step)
                
                # Validate step result
                validation = await self._validate_step_result(result)
                
                if not validation['valid']:
                    # Replan if validation fails
                    new_plan = await self.rewoo_system.replan(
                        plan,
                        f"Step validation failed: {validation['reason']}"
                    )
                    return await self._execute_plan(new_plan)
                    
                results.append(result)
                
            return self._aggregate_results(results)
            
        except Exception as e:
            self.logger.error(f"Plan execution failed: {str(e)}")
            raise

    async def cleanup(self) -> None:
        """Cleanup agent resources"""
        try:
            if self.evidence_store:
                await self.evidence_store.cleanup()
            if self.rewoo_system:
                await self.rewoo_system.cleanup()
            if self.planning_system:
                await self.planning_system.cleanup()
                
            self.initialized = False
            self._initializing = False
            
            self.logger.info(f"Layer agent {self.name} cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Layer agent cleanup failed: {str(e)}")
            raise




    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task with REWOO capabilities and layer coordination."""
        if not self.initialized:
            raise RuntimeError("Boss agent not initialized")
            
        task_id = f"task_{datetime.now().timestamp()}"
        
        try:
            # Create execution plan using REWOO
            plan = await self.rewoo_system.create_plan(task)
            
            # Process through layers
            results = await self._process_through_layers(task_id, plan)
            
            # Store execution evidence
            await self.evidence_store.store_evidence(
                task_id,
                {
                    'task': task,
                    'plan': plan,
                    'results': results,
                    'timestamp': datetime.now().isoformat()
                },
                {'type': 'task_complete'}
            )
            
            return {
                'task_id': task_id,
                'results': results,
                'metadata': self._generate_execution_metadata(task_id)
            }
            
        except Exception as e:
            self.logger.error(f"Task processing failed: {str(e)}")
            await self._handle_task_error(task_id, e)
            raise
    async def _initialize_evidence_store(self):
        """Initialize evidence store with local implementation."""
        try:
            evidence_store = EvidenceStore()  # Use local EvidenceStore class
            await evidence_store.initialize()
            self.logger.info("Evidence store initialized successfully")
            return evidence_store
        except Exception as e:
            self.logger.error(f"Evidence store initialization failed: {e}")
            raise

    
    
    async def _validate_step_result(self, result: Dict[str, Any]) -> Dict[str, bool]:
        """Enhanced validation with comprehensive checks."""
        try:
            validation = {
                'valid': True,
                'reason': None,
                'details': {},
                'recovery_needed': False
            }
            
            # Check basic result structure
            if not self._validate_result_structure(result):
                return self._failed_validation("Invalid result structure")
            
            # Validate layer execution
            layer_validation = await self._validate_layer_execution(result)
            if not layer_validation['valid']:
                return layer_validation
            
            # Check result quality
            quality_validation = await self._validate_result_quality(result)
            if not quality_validation['valid']:
                return quality_validation
            
            # Validate consistency across agents
            consistency_validation = await self._validate_consistency(result)
            if not consistency_validation['valid']:
                return consistency_validation
            
            # Update metrics
            self._update_validation_metrics(result)
            
            return validation
            
        except Exception as e:
            self.logger.error(f"Result validation failed: {e}")
            return self._failed_validation(str(e))

    async def _handle_agent_failure(self, agent_name: str, error: Exception) -> Dict[str, Any]:
        """Handle individual agent failures."""
        try:
            self.logger.warning(f"Agent failure detected: {agent_name}")
            
            # Get agent layer and configuration
            layer_id = self._get_agent_layer(agent_name)
            
            # Try to restart agent
            if await self._attempt_agent_restart(agent_name):
                return {'recovered': True, 'action': 'restart'}
            
            # Try to reassign tasks
            if await self._reassign_agent_tasks(agent_name, layer_id):
                return {'recovered': True, 'action': 'reassign'}
            
            # Fall back to degraded operation
            return await self._initiate_degraded_operation(layer_id)
            
        except Exception as e:
            self.logger.error(f"Agent failure recovery failed: {e}")
            raise

    async def _handle_layer_failure(self, layer_id: int, error: Exception) -> Dict[str, Any]:
        """Handle entire layer failures."""
        try:
            self.logger.warning(f"Layer failure detected: {layer_id}")
            
            # Try to restart layer
            if await self._attempt_layer_restart(layer_id):
                return {'recovered': True, 'action': 'layer_restart'}
            
            # Try to skip layer if possible
            if await self._can_skip_layer(layer_id):
                return {'recovered': True, 'action': 'layer_skip'}
            
            # Fall back to emergency processing
            return await self._emergency_processing(layer_id)
            
        except Exception as e:
            self.logger.error(f"Layer failure recovery failed: {e}")
            raise

    def _failed_validation(self, reason: str) -> Dict[str, bool]:
        """Create failed validation result with details."""
        return {
            'valid': False,
            'reason': reason,
            'details': {
                'timestamp': datetime.now().isoformat(),
                'recoverable': True
            },
            'recovery_needed': True
        }

    async def coordinate_layers(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced layer coordination with sophisticated error handling."""
        task_id = str(uuid.uuid4())
        try:
            # Initialize coordination state
            self.coordination_state['task_progress'][task_id] = {
                'status': 'started',
                'current_layer': 0,
                'layer_results': {},
                'start_time': datetime.now().isoformat()
            }
            
            results = {}
            current_input = task
            
            # Process through layers
            for layer_id in sorted(self.layer_agents.keys()):
                try:
                    # Execute layer with timeout and retries
                    layer_result = await self._execute_layer_with_recovery(
                        layer_id,
                        current_input,
                        task_id
                    )
                    
                    # Validate and store results
                    if await self._validate_and_store_layer_result(layer_id, layer_result):
                        results[layer_id] = layer_result
                        current_input = self._prepare_next_layer_input(layer_result)
                    else:
                        raise ValueError(f"Layer {layer_id} validation failed")
                        
                except Exception as e:
                    # Handle layer failure
                    recovery_result = await self._handle_layer_failure(layer_id, e)
                    if not recovery_result.get('recovered'):
                        raise
                    
                    # Update metrics
                    self.metrics['recovery_attempts'][layer_id] += 1
                    
            return self._prepare_final_result(results, task_id)
            
        except Exception as e:
            self.logger.error(f"Layer coordination failed: {e}")
            raise
        finally:
            # Update coordination state
            self.coordination_state['task_progress'][task_id]['status'] = 'completed'
            self.coordination_state['task_progress'][task_id]['end_time'] = datetime.now().isoformat()

    async def register_layer_agent(self, agent: 'BaseAgent') -> None:
        """Register a layer agent for coordination."""
        try:
            layer_id = agent.model_info.get('layer_id')
            if layer_id is None:
                raise ValueError(f"Agent {agent.name} missing layer_id in model_info")
                
            self.layer_agents[layer_id].append(agent)
            self.logger.info(f"Registered agent {agent.name} to layer {layer_id}")
        except Exception as e:
            self.logger.error(f"Agent registration failed: {e}")
            raise

    async def create_plan(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Create execution plan with REWOO capabilities."""
        try:
            # Get current observation
            observation = await self.rewoo_system.observe(task)
            
            # Create initial plan
            plan = await self.planning_system.create_plan(task, observation)
            
            # Store planning evidence
            await self.evidence_store.store_evidence(
                str(datetime.now().timestamp()),
                {
                    'task': task,
                    'plan': plan,
                    'observation': observation
                },
                {'type': 'planning', 'agent': self.name}
            )
            
            return plan
        except Exception as e:
            self.logger.error(f"Plan creation failed: {e}")
            raise

    async def _execute_step(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single step through layer coordination."""
        try:
            layer_id = step.get('layer_id')
            if layer_id not in self.layer_agents:
                raise ValueError(f"No agents registered for layer {layer_id}")
                
            # Execute step across layer agents
            results = await asyncio.gather(*[
                agent.process_task(step['task'])
                for agent in self.layer_agents[layer_id]
            ])
            
            return {
                'layer_id': layer_id,
                'results': results,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Step execution failed: {e}")
            raise

    async def _validate_step_result(self, result: Dict[str, Any]) -> Dict[str, bool]:
        """Validate layer execution results."""
        try:
            validation = {
                'valid': True,
                'reason': None
            }
            
            # Check for empty results
            if not result.get('results'):
                validation.update({
                    'valid': False,
                    'reason': 'No results from layer execution'
                })
                return validation
                
            # Check for failed executions
            failed_results = [r for r in result['results'] if 'error' in r]
            if failed_results:
                validation.update({
                    'valid': False,
                    'reason': f"Layer execution failures: {len(failed_results)}"
                })
                
            return validation
        except Exception as e:
            self.logger.error(f"Result validation failed: {e}")
            return {'valid': False, 'reason': str(e)}

    def _aggregate_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate layer results into final output."""
        try:
            return {
                'layer_results': {
                    result['layer_id']: result['results']
                    for result in results
                },
                'timestamp': datetime.now().isoformat(),
                'metadata': {
                    'num_layers': len(results),
                    'total_agents': sum(
                        len(result['results'])
                        for result in results
                    )
                }
            }
        except Exception as e:
            self.logger.error(f"Results aggregation failed: {e}")
            raise

class LayerCoordinator:
    """Enhanced layer coordination with evidence tracking"""
    def __init__(self, config: EnhancedConfig):
        self.config = config
        self.evidence_manager = EvidenceManager()
        self.logger = logging.getLogger(__name__)
        
    async def coordinate_layer_execution(self, task: Dict[str, Any], layer_id: int) -> Dict[str, Any]:
        """Coordinate execution with evidence validation"""
        try:
            # Get layer agents
            agents = self.config.layer_agents[layer_id]
            
            # Execute with evidence tracking
            results = []
            for agent in agents:
                result = await agent.process_task(task)
                await self.evidence_manager.store_evidence(
                    f"layer_{layer_id}_{agent.name}",
                    result
                )
                results.append(result)
                
            return self._aggregate_results(results)
            
        except Exception as e:
            self.logger.error(f"Layer coordination failed: {e}")
            raise

class Layer1Agent(BaseAgent):
    """Data Integration & Processing Layer Agent"""
    def __init__(self, name: str, model_info: Dict[str, Any], config: 'EnhancedConfig'):
        super().__init__(name, model_info, config)
        self.data_processors = {}
        self.schema_validators = {}
        self.transformation_rules = {}
        self.evidence_store = None

    async def initialize(self) -> None:
        """Initialize Layer1 agent with data processing capabilities"""
        try:
            # Initialize core components
            await self._initialize_core_components()
            
            # Initialize specialized components
            await self._initialize_data_processors()
            await self._initialize_schema_validators()
            
            self.initialized = True
            self.logger.info(f"Layer1 agent {self.name} initialized successfully")
        except Exception as e:
            self.logger.error(f"Layer1 agent initialization failed: {e}")
            raise

    async def _initialize_data_processors(self) -> None:
        """Initialize data processing components"""
        self.data_processors = {
            'csv': self._process_csv_data,
            'json': self._process_json_data,
            'structured': self._process_structured_data,
            'unstructured': self._process_unstructured_data
        }

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process data integration tasks"""
        try:
            # Extract task parameters
            data_type = task.get('data_type', 'structured')
            processor = self.data_processors.get(data_type)
            
            if not processor:
                raise ValueError(f"Unsupported data type: {data_type}")
                
            # Process data
            result = await processor(task['data'])
            
            # Store evidence
            await self.evidence_store.store_evidence(
                f"layer1_{datetime.now().timestamp()}",
                result,
                {'processor': data_type}
            )
            
            return result
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            raise
    
    async def _initialize_agent(self) -> None:
        """Initialize Layer1 agent-specific components"""
        self.specializations = {
            'task_analysis': self._analyze_task,
            'requirement_extraction': self._extract_requirements,
            'context_enhancement': self._enhance_context
        }
        self.logger.info(f"Layer1 agent {self.name} components initialized")
    async def _setup_layer_capabilities(self) -> None:
        """Setup specialized capabilities for Layer1"""
        await super()._setup_layer_capabilities()
        self.layer_capabilities.update([
            'data_extraction',
            'initial_analysis',
            'pattern_recognition',
            'data_validation'
        ])

    async def _process_csv_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process CSV data with validation and cleaning"""
        try:
            # Standardize schema
            standardized_df = self.standardize_schema(data)
            
            # Store evidence
            await self.evidence_store.store_evidence(
                f"csv_processing_{datetime.now().timestamp()}",
                standardized_df,
                {'type': 'csv_processing'}
            )
            
            return standardized_df
        except Exception as e:
            self.logger.error(f"CSV processing failed: {e}")
            raise
            
    async def _analyze_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze task requirements and structure"""
        # Implement task analysis logic
        pass

    async def _extract_requirements(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract task requirements"""
        # Implement requirement extraction logic
        pass

    async def _enhance_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance task context with additional information"""
        # Implement context enhancement logic
        pass

class Layer2Agent(BaseAgent):
    """Knowledge Graph Construction & Analysis Layer Agent"""
    def __init__(self, name: str, model_info: Dict[str, Any], config: 'EnhancedConfig'):
        super().__init__(name, model_info, config)
        self.graph_builders = {}
        self.ontology_mappers = {}
        self.relationship_extractors = {}
        self.pattern_detectors = {}
        self.analysis_engines = {}
    
    
    async def initialize(self) -> None:
        """Initialize Layer2 agent with graph capabilities"""
        try:
            # Initialize core components
            await self._initialize_core_components()
            
            # Initialize specialized components
            await self._initialize_graph_builders()
            await self._initialize_ontology_mappers()
            
            self.initialized = True
            self.logger.info(f"Layer2 agent {self.name} initialized successfully")
        except Exception as e:
            self.logger.error(f"Layer2 agent initialization failed: {e}")
            raise

    async def _initialize_graph_builders(self) -> None:
        """Initialize graph construction components"""
        self.graph_builders = {
            'entity': self._build_entity_nodes,
            'relationship': self._build_relationships,
            'attribute': self._build_attributes
        }

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process knowledge graph construction tasks"""
        try:
            # Extract task parameters
            graph_operation = task.get('operation', 'entity')
            builder = self.graph_builders.get(graph_operation)
            
            if not builder:
                raise ValueError(f"Unsupported graph operation: {graph_operation}")
                
            # Build graph components
            result = await builder(task['data'])
            
            # Store evidence
            await self.evidence_store.store_evidence(
                f"layer2_{datetime.now().timestamp()}",
                result,
                {'operation': graph_operation}
            )
            
            return result
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            raise

    async def _setup_layer_capabilities(self) -> None:
        """Setup specialized capabilities for Layer2"""
        self.layer_capabilities = {
            'pattern_analysis': self._analyze_patterns,
            'insight_generation': self._generate_insights,
            'data_validation': self._validate_data
        }

    async def _initialize_agent(self) -> None:
        """Initialize agent-specific components"""
        self.pattern_detectors = {}
        self.analysis_engines = {}
        self.logger.info(f"Layer2 agent {self.name} components initialized")

    async def _analyze_patterns(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze patterns in input data"""
        try:
            patterns = {
                'temporal_patterns': self._analyze_temporal_patterns(data),
                'behavioral_patterns': self._analyze_behavioral_patterns(data),
                'correlation_patterns': self._analyze_correlations(data)
            }
            return {
                'patterns': patterns,
                'confidence': self._calculate_pattern_confidence(patterns),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Pattern analysis failed: {e}")
            raise

    async def _generate_insights(self, patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Generate insights from patterns"""
        try:
            insights = {
                'key_findings': self._extract_key_findings(patterns),
                'recommendations': self._generate_recommendations(patterns),
                'priority_actions': self._prioritize_actions(patterns)
            }
            return {
                'insights': insights,
                'confidence': self._calculate_insight_confidence(insights),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Insight generation failed: {e}")
            raise

    async def _validate_data(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Validate input data"""
        try:
            validation_results = {
                'structure_valid': self._validate_structure(data),
                'content_valid': self._validate_content(data),
                'relationships_valid': self._validate_relationships(data)
            }
            return validation_results
        except Exception as e:
            self.logger.error(f"Data validation failed: {e}")
            raise
    async def _build_entity_nodes(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build knowledge graph entity nodes"""
        try:
            entities = []
            for key, value in data.items():
                entity = {
                    'id': str(uuid.uuid4()),
                    'type': self._determine_entity_type(value),
                    'properties': value
                }
                entities.append(entity)
                
            # Store evidence
            await self.evidence_store.store_evidence(
                f"entity_creation_{datetime.now().timestamp()}",
                entities,
                {'type': 'entity_creation'}
            )
            
            return entities
        except Exception as e:
            self.logger.error(f"Entity node creation failed: {e}")
            raise
    # Helper methods
    def _analyze_temporal_patterns(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement temporal pattern analysis

    def _analyze_behavioral_patterns(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement behavioral pattern analysis

    def _analyze_correlations(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement correlation analysis

    def _calculate_pattern_confidence(self, patterns: Dict[str, Any]) -> float:
        return 0.8  # Implement confidence calculation

    def _extract_key_findings(self, patterns: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement key findings extraction

    def _generate_recommendations(self, patterns: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement recommendation generation

    def _prioritize_actions(self, patterns: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement action prioritization

    def _calculate_insight_confidence(self, insights: Dict[str, Any]) -> float:
        return 0.8  # Implement confidence calculation

    def _validate_structure(self, data: Dict[str, Any]) -> bool:
        return True  # Implement structure validation

    def _validate_content(self, data: Dict[str, Any]) -> bool:
        return True  # Implement content validation

    def _validate_relationships(self, data: Dict[str, Any]) -> bool:
        return True  # Implement relationship validation

    
    
    
    async def _process_data(self, data: Any) -> Any:
        """Process input data with advanced validation and transformation."""
        try:
            # Validate input data
            validated_data = await self._validate_input_data(data)
            
            # Clean and normalize
            cleaned_data = await self._clean_and_normalize(validated_data)
            
            # Apply transformations
            transformed_data = await self._apply_transformations(cleaned_data)
            
            # Validate output
            final_data = await self._validate_output_data(transformed_data)
            
            return {
                'processed_data': final_data,
                'processing_steps': self._get_processing_steps(),
                'validation_results': self._get_validation_results(),
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'quality_score': self._calculate_quality_score(final_data)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Data processing failed: {e}")
            raise

    async def _transform_data(self, data: Any) -> Any:
        """Transform data with advanced processing capabilities."""
        try:
            # Analyze data structure
            structure_analysis = await self._analyze_data_structure(data)
            
            # Determine optimal transformations
            transformation_plan = await self._create_transformation_plan(
                structure_analysis
            )
            
            # Apply transformations
            transformed_data = await self._apply_transformation_sequence(
                data,
                transformation_plan
            )
            
            # Validate transformation results
            validated_data = await self._validate_transformation(transformed_data)
            
            return {
                'transformed_data': validated_data,
                'transformation_plan': transformation_plan,
                'validation_results': self._get_validation_results(),
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'transformation_score': self._calculate_transform_score(
                        data,
                        validated_data
                    )
                }
            }
            
        except Exception as e:
            self.logger.error(f"Data transformation failed: {e}")
            raise

    async def _validate_data(self, data: Any) -> bool:
        """Comprehensive data validation with detailed reporting."""
        try:
            validation_results = {
                'schema_validation': await self._validate_schema(data),
                'content_validation': await self._validate_content(data),
                'relationship_validation': await self._validate_relationships(data),
                'quality_validation': await self._validate_quality(data)
            }
            
            # Calculate overall validation score
            validation_score = self._calculate_validation_score(validation_results)
            
            return {
                'valid': validation_score >= self.config.validation_threshold,
                'validation_results': validation_results,
                'validation_score': validation_score,
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'threshold_used': self.config.validation_threshold
                }
            }
            
        except Exception as e:
            self.logger.error(f"Data validation failed: {e}")
            raise
            
            
class Layer3Agent(BaseAgent):
    """Analysis & Decision Making Layer Agent"""
    def __init__(self, name: str, model_info: Dict[str, Any], config: 'EnhancedConfig'):
        super().__init__(name, model_info, config)
        self.analyzers = {
            'pattern': self._analyze_patterns,
            'trend': self._analyze_trends,
            'anomaly': self._detect_anomalies,
            'correlation': self._analyze_correlations
        }
        self.decision_makers = {}
        self.optimization_engines = {}


    async def initialize(self) -> None:
        """Initialize Layer3 agent with analysis capabilities"""
        try:
            # Initialize core components
            await self._initialize_core_components()
            
            # Initialize specialized components
            await self._initialize_analyzers()
            await self._initialize_decision_makers()
            
            self.initialized = True
            self.logger.info(f"Layer3 agent {self.name} initialized successfully")
        except Exception as e:
            self.logger.error(f"Layer3 agent initialization failed: {e}")
            raise

    async def _initialize_analyzers(self) -> None:
        """Initialize analysis components"""
        self.analyzers = {
            'pattern': self._analyze_patterns,
            'trend': self._analyze_trends,
            'anomaly': self._detect_anomalies,
            'correlation': self._analyze_correlations
        }

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process analysis and decision-making tasks"""
        try:
            # Extract task parameters
            analysis_type = task.get('analysis_type', 'pattern')
            analyzer = self.analyzers.get(analysis_type)
            
            if not analyzer:
                raise ValueError(f"Unsupported analysis type: {analysis_type}")
                
            # Perform analysis
            result = await analyzer(task['data'])
            
            # Store evidence
            await self.evidence_store.store_evidence(
                f"layer3_{datetime.now().timestamp()}",
                result,
                {'analysis': analysis_type}
            )
            
            return result
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            raise


    async def _setup_layer_capabilities(self) -> None:
        """Setup specialized capabilities for Layer3"""
        self.layer_capabilities = {
            'optimization': self._optimize_solution,
            'decision_making': self._make_decisions,
            'validation': self._validate_results
        }

    async def _initialize_agent(self) -> None:
        """Initialize agent-specific components"""
        self.optimization_engines = {}
        self.decision_makers = {}
        self.logger.info(f"Layer3 agent {self.name} components initialized")

    async def _optimize_solution(self, solution: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize given solution"""
        try:
            optimized = {
                'parameters': self._optimize_parameters(solution),
                'constraints': self._validate_constraints(solution),
                'improvements': self._generate_improvements(solution)
            }
            return {
                'optimized_solution': optimized,
                'optimization_score': self._calculate_optimization_score(optimized),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Solution optimization failed: {e}")
            raise

    async def _make_decisions(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Make decisions based on context"""
        try:
            decisions = {
                'actions': self._determine_actions(context),
                'priorities': self._set_priorities(context),
                'timeline': self._create_timeline(context)
            }
            return {
                'decisions': decisions,
                'confidence': self._calculate_decision_confidence(decisions),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Decision making failed: {e}")
            raise
    async def _analyze_trends(self, data: Any) -> List[Dict[str, Any]]:
        """Analyze trends in data using advanced algorithms"""
        try:
            # Initialize analysis context
            analysis_context = await self._initialize_analysis_context(data)
            
            # Perform multi-level trend analysis
            trends = {
                'temporal': await self._analyze_temporal_trends(data),
                'behavioral': await self._analyze_behavioral_trends(data),
                'sequential': await self._analyze_sequential_trends(data),
                'correlational': await self._analyze_correlations(data)
            }
            
            # Validate trends
            validated_trends = await self._validate_trends(trends)
            
            # Calculate trend significance
            significant_trends = await self._calculate_trend_significance(validated_trends)
            
            # Store evidence
            await self.evidence_store.store_evidence(
                f"trend_analysis_{datetime.now().timestamp()}",
                {
                    'trends': significant_trends,
                    'context': analysis_context,
                    'metadata': {
                        'timestamp': datetime.now().isoformat(),
                        'analysis_metrics': self._get_analysis_metrics(trends)
                    }
                },
                {'type': 'trend_analysis'}
            )
            
            return significant_trends
            
        except Exception as e:
            self.logger.error(f"Trend analysis failed: {e}")
            raise

    async def _analyze_temporal_trends(self, data: Any) -> Dict[str, Any]:
        """Analyze temporal patterns in data"""
        try:
            return {
                'daily_patterns': self._analyze_daily_patterns(data),
                'weekly_patterns': self._analyze_weekly_patterns(data),
                'seasonal_patterns': self._analyze_seasonal_patterns(data)
            }
        except Exception as e:
            self.logger.error(f"Temporal trend analysis failed: {e}")
            raise

    async def _analyze_behavioral_trends(self, data: Any) -> Dict[str, Any]:
        """Analyze behavioral patterns"""
        try:
            return {
                'user_segments': self._analyze_user_segments(data),
                'interaction_patterns': self._analyze_interactions(data),
                'conversion_patterns': self._analyze_conversions(data)
            }
        except Exception as e:
            self.logger.error(f"Behavioral trend analysis failed: {e}")
            raise

    async def _validate_results(self, results: Dict[str, Any]) -> Dict[str, bool]:
        """Validate optimization results"""
        try:
            validation = {
                'completeness': self._check_completeness(results),
                'consistency': self._check_consistency(results),
                'feasibility': self._check_feasibility(results)
            }
            return validation
        except Exception as e:
            self.logger.error(f"Results validation failed: {e}")
            raise

    # Helper methods
    def _optimize_parameters(self, solution: Dict[str, Any]) -> Dict[str, Any]:
        return {}  # Implement parameter optimization

    def _validate_constraints(self, solution: Dict[str, Any]) -> List[bool]:
        return []  # Implement constraint validation

    def _generate_improvements(self, solution: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement improvement generation

    def _calculate_optimization_score(self, optimized: Dict[str, Any]) -> float:
        return 0.8  # Implement optimization scoring

    def _determine_actions(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement action determination

    def _set_priorities(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement priority setting

    def _create_timeline(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement timeline creation

    def _calculate_decision_confidence(self, decisions: Dict[str, Any]) -> float:
        return 0.8  # Implement confidence calculation

    def _check_completeness(self, results: Dict[str, Any]) -> bool:
        return True  # Implement completeness check

    def _check_consistency(self, results: Dict[str, Any]) -> bool:
        return True  # Implement consistency check

    def _check_feasibility(self, results: Dict[str, Any]) -> bool:
        return True  # Implement feasibility check


    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task with Layer3-specific handling"""
        try:
            # Optimize solutions
            optimized_solution = await self._optimize_solution(task)
            
            # Generate strategies
            strategies = await self._generate_strategies(optimized_solution)
            
            # Analyze impact
            impact_analysis = await self._analyze_impact(strategies)
            
            return {
                'optimized_solution': optimized_solution,
                'strategies': strategies,
                'impact_analysis': impact_analysis,
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'agent': self.name
                }
            }
        except Exception as e:
            self.logger.error(f"Layer3 processing failed: {e}")
            raise
    
    async def _analyze_patterns(self, data: Any) -> List[Dict[str, Any]]:
        """Analyze complex patterns in data using advanced algorithms."""
        try:
            # Initialize pattern analysis
            analysis_context = await self._initialize_analysis_context(data)
            
            # Perform multi-level pattern analysis
            patterns = {
                'temporal': await self._analyze_temporal_patterns(data),
                'behavioral': await self._analyze_behavioral_patterns(data),
                'sequential': await self._analyze_sequential_patterns(data),
                'correlational': await self._analyze_correlations(data)
            }
            
            # Validate patterns
            validated_patterns = await self._validate_patterns(patterns)
            
            # Calculate pattern significance
            significant_patterns = await self._calculate_pattern_significance(
                validated_patterns
            )
            
            # Generate pattern insights
            pattern_insights = await self._generate_pattern_insights(
                significant_patterns
            )
            
            return {
                'patterns': significant_patterns,
                'insights': pattern_insights,
                'confidence_scores': self._calculate_pattern_confidence(significant_patterns),
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'analysis_metrics': self._get_analysis_metrics(patterns)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Pattern analysis failed: {e}")
            raise

    async def _generate_insights(self, patterns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate actionable insights from identified patterns."""
        try:
            # Group related patterns
            pattern_groups = await self._group_related_patterns(patterns)
            
            # Generate initial insights
            raw_insights = await self._extract_raw_insights(pattern_groups)
            
            # Enrich insights with context
            enriched_insights = await self._enrich_insights_with_context(raw_insights)
            
            # Validate insights
            validated_insights = await self._validate_insights(enriched_insights)
            
            # Prioritize insights
            prioritized_insights = await self._prioritize_insights(validated_insights)
            
            # Add evidence support
            evidenced_insights = await self._add_evidence_support(prioritized_insights)
            
            return {
                'insights': evidenced_insights,
                'priority_levels': self._get_priority_levels(evidenced_insights),
                'evidence_strength': self._calculate_evidence_strength(evidenced_insights),
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'confidence_scores': self._calculate_insight_confidence(evidenced_insights)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Insight generation failed: {e}")
            raise

    async def _create_recommendations(self, insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create actionable recommendations based on insights."""
        try:
            # Initialize recommendation context
            context = await self._initialize_recommendation_context(insights)
            
            # Generate initial recommendations
            raw_recommendations = await self._generate_raw_recommendations(insights)
            
            # Validate feasibility
            feasible_recommendations = await self._validate_recommendation_feasibility(
                raw_recommendations
            )
            
            # Add implementation details
            detailed_recommendations = await self._add_implementation_details(
                feasible_recommendations
            )
            
            # Prioritize recommendations
            prioritized_recommendations = await self._prioritize_recommendations(
                detailed_recommendations
            )
            
            # Add expected impact analysis
            impact_analyzed_recommendations = await self._analyze_expected_impact(
                prioritized_recommendations
            )
            
            return {
                'recommendations': impact_analyzed_recommendations,
                'priority_matrix': self._generate_priority_matrix(impact_analyzed_recommendations),
                'impact_analysis': self._generate_impact_analysis(impact_analyzed_recommendations),
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'confidence_levels': self._calculate_recommendation_confidence(
                        impact_analyzed_recommendations
                    )
                }
            }
            
        except Exception as e:
            self.logger.error(f"Recommendation creation failed: {e}")
            raise
class Layer4Agent(BaseAgent):
    """Synthesis & Output Generation Layer Agent"""
    def __init__(self, name: str, model_info: Dict[str, Any], config: 'EnhancedConfig'):
        super().__init__(name, model_info, config)
        self.synthesizers = {
            'insight': self._synthesize_insights,
            'recommendation': self._synthesize_recommendations,
            'summary': self._synthesize_summaries,
            'action': self._synthesize_actions
        }
        self.generators = {}
        self.validators = {}

    async def initialize(self) -> None:
        """Initialize Layer4 agent with synthesis capabilities"""
        try:
            # Initialize core components
            await self._initialize_core_components()
            
            # Initialize specialized components
            await self._initialize_synthesizers()
            await self._initialize_generators()
            
            self.initialized = True
            self.logger.info(f"Layer4 agent {self.name} initialized successfully")
        except Exception as e:
            self.logger.error(f"Layer4 agent initialization failed: {e}")
            raise

    async def _initialize_synthesizers(self) -> None:
        """Initialize synthesis components"""
        self.synthesizers = {
            'insight': self._synthesize_insights,
            'recommendation': self._synthesize_recommendations,
            'summary': self._synthesize_summaries,
            'action': self._synthesize_actions
        }

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process synthesis and output generation tasks"""
        try:
            # Extract task parameters
            synthesis_type = task.get('synthesis_type', 'insight')
            synthesizer = self.synthesizers.get(synthesis_type)
            
            if not synthesizer:
                raise ValueError(f"Unsupported synthesis type: {synthesis_type}")
                
            # Perform synthesis
            result = await synthesizer(task['data'])
            
            # Store evidence
            await self.evidence_store.store_evidence(
                f"layer4_{datetime.now().timestamp()}",
                result,
                {'synthesis': synthesis_type}
            )
            
            return result
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            raise
    async def _setup_layer_capabilities(self) -> None:
        """Setup specialized capabilities for Layer4"""
        self.layer_capabilities = {
            'synthesis': self._synthesize_results,
            'final_validation': self._validate_final,
            'quality_assurance': self._ensure_quality
        }

    async def _initialize_agent(self) -> None:
        """Initialize agent-specific components"""
        self.synthesis_engines = {}
        self.final_validators = {}
        self.logger.info(f"Layer4 agent {self.name} components initialized")

    async def _synthesize_insights(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Synthesize insights from analyzed data"""
        try:
            # Group related patterns
            pattern_groups = await self._group_related_patterns(data)
            
            # Generate initial insights
            raw_insights = await self._extract_raw_insights(pattern_groups)
            
            # Enrich insights with context
            enriched_insights = await self._enrich_insights_with_context(raw_insights)
            
            # Validate insights
            validated_insights = await self._validate_insights(enriched_insights)
            
            # Prioritize insights
            prioritized_insights = await self._prioritize_insights(validated_insights)
            
            # Add evidence support
            evidenced_insights = await self._add_evidence_support(prioritized_insights)
            
            # Store synthesis evidence
            await self.evidence_store.store_evidence(
                f"insight_synthesis_{datetime.now().timestamp()}",
                {
                    'insights': evidenced_insights,
                    'priority_levels': self._get_priority_levels(evidenced_insights),
                    'evidence_strength': self._calculate_evidence_strength(evidenced_insights),
                    'metadata': {
                        'timestamp': datetime.now().isoformat(),
                        'confidence_scores': self._calculate_insight_confidence(evidenced_insights)
                    }
                },
                {'type': 'insight_synthesis'}
            )
            
            return {
                'insights': evidenced_insights,
                'priority_levels': self._get_priority_levels(evidenced_insights),
                'evidence_strength': self._calculate_evidence_strength(evidenced_insights),
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'confidence_scores': self._calculate_insight_confidence(evidenced_insights)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Insight synthesis failed: {e}")
            raise

    async def _extract_raw_insights(self, pattern_groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract initial insights from pattern groups"""
        try:
            insights = []
            for group in pattern_groups:
                insight = {
                    'id': str(uuid.uuid4()),
                    'type': self._determine_insight_type(group),
                    'content': await self._generate_insight_content(group),
                    'confidence': self._calculate_confidence(group),
                    'evidence': self._collect_evidence(group)
                }
                insights.append(insight)
            return insights
        except Exception as e:
            self.logger.error(f"Raw insight extraction failed: {e}")
            raise

    async def _enrich_insights_with_context(self, insights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich insights with additional context"""
        try:
            enriched_insights = []
            for insight in insights:
                context = await self._gather_context(insight)
                enriched_insight = {
                    **insight,
                    'context': context,
                    'implications': await self._analyze_implications(insight, context),
                    'recommendations': await self._generate_recommendations(insight, context)
                }
                enriched_insights.append(enriched_insight)
            return enriched_insights
        except Exception as e:
            self.logger.error(f"Insight enrichment failed: {e}")
            raise
            
    async def _validate_final(self, synthesis: Dict[str, Any]) -> Dict[str, bool]:
        """Perform final validation of synthesized results"""
        try:
            validation = {
                'completeness': self._validate_completeness(synthesis),
                'coherence': self._validate_coherence(synthesis),
                'actionability': self._validate_actionability(synthesis)
            }
            return validation
        except Exception as e:
            self.logger.error(f"Final validation failed: {e}")
            raise

    async def _ensure_quality(self, output: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure quality of final output"""
        try:
            quality_checks = {
                'standards_met': self._check_quality_standards(output),
                'improvements_needed': self._identify_improvements(output),
                'final_score': self._calculate_quality_score(output)
            }
            return quality_checks
        except Exception as e:
            self.logger.error(f"Quality assurance failed: {e}")
            raise

    # Helper methods
    def _combine_insights(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {}  # Implement insight combination

    def _generate_final_recommendations(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return []  # Implement final recommendation generation

    def _create_action_plan(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {}  # Implement action plan creation

    def _calculate_synthesis_confidence(self, synthesis: Dict[str, Any]) -> float:
        return 0.8  # Implement confidence calculation

    def _validate_completeness(self, synthesis: Dict[str, Any]) -> bool:
        return True  # Implement completeness validation

    def _validate_coherence(self, synthesis: Dict[str, Any]) -> bool:
        return True  # Implement coherence validation

    def _validate_actionability(self, synthesis: Dict[str, Any]) -> bool:
        return True  # Implement actionability validation

    def _check_quality_standards(self, output: Dict[str, Any]) -> Dict[str, bool]:
        return {}  # Implement quality standards check

    def _identify_improvements(self, output: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []  # Implement improvement identification

    def _calculate_quality_score(self, output: Dict[str, Any]) -> float:
        return 0.8  # Implement quality scoring


    

class BossAgent(BaseAgent):
    """Enhanced boss agent with comprehensive planning and optimization capabilities"""
    
    def __init__(self, name: str, model_info: Dict[str, Any], config: 'EnhancedConfig'):
        super().__init__(name, model_info, config)
        self.layer_agents = defaultdict(list)
        self.active_tasks = {}
        self.coordination_state = {}
        
        # Core components
        self.active_agents = {}
        self.layer_agents = defaultdict(list)
        self.agent_states = {}
        
        # Task management
        self.task_queue = asyncio.Queue()
        self.task_history = []
        
        # State management
        self.state = defaultdict(dict)
        self._initializing = False
        
        # Initialize communication system
        self.communication_system = None
        self.communication_channels = {}
        self.message_queue = asyncio.Queue()
        self.channel_states = defaultdict(dict)
        self.communication_metrics = defaultdict(Counter)
        
        # Performance tracking
        self.metrics = defaultdict(Counter)
        self.performance_tracker = defaultdict(list)
        
        # Caching
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.planning_cache = TTLCache(maxsize=500, ttl=1800)
        
        # Recovery
        self.recovery_strategies = {}
        self.active_recoveries = set()
        
        # Logger
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{name}")

    async def initialize(self) -> None:
        """Initialize boss agent with proper error handling"""
        try:
            # Initialize evidence store
            await self._initialize_evidence_store()
            
            # Initialize communication
            await self._initialize_communication()
            
            # Initialize layer agents
            await self._initialize_layer_agents()
            
            # Initialize core components
            await self._initialize_core()
            
            # Initialize REWOO capabilities
            await self._initialize_rewoo()
            
            # Initialize layer coordination
            await self._initialize_layer_coordination()
            
            # Setup planning system
            await self._setup_planning()
            
            # Initialize REWOO capabilities
            await self._initialize_rewoo_capabilities()
            
            # Setup communication
            await self._setup_communication()
            
            # Initialize agent management
            await self._initialize_agent_management()
            
            self.initialized = True
            self.logger.info(f"Boss agent {self.name} initialized successfully")
        except Exception as e:
            self.logger.error(f"Boss agent initialization failed: {e}")
            await self.cleanup()
            raise
        finally:
            self._initializing = False

    async def _initialize_rewoo(self) -> None:
        """Initialize REWOO system and related components"""
        try:
            # Initialize REWOO system
            self.rewoo_system = REWOOSystem(self.config)
            await self.rewoo_system.initialize()
            
            # Initialize evidence store if not already initialized
            if not hasattr(self, 'evidence_store') or not self.evidence_store:
                self.evidence_store = EvidenceStore(self.config.evidence_store_config)
                await self.evidence_store.initialize()
            
            # Initialize planning system
            self.planning_system = PlanningSystem(self.config)
            await self.planning_system.initialize()
            
            # Initialize REWOO configuration
            self.rewoo_config = REWOOConfig(
                enabled=True,
                max_planning_steps=5,
                evidence_threshold=0.8,
                context_window=4096,
                planning_temperature=0.7
            )
            
            self.logger.info("REWOO system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"REWOO system initialization failed: {e}")
            raise

    async def _initialize_core(self) -> None:
        """Initialize core components"""
        try:
            # Initialize metrics tracking
            self.metrics = {
                'tasks_processed': Counter(),
                'plans_created': Counter(),
                'optimizations_performed': Counter(),
                'errors': Counter()
            }
            
            # Initialize state tracking
            self.state.update({
                'agent_status': {},
                'task_status': {},
                'resource_usage': {}
            })
            
            # Setup recovery strategies
            self.recovery_strategies = {
                'task_failure': self._handle_task_failure,
                'plan_failure': self._handle_plan_failure,
                'optimization_failure': self._handle_optimization_failure
            }
            
            self.logger.info("Core components initialized successfully")
        except Exception as e:
            self.logger.error(f"Core initialization failed: {e}")
            raise
            
    async def _initialize_evidence_store(self):
        """Initialize evidence store"""
        try:
            evidence_store = EvidenceStore()
            await evidence_store.initialize()
            return evidence_store
        except Exception as e:
            self.logger.error(f"Evidence store initialization failed: {e}")
            raise
    async def initiate_collaboration(self, task: Dict[str, Any]):
        """Coordinate task execution across layers with REWOO integration"""
        plan = await self.rewoo_system.create_plan(task)
        results = await self._process_through_layers(plan)
        return results
    async def _initialize_rewoo(self) -> None:
        """Initialize REWOO system and capabilities"""
        try:
            # Initialize REWOO system
            self.rewoo_system = REWOOSystem(self.config)
            await self.rewoo_system.initialize()
            
            # Initialize evidence store if not already initialized
            if not hasattr(self, 'evidence_store') or not self.evidence_store:
                self.evidence_store = EvidenceStore(self.config.evidence_store_config)
                await self.evidence_store.initialize()
            
            # Initialize planning system
            self.planning_system = PlanningSystem(self.config)
            await self.planning_system.initialize()
            
            # Initialize REWOO configuration
            self.rewoo_config = REWOOConfig(
                enabled=True,
                max_planning_steps=5,
                evidence_threshold=0.8,
                context_window=4096,
                planning_temperature=0.7
            )
            
            self.logger.info("REWOO system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"REWOO system initialization failed: {e}")
            raise

    async def _setup_planning(self) -> None:
        """Initialize planning capabilities"""
        try:
            self.planning_capabilities = {
                'task_decomposition': self._decompose_task,
                'evidence_planning': self._plan_evidence_collection,
                'execution_planning': self._create_execution_plan,
                'optimization': self._optimize_plan,
                'replan': self._generate_replan,
                'validation': self._validate_plan
            }
            
            # Initialize planning metrics
            self.planning_metrics = {
                'plans_created': Counter(),
                'optimizations': Counter(),
                'replans': Counter(),
                'execution_times': []
            }
            
            self.logger.info("Planning capabilities initialized successfully")
        except Exception as e:
            self.logger.error(f"Planning setup failed: {e}")
            raise
    async def _setup_communication(self) -> None:
        """Setup communication channels with proper error handling"""
        try:
            # Initialize communication system if not already initialized
            if not hasattr(self, 'communication_system') or not self.communication_system:
                self.communication_system = CommunicationSystem()
                await self.communication_system.initialize()
        
            # Register with communication system
            await self.communication_system.register_agent(
                self.name,
                ['system', 'coordination', 'task_assignment']
            )
        
            # Set up layer coordination channels
            for layer_id in range(1, len(self.config.layer_configs) + 1):
                channel_name = f"layer_{layer_id}"
                await self.communication_system.create_channel(channel_name)
                await self.communication_system.register_agent(self.name, [channel_name])
            
            self.logger.info("Communication setup completed successfully")
        
        except Exception as e:
            self.logger.error(f"Communication setup failed: {e}")
            raise

    async def _setup_layer_channels(self) -> None:
        """Setup communication channels for each layer"""
        try:
            for layer_id in range(1, len(self.config.layer_configs) + 1):
                channel_name = f"layer_{layer_id}"
                await self.communication_system.create_channel(channel_name)
                
                # Setup layer-specific message handlers
                await self._setup_layer_handler(layer_id, channel_name)
                
                self.channel_states[channel_name] = {
                    'active': True,
                    'layer_id': layer_id,
                    'message_count': 0,
                    'last_activity': datetime.now().isoformat()
                }

            self.logger.info("Layer channels setup completed")

        except Exception as e:
            self.logger.error(f"Layer channel setup failed: {e}")
            raise

    async def _initialize_message_handlers(self) -> None:
        """Initialize message handlers for different types of communications"""
        try:
            self.message_handlers = {
                'task_assignment': self._handle_task_assignment,
                'result_collection': self._handle_result_collection,
                'error_notification': self._handle_error_notification,
                'system_message': self._handle_system_message,
                'coordination': self._handle_coordination_message
            }

            # Setup message processing loop
            self.message_processor = asyncio.create_task(self._process_messages())
            
            self.logger.info("Message handlers initialized successfully")

        except Exception as e:
            self.logger.error(f"Message handler initialization failed: {e}")
            raise
    async def _initialize_layer_coordination(self) -> None:
        """Initialize layer coordination"""
        try:
            for layer_id in range(1, 5):
                await self._initialize_layer(layer_id)
                await self._setup_communication(layer_id)
                await self._verify_layer(layer_id)
        except Exception as e:
            self.logger.error(f"Layer coordination failed: {e}")
            raise
    async def _process_messages(self) -> None:
        """Process incoming messages continuously"""
        while True:
            try:
                message = await self.message_queue.get()
                
                handler = self.message_handlers.get(message['type'])
                if handler:
                    await handler(message)
                else:
                    self.logger.warning(f"No handler for message type: {message['type']}")
                
                # Update metrics
                self.communication_metrics['processed_messages'][message['type']] += 1

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Message processing failed: {e}")
                self.communication_metrics['processing_errors'] += 1
                
    async def send_to_next_layer(self, message: Dict[str, Any]) -> None:
        """Boss agent specialized implementation for sending to next layer"""
        if not self.initialized:
            raise RuntimeError(f"Boss agent {self.name} not initialized")
            
        try:
            # Boss agent can send to any layer
            target_layer = message.get('target_layer', 1)  # Default to first layer
            
            enhanced_message = {
                'sender': self.name,
                'content': message,
                'timestamp': datetime.now().isoformat(),
                'metadata': {
                    'source': 'boss_agent',
                    'message_type': 'instruction',
                    'priority': message.get('priority', 'normal'),
                    'task_id': message.get('task_id', str(uuid.uuid4()))
                }
            }
            
            # Track task state
            self.active_tasks[enhanced_message['metadata']['task_id']] = {
                'status': 'dispatched',
                'target_layer': target_layer,
                'timestamp': datetime.now().isoformat()
            }
            
            await self.communication_system.send_to_layer(target_layer, enhanced_message)
            self.metrics['tasks_dispatched'][f'layer_{target_layer}'] += 1
            
        except Exception as e:
            self.logger.error(f"Boss agent failed to send message: {e}")
            raise

    async def receive_from_previous_layer(self) -> Optional[Dict[str, Any]]:
        """Boss agent specialized implementation for receiving from previous layer"""
        if not self.initialized:
            raise RuntimeError(f"Boss agent {self.name} not initialized")
            
        try:
            # Boss agent can receive from any layer
            messages = []
            for layer_id in self.layer_agents.keys():
                message = await self.communication_system.receive_from_layer(layer_id)
                if message:
                    messages.append(message)
                    
                    # Update task state if applicable
                    task_id = message.get('metadata', {}).get('task_id')
                    if task_id in self.active_tasks:
                        self.active_tasks[task_id].update({
                            'status': 'response_received',
                            'response_time': datetime.now().isoformat()
                        })
                    
                    self.metrics['responses_received'][f'layer_{layer_id}'] += 1
            
            return messages if messages else None
            
        except Exception as e:
            self.logger.error(f"Boss agent failed to receive messages: {e}")
            raise

    async def broadcast_to_all_layers(self, message: Dict[str, Any]) -> None:
        """Broadcast message to all layers"""
        try:
            broadcast_id = str(uuid.uuid4())
            for layer_id in self.layer_agents.keys():
                enhanced_message = {
                    'sender': self.name,
                    'content': message,
                    'timestamp': datetime.now().isoformat(),
                    'metadata': {
                        'broadcast_id': broadcast_id,
                        'message_type': 'broadcast',
                        'target_layer': layer_id
                    }
                }
                await self.communication_system.send_to_layer(layer_id, enhanced_message)
                
            self.metrics['broadcasts_sent'] += 1
            
        except Exception as e:
            self.logger.error(f"Broadcast failed: {e}")
            raise
    async def _handle_task_assignment(self, message: Dict[str, Any]) -> None:
        """Handle task assignment messages"""
        try:
            task_id = message.get('task_id')
            if not task_id:
                raise ValueError("Missing task_id in task assignment message")

            # Process task assignment
            result = await self._process_task_assignment(message)
            
            # Send response
            await self.communication_system.send_message(
                self.name,
                message['sender'],
                {
                    'type': 'task_response',
                    'task_id': task_id,
                    'result': result
                }
            )

        except Exception as e:
            self.logger.error(f"Task assignment handling failed: {e}")
            await self._handle_message_error(message, e)

    async def _handle_result_collection(self, message: Dict[str, Any]) -> None:
        """Handle result collection messages"""
        try:
            result_id = message.get('result_id')
            if not result_id:
                raise ValueError("Missing result_id in result collection message")

            # Process result
            processed_result = await self._process_result(message)
            
            # Store result
            await self._store_result(result_id, processed_result)
            
            # Send acknowledgment
            await self.communication_system.send_message(
                self.name,
                message['sender'],
                {
                    'type': 'result_ack',
                    'result_id': result_id,
                    'status': 'success'
                }
            )

        except Exception as e:
            self.logger.error(f"Result collection handling failed: {e}")
            await self._handle_message_error(message, e)

    async def _handle_error_notification(self, message: Dict[str, Any]) -> None:
        """Handle error notification messages"""
        try:
            error_id = message.get('error_id')
            if not error_id:
                raise ValueError("Missing error_id in error notification message")

            # Process error
            await self._process_error_notification(message)
            
            # Update error metrics
            self.communication_metrics['error_notifications'] += 1

        except Exception as e:
            self.logger.error(f"Error notification handling failed: {e}")
            self.communication_metrics['handler_errors'] += 1

    async def _handle_system_message(self, message: Dict[str, Any]) -> None:
        """Handle system-level messages"""
        try:
            message_type = message.get('system_type')
            if not message_type:
                raise ValueError("Missing system_type in system message")

            system_handlers = {
                'status_update': self._handle_status_update,
                'configuration_change': self._handle_configuration_change,
                'system_command': self._handle_system_command
            }

            handler = system_handlers.get(message_type)
            if handler:
                await handler(message)
            else:
                self.logger.warning(f"No handler for system message type: {message_type}")

        except Exception as e:
            self.logger.error(f"System message handling failed: {e}")
            await self._handle_message_error(message, e)

    async def _verify_communication(self) -> bool:
        """Verify communication system functionality"""
        try:
            if not self.communication_system:
                return False

            # Verify core channels
            for channel in ['system', 'coordination', 'task_assignment']:
                if not await self.communication_system.verify_channel(channel):
                    return False

            # Test message sending
            test_message = {
                'type': 'test',
                'content': 'verification'
            }
            
            await self.communication_system.send_message(
                self.name,
                'system',
                test_message
            )

            return True

        except Exception as e:
            self.logger.error(f"Communication verification failed: {e}")
            return False

    async def send_message(self, target: str, message_type: str, content: Any) -> None:
        """Send message through communication system"""
        try:
            if not self.communication_system:
                raise RuntimeError("Communication system not initialized")

            message = {
                'type': message_type,
                'content': content,
                'sender': self.name,
                'timestamp': datetime.now().isoformat()
            }

            await self.communication_system.send_message(
                self.name,
                target,
                message
            )

            # Update metrics
            self.communication_metrics['sent_messages'][message_type] += 1

        except Exception as e:
            self.logger.error(f"Message sending failed: {e}")
            raise

    async def cleanup_communication(self) -> None:
        """Cleanup communication system resources"""
        try:
            if self.message_processor and not self.message_processor.done():
                self.message_processor.cancel()
                await self.message_processor

            if self.communication_system:
                await self.communication_system.cleanup()
                self.communication_system = None

            self.channel_states.clear()
            self.communication_metrics.clear()
            
            self.logger.info("Communication system cleaned up successfully")

        except Exception as e:
            self.logger.error(f"Communication cleanup failed: {e}")
            raise
    async def _optimize_plan(self, plan: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Optimize execution plan based on context and constraints"""
        try:
            # Check cache
            cache_key = self._generate_cache_key(plan)
            if cache_key in self.planning_cache:
                return self.planning_cache[cache_key]

            # Perform optimization analysis
            optimization_context = await self._create_optimization_context(plan, context)
            
            # Generate optimization strategies
            strategies = await self._generate_optimization_strategies(optimization_context)
            
            # Evaluate and select best strategy
            selected_strategy = await self._select_optimization_strategy(strategies)
            
            # Apply optimization
            optimized_plan = await self._apply_optimization(plan, selected_strategy)
            
            # Validate optimized plan
            if not await self._validate_plan(optimized_plan):
                raise ValueError("Optimized plan failed validation")
            
            # Cache result
            self.planning_cache[cache_key] = optimized_plan
            
            # Update metrics
            self.metrics['optimizations_performed'][selected_strategy['type']] += 1
            
            return optimized_plan
            
        except Exception as e:
            self.logger.error(f"Plan optimization failed: {e}")
            raise

    async def _create_optimization_context(self, plan: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create context for plan optimization"""
        try:
            return {
                'plan': plan,
                'user_context': context or {},
                'system_state': await self._get_system_state(),
                'resource_metrics': await self._get_resource_metrics(),
                'performance_history': self.performance_tracker,
                'optimization_history': self.metrics['optimizations_performed']
            }
        except Exception as e:
            self.logger.error(f"Optimization context creation failed: {e}")
            raise

    async def _generate_optimization_strategies(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate potential optimization strategies"""
        try:
            strategies = []
            
            # Resource optimization
            if await self._can_optimize_resources(context):
                strategies.append({
                    'type': 'resource_optimization',
                    'priority': 1,
                    'impact': await self._estimate_resource_optimization_impact(context)
                })
            
            # Performance optimization
            if await self._can_optimize_performance(context):
                strategies.append({
                    'type': 'performance_optimization',
                    'priority': 2,
                    'impact': await self._estimate_performance_optimization_impact(context)
                })
            
            # Parallel execution optimization
            if await self._can_optimize_parallelism(context):
                strategies.append({
                    'type': 'parallel_optimization',
                    'priority': 3,
                    'impact': await self._estimate_parallel_optimization_impact(context)
                })
            
            return strategies
            
        except Exception as e:
            self.logger.error(f"Strategy generation failed: {e}")
            raise

    async def _select_optimization_strategy(self, strategies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Select best optimization strategy"""
        try:
            if not strategies:
                raise ValueError("No optimization strategies available")
            
            # Score strategies
            scored_strategies = []
            for strategy in strategies:
                score = await self._evaluate_strategy(strategy)
                scored_strategies.append((score, strategy))
            
            # Select highest scoring strategy
            return max(scored_strategies, key=lambda x: x[0])[1]
            
        except Exception as e:
            self.logger.error(f"Strategy selection failed: {e}")
            raise

    async def _apply_optimization(self, plan: Dict[str, Any], strategy: Dict[str, Any]) -> Dict[str, Any]:
        """Apply selected optimization strategy to plan"""
        try:
            optimizers = {
                'resource_optimization': self._optimize_resources,
                'performance_optimization': self._optimize_performance,
                'parallel_optimization': self._optimize_parallelism
            }
            
            optimizer = optimizers.get(strategy['type'])
            if not optimizer:
                raise ValueError(f"Unknown optimization type: {strategy['type']}")
            
            optimized_plan = await optimizer(plan)
            
            # Add optimization metadata
            optimized_plan['metadata'] = {
                **plan.get('metadata', {}),
                'optimization': {
                    'strategy': strategy['type'],
                    'timestamp': datetime.now().isoformat(),
                    'impact_estimate': strategy['impact']
                }
            }
            
            return optimized_plan
            
        except Exception as e:
            self.logger.error(f"Optimization application failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup boss agent resources"""
        try:
            # Cleanup active tasks
            while not self.task_queue.empty():
                await self.task_queue.get()
            
            # Clear caches
            self.cache.clear()
            self.planning_cache.clear()
            
            # Reset states
            self.state.clear()
            self.coordination_state.clear()
            
            # Clear metrics
            self.metrics.clear()
            self.performance_tracker.clear()
            
            # Reset flags
            self.initialized = False
            self._initializing = False
            
            self.logger.info(f"Boss agent {self.name} cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Boss agent cleanup failed: {e}")
            raise
            
            
            #
    async def _decompose_task(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Decompose task into subtasks with evidence requirements"""
        try:
            subtasks = []
            
            # Analyze task requirements
            requirements = await self._analyze_requirements(task)
            
            # Generate subtasks based on requirements
            for req in requirements:
                subtask = {
                    'id': str(uuid.uuid4()),
                    'type': req.get('type', 'default'),
                    'description': req.get('description', ''),
                    'requirements': req.get('requirements', []),
                    'evidence_required': req.get('evidence_required', []),
                    'dependencies': req.get('dependencies', [])
                }
                subtasks.append(subtask)
            
            return subtasks
            
        except Exception as e:
            self.logger.error(f"Task decomposition failed: {e}")
            raise

    async def _plan_evidence_collection(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Plan evidence collection strategy"""
        try:
            evidence_plan = {
                'required_evidence': [],
                'collection_strategy': {},
                'validation_rules': {}
            }
            
            # Identify required evidence
            for subtask in await self._decompose_task(task):
                for evidence_req in subtask['evidence_required']:
                    evidence_plan['required_evidence'].append({
                        'type': evidence_req['type'],
                        'description': evidence_req['description'],
                        'source': evidence_req.get('source', 'any'),
                        'validation_rules': evidence_req.get('validation_rules', {})
                    })
            
            # Define collection strategy
            evidence_plan['collection_strategy'] = {
                'parallel_collection': True,
                'max_retries': 3,
                'timeout': 30,
                'sources': ['knowledge_graph', 'external_apis', 'user_input']
            }
            
            # Define validation rules
            evidence_plan['validation_rules'] = {
                'completeness_threshold': 0.8,
                'freshness_threshold': 3600,  # 1 hour
                'consistency_check': True
            }
            
            return evidence_plan
            
        except Exception as e:
            self.logger.error(f"Evidence collection planning failed: {e}")
            raise

    async def _create_execution_plan(self, task: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create execution plan with proper error handling"""
        try:
            # Generate initial plan
            plan = await self._generate_initial_plan(task, context)
            
            # Validate plan
            if not await self._validate_plan(plan):
                plan = await self._replan(plan, "Initial plan validation failed")
            
            # Store plan in cache
            cache_key = self._generate_cache_key(task)
            self.planning_cache[cache_key] = plan
            
            return plan
            
        except Exception as e:
            self.logger.error(f"Execution plan creation failed: {e}")
            raise

    async def _validate_plan(self, plan: Dict[str, Any]) -> bool:
        """Validate execution plan"""
        try:
            # Check required components
            if not all(key in plan for key in ['steps', 'evidence_strategy', 'validation_rules']):
                return False
                
            # Validate each step
            for step in plan['steps']:
                if not await self._validate_step(step):
                    return False
                    
            # Validate evidence strategy
            if not await self._validate_evidence_strategy(plan['evidence_strategy']):
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Plan validation failed: {e}")
            return False

    
    async def _initialize_agent_management(self) -> None:
        """Initialize agent management capabilities"""
        try:
            # Initialize agent tracking
            self.active_agents = {}
            self.agent_states = {}
            
            # Initialize task queue
            self.task_queue = asyncio.Queue()
            
            # Initialize metrics
            self.metrics = defaultdict(Counter)
            
            self.logger.info("Agent management initialized successfully")
        except Exception as e:
            self.logger.error(f"Agent management initialization failed: {e}")
            raise

    async def _initialize_core_components(self) -> None:
        """Initialize core components with validation"""
        try:
            # Initialize evidence store
            self.evidence_store = EvidenceStore(self.config.evidence_store_config)
            await self.evidence_store.initialize()
            
            # Initialize planning system
            self.planning_system = PlanningSystem(self.config)
            await self.planning_system.initialize()
            
            # Initialize agent tracking
            self.active_agents = {}
            self.agent_states = {}
            
            # Initialize metrics tracking
            self.metrics = {
                'tasks_processed': Counter(),
                'coordination_events': Counter(),
                'recovery_attempts': Counter(),
                'processing_times': []
            }
            
            self.logger.info("Core components initialized successfully")
        except Exception as e:
            self.logger.error(f"Core component initialization failed: {e}")
            raise
    async def _initialize_layer_agents(self) -> None:
        """Initialize layer agents with proper error handling"""
        try:
            for layer_id in range(1, 5):  # Layers 1-4
                self.active_agents[layer_id] = []
                agent_configs = self._get_layer_configs(layer_id)
                
                for config in agent_configs:
                    agent = await self._create_layer_agent(config)
                    if agent:
                        self.active_agents[layer_id].append(agent)
                        await self._register_agent(agent)
                        
            self.logger.info(f"Initialized {sum(len(agents) for agents in self.active_agents.values())} layer agents")
        except Exception as e:
            self.logger.error(f"Layer agent initialization failed: {e}")
            raise

    async def _register_agent(self, agent: BaseAgent) -> None:
        """Register agent with proper validation"""
        try:
            layer_id = agent.model_info.get('layer_id')
            if layer_id is None:
                raise ValueError(f"Agent {agent.name} missing layer_id")
                
            self.agent_states[agent.name] = {
                'status': 'active',
                'layer': layer_id,
                'initialized_at': datetime.now().isoformat()
            }
            
            self.logger.info(f"Registered agent {agent.name} to layer {layer_id}")
        except Exception as e:
            self.logger.error(f"Agent registration failed: {e}")
            raise
    
    async def _initialize_rewoo_components(self) -> None:
        """Initialize REWOO components with validation"""
        try:
            self.rewoo_system = REWOOSystem(self.config)
            await self.rewoo_system.initialize()
            
            # Verify REWOO initialization
            if not await self._verify_rewoo_system():
                raise InitializationError("REWOO system verification failed")
                
            self.logger.info("REWOO components initialized successfully")
        except Exception as e:
            self.logger.error(f"REWOO component initialization failed: {e}")
            raise

    async def _initialize_layer_coordination(self) -> None:
        """Initialize layer coordination with enhanced validation"""
        try:
            self.layer_states = {
                layer_id: {
                    'initialized': False,
                    'agents': [],
                    'status': 'pending',
                    'metrics': defaultdict(Counter)
                }
                for layer_id in range(1, self.config.num_layers + 1)
            }
            
            # Initialize layers sequentially
            for layer_id in sorted(self.layer_agents.keys()):
                await self._initialize_layer(layer_id)
                
            # Verify layer initialization
            if not await self._verify_layer_initialization():
                raise InitializationError("Layer initialization verification failed")
                
            self.logger.info("Layer coordination initialized successfully")
        except Exception as e:
            self.logger.error(f"Layer coordination failed: {e}")
            raise

    async def _initialize_recovery_mechanisms(self) -> None:
        """Initialize recovery mechanisms"""
        try:
            self.recovery_strategies = {
                'agent_failure': self._handle_agent_failure,
                'layer_failure': self._handle_layer_failure,
                'communication_failure': self._handle_communication_failure,
                'task_failure': self._handle_task_failure
            }
            
            self.logger.info("Recovery mechanisms initialized successfully")
        except Exception as e:
            self.logger.error(f"Recovery mechanism initialization failed: {e}")
            raise

    async def coordinate_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Coordinate task execution with comprehensive monitoring"""
        task_id = f"task_{datetime.now().timestamp()}"
        
        try:
            # Create execution plan
            plan = await self.rewoo_system.create_plan(task)
            
            # Track task state
            self.active_tasks[task_id] = {
                'status': 'processing',
                'plan': plan,
                'start_time': datetime.now(),
                'layer_results': {}
            }
            
            # Process through layers
            results = await self._process_through_layers(task_id, plan)
            
            # Store execution evidence
            await self.evidence_store.store_evidence(
                task_id,
                {
                    'task': task,
                    'plan': plan,
                    'results': results,
                    'metrics': self._get_execution_metrics(task_id)
                },
                {'type': 'task_complete'}
            )
            
            return self._prepare_final_result(task_id, results)
            
        except Exception as e:
            self.logger.error(f"Task coordination failed: {str(e)}")
            await self._handle_coordination_error(task_id, e)
            raise

    async def _verify_initialization(self) -> bool:
        """Verify complete system initialization"""
        try:
            # Verify core components
            if not all([self.evidence_store, self.planning_system, self.rewoo_system]):
                return False
                
            # Verify layer initialization
            if not all(state['initialized'] for state in self.layer_states.values()):
                return False
                
            # Verify communication system
            if not await self._verify_communication():
                return False
                
            return True
        except Exception as e:
            self.logger.error(f"Initialization verification failed: {e}")
            return False

    async def _handle_agent_failure(self, agent_name: str, error: Exception) -> Dict[str, Any]:
        """Handle individual agent failures with recovery"""
        try:
            self.logger.warning(f"Agent failure detected: {agent_name}")
            
            # Get agent layer and configuration
            layer_id = self._get_agent_layer(agent_name)
            
            # Try to restart agent
            if await self._attempt_agent_restart(agent_name):
                return {'recovered': True, 'action': 'restart'}
            
            # Try to reassign tasks
            if await self._reassign_agent_tasks(agent_name, layer_id):
                return {'recovered': True, 'action': 'reassign'}
            
            # Fall back to degraded operation
            return await self._initiate_degraded_operation(layer_id)
            
        except Exception as e:
            self.logger.error(f"Agent failure recovery failed: {e}")
            raise

    def get_coordination_metrics(self) -> Dict[str, Any]:
        """Get comprehensive coordination metrics"""
        return {
            'tasks_processed': dict(self.metrics['tasks_processed']),
            'coordination_events': dict(self.metrics['coordination_events']),
            'recovery_attempts': dict(self.metrics['recovery_attempts']),
            'average_processing_time': statistics.mean(self.metrics['processing_times'])
                if self.metrics['processing_times'] else 0,
            'layer_metrics': {
                layer_id: dict(state['metrics'])
                for layer_id, state in self.layer_states.items()
            }
        }

    
    async def initiate_collaboration(self, task: Dict[str, Any]) -> None:
        """Coordinate task execution across layers with REWOO integration"""
        try:
            # Create execution plan using REWOO
            plan = await self.rewoo_system.create_plan(task)
            
            # Process through layers with monitoring
            results = {}
            for layer_id in sorted(self.layer_agents.keys()):
                layer_result = await self._execute_layer(
                    layer_id,
                    plan,
                    results.get(layer_id - 1) if layer_id > 0 else None
                )
                results[layer_id] = layer_result
                
                # Store evidence
                await self.evidence_store.store_evidence(
                    f"layer_{layer_id}_result",
                    layer_result,
                    {'layer': layer_id, 'task': task}
                )
            
            return results
            
        except Exception as e:
            self.logger.error(f"Collaboration failed: {e}")
            raise
    async def _initialize_agent_coordination(self) -> None:
        """Initialize agent coordination with proper error handling"""
        try:
            if not self.communication_system:
                raise ConfigurationError("Communication system not initialized")
            
            # Register agents with communication system
            for layer_id, agents in self.layer_agents.items():
                for agent in agents:
                    channels = self._get_agent_channels(agent, layer_id)
                    await self.communication_system.register_agent(
                        agent.name,
                        channels
                    )
                
            self.logger.info("Agent coordination initialized successfully")
        except Exception as e:
            self.logger.error(f"Agent coordination initialization failed: {e}")
            raise
        
    def _get_agent_channels(self, agent: 'BaseAgent', layer_id: int) -> List[str]:
        """Get appropriate channels for an agent based on its layer and type."""
        channels = ['task_assignment', 'agent_communication']
    
        # Add layer-specific channels
        channels.append(f'layer_{layer_id}')
    
        # Add special channels for boss agent
        if isinstance(agent, BossAgent):
            channels.extend(['system', 'coordination'])
    
        return channels
    
    

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task with comprehensive monitoring"""
        if not self.initialized:
            raise RuntimeError(f"Boss agent {self.name} not initialized")
            
        task_id = f"task_{datetime.now().timestamp()}"
        
        try:
            # Create execution plan
            plan = await self.rewoo_system.create_plan(task)
            
            # Process through layers
            results = await self._process_through_layers(task_id, plan)
            
            # Store execution evidence
            await self.evidence_store.store_evidence(
                task_id,
                {
                    'task': task,
                    'plan': plan,
                    'results': results,
                    'metadata': self._generate_execution_metadata(task_id)
                },
                {'type': 'task_complete'}
            )
            
            return {
                'task_id': task_id,
                'results': results,
                'metadata': self._generate_execution_metadata(task_id)
            }
            
        except Exception as e:
            self.logger.error(f"Task processing failed: {str(e)}")
            await self._handle_task_error(task_id, e)
            raise
        
    async def coordinate_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Coordinate task execution with comprehensive monitoring"""
        task_id = f"task_{datetime.now().timestamp()}"
        
        try:
            # Create execution plan
            plan = await self.rewoo_system.create_plan(task)
            
            # Track task state
            self.active_tasks[task_id] = {
                'status': 'processing',
                'plan': plan,
                'start_time': datetime.now(),
                'layer_results': {}
            }
            
            # Process through layers with monitoring
            results = await self._process_through_layers(task_id, plan)
            
            # Store execution evidence
            await self.evidence_store.store_evidence(
                task_id,
                {
                    'task': task,
                    'plan': plan,
                    'results': results,
                    'metrics': self._get_execution_metrics(task_id)
                },
                {'type': 'task_complete'}
            )
            
            return self._prepare_final_result(task_id, results)
            
        except Exception as e:
            self.logger.error(f"Task coordination failed: {str(e)}")
            await self._handle_coordination_error(task_id, e)
            raise
    async def _verify_rewoo_system(self) -> bool:
        """Verify REWOO system initialization and functionality"""
        try:
            if not self.rewoo_system:
                self.logger.error("REWOO system not initialized")
                return False
            
            if not self.rewoo_system.initialized:
                self.logger.error("REWOO system not marked as initialized")
                return False
            
            # Verify required components
            required_components = ['evidence_store', 'planning_system']
            for component in required_components:
                if not hasattr(self.rewoo_system, component):
                    self.logger.error(f"REWOO system missing {component}")
                    return False
                
            return True
        except Exception as e:
            self.logger.error(f"REWOO system verification failed: {e}")
            return False
    async def _process_through_layers(self, task_id: str, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Process task through agent layers with enhanced monitoring and recovery."""
        results = {}
        current_input = plan
        
        try:
            for layer_id in sorted(self.layer_agents.keys()):
                # Execute layer with monitoring
                layer_result = await self._execute_layer_with_monitoring(
                    layer_id,
                    current_input,
                    task_id
                )
                
                # Validate layer result
                if not await self._validate_layer_result(layer_id, layer_result):
                    # Attempt recovery or reprocessing
                    layer_result = await self._recover_layer_execution(
                        layer_id,
                        current_input,
                        task_id
                    )
                
                # Store layer result
                results[layer_id] = layer_result
                current_input = self._prepare_next_layer_input(layer_result)
                
                # Update task state
                self.active_tasks[task_id]['layer_results'][layer_id] = layer_result
                
            return results
            
        except Exception as e:
            self.logger.error(f"Layer processing failed: {str(e)}")
            raise

    async def _execute_layer_with_monitoring(
        self,
        layer_id: int,
        input_data: Dict[str, Any],
        task_id: str
    ) -> Dict[str, Any]:
        """Execute layer with comprehensive monitoring."""
        start_time = time.time()
        
        try:
            # Get layer agents
            layer_agents = self.active_agents[layer_id]
            if not layer_agents:
                raise ValueError(f"No agents found for layer {layer_id}")

            # Execute agents in parallel
            tasks = [
                self._execute_agent_with_monitoring(
                    agent,
                    input_data,
                    task_id,
                    layer_id
                )
                for agent in layer_agents
            ]
            
            results = await asyncio.gather(*tasks)
            
            # Update metrics
            execution_time = time.time() - start_time
            self._update_layer_metrics(layer_id, execution_time, len(results))
            
            return self._aggregate_layer_results(results, layer_id)
            
        except Exception as e:
            self.logger.error(f"Layer execution failed: {str(e)}")
            raise

    async def _execute_agent_with_monitoring(
        self,
        agent: BaseAgent,
        input_data: Dict[str, Any],
        task_id: str,
        layer_id: int
    ) -> Dict[str, Any]:
        """Execute single agent with monitoring."""
        start_time = time.time()
        
        try:
            # Update agent state
            self.layer_states[layer_id][agent.name]['status'] = 'processing'
            
            # Execute agent
            result = await agent.process_task(input_data)
            
            # Update metrics
            execution_time = time.time() - start_time
            self._update_agent_metrics(agent.name, execution_time)
            
            # Update agent state
            self.layer_states[layer_id][agent.name].update({
                'status': 'active',
                'last_execution': datetime.now().isoformat(),
                'last_execution_time': execution_time
            })
            
            return result
            
        except Exception as e:
            self.logger.error(f"Agent execution failed: {str(e)}")
            self.layer_states[layer_id][agent.name]['status'] = 'error'
            raise

    async def _validate_layer_result(self, layer_id: int, result: Dict[str, Any]) -> bool:
        """Validate layer execution results."""
        try:
            # Basic validation
            if not result:
                return False
                
            # Validate required fields
            required_fields = ['results', 'metadata']
            if not all(field in result for field in required_fields):
                return False
                
            # Validate results
            if not result['results']:
                return False
                
            # Validate metadata
            if not self._validate_metadata(result['metadata']):
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Layer result validation failed: {str(e)}")
            return False

    async def _recover_layer_execution(
        self,
        layer_id: int,
        input_data: Dict[str, Any],
        task_id: str
    ) -> Dict[str, Any]:
        """Attempt to recover from layer execution failure."""
        try:
            # Log recovery attempt
            self.logger.info(f"Attempting to recover layer {layer_id} execution")
            
            # Get recovery strategy
            strategy = await self._get_recovery_strategy(layer_id, task_id)
            
            # Execute recovery
            if strategy == 'retry':
                return await self._retry_layer_execution(layer_id, input_data, task_id)
            elif strategy == 'fallback':
                return await self._execute_fallback_layer(layer_id, input_data, task_id)
            else:
                raise ValueError(f"Unknown recovery strategy: {strategy}")
                
        except Exception as e:
            self.logger.error(f"Layer recovery failed: {str(e)}")
            raise

    def _prepare_next_layer_input(self, layer_result: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare input for next layer."""
        try:
            return {
                'previous_results': layer_result['results'],
                'metadata': layer_result['metadata'],
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Input preparation failed: {str(e)}")
            raise

    def _prepare_final_result(self, task_id: str, results: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare final execution result."""
        try:
            return {
                'task_id': task_id,
                'results': results,
                'metadata': {
                    'completion_time': datetime.now().isoformat(),
                    'execution_time': (
                        datetime.now() - self.active_tasks[task_id]['start_time']
                    ).total_seconds(),
                    'metrics': self._get_execution_metrics(task_id)
                }
            }
        except Exception as e:
            self.logger.error(f"Result preparation failed: {str(e)}")
            raise

    async def _handle_coordination_error(self, task_id: str, error: Exception) -> None:
        """Handle task coordination errors."""
        try:
            # Update task state
            if task_id in self.active_tasks:
                self.active_tasks[task_id].update({
                    'status': 'failed',
                    'error': str(error),
                    'error_time': datetime.now().isoformat()
                })
            
            # Store error evidence
            await self.evidence_store.store_evidence(
                f"error_{task_id}",
                {
                    'error': str(error),
                    'traceback': error.__traceback__,
                    'timestamp': datetime.now().isoformat()
                },
                {'type': 'error'}
            )
            
            # Update metrics
            self.metrics['coordination_errors'] += 1
            
        except Exception as e:
            self.logger.error(f"Error handling failed: {str(e)}")
            raise

    def _update_layer_metrics(self, layer_id: int, execution_time: float, result_count: int) -> None:
        """Update layer execution metrics."""
        try:
            self.metrics['layer_execution_time'][layer_id].append(execution_time)
            self.metrics['layer_result_count'][layer_id].append(result_count)
        except Exception as e:
            self.logger.error(f"Metrics update failed: {str(e)}")

    def _update_agent_metrics(self, agent_name: str, execution_time: float) -> None:
        """Update agent execution metrics."""
        try:
            self.metrics['agent_execution_time'][agent_name].append(execution_time)
            self.metrics['agent_executions'][agent_name] += 1
        except Exception as e:
            self.logger.error(f"Metrics update failed: {str(e)}")

    def _get_execution_metrics(self, task_id: str) -> Dict[str, Any]:
        """Get comprehensive execution metrics."""
        try:
            task_data = self.active_tasks[task_id]
            return {
                'execution_time': (datetime.now() - task_data['start_time']).total_seconds(),
                'layer_metrics': {
                    layer_id: {
                        'execution_time': statistics.mean(
                            self.metrics['layer_execution_time'][layer_id]
                        ),
                        'result_count': statistics.mean(
                            self.metrics['layer_result_count'][layer_id]
                        )
                    }
                    for layer_id in self.layer_agents.keys()
                },
                'agent_metrics': {
                    agent_name: {
                        'execution_time': statistics.mean(
                            self.metrics['agent_execution_time'][agent_name]
                        ),
                        'execution_count': self.metrics['agent_executions'][agent_name]
                    }
                    for agent_name in self.metrics['agent_executions'].keys()
                }
            }
        except Exception as e:
            self.logger.error(f"Metrics retrieval failed: {str(e)}")
            return {}
            
            


    async def coordinate_layers(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Coordinate task execution across layers with enhanced monitoring."""
        task_id = f"task_{datetime.now().timestamp()}"
        
        try:
            # Create execution plan using REWOO
            plan = await self.rewoo_system.create_plan(task)
            
            # Track task state
            self.active_tasks[task_id] = {
                'status': 'processing',
                'plan': plan,
                'start_time': datetime.now(),
                'layer_results': {},
                'metrics': defaultdict(list)
            }
            
            # Process through layers with monitoring
            results = await self._process_through_layers(task_id, plan)
            
            # Store execution evidence
            await self.evidence_store.store_evidence(
                task_id,
                {
                    'task': task,
                    'plan': plan,
                    'results': results,
                    'metrics': self._get_execution_metrics(task_id)
                },
                {'type': 'task_complete'}
            )
            
            return self._prepare_final_result(task_id, results)
            
        except Exception as e:
            self.logger.error(f"Task coordination failed: {str(e)}")
            await self._handle_coordination_error(task_id, e)
            raise

    async def _process_through_layers(self, task_id: str, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Process task through layers with comprehensive monitoring and recovery"""
        results = {}
        current_layer = 0
        
        try:
            for layer_id in sorted(self.layer_agents.keys()):
                current_layer = layer_id
                
                # Execute layer with monitoring and recovery
                layer_result = await self._execute_layer_with_recovery(
                    layer_id,
                    task_id,
                    plan,
                    results.get(layer_id - 1) if layer_id > 0 else None
                )
                
                # Validate layer result
                if not await self._validate_layer_result(layer_id, layer_result):
                    raise ValidationError(f"Layer {layer_id} validation failed")
                
                results[layer_id] = layer_result
                
                # Update task state
                await self._update_task_state(task_id, layer_id, layer_result)
                
            return self._prepare_final_result(results)
            
        except Exception as e:
            self.logger.error(f"Layer {current_layer} processing failed: {str(e)}")
            await self._handle_layer_failure(current_layer, task_id, e)
            raise

    async def _execute_layer_with_recovery(
        self,
        layer_id: int,
        task_id: str,
        plan: Dict[str, Any],
        previous_result: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute layer with comprehensive error handling and recovery"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Get active agents for layer
                agents = self.layer_agents.get(layer_id, [])
                if not agents:
                    raise ValueError(f"No agents available for layer {layer_id}")
                
                # Execute agents in parallel with timeout
                tasks = [
                    self._execute_agent_with_monitoring(
                        agent,
                        task_id,
                        plan,
                        previous_result
                    ) for agent in agents
                ]
                
                results = await asyncio.gather(*tasks)
                
                # Aggregate results
                aggregated_result = await self._aggregate_layer_results(
                    layer_id,
                    results,
                    plan
                )
                
                return aggregated_result
                
            except Exception as e:
                retry_count += 1
                if retry_count == max_retries:
                    raise
                
                self.logger.warning(
                    f"Layer {layer_id} execution failed (attempt {retry_count}): {str(e)}"
                )
                await asyncio.sleep(2 ** retry_count)  # Exponential backoff

    async def _execute_agent_with_monitoring(
        self,
        agent: BaseAgent,
        task_id: str,
        plan: Dict[str, Any],
        previous_result: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute single agent with comprehensive monitoring"""
        start_time = time.time()
        
        try:
            # Update agent state
            self.agent_states[agent.name] = {
                'status': 'processing',
                'task_id': task_id,
                'start_time': start_time
            }
            
            # Prepare agent input
            agent_input = self._prepare_agent_input(
                plan,
                previous_result,
                agent.name
            )
            
            # Execute agent with timeout
            result = await asyncio.wait_for(
                agent.process_task(agent_input),
                timeout=300  # 5 minutes timeout
            )
            
            # Update metrics
            execution_time = time.time() - start_time
            self._update_agent_metrics(agent.name, execution_time)
            
            # Update agent state
            self.agent_states[agent.name].update({
                'status': 'completed',
                'end_time': time.time(),
                'execution_time': execution_time
            })
            
            return result
            
        except Exception as e:
            self.logger.error(f"Agent {agent.name} execution failed: {str(e)}")
            self.agent_states[agent.name]['status'] = 'failed'
            raise

    async def _validate_layer_result(self, layer_id: int, result: Dict[str, Any]) -> bool:
        """Validate layer execution results with comprehensive checks"""
        try:
            # Basic validation
            if not result:
                self.logger.error(f"Layer {layer_id} returned empty result")
                return False
            
            # Validate required fields
            required_fields = ['outputs', 'metadata', 'evidence']
            if not all(field in result for field in required_fields):
                self.logger.error(
                    f"Layer {layer_id} result missing required fields: "
                    f"{set(required_fields) - set(result.keys())}"
                )
                return False
            
            # Validate evidence
            if not await self._validate_evidence(result['evidence']):
                self.logger.error(f"Layer {layer_id} evidence validation failed")
                return False
            
            # Validate outputs format and content
            if not self._validate_outputs(result['outputs']):
                self.logger.error(f"Layer {layer_id} outputs validation failed")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Layer {layer_id} result validation failed: {str(e)}")
            return False
    
    async def _generate_replan(self, original_plan: Dict[str, Any], reason: str) -> Dict[str, Any]:
        """Generate new plan when original plan fails"""
        try:
            # Log replanning attempt
            self.logger.info(f"Generating replan. Reason: {reason}")
            self.metrics['replans_triggered'][reason] += 1
            
            # Create replanning context
            replan_context = {
                'original_plan': original_plan,
                'failure_reason': reason,
                'attempt': len(self.planning_history) + 1,
                'timestamp': datetime.now().isoformat()
            }
            
            # Store original plan in history
            self.planning_history.append({
                'plan': original_plan,
                'status': 'failed',
                'reason': reason,
                'timestamp': datetime.now().isoformat()
            })
            
            # Generate new plan with enhanced context
            new_plan = await self._create_enhanced_plan(
                original_plan['task'],
                replan_context
            )
            
            # Validate new plan
            if not await self._validate_plan(new_plan):
                raise ValueError("Generated replan failed validation")
            
            return new_plan
            
        except Exception as e:
            self.logger.error(f"Replan generation failed: {e}")
            raise

    async def _create_enhanced_plan(self, task: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Create enhanced plan with additional context and validation"""
        try:
            # Analyze previous failures
            failure_analysis = await self._analyze_failure(context)
            
            # Generate alternative approaches
            alternatives = await self._generate_alternatives(task, failure_analysis)
            
            # Select best alternative
            selected_approach = await self._select_best_alternative(alternatives)
            
            # Create new plan
            new_plan = {
                'task': task,
                'approach': selected_approach,
                'steps': await self._generate_enhanced_steps(task, selected_approach),
                'evidence_strategy': await self._create_enhanced_evidence_strategy(task, context),
                'validation_rules': await self._create_enhanced_validation_rules(context),
                'metadata': {
                    'created_at': datetime.now().isoformat(),
                    'context': context,
                    'failure_analysis': failure_analysis
                }
            }
            
            return new_plan
            
        except Exception as e:
            self.logger.error(f"Enhanced plan creation failed: {e}")
            raise

    async def _analyze_failure(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze plan failure for better replanning"""
        try:
            original_plan = context['original_plan']
            failure_reason = context['failure_reason']
            
            analysis = {
                'failure_type': self._categorize_failure(failure_reason),
                'failed_components': await self._identify_failed_components(original_plan),
                'impact_analysis': await self._analyze_failure_impact(original_plan),
                'recovery_suggestions': await self._generate_recovery_suggestions(original_plan)
            }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Failure analysis failed: {e}")
            raise

    async def _generate_alternatives(self, task: Dict[str, Any], failure_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate alternative approaches based on failure analysis"""
        try:
            alternatives = []
            
            # Generate different strategic approaches
            strategies = [
                self._generate_conservative_approach,
                self._generate_aggressive_approach,
                self._generate_balanced_approach
            ]
            
            # Create alternatives using different strategies
            for strategy in strategies:
                alternative = await strategy(task, failure_analysis)
                if alternative:
                    alternatives.append(alternative)
            
            return alternatives
            
        except Exception as e:
            self.logger.error(f"Alternative generation failed: {e}")
            raise

    async def _select_best_alternative(self, alternatives: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Select best alternative based on multiple criteria"""
        try:
            if not alternatives:
                raise ValueError("No alternatives available")
                
            # Score each alternative
            scored_alternatives = []
            for alt in alternatives:
                score = await self._evaluate_alternative(alt)
                scored_alternatives.append((score, alt))
            
            # Select highest scoring alternative
            best_alternative = max(scored_alternatives, key=lambda x: x[0])[1]
            
            return best_alternative
            
        except Exception as e:
            self.logger.error(f"Alternative selection failed: {e}")
            raise

    async def _evaluate_alternative(self, alternative: Dict[str, Any]) -> float:
        """Evaluate alternative approach based on multiple criteria"""
        try:
            criteria = {
                'feasibility': self._evaluate_feasibility,
                'reliability': self._evaluate_reliability,
                'efficiency': self._evaluate_efficiency,
                'risk_level': self._evaluate_risk
            }
            
            scores = {}
            for criterion, evaluator in criteria.items():
                scores[criterion] = await evaluator(alternative)
            
            # Calculate weighted score
            weights = {
                'feasibility': 0.3,
                'reliability': 0.3,
                'efficiency': 0.2,
                'risk_level': 0.2
            }
            
            final_score = sum(scores[k] * weights[k] for k in scores)
            
            return final_score
            
        except Exception as e:
            self.logger.error(f"Alternative evaluation failed: {e}")
            return 0.0

    def _categorize_failure(self, failure_reason: str) -> str:
        """Categorize failure type for better recovery"""
        failure_categories = {
            'validation': ['invalid', 'validation failed', 'inconsistent'],
            'resource': ['timeout', 'memory', 'cpu'],
            'data': ['missing data', 'corrupt data', 'invalid format'],
            'logic': ['logic error', 'algorithm failed', 'calculation error']
        }
        
        for category, keywords in failure_categories.items():
            if any(keyword in failure_reason.lower() for keyword in keywords):
                return category
                
        return 'unknown'

    async def _identify_failed_components(self, plan: Dict[str, Any]) -> List[str]:
        """Identify failed components in the plan"""
        failed_components = []
        
        try:
            # Check each plan component
            for step in plan.get('steps', []):
                if not await self._validate_step(step):
                    failed_components.append(f"step_{step.get('id')}")
                    
            if not await self._validate_evidence_strategy(plan.get('evidence_strategy', {})):
                failed_components.append('evidence_strategy')
                
            if not await self._validate_validation_rules(plan.get('validation_rules', {})):
                failed_components.append('validation_rules')
                
            return failed_components
            
        except Exception as e:
            self.logger.error(f"Failed component identification failed: {e}")
            return ['unknown']
    async def _handle_layer_failure(self, layer_id: int, task_id: str, error: Exception) -> None:
        """Handle layer execution failure with recovery attempts"""
        try:
            # Log failure
            self.logger.error(f"Layer {layer_id} failed for task {task_id}: {str(error)}")
            
            # Store failure evidence
            await self.evidence_store.store_evidence(
                f"error_{task_id}_layer_{layer_id}",
                {
                    'error': str(error),
                    'layer_id': layer_id,
                    'task_id': task_id,
                    'timestamp': datetime.now().isoformat()
                },
                {'type': 'layer_failure'}
            )
            
            # Update metrics
            self.metrics['layer_failures'][layer_id] += 1
            
            # Attempt recovery
            recovery_successful = await self._attempt_layer_recovery(
                layer_id,
                task_id
            )
            
            if not recovery_successful:
                self.logger.error(f"Layer {layer_id} recovery failed")
                raise
                
        except Exception as e:
            self.logger.error(f"Layer failure handling failed: {str(e)}")
            raise
    async def _handle_task_failure(self, task_id: str, error: Exception) -> Dict[str, Any]:
        """Handle task execution failures with comprehensive recovery"""
        try:
            self.logger.error(f"Task {task_id} failed: {str(error)}")
            
            # Record failure
            failure_record = {
                'task_id': task_id,
                'error': str(error),
                'timestamp': datetime.now().isoformat(),
                'type': 'task_failure'
            }
            
            # Update task state
            self.state['task_status'][task_id] = 'failed'
            
            # Update metrics
            self.metrics['errors']['task_failures'] += 1
            
            # Determine recovery strategy
            recovery_strategy = await self._determine_recovery_strategy(task_id, error)
            
            # Execute recovery
            recovery_result = await self._execute_recovery_strategy(
                task_id,
                recovery_strategy,
                failure_record
            )
            
            # Store failure and recovery information
            await self._store_failure_record(failure_record, recovery_result)
            
            return {
                'status': 'recovered' if recovery_result['success'] else 'failed',
                'original_error': str(error),
                'recovery_strategy': recovery_strategy,
                'recovery_result': recovery_result
            }
            
        except Exception as e:
            self.logger.error(f"Error handling failed for task {task_id}: {e}")
            raise

    async def _handle_plan_failure(self, plan_id: str, error: Exception) -> Dict[str, Any]:
        """Handle planning failures with recovery options"""
        try:
            self.logger.error(f"Plan {plan_id} failed: {str(error)}")
            
            # Record failure
            failure_record = {
                'plan_id': plan_id,
                'error': str(error),
                'timestamp': datetime.now().isoformat(),
                'type': 'plan_failure'
            }
            
            # Update metrics
            self.metrics['errors']['plan_failures'] += 1
            
            # Get original plan
            original_plan = await self._get_plan(plan_id)
            
            # Generate alternative plan
            alternative_plan = await self._generate_alternative_plan(
                original_plan,
                failure_record
            )
            
            # Validate alternative plan
            if await self._validate_plan(alternative_plan):
                recovery_result = {
                    'success': True,
                    'alternative_plan': alternative_plan
                }
            else:
                recovery_result = {
                    'success': False,
                    'reason': 'Alternative plan validation failed'
                }
            
            # Store failure and recovery information
            await self._store_failure_record(failure_record, recovery_result)
            
            return {
                'status': 'recovered' if recovery_result['success'] else 'failed',
                'original_error': str(error),
                'recovery_result': recovery_result
            }
            
        except Exception as e:
            self.logger.error(f"Plan failure handling failed: {e}")
            raise

    async def _handle_optimization_failure(self, optimization_id: str, error: Exception) -> Dict[str, Any]:
        """Handle optimization failures with fallback strategies"""
        try:
            self.logger.error(f"Optimization {optimization_id} failed: {str(error)}")
            
            # Record failure
            failure_record = {
                'optimization_id': optimization_id,
                'error': str(error),
                'timestamp': datetime.now().isoformat(),
                'type': 'optimization_failure'
            }
            
            # Update metrics
            self.metrics['errors']['optimization_failures'] += 1
            
            # Get original optimization context
            original_context = await self._get_optimization_context(optimization_id)
            
            # Try fallback optimization strategy
            fallback_result = await self._execute_fallback_optimization(
                original_context,
                failure_record
            )
            
            # Store failure and recovery information
            await self._store_failure_record(failure_record, fallback_result)
            
            return {
                'status': 'recovered' if fallback_result['success'] else 'failed',
                'original_error': str(error),
                'fallback_result': fallback_result
            }
            
        except Exception as e:
            self.logger.error(f"Optimization failure handling failed: {e}")
            raise

    async def _determine_recovery_strategy(self, task_id: str, error: Exception) -> Dict[str, Any]:
        """Determine appropriate recovery strategy based on error type and context"""
        try:
            error_type = type(error).__name__
            error_message = str(error)
            
            # Get task context
            task_context = await self._get_task_context(task_id)
            
            # Analyze error pattern
            error_pattern = self._analyze_error_pattern(error_type, error_message)
            
            # Get historical recovery data
            historical_data = await self._get_historical_recovery_data(error_pattern)
            
            # Select strategy based on analysis
            strategy = {
                'type': self._select_strategy_type(error_pattern, historical_data),
                'parameters': self._generate_strategy_parameters(error_pattern, task_context),
                'fallback_options': self._get_fallback_options(error_pattern)
            }
            
            return strategy
            
        except Exception as e:
            self.logger.error(f"Recovery strategy determination failed: {e}")
            raise

    async def _execute_recovery_strategy(self,
                                       task_id: str,
                                       strategy: Dict[str, Any],
                                       failure_record: Dict[str, Any]) -> Dict[str, Any]:
        """Execute selected recovery strategy"""
        try:
            strategy_type = strategy['type']
            
            recovery_actions = {
                'retry': self._execute_retry_strategy,
                'fallback': self._execute_fallback_strategy,
                'compensate': self._execute_compensation_strategy,
                'escalate': self._execute_escalation_strategy
            }
            
            if strategy_type not in recovery_actions:
                raise ValueError(f"Unknown recovery strategy type: {strategy_type}")
            
            recovery_action = recovery_actions[strategy_type]
            result = await recovery_action(task_id, strategy, failure_record)
            
            # Update recovery metrics
            self.metrics['recovery_attempts'][strategy_type] += 1
            if result['success']:
                self.metrics['successful_recoveries'][strategy_type] += 1
            
            return result
            
        except Exception as e:
            self.logger.error(f"Recovery strategy execution failed: {e}")
            raise

    async def _store_failure_record(self,
                                  failure_record: Dict[str, Any],
                                  recovery_result: Dict[str, Any]) -> None:
        """Store failure and recovery information"""
        try:
            record = {
                **failure_record,
                'recovery_result': recovery_result,
                'stored_at': datetime.now().isoformat()
            }
            
            # Store in cache for quick access
            cache_key = f"failure_{failure_record['type']}_{datetime.now().timestamp()}"
            self.cache[cache_key] = record
            
            # Update failure history
            self.failure_history.append(record)
            
            # Trim history if needed
            if len(self.failure_history) > self.max_history_size:
                self.failure_history = self.failure_history[-self.max_history_size:]
                
        except Exception as e:
            self.logger.error(f"Failure record storage failed: {e}")
            raise

    def _analyze_error_pattern(self, error_type: str, error_message: str) -> Dict[str, Any]:
        """Analyze error pattern for recovery strategy selection"""
        patterns = {
            'timeout': ['timeout', 'deadline exceeded', 'too slow'],
            'resource': ['memory', 'cpu', 'disk', 'resource'],
            'validation': ['invalid', 'validation failed', 'constraint'],
            'permission': ['permission', 'unauthorized', 'forbidden'],
            'data': ['data', 'format', 'schema', 'type']
        }
        
        matched_patterns = []
        for pattern_type, keywords in patterns.items():
            if any(keyword in error_message.lower() for keyword in keywords):
                matched_patterns.append(pattern_type)
                
        return {
            'error_type': error_type,
            'matched_patterns': matched_patterns,
            'severity': self._determine_error_severity(error_type, matched_patterns)
        }

    def _determine_error_severity(self, error_type: str, patterns: List[str]) -> str:
        """Determine error severity based on type and patterns"""
        critical_patterns = {'timeout', 'resource', 'permission'}
        high_patterns = {'validation', 'data'}
        
        if any(pattern in critical_patterns for pattern in patterns):
            return 'critical'
        elif any(pattern in high_patterns for pattern in patterns):
            return 'high'
        return 'medium'
    def _prepare_final_result(self, results: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
        """Prepare final execution result with comprehensive metadata"""
        try:
            return {
                'results': results,
                'metadata': {
                    'completion_time': datetime.now().isoformat(),
                    'layer_metrics': self._get_layer_metrics(),
                    'agent_metrics': self._get_agent_metrics(),
                    'execution_summary': self._generate_execution_summary()
                }
            }
        except Exception as e:
            self.logger.error(f"Final result preparation failed: {str(e)}")
            raise
            
            #
    
    async def _create_layer_agent(self, agent_name: str, layer_id: int) -> Optional[BaseAgent]:
        """Create and initialize a layer agent"""
        try:
            agent_config = self.config.get_agent_config(f"{agent_name}_{layer_id}")
            if not agent_config:
                self.logger.warning(f"Configuration not found for agent {agent_name}_{layer_id}")
                return None

            agent_class = self._get_agent_class(agent_config.type)
            agent = agent_class(
                name=f"{agent_name}_{layer_id}",
                model_info=agent_config.model_config.to_dict(),
                config=self.config  # Pass the config, no 'communication' variable
            )
            await agent.initialize()
            return agent
        except Exception as e:
            self.logger.error(f"Agent creation failed for {agent_name}_{layer_id}: {e}")
            return None
        
    # Update BossAgent registration method
    async def _register_agent(self, agent: BaseAgent) -> None:
        """Register an agent with proper layer validation"""
        try:
            # Get layer_id from model_info
            layer_id = agent.model_info.get('layer_id')
        
            # Enhanced validation
            if layer_id is None:
                raise ValueError(f"Agent {agent.name} missing layer_id in model_info")
        
            if not isinstance(layer_id, int) or layer_id < 0:
                raise ValueError(f"Invalid layer_id {layer_id} for agent {agent.name}")
            
            # Initialize layer list if needed
            if layer_id not in self.layer_agents:
                self.layer_agents[layer_id] = []
            
            # Add agent to appropriate layer
            self.layer_agents[layer_id].append(agent)
        
            # Update agent state
            self.agent_states[agent.name] = {
                'status': 'active',
                'layer': layer_id,
                'initialized_at': datetime.now().isoformat()
            }
        
            self.logger.info(f"Registered agent {agent.name} to layer {layer_id}")
        
        except Exception as e:
            self.logger.error(f"Agent registration failed: {e}")
            raise

    async def _initialize_layer_coordination(self) -> None:
        """Initialize layer coordination with enhanced error handling"""
        try:
            self.layer_agents = defaultdict(list)
            self.layer_states = defaultdict(dict)
            
            # Initialize each layer sequentially
            for layer_id in range(1, 5):  # Layers 1-4
                await self._initialize_layer(layer_id)
                
            self.logger.info(f"Initialized {sum(len(agents) for agents in self.layer_agents.values())} layer agents")
        except Exception as e:
            self.logger.error(f"Layer coordination initialization failed: {e}")
            raise

    async def _initialize_layer(self, layer_id: int) -> None:
        """Initialize specific layer with proper error handling"""
        try:
            agent_configs = [
                config for config in self.config.agent_configs
                if config.model_config.layer_id == layer_id
            ]
            
            for agent_config in agent_configs:
                agent = await self._create_layer_agent(agent_config)
                if agent:
                    await self._register_agent(agent)
                    self.layer_states[layer_id][agent.name] = {
                        'status': 'active',
                        'initialized_at': datetime.now().isoformat()
                    }
            
            self.logger.info(f"Layer {layer_id} initialized successfully")
        except Exception as e:
            self.logger.error(f"Layer {layer_id} initialization failed: {e}")
            raise

    async def _create_layer_agent(self, agent_config: 'AgentConfig') -> Optional[BaseAgent]:
        """Create layer agent with enhanced error handling"""
        try:
            agent_class = self._get_agent_class(agent_config.type)
            agent = agent_class(
                name=agent_config.name,
                model_info=agent_config.model_config.to_dict(),
                config=self.config
            )
            await agent.initialize()
            return agent
        except Exception as e:
            self.logger.error(f"Agent creation failed for {agent_config.name}: {e}")
            return None

    
    
    async def _initialize_model_configs(self) -> None:
        """Initialize model configurations with proper error handling."""
        try:
            if not self.api_settings:
                await self._initialize_api_settings()
            
            # Example model configurations:
            self.model_configs = {
                'BossAgent': ModelConfig(
                    model_name="claude-3-5-sonnet@20240620",
                    api_key=os.getenv("ANTHROPIC_API_KEY"),
                    layer_id=0,
                    pool_size=1,
                    metadata={
                        "role": "coordinator",
                        "capabilities": ["planning", "delegation", "synthesis"],
                        "type": "boss",
                        "client": "anthropic",
                        "model_family": "claude-3"
                    }
                ),
                'Layer1Agent': ModelConfig(
                    model_name="o1-2024-12-17",
                    api_key=os.getenv("OPENAI_API_KEY"),
                    layer_id=1,
                    pool_size=1,
                    context_window=200000,
                    max_tokens=100000,
                    metadata={
                        "role": "processor",
                        "capabilities": ["data_analysis", "transformation"],
                        "type": "worker",
                        "client": "together",
                        "model_family": "o1"
                    }
                ),
                'Layer2Agent': ModelConfig(
                    model_name="gemini-2.0-flash-exp",
                    api_key=None,  # We'll rely on vertex_ai_credentials
                    layer_id=2,
                    pool_size=1,
                    context_window=65536,
                    metadata={
                        "role": "reasoner",
                        "capabilities": ["complex_reasoning", "decision_making"],
                        "type": "worker",
                        "client": "vertex",
                        "model_family": "gemini"
                    }
                ),
                'Layer3Agent': ModelConfig(
                    model_name="mistral-large-2",
                    api_key=os.getenv("MISTRAL_API_KEY"),
                    layer_id=3,
                    pool_size=1,
                    context_window=32768,
                    metadata={
                        "role": "validator",
                        "capabilities": ["verification", "quality_assurance"],
                        "type": "worker",
                        "client": "mistral",
                        "model_family": "mistral"
                    }
                ),
                'Layer4Agent': ModelConfig(
                    model_name="o1-mini-2024-09-12",
                    api_key=os.getenv("OPENAI_API_KEY"),
                    layer_id=4,
                    pool_size=1,
                    context_window=128000,
                    max_tokens=65536,
                    metadata={
                        "role": "synthesizer",
                        "capabilities": ["integration", "summarization"],
                        "type": "worker",
                        "client": "together",
                        "model_family": "o1-mini"
                    }
                ),
            }
            self.logger.info("Model configurations initialized successfully")
        except Exception as e:
            self.logger.error(f"Model configuration initialization failed: {e}")
            raise
    
    async def _initialize_layer_agents(self) -> None:
        """Initialize layer agents with proper error handling"""
        try:
            for layer_id in range(1, 5):  # Layers 1-4
                layer_config = self.config.layer_configs.get(layer_id)
                if not layer_config:
                    continue
                
                self.layer_agents[layer_id] = []
                for agent_name in layer_config.agents:
                    agent = await self._create_layer_agent(agent_name, layer_id)
                    if agent:
                        self.layer_agents[layer_id].append(agent)
                        self.logger.info(f"Created agent {agent_name} for layer {layer_id}")

            self.logger.info(f"Initialized {sum(len(agents) for agents in self.layer_agents.values())} layer agents")
        except Exception as e:
            self.logger.error(f"Layer agent initialization failed: {e}")
            raise

    async def _create_layer_agent(self, agent_name: str, layer_id: int) -> Optional[BaseAgent]:
        """Create and initialize a layer agent"""
        try:
            # Get agent configuration
            model_config = self.config.get_model_config(agent_name)
            if not model_config:
                self.logger.warning(f"No model configuration found for agent {agent_name}")
                return None

            # Create agent instance
            agent_class = self._get_agent_class(agent_name)
            agent = agent_class(
                name=f"{agent_name}_{layer_id}",
                model_info=model_config.to_dict(),
                config=self.config
            )
            await agent.initialize()
            return agent
        except Exception as e:
            self.logger.error(f"Agent creation failed for {agent_name}: {e}")
            return None
    
    async def _initialize_evidence_store(self):
        """Initialize evidence store"""
        evidence_store = EvidenceStore()
        await evidence_store.initialize()
        return evidence_store

    async def _initialize_rewoo_system(self):
        """Initialize REWOO system"""
        rewoo_system = REWOOSystem(self.config)
        await rewoo_system.initialize()
        return rewoo_system

    async def _initialize_planning_system(self):
        """Initialize planning system (assume this is defined)"""
        planning_system = PlanningSystem(self.config)
        await planning_system.initialize()
        return planning_system

    
    def _get_agent_class(self, agent_type: str) -> Type[BaseAgent]:
        """Get the appropriate agent class based on type"""
        agent_classes = {
            'Layer1Agent': Layer1Agent,
            'Layer2Agent': Layer2Agent,
            'Layer3Agent': Layer3Agent,
            'Layer4Agent': Layer4Agent
        }

        if agent_type not in agent_classes:
            raise ValueError(f"Unknown agent type: {agent_type}")

        return agent_classes[agent_type]

    async def cleanup(self) -> None:
        """Cleanup boss agent resources"""
        try:
            # Cleanup core systems
            if self.rewoo_system:
                await self.rewoo_system.cleanup()
                
            if self.evidence_store:
                await self.evidence_store.cleanup()
                
            if self.planning_system:
                await self.planning_system.cleanup()
                
            # Cleanup layer agents
            if hasattr(self, 'active_agents'):
                for layer_agents in self.active_agents.values():
                    for agent in layer_agents:
                        await agent.cleanup()
            
            # Clear collections
            self.active_agents.clear()
            self.layer_agents.clear()
            self.metrics.clear()
            if hasattr(self, 'result_cache'):
                self.result_cache.clear()
            
            self.initialized = False
            self._initializing = False
            
            self.logger.info(f"Boss agent {self.name} cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Boss agent cleanup failed: {str(e)}")
            raise

    async def _register_agent(self, agent: BaseAgent) -> None:
        """Register an agent with the boss agent"""
        try:
            layer_id = agent.model_info.get('layer_id')
            if layer_id is None:
                raise ValueError(f"Agent {agent.name} missing layer_id in model_info")
                
            self.layer_agents[layer_id].append(agent)
            self.logger.info(f"Registered agent {agent.name} to layer {layer_id}")
        except Exception as e:
            self.logger.error(f"Agent registration failed: {e}")
            raise

    async def create_plan(self, task: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Create execution plan with REWOO capabilities"""
        if not self.initialized:
            raise RuntimeError(f"Boss agent {self.name} not initialized")
            
        try:
            plan = await self.rewoo_system.create_plan(task)
            await self.evidence_store.store_evidence(
                str(datetime.now().timestamp()),
                plan,
                {'task': task}
            )
            return plan
        except Exception as e:
            self.logger.error(f"Plan creation failed: {e}")
            raise

    async def coordinate_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Coordinate task execution with REWOO integration"""
        if not self.initialized:
            raise RuntimeError("Boss agent not initialized")
            
        task_id = f"task_{datetime.now().timestamp()}"
        
        try:
            # Create execution plan
            plan = await self.rewoo_system.create_plan(task)
            
            # Track task state
            self.active_tasks[task_id] = {
                'status': 'processing',
                'plan': plan,
                'start_time': datetime.now(),
                'layer_results': {}
            }
            
            current_input = task
            for layer_id in sorted(self.layer_agents.keys()):
                layer_result = await self._process_layer(layer_id, current_input, plan)
                self.active_tasks[task_id]['layer_results'][layer_id] = layer_result
                await self.evidence_store.store_evidence(
                    f"{task_id}_layer_{layer_id}",
                    layer_result,
                    {'layer_id': layer_id}
                )
                current_input = self._prepare_next_layer_input(layer_result)
                
            final_result = await self._aggregate_results(
                task_id,
                self.active_tasks[task_id]['layer_results']
            )
            
            return final_result
        except Exception as e:
            self.logger.error(f"Task coordination failed: {str(e)}")
            await self._handle_task_error(task_id, e)
            raise
    
    
    async def _initialize_agent_management(self) -> None:
        """Initialize agent management capabilities."""
        try:
            self.active_agents = defaultdict(list)
            self.agent_states = defaultdict(dict)
            self.task_queue = asyncio.Queue()
            
            self.logger.info("Agent management initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Agent management initialization failed: {e}")
            raise
    async def _setup_communication_channels(self) -> None:
        """Setup communication channels with enhanced error handling"""
        try:
            channels = ['coordination', 'task_assignment', 'result_collection']
            for channel in channels:
                await self.communication_system.create_channel(channel)
                await self._verify_channel_creation(channel)
        except Exception as e:
            self.logger.error(f"Communication setup failed: {e}")
            raise
    async def _initialize_rewoo_capabilities(self) -> None:
        """Initialize REWOO system with enhanced planning"""
        try:
            self.rewoo_system = REWOOSystem(self.config)
            await self.rewoo_system.initialize()
            self.planning_capabilities = await self._setup_planning()
            await self._verify_rewoo_initialization()
        except Exception as e:
            self.logger.error(f"REWOO initialization failed: {e}")
            raise
    
        
    

    




    async def _initialize_agent_factory(self):
        """Initialize agent factory"""
        from .agent_factory import AgentFactory
        factory = AgentFactory(self.config)
        await factory.initialize()
        return factory

    
    async def _verify_rewoo_initialization(self) -> bool:
        """Verify REWOO system initialization and capabilities"""
        try:
            # Check REWOO system existence and initialization
            if not hasattr(self, 'rewoo_system') or not self.rewoo_system:
                self.logger.error("REWOO system not initialized")
                return False

            # Verify required components
            components = await self._verify_rewoo_components()
            if not components['success']:
                self.logger.error(f"REWOO component verification failed: {components['reason']}")
                return False

            # Verify evidence management
            evidence = await self._verify_evidence_management()
            if not evidence['success']:
                self.logger.error(f"Evidence management verification failed: {evidence['reason']}")
                return False

            # Verify planning capabilities
            planning = await self._verify_planning_capabilities()
            if not planning['success']:
                self.logger.error(f"Planning capabilities verification failed: {planning['reason']}")
                return False

            # Verify observation system
            observation = await self._verify_observation_system()
            if not observation['success']:
                self.logger.error(f"Observation system verification failed: {observation['reason']}")
                return False

            self.logger.info("REWOO initialization verified successfully")
            return True

        except Exception as e:
            self.logger.error(f"REWOO verification failed: {e}")
            return False

    async def _verify_rewoo_components(self) -> Dict[str, Any]:
        """Verify REWOO system components"""
        try:
            required_components = {
                'evidence_store': self._verify_evidence_store,
                'planning_system': self._verify_planning_system,
                'observation_system': self._verify_observation_system,
                'execution_system': self._verify_execution_system
            }

            verification_results = {}
            for component, verifier in required_components.items():
                result = await verifier()
                verification_results[component] = result

            all_verified = all(result['success'] for result in verification_results.values())
            
            return {
                'success': all_verified,
                'results': verification_results,
                'reason': None if all_verified else "Component verification failed"
            }

        except Exception as e:
            self.logger.error(f"Component verification failed: {e}")
            return {
                'success': False,
                'results': {},
                'reason': str(e)
            }

    async def _verify_evidence_store(self) -> Dict[str, bool]:
        """Verify evidence store functionality"""
        try:
            # Check evidence store initialization
            if not hasattr(self, 'evidence_store') or not self.evidence_store:
                return {'success': False, 'reason': "Evidence store not initialized"}

            # Verify basic operations
            test_evidence = {
                'content': 'test',
                'source': 'verification',
                'timestamp': datetime.now().isoformat()
            }

            # Test store operation
            evidence_id = await self.evidence_store.store_evidence(
                'test_evidence',
                test_evidence,
                {'type': 'verification'}
            )

            # Test retrieval operation
            retrieved = await self.evidence_store.get_evidence(evidence_id)
            if not retrieved:
                return {'success': False, 'reason': "Evidence retrieval failed"}

            return {'success': True, 'reason': None}

        except Exception as e:
            self.logger.error(f"Evidence store verification failed: {e}")
            return {'success': False, 'reason': str(e)}

    async def _verify_planning_system(self) -> Dict[str, bool]:
        """Verify planning system functionality"""
        try:
            # Check planning system initialization
            if not hasattr(self, 'planning_system') or not self.planning_system:
                return {'success': False, 'reason': "Planning system not initialized"}

            # Verify planning capabilities
            required_capabilities = {
                'create_plan',
                'validate_plan',
                'optimize_plan',
                'replan'
            }

            missing_capabilities = [
                cap for cap in required_capabilities
                if not hasattr(self.planning_system, cap)
            ]

            if missing_capabilities:
                return {
                    'success': False,
                    'reason': f"Missing capabilities: {missing_capabilities}"
                }

            return {'success': True, 'reason': None}

        except Exception as e:
            self.logger.error(f"Planning system verification failed: {e}")
            return {'success': False, 'reason': str(e)}

    async def _verify_observation_system(self) -> Dict[str, bool]:
        """Verify observation system functionality"""
        try:
            # Check observation system initialization
            if not hasattr(self, 'observation_system') or not self.observation_system:
                return {'success': False, 'reason': "Observation system not initialized"}

            # Test observation capability
            test_context = {'type': 'verification'}
            observation = await self.observation_system.observe(test_context)

            if not observation:
                return {'success': False, 'reason': "Observation failed"}

            required_fields = {'timestamp', 'context', 'state'}
            missing_fields = [
                field for field in required_fields
                if field not in observation
            ]

            if missing_fields:
                return {
                    'success': False,
                    'reason': f"Missing observation fields: {missing_fields}"
                }

            return {'success': True, 'reason': None}

        except Exception as e:
            self.logger.error(f"Observation system verification failed: {e}")
            return {'success': False, 'reason': str(e)}

    async def _verify_execution_system(self) -> Dict[str, bool]:
        """Verify execution system functionality"""
        try:
            # Check execution system initialization
            if not hasattr(self, 'execution_system') or not self.execution_system:
                return {'success': False, 'reason': "Execution system not initialized"}

            # Verify execution capabilities
            required_capabilities = {
                'execute_plan',
                'validate_execution',
                'handle_failure'
            }

            missing_capabilities = [
                cap for cap in required_capabilities
                if not hasattr(self.execution_system, cap)
            ]

            if missing_capabilities:
                return {
                    'success': False,
                    'reason': f"Missing capabilities: {missing_capabilities}"
                }

            return {'success': True, 'reason': None}

        except Exception as e:
            self.logger.error(f"Execution system verification failed: {e}")
            return {'success': False, 'reason': str(e)}

    async def _verify_evidence_management(self) -> Dict[str, bool]:
        """Verify evidence management functionality"""
        try:
            # Verify evidence store operations
            store_verification = await self._verify_evidence_store()
            if not store_verification['success']:
                return store_verification

            # Verify evidence flow
            evidence_flow = await self._verify_evidence_flow()
            if not evidence_flow['success']:
                return evidence_flow

            # Verify evidence validation
            validation = await self._verify_evidence_validation()
            if not validation['success']:
                return validation

            return {'success': True, 'reason': None}

        except Exception as e:
            self.logger.error(f"Evidence management verification failed: {e}")
            return {'success': False, 'reason': str(e)}

    async def _verify_evidence_flow(self) -> Dict[str, bool]:
        """Verify evidence flow between components"""
        try:
            # Test evidence generation
            test_evidence = await self._generate_test_evidence()
            
            # Test evidence propagation
            propagation = await self._verify_evidence_propagation(test_evidence)
            if not propagation['success']:
                return propagation

            # Test evidence consumption
            consumption = await self._verify_evidence_consumption(test_evidence)
            if not consumption['success']:
                return consumption

            return {'success': True, 'reason': None}

        except Exception as e:
            self.logger.error(f"Evidence flow verification failed: {e}")
            return {'success': False, 'reason': str(e)}

    async def _verify_evidence_validation(self) -> Dict[str, bool]:
        """Verify evidence validation functionality"""
        try:
            # Test valid evidence
            valid_evidence = await self._generate_test_evidence()
            valid_result = await self._validate_evidence(valid_evidence)
            if not valid_result['success']:
                return valid_result

            # Test invalid evidence
            invalid_evidence = {'invalid': 'evidence'}
            invalid_result = await self._validate_evidence(invalid_evidence)
            if invalid_result['success']:
                return {
                    'success': False,
                    'reason': "Validation failed to detect invalid evidence"
                }

            return {'success': True, 'reason': None}

        except Exception as e:
            self.logger.error(f"Evidence validation verification failed: {e}")
            return {'success': False, 'reason': str(e)}

    async def _generate_test_evidence(self) -> Dict[str, Any]:
        """Generate test evidence for verification"""
        return {
            'content': 'test_content',
            'source': 'verification',
            'timestamp': datetime.now().isoformat(),
            'metadata': {
                'type': 'test',
                'version': '1.0'
            }
        }

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task with REWOO capabilities"""
        if not self.initialized:
            raise RuntimeError(f"Boss agent {self.name} not initialized")
            
        try:
            # Create execution plan
            plan = await self.create_plan(task)
            
            # Execute through layers
            results = await self._process_through_layers(plan)
            
            # Store execution evidence
            await self.evidence_store.store_evidence(
                str(datetime.now().timestamp()),
                results,
                {'task': task}
            )
            
            return results
            
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            raise
            

    async def _process_through_layers(self, task_id: str, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Process task through layers with comprehensive monitoring and recovery"""
        results = {}
        current_layer = 0
        
        try:
            for layer_id in sorted(self.layer_agents.keys()):
                current_layer = layer_id
                
                # Execute layer with monitoring and recovery
                layer_result = await self._execute_layer_with_recovery(
                    layer_id,
                    task_id,
                    plan,
                    max_retries=3
                )
                
                # Store layer results
                results[layer_id] = layer_result
                
                # Update plan based on layer results
                plan = await self._update_plan_with_evidence(plan, layer_result)
                
                # Store layer completion evidence
                await self.evidence_store.store_evidence(
                    f"{task_id}_layer_{layer_id}",
                    {
                        'layer_id': layer_id,
                        'result': layer_result,
                        'updated_plan': plan
                    },
                    {'type': 'layer_complete'}
                )
                
            return results
            
        except Exception as e:
            self.logger.error(f"Layer processing failed at layer {current_layer}: {str(e)}")
            await self._handle_layer_failure(task_id, current_layer, e)
            raise

    async def _execute_layer_with_recovery(
        self,
        layer_id: int,
        task_id: str,
        plan: Dict[str, Any],
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """Execute layer with retry mechanism and error recovery"""
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                # Get available agents for layer
                agents = self._get_available_agents(layer_id)
                if not agents:
                    raise ValueError(f"No available agents for layer {layer_id}")
                
                # Execute agents in parallel with timeout
                tasks = [
                    self._execute_agent_with_timeout(
                        agent,
                        task_id,
                        plan,
                        timeout=30.0
                    )
                    for agent in agents
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle any failed agent executions
                valid_results = []
                for idx, result in enumerate(results):
                    if isinstance(result, Exception):
                        self.logger.warning(
                            f"Agent {agents[idx].name} failed: {str(result)}"
                        )
                        continue
                    valid_results.append(result)
                
                if not valid_results:
                    raise RuntimeError("All agents failed to execute")
                
                # Aggregate results
                return await self._aggregate_layer_results(valid_results, layer_id)
                
            except Exception as e:
                retry_count += 1
                last_error = e
                
                if retry_count == max_retries:
                    self.logger.error(
                        f"Layer {layer_id} failed after {max_retries} attempts: {str(e)}"
                    )
                    raise
                
                # Exponential backoff
                await asyncio.sleep(2 ** retry_count)
                
                self.logger.warning(
                    f"Retrying layer {layer_id}, attempt {retry_count + 1}/{max_retries}"
                )

    async def _execute_agent_with_timeout(
        self,
        agent: BaseAgent,
        task_id: str,
        plan: Dict[str, Any],
        timeout: float
    ) -> Dict[str, Any]:
        """Execute single agent with timeout"""
        try:
            # Create task with timeout
            task = asyncio.create_task(
                agent.process_task({
                    'task_id': task_id,
                    'plan': plan,
                    'timestamp': datetime.now().isoformat()
                })
            )
            
            # Wait for result with timeout
            result = await asyncio.wait_for(task, timeout=timeout)
            
            # Update agent metrics
            self._update_agent_metrics(agent.name, 'success')
            
            return result
            
        except asyncio.TimeoutError:
            self._update_agent_metrics(agent.name, 'timeout')
            raise RuntimeError(f"Agent {agent.name} execution timed out")
            
        except Exception as e:
            self._update_agent_metrics(agent.name, 'error')
            raise

    async def _update_plan_with_evidence(
        self,
        plan: Dict[str, Any],
        layer_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update plan based on layer execution evidence"""
        try:
            # Extract evidence from layer result
            evidence = await self._extract_evidence(layer_result)
            
            # Update plan using REWOO
            updated_plan = await self.rewoo_system.update_plan(
                plan,
                evidence
            )
            
            return updated_plan
            
        except Exception as e:
            self.logger.error(f"Plan update failed: {str(e)}")
            return plan  # Return original plan on failure

    def _update_agent_metrics(self, agent_name: str, status: str) -> None:
        """Update agent execution metrics"""
        self.metrics['agent_executions'][agent_name] += 1
        self.metrics['agent_status'][agent_name][status] += 1
        
        if status == 'success':
            self.metrics['successful_executions'] += 1
        elif status in ('error', 'timeout'):
            self.metrics['failed_executions'] += 1

    async def _handle_layer_failure(
        self,
        task_id: str,
        layer_id: int,
        error: Exception
    ) -> None:
        """Handle layer execution failure"""
        try:
            # Store failure evidence
            await self.evidence_store.store_evidence(
                f"{task_id}_layer_{layer_id}_failure",
                {
                    'error': str(error),
                    'layer_id': layer_id,
                    'timestamp': datetime.now().isoformat()
                },
                {'type': 'layer_failure'}
            )
            
            # Update metrics
            self.metrics['layer_failures'][layer_id] += 1
            
            # Attempt recovery if possible
            if await self._can_recover_layer(layer_id):
                await self._initiate_layer_recovery(task_id, layer_id)
            
        except Exception as e:
            self.logger.error(f"Error handling failed: {str(e)}")
            raise

    def _generate_execution_metadata(self, task_id: str) -> Dict[str, Any]:
        """Generate execution metadata"""
        return {
            'task_id': task_id,
            'boss_agent': self.name,
            'start_time': datetime.now().isoformat(),
            'layer_count': len(self.active_agents),
            'active_agents': {
                layer_id: len(agents)
                for layer_id, agents in self.active_agents.items()
            },
            'metrics': dict(self.metrics)
        }
        
    def _setup_logging(self):
        """Setup logging for the agent"""
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        
    async def _handle_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Enhanced error handling with recovery attempts"""
        error_id = f"error_{datetime.now().timestamp()}"
        try:
            # Log error details
            await self.evidence_store.store_evidence(
                error_id,
                {
                    'error': str(error),
                    'context': context,
                    'stack_trace': traceback.format_exc()
                },
                {'type': 'error'}
            )
            
            # Attempt recovery based on error type
            recovery_result = await self._attempt_recovery(error, context)
            
            # Update metrics
            self.metrics['errors'][type(error).__name__] += 1
            if recovery_result.get('success'):
                self.metrics['successful_recoveries'] += 1
            
        except Exception as e:
            self.logger.error(f"Error handling failed: {e}")
            raise

    

    async def _process_without_rewoo(self, task: Dict[str, Any], evidence: Dict[str, Any]) -> Dict[str, Any]:
        """Process task through layer agents without REWOO"""
        try:
            current_task = task
            layer_results = {}
            
            # Process through each layer
            for layer_id in sorted(self.layer_agents.keys()):
                layer_result = await self._process_layer(layer_id, current_task, evidence)
                layer_results[layer_id] = layer_result
                current_task = self._prepare_next_layer_task(current_task, layer_result)
            
            return {
                'final_result': current_task,
                'layer_results': layer_results
            }
            
        except Exception as e:
            self.logger.error(f"Layer processing failed: {str(e)}")
            raise

    

    

    async def _initialize_knowledge_graph(self) -> None:
        """Initialize knowledge graph components"""
        try:
            # Initialize Neo4j connection if configured
            if hasattr(self.config, 'neo4j_uri'):
                self.neo4j_client = GraphDatabase.driver(
                    self.config.neo4j_uri,
                    auth=(self.config.neo4j_user, self.config.neo4j_password)
                )
            
            # Initialize graph data structures
            self.knowledge_graph = nx.DiGraph()
        
            self.logger.info("Knowledge graph initialized successfully")
        
        except Exception as e:
            self.logger.error(f"Knowledge graph initialization failed: {str(e)}")
            raise

    def _get_agent_class(self, agent_type: str) -> Type[BaseAgent]:
        """Get the appropriate agent class based on type"""
        agent_classes = {
            'Layer1Agent': Layer1Agent,
            'Layer2Agent': Layer2Agent,
            'Layer3Agent': Layer3Agent,
            'Layer4Agent': Layer4Agent
        }
    
        if agent_type not in agent_classes:
            raise ValueError(f"Unknown agent type: {agent_type}")
        
        return agent_classes[agent_type]

    

    async def _initialize_communication(self):
        """Initialize communication system"""
        self.channels = {
            'task_assignment': asyncio.Queue(),
            'result_collection': asyncio.Queue()
        }

    

    async def _generate_steps(self, task: str) -> List[Dict[str, Any]]:
        """Generate execution steps for a task"""
        try:
            steps = [
                {
                    "step_id": "analyze",
                    "action": "analyze_requirements",
                    "parameters": {"task": task}
                },
                {
                    "step_id": "plan",
                    "action": "create_execution_plan",
                    "parameters": {"task": task}
                },
                {
                    "step_id": "delegate",
                    "action": "delegate_to_layers",
                    "parameters": {"task": task}
                }
            ]
            return steps
        except Exception as e:
            self.logger.error(f"Step generation failed: {str(e)}")
            raise

    async def _execute_step(self, step: Dict[str, Any]) -> Any:
        """Execute a single step with error handling"""
        try:
            if step["action"] == "analyze_requirements":
                return await self._analyze_requirements(step["parameters"])
            elif step["action"] == "create_execution_plan":
                return await self._create_execution_plan(step["parameters"])
            elif step["action"] == "delegate_to_layers":
                return await self._delegate_to_layers(step["parameters"])
            else:
                raise ValueError(f"Unknown action: {step['action']}")
        except Exception as e:
            self.logger.error(f"Step execution failed: {str(e)}")
            raise

    def _aggregate_results(self, results: List[Any]) -> Dict[str, Any]:
        """Aggregate step results into final output"""
        try:
            return {
                "analysis": results[0],
                "plan": results[1],
                "delegation": results[2],
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "num_steps": len(results)
                }
            }
        except Exception as e:
            self.logger.error(f"Result aggregation failed: {str(e)}")
            raise
            
    async def _handle_task_error(self, task_id: str, error: Exception) -> None:
        """Handle task processing errors"""
        try:
            if task_id in self.active_tasks:
                self.active_tasks[task_id].update({
                    'status': 'failed',
                    'error': str(error),
                    'end_time': datetime.now()
                })
            self.metrics["task_errors"] += 1
            
            # Store error in cache for debugging
            await self.result_cache.set(
                f"error_{task_id}",
                {
                    "error": str(error),
                    "timestamp": datetime.now().isoformat(),
                    "task": self.active_tasks.get(task_id)
                }
            )
            self.logger.error(f"Task {task_id} failed: {str(error)}")
        except Exception as e:
            self.logger.error(f"Error handling task error: {str(e)}")

   

    async def _analyze_task_requirements(self, task: str) -> Dict[str, Any]:
        """Analyze task requirements and complexity"""
        try:
            # Task Decomposition
            components = await self._decompose_task(task)
            
            # Complexity Analysis
            complexity = await self._assess_complexity(components)
            
            # Resource Requirements
            resources = await self._estimate_resources(complexity)
            
            # Risk Assessment
            risks = await self._assess_risks(components, complexity)
            
            return {
                "components": components,
                "complexity": complexity,
                "resources": resources,
                "risks": risks
            }
        except Exception as e:
            self.logger.error(f"Task analysis failed: {str(e)}")
            raise

    async def assign_task(self, task_data: Dict[str, Any]):
        """Enhanced task assignment with configuration management"""
        try:
            task_id = str(uuid.uuid4())
            self.active_tasks[task_id] = {
                'data': task_data,
                'status': 'assigned',
                'start_time': datetime.now(),
                'configuration': self.config_manager.get_config('task_defaults', {})
            }
            
            # Determine target layer using enhanced configuration
            target_layer = self._determine_target_layer(task_data)
            
            # Get layer-specific configuration
            layer_config = self.config_manager.get_config(
                f'layer_{target_layer}_config',
                {}
            )
            
            # Assign to agents with configuration
            for agent in self.layer_agents[target_layer]:
                await self.channels['task_assignment'].put({
                    'task_id': task_id,
                    'agent': agent,
                    'data': task_data,
                    'config': layer_config
                })
            
            self.logger.info(f"Task {task_id} assigned to layer {target_layer}")
            
        except Exception as e:
            self.logger.error(f"Task assignment failed: {e}")
            raise

    async def _process_bigquery_data_with_retry(self, input_data: str, max_retries: int = 3) -> str:
        """Process BigQuery data with retry mechanism"""
        for attempt in range(max_retries):
            try:
                return await self._process_bigquery_data(input_data)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

    async def _process_bigquery_data(self, input_data: str) -> str:
        """Process data from BigQuery"""
        if not self.config.bigquery_client:
            raise ConfigurationError("BigQuery client not configured")
            
        client = self.config.bigquery_client
        try:
            query = f"""
            SELECT *
            FROM `{self.config.project_id}.{self.config.dataset_id}.customer_interactions`
            WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            """
            query_job = await asyncio.to_thread(client.query, query)
            results = await asyncio.to_thread(query_job.result)
            
            processed_data = []
            for row in results:
                processed_data.append({
                    'customer_id': row['customer_id'],
                    'interaction_type': row['interaction_type'],
                    'timestamp': row['timestamp'].isoformat(),
                    'value': row['value']
                })
            
            return json.dumps(processed_data)
            
        except Exception as e:
            self.logger.error(f"Error processing BigQuery data: {str(e)}")
            raise

    async def create_knowledge_graph_async(self, data: Optional[pd.DataFrame]) -> Optional[nx.DiGraph]:
        """Create knowledge graph from DataFrame asynchronously"""
        if data is None:
            return None
            
        try:
            self.logger.info(f"{self.name} creating knowledge graph")
            graph = nx.DiGraph()
            
            # Create nodes from DataFrame columns
            for col in data.columns:
                graph.add_node(col, type='attribute')
                
            # Create edges based on relationships
            for col1 in data.columns:
                for col2 in data.columns:
                    if col1 != col2:
                        correlation = data[col1].corr(data[col2])
                        if not pd.isna(correlation) and abs(correlation) > 0.5:
                            graph.add_edge(col1, col2, weight=correlation)
            
            self.knowledge_graph = graph
            return graph
            
        except Exception as e:
            self.logger.error(f"Knowledge graph creation failed: {e}")
            return None

    async def update_knowledge_graph(self, new_data: pd.DataFrame) -> None:
        """Update existing knowledge graph with new data"""
        try:
            temp_graph = await self.create_knowledge_graph_async(new_data)
            if temp_graph is None:
                return
                
            self.knowledge_graph = nx.compose(self.knowledge_graph, temp_graph)
            
            for u, v, data in temp_graph.edges(data=True):
                if self.knowledge_graph.has_edge(u, v):
                    old_weight = self.knowledge_graph[u][v]['weight']
                    new_weight = data['weight']
                    self.knowledge_graph[u][v]['weight'] = (old_weight + new_weight) / 2
                    
            self.logger.info("Knowledge graph updated successfully")
            
        except Exception as e:
            self.logger.error(f"Knowledge graph update failed: {e}")
            raise

    def get_graph_insights(self) -> Dict[str, Any]:
        """Extract insights from the knowledge graph"""
        try:
            insights = {
                "node_count": self.knowledge_graph.number_of_nodes(),
                "edge_count": self.knowledge_graph.number_of_edges(),
                "centrality": nx.degree_centrality(self.knowledge_graph),
                "communities": list(nx.community.greedy_modularity_communities(
                    self.knowledge_graph.to_undirected()
                )),
                "important_nodes": sorted(
                    nx.pagerank(self.knowledge_graph).items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5]
            }
            return insights
            
        except Exception as e:
            self.logger.error(f"Failed to extract graph insights: {e}")
            return {
                "node_count": 0,
                "edge_count": 0,
                "centrality": {},
                "communities": [],
                "important_nodes": []
            }

    def export_graph(self, format: str = 'graphml') -> Optional[str]:
        """Export knowledge graph in specified format"""
        try:
            if format == 'graphml':
                nx.write_graphml(self.knowledge_graph, 'knowledge_graph.graphml')
                return 'knowledge_graph.graphml'
            elif format == 'gexf':
                nx.write_gexf(self.knowledge_graph, 'knowledge_graph.gexf')
                return 'knowledge_graph.gexf'
            else:
                raise ValueError(f"Unsupported export format: {format}")
                
        except Exception as e:
            self.logger.error(f"Graph export failed: {e}")
            return None

    def standardize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize DataFrame schema"""
        try:
            standardized_columns = {
                'customer_id': 'customer_id',
                'timestamp': 'event_timestamp',
                'event_type': 'event_type',
                'value': 'event_value',
            }

            df = df.rename(columns=standardized_columns)
            required_columns = [
                'customer_id',
                'event_timestamp',
                'event_type',
                'event_value',
                'source'
            ]
            
            for col in required_columns:
                if col not in df.columns:
                    df[col] = np.nan
                    
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce')
            df['event_value'] = pd.to_numeric(df['event_value'], errors='coerce')
            
            return df[required_columns]
        except Exception as e:
            self.logger.error(f"Schema standardization failed: {e}")
            raise

    def get_metrics(self) -> Dict[str, Any]:
        """Get agent metrics"""
        return {
            'tasks_processed': dict(self.metrics['tasks_processed']),
            'average_processing_time': np.mean(self.metrics['processing_time']) if self.metrics['processing_time'] else 0,
            'total_tasks': len(self.task_history),
            'error_rate': len([t for t in self.task_history if t.get('status') == 'failed']) / len(self.task_history) if self.task_history else 0
        }

    def _determine_target_layer(self, task_data: Dict[str, Any]) -> int:
        """Determine the appropriate layer for a task based on complexity and priority"""
        try:
            complexity = task_data.get('complexity', 1)
            priority = task_data.get('priority', 1)
            
            # Simple mapping based on complexity and priority
            layer = min(max(complexity + priority - 1, 1), max(self.layer_agents.keys()))
            
            return layer
        except Exception as e:
            self.logger.error(f"Layer determination failed: {e}")
            return 1  # Default to first layer on error

    async def _process_through_layers(self, task_id: str, initial_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Process task through all layers with proper coordination"""
        results = {}
        try:
            for layer_num in sorted(self.layer_agents.keys()):
                if layer_num == 0:  # Skip boss layer
                    continue
                
                layer_results = await self._execute_layer(
                    task_id,
                    layer_num,
                    self.layer_agents[layer_num],
                    initial_plan
                )
                results[layer_num] = layer_results
                
                # Update plan based on layer results
                initial_plan = await self._update_plan(initial_plan, layer_results)
            
            return results
        except Exception as e:
            self.logger.error(f"Layer processing failed: {str(e)}")
            raise



from typing import Dict, List, Any, Optional, Type
from datetime import datetime
from collections import defaultdict, Counter
import logging




 
class PandasNumpyEncoder(json.JSONEncoder):
    """JSON encoder for Pandas and NumPy data types."""
    def default(self, obj):
        if isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient='split')
        elif isinstance(obj, pd.Series):
            return obj.to_dict()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.generic):
            return obj.item()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif pd.isna(obj):
            return None
        return super(PandasNumpyEncoder, self).default(obj)
  
# ================================
#  Necessary Imports
# ================================
import logging
import os
import re
import time
import json
import asyncio
import psutil
import hashlib
import numpy as np
import networkx as nx
from typing import Dict, List, Any, Optional, Union, Type, Callable
from dataclasses import dataclass, field
from collections import defaultdict, Counter, deque
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from cachetools import TTLCache
import aiohttp
from dotenv import load_dotenv


class DataPipelineOptimizer:
    """Optimizes data pipeline for BigQuery and Neo4j operations"""
    def __init__(self, config: EnhancedConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.metrics = defaultdict(Counter)
        self.query_cache = TTLCache(maxsize=config.pipeline_config.get('cache_size', 1000),
                                  ttl=config.pipeline_config.get('timeout', 3600))
        self.batch_size = config.pipeline_config.get('batch_size', 1000)
        self.max_workers = config.pipeline_config.get('max_workers', 10)
        self.initialized = False

        
    async def initialize(self) -> None:
        """Initialize optimizer"""
        try:
            # Initialize cache
            self.query_cache.clear()
            
            # Initialize metrics
            self.metrics.clear()
            
            self.initialized = True
            self.logger.info("Data pipeline optimizer initialized successfully")
        except Exception as e:
            self.logger.error(f"Data pipeline optimizer initialization failed: {e}")
            raise
    async def cleanup(self) -> None:
        """Cleanup optimizer resources"""
        try:
            self.query_cache.clear()
            self.metrics.clear()
            self.initialized = False
            self.logger.info("Data pipeline optimizer cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Data pipeline optimizer cleanup failed: {e}")
            raise
    async def optimize_bigquery_query(self, query: str) -> str:
        """Optimize BigQuery query for better performance"""
        try:
            # Apply query optimization rules
            optimized_query = self._apply_query_optimizations(query)
            
            # Validate optimized query
            if not self._validate_query(optimized_query):
                raise ValueError("Query optimization resulted in invalid query")
            
            return optimized_query
            
        except Exception as e:
            self.logger.error(f"Query optimization failed: {e}")
            raise

    async def optimize_neo4j_query(self, query: str) -> str:
        """Optimize Neo4j query for better performance"""
        try:
            # Apply Neo4j-specific optimizations
            optimized_query = self._apply_neo4j_optimizations(query)
            
            # Validate optimized query
            if not self._validate_neo4j_query(optimized_query):
                raise ValueError("Neo4j query optimization resulted in invalid query")
            
            return optimized_query
            
        except Exception as e:
            self.logger.error(f"Neo4j query optimization failed: {e}")
            raise

    async def batch_process_data(self, data: List[Dict[str, Any]], target: str) -> None:
        """Process data in optimized batches"""
        try:
            batches = [data[i:i + self.batch_size] for i in range(0, len(data), self.batch_size)]
            
            async with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                tasks = []
                for batch in batches:
                    if target == 'bigquery':
                        task = executor.submit(self._process_bigquery_batch, batch)
                    elif target == 'neo4j':
                        task = executor.submit(self._process_neo4j_batch, batch)
                    else:
                        raise ValueError(f"Unsupported target: {target}")
                    tasks.append(task)
                
                # Wait for all tasks to complete
                for future in as_completed(tasks):
                    result = future.result()
                    self.metrics['processed_batches'] += 1
                    
        except Exception as e:
            self.logger.error(f"Batch processing failed: {e}")
            raise

    def _apply_query_optimizations(self, query: str) -> str:
        """Apply optimization rules to BigQuery query"""
        optimizations = [
            self._optimize_joins,
            self._optimize_filters,
            self._optimize_aggregations,
            self._optimize_partitioning
        ]
        
        optimized_query = query
        for optimization in optimizations:
            optimized_query = optimization(optimized_query)
            
        return optimized_query

    def _apply_neo4j_optimizations(self, query: str) -> str:
        """Apply Neo4j-specific optimizations"""
        optimizations = [
            self._optimize_node_patterns,
            self._optimize_relationships,
            self._optimize_indexes,
            self._optimize_parameters
        ]
        
        optimized_query = query
        for optimization in optimizations:
            optimized_query = optimization(optimized_query)
            
        return optimized_query

    async def _process_bigquery_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process a batch of data for BigQuery"""
        try:
            # Prepare data for insertion
            rows = [self._prepare_bigquery_row(row) for row in batch]
            
            # Insert data
            table = self.config.bigquery_client.get_table(self.config.table_id)
            errors = self.config.bigquery_client.insert_rows_json(table, rows)
            
            if errors:
                raise Exception(f"Errors inserting rows: {errors}")
                
        except Exception as e:
            self.logger.error(f"BigQuery batch processing failed: {e}")
            raise

    async def _process_neo4j_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process a batch of data for Neo4j"""
        try:
            # Prepare Neo4j transaction
            async with self.config.neo4j_client.session() as session:
                tx = await session.begin_transaction()
                try:
                    for row in batch:
                        query = self._prepare_neo4j_query(row)
                        await tx.run(query)
                    await tx.commit()
                except Exception as e:
                    await tx.rollback()
                    raise
                
        except Exception as e:
            self.logger.error(f"Neo4j batch processing failed: {e}")
            raise
            
            
class AgentCoordinator:
    """Coordinates interactions between agents in the MoA framework"""
    def __init__(self, config: EnhancedConfig):
        self.config = config
        self.active_agents = {}
        self.task_queue = asyncio.Queue()
        self.logger = logging.getLogger(__name__)
        self.result_cache = TTLCache(maxsize=1000, ttl=3600)
        self.metrics = defaultdict(Counter)
        self.coordination_state = {}
        self.evidence_store = EvidenceStore()
       
        
    async def initialize(self) -> None:
        """Initialize the coordination system"""
        try:
            # Initialize boss agent first
            self.boss_agent = await self._initialize_boss_agent()
            
            # Initialize layer agents
            await self._initialize_layer_agents()
            
            # Initialize communication channels
            await self._setup_communication_channels()
            
            # Initialize REWOO capabilities
            await self._initialize_rewoo_capabilities()
            
            self.logger.info("Agent coordination system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Agent coordination initialization failed: {str(e)}")
            await self.cleanup()
            raise

    async def cleanup(self) -> None:
        """Cleanup coordination resources"""
        try:
            if hasattr(self, 'boss_agent'):
                await self.boss_agent.cleanup()
            
            for agents in self.active_agents.values():
                for agent in agents:
                    await agent.cleanup()
            
            self.active_agents.clear()
            self.coordination_state.clear()
            self.metrics.clear()
            
            self.logger.info("Agent coordination system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Coordination cleanup failed: {str(e)}")
            raise
    async def _initialize_boss_agent(self) -> BossAgent:
        """Initialize the boss agent with enhanced coordination capabilities"""
        try:
            boss_config = self.config.get_boss_config()
            if not boss_config:
                raise ValueError("Boss agent configuration not found")
                
            boss_agent = BossAgent(
                name="MainBoss",
                model_info=boss_config.to_dict(),
                config=self.config
            )
            
            await boss_agent.initialize()
            self.logger.info("Boss agent initialized successfully")
            return boss_agent
            
        except Exception as e:
            self.logger.error(f"Boss agent initialization failed: {str(e)}")
            raise

    async def _initialize_layer_agents(self) -> None:
        """Initialize layer agents with improved coordination"""
        try:
            for layer_id, layer_config in self.config.layer_configs.items():
                self.active_agents[layer_id] = []
                
                for agent_name in layer_config['agents']:
                    model_config = self.config.get_model_config(agent_name)
                    if model_config:
                        agent = await self._create_layer_agent(
                            agent_name,
                            model_config,
                            layer_id
                        )
                        if agent:
                            self.active_agents[layer_id].append(agent)
                            
            self.logger.info(f"Initialized {sum(len(agents) for agents in self.active_agents.values())} layer agents")
            
        except Exception as e:
            self.logger.error(f"Layer agent initialization failed: {str(e)}")
            raise

    
    
    async def coordinate_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Coordinate task execution with enhanced error handling"""
        task_id = f"task_{datetime.now().timestamp()}"
        try:
            # Create execution plan
            plan = await self._create_execution_plan(task)
        
            # Track task state
            self.coordination_state[task_id] = {
                'status': 'processing',
                'plan': plan,
                'start_time': datetime.now(),
                'layer_results': {}
            }
        
            # Process through layers
            current_input = task
            for layer_id in sorted(self.active_agents.keys()):
                layer_result = await self._process_layer(
                    layer_id,
                    current_input,
                    plan
                )
            
                self.coordination_state[task_id]['layer_results'][layer_id] = layer_result
                current_input = self._prepare_next_layer_input(layer_result)
            
            # Finalize results
            final_result = await self._aggregate_results(
                task_id,
                self.coordination_state[task_id]['layer_results']
            )
        
            self.coordination_state[task_id]['status'] = 'completed'
            self.metrics['tasks_completed'] += 1
        
            return final_result
        
        except Exception as e:
            self.logger.error(f"Task coordination failed: {str(e)}")
            self.coordination_state[task_id]['status'] = 'failed'
            self.metrics['tasks_failed'] += 1
            raise
        finally:
            self.coordination_state[task_id]['end_time'] = datetime.now()

    async def _process_layer(self,
                           layer_id: int,
                           input_data: Any,
                           plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process input through a specific layer"""
        try:
            layer_agents = self.active_agents[layer_id]
            
            # Execute agents in parallel
            tasks = [
                agent.process_task({
                    'input': input_data,
                    'plan': plan,
                    'layer_context': self._get_layer_context(layer_id)
                })
                for agent in layer_agents
            ]
            
            results = await asyncio.gather(*tasks)
            
            # Store layer metrics
            self.metrics[f'layer_{layer_id}_processed'] += 1
            
            return results
            
        except Exception as e:
            self.logger.error(f"Layer {layer_id} processing failed: {str(e)}")
            raise

    def _get_layer_context(self, layer_id: int) -> Dict[str, Any]:
        """Get context information for a specific layer"""
        return {
            'layer_id': layer_id,
            'agent_count': len(self.active_agents[layer_id]),
            'layer_type': self.config.layer_configs[layer_id]['type'],
            'timestamp': datetime.now().isoformat()
        }

    async def _aggregate_results(self,
                               task_id: str,
                               layer_results: Dict[int, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Aggregate results from all layers"""
        try:
            aggregated_result = {
                'task_id': task_id,
                'layer_results': layer_results,
                'metadata': {
                    'completion_time': datetime.now().isoformat(),
                    'processing_time': (
                        datetime.now() - self.coordination_state[task_id]['start_time']
                    ).total_seconds(),
                    'layer_metrics': {
                        layer_id: self._calculate_layer_metrics(results)
                        for layer_id, results in layer_results.items()
                    }
                }
            }
            
            return aggregated_result
            
        except Exception as e:
            self.logger.error(f"Result aggregation failed: {str(e)}")
            raise

    def _calculate_layer_metrics(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate metrics for layer results"""
        return {
            'response_count': len(results),
            'average_confidence': statistics.mean(
                result.get('confidence', 0) for result in results
            ),
            'success_rate': sum(
                1 for r in results if r.get('status') == 'success'
            ) / len(results) if results else 0
        }

    async def cleanup(self) -> None:
        """Cleanup coordination resources"""
        try:
            # Cleanup boss agent
            if hasattr(self, 'boss_agent'):
                await self.boss_agent.cleanup()
            
            # Cleanup layer agents
            for agents in self.active_agents.values():
                for agent in agents:
                    await agent.cleanup()
            
            # Clear state
            self.active_agents.clear()
            self.coordination_state.clear()
            self.metrics.clear()
            
            self.logger.info("Agent coordination system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Coordination cleanup failed: {str(e)}")
            raise
            
            
class MetricsTracker:
    """Track system metrics"""
    def __init__(self):
        self.metrics = defaultdict(Counter)
        self.timings = defaultdict(list)
        self.start_time = datetime.now()
        self.initialized = False
        self.logger = logging.getLogger(__name__)

    async def initialize(self) -> None:
        """Initialize metrics tracker"""
        try:
            self.metrics.clear()
            self.timings.clear()
            self.start_time = datetime.now()
            self.initialized = True
            self.logger.info("Metrics tracker initialized successfully")
        except Exception as e:
            self.logger.error(f"Metrics tracker initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup metrics tracker resources"""
        try:
            self.metrics.clear()
            self.timings.clear()
            self.initialized = False
            self.logger.info("Metrics tracker cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Metrics tracker cleanup failed: {e}")
            raise
    def record_metric(self, category: str, name: str, value: Any = 1):
        """Record a metric"""
        self.metrics[category][name] += value

    def record_timing(self, category: str, duration: float):
        """Record timing information"""
        self.timings[category].append(duration)

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return {
            'metrics': {k: dict(v) for k, v in self.metrics.items()},
            'timings': {k: {
                'avg': sum(v)/len(v) if v else 0,
                'min': min(v) if v else 0,
                'max': max(v) if v else 0
            } for k, v in self.timings.items()},
            'uptime': (datetime.now() - self.start_time).total_seconds()
        }

import logging
import asyncio
from typing import Dict, Any, Optional, Type
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, Counter
from cachetools import TTLCache

@dataclass
class AgentFactoryConfig:
    """Configuration for agent factory."""
    enabled: bool = True
    max_agents: int = 10
    initialization_timeout: int = 30
    retry_attempts: int = 3
    metadata: Dict[str, Any] = field(default_factory=dict)

class PatternMatcher:
    """Pattern matching capabilities for evidence analysis"""
    def __init__(self):
        self.patterns = {}
        self.logger = logging.getLogger(__name__)
        
    async def initialize(self):
        """Initialize pattern matching capabilities"""
        try:
            self.patterns = {
                'temporal': self._compile_temporal_patterns(),
                'structural': self._compile_structural_patterns(),
                'semantic': self._compile_semantic_patterns()
            }
            self.logger.info("Pattern matcher initialized successfully")
        except Exception as e:
            self.logger.error(f"Pattern matcher initialization failed: {e}")
            raise
            
    def _compile_temporal_patterns(self):
        return {}  # Implement temporal pattern compilation
        
    def _compile_structural_patterns(self):
        return {}  # Implement structural pattern compilation
        
    def _compile_semantic_patterns(self):
        return {}  # Implement semantic pattern compilation

class ConfidenceCalculator:
    """Calculate confidence scores for evidence"""
    def __init__(self):
        self.metrics = defaultdict(float)
        self.logger = logging.getLogger(__name__)
        
    async def initialize(self):
        """Initialize confidence calculation capabilities"""
        try:
            self.metrics = {
                'source_reliability': 0.8,
                'temporal_relevance': 0.9,
                'content_quality': 0.85
            }
            self.logger.info("Confidence calculator initialized successfully")
        except Exception as e:
            self.logger.error(f"Confidence calculator initialization failed: {e}")
            raise

class AgentFactory:
    """Enhanced agent factory with proper initialization phases"""
    def __init__(self, config: 'EnhancedConfig'):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.initialized = False
        self._initializing = False
        self.agents = {}
        self.agent_states = {}
        self.agent_pools = defaultdict(list)
        self.templates = {}
        self.metrics = defaultdict(Counter)
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self._initialization_state = defaultdict(bool)

    async def initialize(self) -> None:
        """Initialize agent factory with proper phasing"""
        if self._initializing or self.initialized:
            return
            
        self._initializing = True
        try:
            # Phase 1: Core initialization
            await self._initialize_core()
            self._initialization_state['core'] = True
            
            # Phase 2: Template initialization
            await self._initialize_templates()
            self._initialization_state['templates'] = True
            
            # Phase 3: Component initialization
            await self._initialize_components()
            self._initialization_state['components'] = True
            
            # Only now initialize pools
            if await self._verify_initialization():
                await self._initialize_pools()
                self._initialization_state['pools'] = True
            
            self.initialized = True
            self.logger.info("Agent factory initialized successfully")
        except Exception as e:
            self.logger.error(f"Agent factory initialization failed: {e}")
            await self.cleanup()
            raise
        finally:
            self._initializing = False

    async def _initialize_core(self) -> None:
        """Initialize core factory components"""
        try:
            self.agents.clear()
            self.agent_states.clear()
            self.metrics.clear()
            self.cache.clear()
            
            # Initialize basic metrics tracking
            self.metrics = {
                'agents_created': Counter(),
                'pools_initialized': Counter(),
                'errors': Counter()
            }
            
            self.logger.info("Core components initialized successfully")
        except Exception as e:
            self.logger.error(f"Core initialization failed: {e}")
            raise

    async def _initialize_templates(self) -> None:
        """Initialize agent templates"""
        try:
            self.templates = {
                'BossAgent': {
                    'class': BossAgent,
                    'config': self.config.get_model_config('BossAgent')
                },
                'Layer1Agent': {
                    'class': Layer1Agent,
                    'config': self.config.get_model_config('Layer1Agent')
                },
                'Layer2Agent': {
                    'class': Layer2Agent,
                    'config': self.config.get_model_config('Layer2Agent')
                },
                'Layer3Agent': {
                    'class': Layer3Agent,
                    'config': self.config.get_model_config('Layer3Agent')
                },
                'Layer4Agent': {
                    'class': Layer4Agent,
                    'config': self.config.get_model_config('Layer4Agent')
                }
            }
            
            self.logger.info("Templates initialized successfully")
        except Exception as e:
            self.logger.error(f"Template initialization failed: {e}")
            raise

    async def _initialize_components(self) -> None:
        """Initialize factory components"""
        try:
            # Initialize agent tracking
            self.agent_states = defaultdict(lambda: {
                'status': 'inactive',
                'initialized_at': None,
                'last_active': None
            })
            
            # Initialize pool tracking
            self.agent_pools = defaultdict(list)
            
            self.logger.info("Components initialized successfully")
        except Exception as e:
            self.logger.error(f"Component initialization failed: {e}")
            raise

    async def _initialize_pools(self) -> None:
        """Initialize agent pools with proper error handling"""
        try:
            for agent_type, template in self.templates.items():
                config = template['config']
                if not config:
                    self.logger.warning(f"No configuration found for {agent_type}")
                    continue
                    
                # Safe access to pool_size with proper error handling
                pool_size = config.get('pool_size', 1) if isinstance(config, dict) else config.get('pool_size', 1)
                self.logger.info(f"Initializing pool for {agent_type} with size {pool_size}")
                
                for _ in range(pool_size):
                    agent = await self._create_agent_instance(
                        agent_type,
                        template['class'],
                        config.to_dict() if hasattr(config, 'to_dict') else config
                    )
                    if agent:
                        self.agent_pools[agent_type].append(agent)
                        self.metrics['pools_initialized'][agent_type] += 1
                        
            self.logger.info("Agent pools initialized successfully")
        except Exception as e:
            self.logger.error(f"Pool initialization failed: {e}")
            raise
    async def create_agent(self, agent_type: str, layer_id: int) -> BaseAgent:
        try:
            agent_config = self._prepare_agent_config(agent_type, layer_id)
            agent_class = self._get_agent_class(agent_type)
            agent = agent_class(
                name=f"{agent_type}_{layer_id}",
                model_info=agent_config.model_config.to_dict(),
                config=self.config
            )
            await agent.initialize()
            return agent
        except Exception as e:
            self.logger.error(f"Agent creation failed: {e}")
            raise

    def _prepare_agent_config(self, agent_type: str, layer_id: int) -> AgentConfig:
        """Prepare hierarchical configuration for agent"""
        base_config = self.config.get_agent_config(agent_type)
        layer_config = self.config.get_layer_config(layer_id)
        return self._merge_configs(base_config, layer_config)

    async def _create_agent_instance(self, agent_type: str, agent_class: Type, config: Dict[str, Any]) -> Optional[BaseAgent]:
        """Create agent instance with proper error handling"""
        try:
            agent = agent_class(
                name=f"{agent_type}_{len(self.agents)}",
                model_info=config,
                config=self.config
            )
            await agent.initialize()
            
            self.agents[agent.name] = agent
            self.metrics['agents_created'][agent_type] += 1
            
            return agent
        except Exception as e:
            self.logger.error(f"Agent instance creation failed: {e}")
            self.metrics['errors']['creation_failed'] += 1
            return None

    async def _verify_initialization(self) -> bool:
        """Verify initialization state"""
        try:
            required_states = {'core', 'templates', 'components'}
            current_states = {
                state for state, initialized in self._initialization_state.items()
                if initialized
            }
            
            if not required_states.issubset(current_states):
                missing_states = required_states - current_states
                self.logger.error(f"Missing initialization states: {missing_states}")
                return False
                
            return True
        except Exception as e:
            self.logger.error(f"Initialization verification failed: {e}")
            return False

    async def cleanup(self) -> None:
        """Cleanup factory resources"""
        try:
            # Cleanup agents
            for agent in self.agents.values():
                try:
                    await agent.cleanup()
                except Exception as e:
                    self.logger.error(f"Agent cleanup failed: {e}")
            
            # Clear collections
            self.agents.clear()
            self.agent_states.clear()
            self.agent_pools.clear()
            self.templates.clear()
            self.cache.clear()
            
            # Reset state
            self.initialized = False
            self._initializing = False
            self._initialization_state.clear()
            
            self.logger.info("Agent factory cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Factory cleanup failed: {e}")
            raise
    def _get_config_value(self, config: Union[Dict[str, Any], ModelConfig], key: str, default: Any = None) -> Any:
        """Safely get configuration value regardless of config type"""
        try:
            if isinstance(config, dict):
                return config.get(key, default)
            elif hasattr(config, 'get'):
                return config.get(key, default)
            elif hasattr(config, key):
                return getattr(config, key)
            return default
        except Exception as e:
            self.logger.error(f"Error accessing config value {key}: {e}")
            return default
    def get_factory_statistics(self) -> Dict[str, Any]:
        """Get factory statistics"""
        return {
            'total_agents': len(self.agents),
            'agents_by_type': dict(self.metrics['agents_created']),
            'pool_sizes': {
                agent_type: len(pool)
                for agent_type, pool in self.agent_pools.items()
            },
            'active_agents': len([
                agent for agent in self.agents.values()
                if agent.initialized
            ])
        }
    
from typing import Dict, List, Any, Optional, Union, Type
from collections import defaultdict, Counter
from cachetools import TTLCache


# moa_system.py
class EnhancedMoASystem:
    def __init__(self, config: EnhancedConfig):
        self.config = config
        self.evidence_manager = EvidenceManager()
        self.agents = {}
        
    async def initialize(self):
        try:
            await self.evidence_manager.initialize()
            await self._initialize_agents()
            # Setup REWOO integration...
        except Exception as e:
            self.logger.error(f"MOA initialization failed: {e}")
            raise

    async def _initialize_agents(self) -> None:
        """Initialize all agents with proper error handling"""
        try:
            agent_factory = AgentFactory(self.config)
            await agent_factory.initialize()
            
            for agent_config in self.config.agent_configs:
                try:
                    agent = await agent_factory.create_agent(
                        agent_config.type,
                        agent_config.model_config,
                        self.communication
                    )
                    self.agents[agent.name] = agent
                    self.agent_states[agent.name] = {
                        'status': 'active',
                        'initialized_at': datetime.now().isoformat()
                    }
                    self.logger.info(f"Initialized agent {agent.name}")
                except Exception as e:
                    self.logger.error(f"Failed to initialize agent {agent_config.name}: {str(e)}")
                    raise
            
            self.logger.info(f"Initialized {len(self.agents)} agents successfully")
        except Exception as e:
            self.logger.error(f"Agent initialization failed: {str(e)}")
            raise

    async def cleanup(self) -> None:
        """Cleanup system resources"""
        try:
            # Cleanup communication channel
            if hasattr(self, 'communication'):
                await self.communication.cleanup()
            
            # Cleanup evidence store
            if hasattr(self, 'evidence_store'):
                await self.evidence_store.cleanup()
            
            # Cleanup agents
            if hasattr(self, 'agents'):
                for agent in self.agents.values():
                    try:
                        await agent.cleanup()
                    except Exception as e:
                        self.logger.error(f"Error cleaning up agent {agent.name}: {str(e)}")
            
            self.initialized = False
            self._initializing = False
            self.logger.info("MoA system cleaned up successfully")
        except Exception as e:
            self.logger.error(f"MoA system cleanup failed: {str(e)}")
            raise

    async def _initialize_components(self) -> None:
        """Initialize core system components."""
        try:
            # Initialize configuration
            if not self.config.initialized:
                await self.config.initialize()
            
            # Initialize metrics tracking
            self.metrics = defaultdict(Counter)
            
            # Initialize caching
            self.cache = TTLCache(maxsize=1000, ttl=3600)
            
            self.logger.info("Core components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Component initialization failed: {str(e)}")
            raise

    async def _initialize_layer_agents(self) -> None:
        """Initialize agents for each layer with proper async handling."""
        try:
            for layer_id, layer_config in self.config.layer_configs.items():
                self.layer_agents[layer_id] = []
                
                for agent_name in layer_config.agents:
                    agent = await self._create_layer_agent(agent_name, layer_id)
                    if agent:
                        self.layer_agents[layer_id].append(agent)
                        self.logger.info(f"Created agent {agent_name} for layer {layer_id}")
                        
            self.logger.info(f"Initialized {sum(len(agents) for agents in self.layer_agents.values())} layer agents")
            
        except Exception as e:
            self.logger.error(f"Layer agent initialization failed: {str(e)}")
            raise

    async def _create_layer_agent(self, agent_name: str, layer_id: int) -> Optional[BaseAgent]:
        """Create and initialize a layer agent."""
        try:
            # Get agent configuration
            agent_config = self.config.get_agent_config(agent_name)
            if not agent_config:
                raise ValueError(f"Configuration not found for agent {agent_name}")
                
            # Create agent instance
            agent_class = self._get_agent_class(agent_config.type)
            agent = agent_class(
                name=f"{agent_name}_{layer_id}",
                model_info=agent_config.model_config.to_dict(),
                config=self.config
            )
            
            # Initialize agent
            await agent.initialize()
            
            # Update metrics
            self.metrics['agents_created'][agent_config.type] += 1
            
            return agent
            
        except Exception as e:
            self.logger.error(f"Agent creation failed for {agent_name}: {str(e)}")
            return None

    def _get_agent_class(self, agent_type: str) -> Type[BaseAgent]:
        """Get the appropriate agent class based on type."""
        agent_classes = {
            'Layer1Agent': Layer1Agent,
            'Layer2Agent': Layer2Agent,
            'Layer3Agent': Layer3Agent,
            'Layer4Agent': Layer4Agent
        }
        
        if agent_type not in agent_classes:
            raise ValueError(f"Unknown agent type: {agent_type}")
            
        return agent_classes[agent_type]

    async def cleanup(self) -> None:
        """Cleanup system resources."""
        try:
            # Cleanup layer agents
            for agents in self.layer_agents.values():
                for agent in agents:
                    await agent.cleanup()
                    
            # Clear collections
            self.layer_agents.clear()
            self.metrics.clear()
            if hasattr(self, 'cache'):
                self.cache.clear()
                
            self.initialized = False
            self._initializing = False
            
            self.logger.info("MoA system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"MoA system cleanup failed: {str(e)}")
            raise

    def get_system_status(self) -> Dict[str, Any]:
        """Get current system status."""
        return {
            'initialized': self.initialized,
            'initializing': self._initializing,
            'agent_counts': {
                layer_id: len(agents)
                for layer_id, agents in self.layer_agents.items()
            },
            'metrics': dict(self.metrics)
        }

    
# Focusing on the EnhancedKnowledgeGraph changes:
class EnhancedKnowledgeGraph:
    """Enhanced knowledge graph with async support"""
    def __init__(self, config: 'EnhancedConfig'):
        self.config = config
        self.driver = None
        self.validators = {}
        self.updaters = {}
        self.logger = logging.getLogger(__name__)
        self.initialized = False

    async def initialize(self):
        """Initialize graph components"""
        try:
            # Initialize graph database connection
            await self._initialize_database()
            
            # Initialize validators
            await self._initialize_validators()
            
            # Initialize updaters
            await self._initialize_updaters()
            
            # Initialize indices
            await self._initialize_indices()
            
            self.initialized = True
            self.logger.info("Knowledge graph initialized successfully")
        except Exception as e:
            self.logger.error(f"Knowledge graph initialization failed: {e}")
            raise

    async def _initialize_database(self):
        """Initialize database connection with proper async handling"""

        # Use async driver for Neo4j:
        self.driver = AsyncGraphDatabase.driver(
            self.config.neo4j_uri,
            auth=(self.config.neo4j_user, self.config.neo4j_password),
            max_connection_lifetime=3600
        )
        
        # Test connection with an async session
        async with self.driver.session() as session:
            await session.run("RETURN 1")
        
        self.logger.info("Graph database connection initialized")

    async def _initialize_indices(self):
        """Initialize graph indices"""
        try:
            # Now this is truly async
            async with self.driver.session() as session:
                await session.run("""
                    CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.id);
                    CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.type);
                    CREATE INDEX IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON (r.type);
                """)
            self.logger.info("Graph indices initialized")
        except Exception as e:
            self.logger.error(f"Index initialization failed: {e}")
            raise

    async def _initialize_validators(self):
        # Assume same as original
        self.validators = {
            'entity': self._validate_entity,
            'relationship': self._validate_relationship,
            'property': self._validate_property,
            'constraint': self._validate_constraint
        }
        self.logger.info("Validators initialized")

    async def _initialize_updaters(self):
        # Assume same as original
        self.updaters = {
            'entity': self._update_entity,
            'relationship': self._update_relationship,
            'property': self._update_property,
            'batch': self._batch_update
        }
        self.logger.info("Updaters initialized")
   

    async def _run_query(self, query: str, parameters: Dict[str, Any] = None):
        """Run Neo4j query asynchronously"""
        try:
            def execute_query():
                with self.driver.session() as session:
                    return session.run(query, parameters)
            
            # Execute query in thread pool
            return await asyncio.get_event_loop().run_in_executor(
                None, execute_query
            )
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise



    async def _initialize_updaters(self):
        """Initialize graph updaters"""
        self.updaters = {
            'entity': self._update_entity,
            'relationship': self._update_relationship,
            'property': self._update_property,
            'batch': self._batch_update
        }
        self.logger.info("Updaters initialized")

    async def update_graph(self, data: Dict[str, Any]) -> None:
        """Update graph with new data"""
        try:
            # Validate data
            validated_data = await self._validate_data(data)
            
            # Extract entities and relationships
            entities = await self._extract_entities(validated_data)
            relationships = await self._extract_relationships(validated_data)
            
            # Update graph using async query execution
            for entity in entities:
                await self._run_query(
                    "MERGE (n:Entity {id: $id}) SET n += $properties",
                    {"id": entity["id"], "properties": entity["properties"]}
                )
            
            for rel in relationships:
                await self._run_query(
                    """
                    MATCH (a:Entity {id: $from_id})
                    MATCH (b:Entity {id: $to_id})
                    MERGE (a)-[r:RELATES]->(b)
                    SET r += $properties
                    """,
                    {
                        "from_id": rel["from_id"],
                        "to_id": rel["to_id"],
                        "properties": rel["properties"]
                    }
                )
            
            # Validate updates
            await self._validate_updates()
            
        except Exception as e:
            self.logger.error(f"Graph update failed: {e}")
            raise

    async def close(self):
        """Close database connection"""
        if self.driver:
            await asyncio.get_event_loop().run_in_executor(
                None, self.driver.close
            )

    async def cleanup(self):
        """Cleanup graph resources"""
        try:
            if self.driver:
                await self.close()
            self.initialized = False
            self.logger.info("Knowledge graph cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Knowledge graph cleanup failed: {e}")
            raise
            
            
    async def _run_query(self, query: str, parameters: Dict[str, Any] = None):
        """Run Neo4j query asynchronously"""
        try:
            def execute_query():
                with self.driver.session() as session:
                    return session.run(query, parameters)
            
            # Execute query in thread pool
            return await asyncio.get_event_loop().run_in_executor(
                None, execute_query
            )
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise


    
   

    async def _validate_entity(self, entity: Dict[str, Any]) -> bool:
        """Validate entity data"""
        required_fields = ['id', 'type', 'properties']
        return all(field in entity for field in required_fields)

    async def _validate_relationship(self, relationship: Dict[str, Any]) -> bool:
        """Validate relationship data"""
        required_fields = ['source_id', 'target_id', 'type', 'properties']
        return all(field in relationship for field in required_fields)

    async def _validate_property(self, property_data: Dict[str, Any]) -> bool:
        """Validate property data"""
        required_fields = ['key', 'value', 'type']
        return all(field in property_data for field in required_fields)

    async def _validate_constraint(self, constraint: Dict[str, Any]) -> bool:
        """Validate constraint data"""
        required_fields = ['type', 'properties']
        return all(field in constraint for field in required_fields)

    async def _update_entity(self, entity: Dict[str, Any]) -> None:
        """Update entity in graph"""
        try:
            async with self.driver.session() as session:
                await session.run("""
                    MERGE (n:Entity {id: $id})
                    SET n += $properties
                """, entity)
        except Exception as e:
            self.logger.error(f"Entity update failed: {e}")
            raise

    async def _update_relationship(self, relationship: Dict[str, Any]) -> None:
        """Update relationship in graph"""
        try:
            async with self.driver.session() as session:
                await session.run("""
                    MATCH (s:Entity {id: $source_id})
                    MATCH (t:Entity {id: $target_id})
                    MERGE (s)-[r:RELATES_TO {type: $type}]->(t)
                    SET r += $properties
                """, relationship)
        except Exception as e:
            self.logger.error(f"Relationship update failed: {e}")
            raise

    async def _update_property(self, property_update: Dict[str, Any]) -> None:
        """Update property in graph"""
        try:
            async with self.driver.session() as session:
                await session.run("""
                    MATCH (n:Entity {id: $entity_id})
                    SET n[$key] = $value
                """, property_update)
        except Exception as e:
            self.logger.error(f"Property update failed: {e}")
            raise

    async def _batch_update(self, updates: List[Dict[str, Any]]) -> None:
        """Perform batch update of graph"""
        try:
            async with self.driver.session() as session:
                for update in updates:
                    if update['type'] == 'entity':
                        await self._update_entity(update['data'])
                    elif update['type'] == 'relationship':
                        await self._update_relationship(update['data'])
                    elif update['type'] == 'property':
                        await self._update_property(update['data'])
        except Exception as e:
            self.logger.error(f"Batch update failed: {e}")
            raise

    async def update_graph(self, data: Dict[str, Any]) -> None:
        """Update graph with new data"""
        try:
            # Validate data
            validated_data = await self._validate_data(data)
            
            # Extract entities and relationships
            entities = await self._extract_entities(validated_data)
            relationships = await self._extract_relationships(validated_data)
            
            # Update graph using async query execution
            for entity in entities:
                await self._run_query(
                    "MERGE (n:Entity {id: $id}) SET n += $properties",
                    {"id": entity["id"], "properties": entity["properties"]}
                )
            
            for rel in relationships:
                await self._run_query(
                    """
                    MATCH (a:Entity {id: $from_id})
                    MATCH (b:Entity {id: $to_id})
                    MERGE (a)-[r:RELATES]->(b)
                    SET r += $properties
                    """,
                    {
                        "from_id": rel["from_id"],
                        "to_id": rel["to_id"],
                        "properties": rel["properties"]
                    }
                )
            
            # Validate updates
            await self._validate_updates()
            
        except Exception as e:
            self.logger.error(f"Graph update failed: {e}")
            raise

    async def close(self):
        """Close database connection"""
        if self.driver:
            await asyncio.get_event_loop().run_in_executor(
                None, self.driver.close
            )



class UnifiedResourceManager:
    """Combined resource management and monitoring"""
    def __init__(self):
        self.resource_metrics = defaultdict(list)
        self.logger = logging.getLogger(__name__)
        self.monitoring = False
        self.thresholds = {
            "cpu_critical": 0.90,
            "memory_critical": 0.90,
            "disk_critical": 0.95,
            "cpu_warning": 0.80,
            "memory_warning": 0.80,
            "disk_warning": 0.85
        }
        self.current_stats = {}
        self.alert_history = []
        self.active_recoveries = set()

    async def initialize(self):
        """Initialize resource manager"""
        try:
            self.monitoring = True
            await self._initialize_resource_pools()
            self.monitoring_task = asyncio.create_task(self._monitor_resources())
            self.logger.info("Resource manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Resource manager initialization failed: {e}")
            raise

    async def _initialize_resource_pools(self):
        """Initialize resource pools"""
        try:
            self.resource_pools = {
                "cpu": {"total": psutil.cpu_count(), "allocated": 0},
                "memory": {"total": psutil.virtual_memory().total, "allocated": 0},
                "disk": {"total": psutil.disk_usage('/').total, "allocated": 0}
            }
        except Exception as e:
            self.logger.error(f"Resource pools initialization failed: {e}")
            raise

    async def _monitor_resources(self):
        """Monitor system resources"""
        while self.monitoring:
            try:
                self.current_stats.update({
                    "cpu_usage": psutil.cpu_percent() / 100,
                    "memory_usage": psutil.virtual_memory().percent / 100,
                    "disk_usage": psutil.disk_usage('/').percent / 100,
                    "network_usage": self._get_network_usage()
                })
                await self._check_thresholds()
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"Resource monitoring error: {e}")
                await asyncio.sleep(5)

    async def cleanup(self):
        """Cleanup resource manager"""
        try:
            self.monitoring = False
            if hasattr(self, 'monitoring_task'):
                self.monitoring_task.cancel()
                try:
                    await self.monitoring_task
                except asyncio.CancelledError:
                    pass
            self.logger.info("Resource manager cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Resource manager cleanup failed: {e}")
            raise


    

    async def allocate_resources(self, requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Allocate resources based on requirements"""
        try:
            allocations = {}
            for resource, amount in requirements.items():
                if resource in self.resource_pools:
                    pool = self.resource_pools[resource]
                    if pool["allocated"] + amount <= pool["total"]:
                        pool["allocated"] += amount
                        allocations[resource] = amount
                    else:
                        raise ResourceError(
                            f"Insufficient {resource} available")
            return allocations
        except Exception as e:
            self.logger.error(f"Resource allocation failed: {e}")
            raise

    def _get_network_usage(self) -> float:
        """Get network usage statistics"""
        try:
            net_io = psutil.net_io_counters()
            return (net_io.bytes_sent + net_io.bytes_recv) / 1024 / 1024  # MB
        except Exception as e:
            self.logger.error(f"Network usage check failed: {e}")
            return 0.0

    async def _check_thresholds(self):
        """Check resource usage against thresholds"""
        alerts = []
        for resource, usage in self.current_stats.items():
            critical_threshold = self.thresholds.get(f"{resource}_critical")
            warning_threshold = self.thresholds.get(f"{resource}_warning")

            if critical_threshold and usage >= critical_threshold:
                alerts.append({
                    "level": "CRITICAL",
                    "resource": resource,
                    "usage": usage,
                    "threshold": critical_threshold
                })
            elif warning_threshold and usage >= warning_threshold:
                alerts.append({
                    "level": "WARNING",
                    "resource": resource,
                    "usage": usage,
                    "threshold": warning_threshold
                })

        if alerts:
            await self._handle_alerts(alerts)

    async def _handle_alerts(self, alerts: List[Dict[str, Any]]):
        """Handle resource alerts"""
        for alert in alerts:
            self.logger.warning(f"Resource Alert: {alert}")
            # Implement alert handling logic here (e.g., scaling, notification)

    async def release_resources(self, allocations: Dict[str, Any]):
        """Release allocated resources"""
        try:
            for resource, amount in allocations.items():
                if resource in self.resource_pools:
                    pool = self.resource_pools[resource]
                    pool["allocated"] -= amount
        except Exception as e:
            self.logger.error(f"Resource release failed: {e}")
            raise


import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import defaultdict, deque
from dataclasses import dataclass, field






import logging
from typing import Dict, List, Any, Callable, Optional
from dataclasses import dataclass

@dataclass
class Evidence:
    """Data class for evidence items"""
    source: str
    content: Any
    timestamp: float
    metadata: Dict[str, Any]

class ValidationSystem:
    """System for validation and quality assurance"""
    def __init__(self):
        """Initialize validation system"""
        self.validators: Dict[str, Callable] = {}
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self._initialized = False
        self.validation_metrics = defaultdict(Counter)
        self.validation_history = []

    async def initialize(self) -> None:
        """Initialize validation system"""
        try:
            self._initialize_validators()
            self._initialized = True
            self.logger.info("Validation system initialized successfully")
        except Exception as e:
            self.logger.error(f"Validation system initialization failed: {e}")
            raise

    def _initialize_validators(self) -> None:
        """Initialize validation rules"""
        self.validators = {
            "evidence": self._validate_evidence,
            "result": self._validate_result,
            "chain": self._validate_chain
        }

    async def cleanup(self) -> None:
        """Cleanup validation system resources"""
        try:
            self.validators.clear()
            self.validation_metrics.clear()
            self.validation_history.clear()
            self._initialized = False
            self.logger.info("Validation system cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Validation system cleanup failed: {e}")
            raise


    

    async def validate_evidence(self, evidence: Evidence) -> Dict[str, Any]:
        """Public method to validate single evidence item"""
        if not self._initialized:
            raise RuntimeError("Validation system not initialized")
            
        try:
            is_valid = await self._validate_evidence(evidence)
            return {
                "valid": is_valid,
                "reason": None if is_valid else "Evidence validation failed"
            }
        except Exception as e:
            self.logger.error(f"Evidence validation failed: {e}")
            return {
                "valid": False,
                "reason": str(e)
            }

    async def validate_result(self, result: Any) -> Dict[str, Any]:
        """Public method to validate execution result"""
        if not self._initialized:
            raise RuntimeError("Validation system not initialized")
            
        try:
            is_valid = await self._validate_result(result)
            return {
                "valid": is_valid,
                "reason": None if is_valid else "Result validation failed"
            }
        except Exception as e:
            self.logger.error(f"Result validation failed: {e}")
            return {
                "valid": False,
                "reason": str(e)
            }

    async def validate_chain(self, chain: List[Evidence]) -> Dict[str, Any]:
        """Public method to validate evidence chain"""
        if not self._initialized:
            raise RuntimeError("Validation system not initialized")
            
        try:
            is_valid = await self._validate_chain(chain)
            return {
                "valid": is_valid,
                "reason": None if is_valid else "Chain validation failed"
            }
        except Exception as e:
            self.logger.error(f"Chain validation failed: {e}")
            return {
                "valid": False,
                "reason": str(e)
            }

    async def _validate_evidence(self, evidence: Evidence) -> bool:
        """Validate single evidence item"""
        try:
            # Basic validation checks
            if not evidence.source:
                return False
                
            if evidence.content is None:
                return False
                
            if not evidence.timestamp:
                return False
                
            # Add additional validation logic here
            # For example:
            # - Check evidence format
            # - Verify source authenticity
            # - Validate content structure
            # - Check timestamp is reasonable
            # - Verify required metadata fields
            
            return True
            
        except Exception as e:
            self.logger.error(f"Evidence validation error: {e}")
            return False

    async def _validate_result(self, result: Any) -> bool:
        """Validate execution result"""
        try:
            # Basic validation checks
            if result is None:
                return False
                
            # Add result-specific validation logic here
            # For example:
            # - Check result format
            # - Validate result content
            # - Verify required fields
            # - Check for reasonable values
            # - Validate against expected schema
            
            return True
            
        except Exception as e:
            self.logger.error(f"Result validation error: {e}")
            return False

    async def _validate_chain(self, chain: List[Evidence]) -> bool:
        """Validate evidence chain"""
        try:
            # Basic validation checks
            if not chain:
                return False
                
            # Validate each evidence item in the chain
            for evidence in chain:
                if not await self._validate_evidence(evidence):
                    return False
                    
            # Add chain-specific validation logic here
            # For example:
            # - Check chain continuity
            # - Verify temporal ordering
            # - Validate chain integrity
            # - Check cross-references
            # - Verify chain completeness
            
            return True
            
        except Exception as e:
            self.logger.error(f"Chain validation error: {e}")
            return False

    async def calculate_confidence(self, result: Any) -> float:
        """Calculate confidence score for a result"""
        try:
            if not self._initialized:
                raise RuntimeError("Validation system not initialized")

            # Basic confidence calculation
            confidence = 1.0
            
            # Add confidence calculation logic here
            # For example:
            # - Quality checks
            # - Consistency checks
            # - Source reliability
            # - Data completeness
            # - Validation strength
            
            return confidence
            
        except Exception as e:
            self.logger.error(f"Confidence calculation failed: {e}")
            return 0.0




class Graph:
    """Graph database wrapper"""

    def __init__(self):
        self.nodes = {}
        self.edges = {}
        self.logger = logging.getLogger(__name__)


class UnifiedLearningSystem:
    """Base learning system with comprehensive pattern recognition"""
    def __init__(self, config: 'EnhancedConfig'):
        self.config = config
        self.learning_patterns = defaultdict(list)
        self.adaptation_rules = {}
        self.performance_history = defaultdict(list)
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            "learning_iterations": 0,
            "pattern_matches": 0,
            "successful_adaptations": 0
        }
        self.pattern_cache = TTLCache(maxsize=1000, ttl=3600)
        self.continuous_learning = EnhancedContinuousLearning()

    async def initialize(self) -> None:
        """Initialize learning system components"""
        try:
            # Initialize pattern storage
            self.learning_patterns.clear()
            
            # Initialize adaptation rules
            self.adaptation_rules = {
                'pattern_recognition': self._recognize_pattern,
                'pattern_adaptation': self._adapt_pattern,
                'performance_tracking': self._track_performance
            }
            
            # Initialize continuous learning
            await self.continuous_learning.initialize()
            
            # Initialize metrics
            self.metrics = {
                "learning_iterations": 0,
                "pattern_matches": 0,
                "successful_adaptations": 0,
                "performance_scores": [],
                "adaptation_history": []
            }
            
            self.logger.info("Learning system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Learning system initialization failed: {e}")
            raise

    async def _recognize_pattern(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recognize patterns in input data"""
        try:
            # Check cache first
            cache_key = self._generate_cache_key(data)
            if cache_key in self.pattern_cache:
                self.metrics["pattern_matches"] += 1
                return self.pattern_cache[cache_key]

            # Extract features
            features = self._extract_features(data)
            
            # Classify pattern
            pattern_type = self._classify_pattern(features)
            
            # Calculate similarity scores
            similarity_scores = self._calculate_similarity_scores(features)
            
            # Analyze trends
            trends = self._analyze_trends(features)
            
            # Calculate confidence
            confidence = self._calculate_confidence(similarity_scores, trends)
            
            pattern_info = {
                "type": pattern_type,
                "features": features,
                "similarity_scores": similarity_scores,
                "trends": trends,
                "confidence": confidence,
                "timestamp": datetime.now().isoformat()
            }
            
            # Cache result
            self.pattern_cache[cache_key] = pattern_info
            
            self.metrics["pattern_matches"] += 1
            return pattern_info
            
        except Exception as e:
            self.logger.error(f"Pattern recognition failed: {e}")
            raise

    async def _adapt_pattern(self, pattern: Dict[str, Any]) -> Dict[str, Any]:
        """Adapt system behavior based on recognized pattern"""
        try:
            # Validate pattern
            if not self._validate_pattern(pattern):
                raise ValueError("Invalid pattern structure")
            
            # Generate adaptation rules
            rules = self._generate_adaptation_rules(pattern)
            
            # Apply rules
            adaptation_result = await self._apply_adaptation_rules(rules)
            
            # Validate adaptation
            if not self._validate_adaptation(adaptation_result):
                raise ValueError("Invalid adaptation result")
            
            # Update metrics
            self.metrics["successful_adaptations"] += 1
            
            return {
                "pattern": pattern,
                "rules": rules,
                "result": adaptation_result,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Pattern adaptation failed: {e}")
            raise

    async def _track_performance(self, execution_data: Dict[str, Any]) -> Dict[str, Any]:
        """Track system performance and learning progress"""
        try:
            # Extract metrics
            metrics = self._extract_performance_metrics(execution_data)
            
            # Calculate scores
            performance_scores = self._calculate_performance_scores(metrics)
            
            # Analyze trends
            performance_trends = self._analyze_performance_trends(metrics)
            
            # Update history
            self.performance_history[execution_data['id']].append({
                "metrics": metrics,
                "scores": performance_scores,
                "trends": performance_trends,
                "timestamp": datetime.now().isoformat()
            })
            
            # Update metrics
            self.metrics["performance_scores"].append(performance_scores)
            
            return {
                "current_metrics": metrics,
                "performance_scores": performance_scores,
                "trends": performance_trends,
                "history_length": len(self.performance_history)
            }
            
        except Exception as e:
            self.logger.error(f"Performance tracking failed: {e}")
            raise

    def _extract_features(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relevant features from input data"""
        features = {
            "input_type": type(data).__name__,
            "complexity": self._calculate_complexity(data),
            "structure": self._analyze_structure(data),
            "metadata": self._extract_metadata(data)
        }
        return features

    def _classify_pattern(self, features: Dict[str, Any]) -> str:
        """Classify pattern based on extracted features"""
        if features["complexity"] > 0.8:
            return "complex"
        elif features["complexity"] > 0.4:
            return "moderate"
        return "simple"

    def _calculate_similarity_scores(self, features: Dict[str, Any]) -> Dict[str, float]:
        """Calculate similarity scores against known patterns"""
        scores = {}
        for pattern_type, patterns in self.learning_patterns.items():
            if patterns:
                similarity = self._calculate_pattern_similarity(features, patterns[-1])
                scores[pattern_type] = similarity
        return scores

    def _analyze_trends(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze trends in feature patterns"""
        return {
            "complexity_trend": self._calculate_trend(features["complexity"]),
            "structure_stability": self._analyze_structure_stability(features["structure"]),
            "metadata_consistency": self._check_metadata_consistency(features["metadata"])
        }

    def _calculate_confidence(self,
                            similarity_scores: Dict[str, float],
                            trends: Dict[str, Any]) -> float:
        """Calculate confidence score for pattern recognition"""
        if not similarity_scores:
            return 0.0
        
        # Weight different factors
        similarity_weight = 0.6
        trend_weight = 0.4
        
        # Calculate weighted scores
        similarity_score = sum(similarity_scores.values()) / len(similarity_scores)
        trend_score = sum(
            1.0 if v > 0.5 else 0.0
            for v in trends.values()
        ) / len(trends)
        
        return similarity_weight * similarity_score + trend_weight * trend_score

    def _generate_cache_key(self, data: Dict[str, Any]) -> str:
        """Generate cache key for pattern recognition results"""
        return hashlib.md5(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()

    def _validate_pattern(self, pattern: Dict[str, Any]) -> bool:
        """Validate pattern structure and content"""
        required_fields = ["type", "features", "confidence"]
        return all(field in pattern for field in required_fields)

    def _validate_adaptation(self, result: Dict[str, Any]) -> bool:
        """Validate adaptation result"""
        required_fields = ["pattern", "rules", "result"]
        return all(field in result for field in required_fields)

    def _extract_performance_metrics(self, data: Dict[str, Any]) -> Dict[str, float]:
        """Extract performance metrics from execution data"""
        return {
            "execution_time": data.get("execution_time", 0.0),
            "success_rate": data.get("success_rate", 0.0),
            "error_rate": data.get("error_rate", 0.0),
            "resource_usage": data.get("resource_usage", 0.0)
        }

    def _calculate_performance_scores(self, metrics: Dict[str, float]) -> Dict[str, float]:
        """Calculate performance scores from metrics"""
        return {
            "efficiency": 1.0 - metrics["execution_time"] / 100.0,
            "reliability": 1.0 - metrics["error_rate"],
            "resource_efficiency": 1.0 - metrics["resource_usage"] / 100.0
        }

class EnhancedContinuousLearning:
    """Enhanced continuous learning system with comprehensive logging"""
    def __init__(self):
        self.learning_history = []
        self.performance_metrics = defaultdict(list)
        self.adaptation_rules = {}
        self.learning_rate = 0.01
        self.min_samples_for_adaptation = 5
        self.logger = logging.getLogger(__name__)
        self.metrics = defaultdict(Counter)
        self.initialized = False

    async def initialize(self) -> None:
        """Initialize continuous learning system"""
        try:
            # Clear existing state
            self.learning_history.clear()
            self.performance_metrics.clear()
            self.adaptation_rules.clear()
            self.metrics.clear()
            
            # Initialize metrics tracking
            self.metrics['updates'] = Counter()
            self.metrics['adaptations'] = Counter()
            self.metrics['errors'] = Counter()
            
            self.initialized = True
            self.logger.info("Continuous learning system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Continuous learning initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup continuous learning resources"""
        try:
            self.learning_history.clear()
            self.performance_metrics.clear()
            self.adaptation_rules.clear()
            self.metrics.clear()
            self.initialized = False
            self.logger.info("Continuous learning system cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Continuous learning cleanup failed: {e}")
            raise
    async def update_metrics(self, execution_id: str, result: Dict[str, Any]) -> None:
        """Update learning metrics with enhanced tracking"""
        try:
            # Calculate comprehensive metrics
            metrics = self._calculate_metrics(result)
            
            # Update performance tracking
            self.performance_metrics[execution_id] = metrics
            
            # Add to learning history with context
            self.learning_history.append({
                'execution_id': execution_id,
                'metrics': metrics,
                'timestamp': datetime.now().isoformat(),
                'context': self._extract_context(result)
            })
            
            # Update metrics
            self.metrics['updates']['total'] += 1
            self.logger.debug(f"Updated metrics for execution {execution_id}")
            
            # Update adaptation rules if enough samples
            if len(self.learning_history) >= self.min_samples_for_adaptation:
                await self._update_adaptation_rules()
                self.metrics['adaptations']['total'] += 1
                self.logger.info("Updated adaptation rules based on new data")
                
        except Exception as e:
            self.logger.error(f"Metrics update failed: {e}")
            self.metrics['errors']['update_failed'] += 1
            raise

    def _calculate_metrics(self, result: Dict[str, Any]) -> Dict[str, float]:
        """Calculate comprehensive performance metrics"""
        try:
            metrics = {}
            
            # Response quality metrics
            if 'response' in result:
                metrics['response_quality'] = self._calculate_response_quality(
                    result['response']
                )
            
            # Execution metrics
            if 'execution_time' in result:
                metrics['execution_efficiency'] = self._calculate_efficiency(
                    result['execution_time']
                )
            
            # Resource usage metrics
            if 'resource_usage' in result:
                metrics['resource_efficiency'] = self._calculate_resource_efficiency(
                    result['resource_usage']
                )
            
            self.logger.debug(f"Calculated metrics: {metrics}")
            return metrics
            
        except Exception as e:
            self.logger.error(f"Metrics calculation failed: {e}")
            self.metrics['errors']['calculation_failed'] += 1
            return {}

    def _extract_context(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relevant context from result"""
        try:
            context = {}
            
            # Extract task information
            if 'task' in result:
                context['task_type'] = self._classify_task(result['task'])
            
            # Extract execution context
            if 'execution_context' in result:
                context['execution_environment'] = result['execution_context']
            
            # Extract agent information
            if 'agent_info' in result:
                context['agent_type'] = result['agent_info'].get('type')
                context['agent_layer'] = result['agent_info'].get('layer')
            
            self.logger.debug(f"Extracted context: {context}")
            return context
            
        except Exception as e:
            self.logger.error(f"Context extraction failed: {e}")
            self.metrics['errors']['context_failed'] += 1
            return {}

    async def _update_adaptation_rules(self) -> None:
        """Update adaptation rules based on learning history"""
        try:
            recent_history = self.learning_history[-self.min_samples_for_adaptation:]
            
            # Analyze performance trends
            trends = self._analyze_performance_trends(recent_history)
            
            # Update rules based on trends
            for metric, trend in trends.items():
                if abs(trend) > self.learning_rate:
                    self.adaptation_rules[metric] = {
                        'trend': trend,
                        'adaptation': self._generate_adaptation_strategy(metric, trend),
                        'timestamp': datetime.now().isoformat()
                    }
            
            self.logger.info(f"Updated {len(trends)} adaptation rules")
            
        except Exception as e:
            self.logger.error(f"Adaptation rules update failed: {e}")
            self.metrics['errors']['adaptation_failed'] += 1
            raise

    def _calculate_response_quality(self, response) -> float:
        """Calculate response quality metric"""
        # Implement response quality calculation logic
        return 0.5

    def _calculate_efficiency(self, execution_time: float) -> float:
        """Calculate execution efficiency metric"""
        return 1.0 / execution_time if execution_time > 0 else 1.0

    def _calculate_resource_efficiency(self, resource_usage: Dict[str, Any]) -> float:
        """Calculate resource efficiency metric"""
        # Implement resource efficiency calculation logic
        return 0.8

    def _classify_task(self, task: Any) -> str:
        """Classify task type"""
        # Implement task classification logic
        return "default_task"

    def _analyze_performance_trends(self, history: List[Dict[str, Any]]) -> Dict[str, float]:
        """Analyze performance trends from history"""
        try:
            trends = {}
            
            # Calculate trends for each metric
            for metric in self.performance_metrics:
                values = [h['metrics'].get(metric, 0) for h in history]
                if values:
                    trends[metric] = self._calculate_trend(values)
            
            self.logger.debug(f"Analyzed trends: {trends}")
            return trends
            
        except Exception as e:
            self.logger.error(f"Trend analysis failed: {e}")
            self.metrics['errors']['trend_analysis_failed'] += 1
            return {}

    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate trend from list of values"""
        if len(values) < 2:
            return 0.0
            
        try:
            # Calculate simple linear trend
            x = list(range(len(values)))
            y = values
            
            # Calculate slope using numpy if available
            if np:
                slope, _ = np.polyfit(x, y, 1)
                return slope
            
            # Fallback to simple calculation
            n = len(values)
            mean_x = sum(x) / n
            mean_y = sum(y) / n
            
            numerator = sum((xi - mean_x) * (yi - mean_y)
                          for xi, yi in zip(x, y))
            denominator = sum((xi - mean_x) ** 2 for xi in x)
            
            return numerator / denominator if denominator != 0 else 0.0
            
        except Exception as e:
            self.logger.error(f"Trend calculation failed: {e}")
            return 0.0

    def _generate_adaptation_strategy(self, metric: str, trend: float) -> str:
        """Generate adaptation strategy based on metric and trend"""
        if trend > 0:
            return f"Increase resource allocation for {metric}"
        else:
            return f"Decrease resource allocation for {metric}"


from openai import AsyncOpenAI, OpenAI
from typing import Dict, Any, Optional, List, Union
import aiohttp
import asyncio
from datetime import datetime, timedelta

class UnifiedLLMManager:
    """Advanced LLM management with rate limiting, caching, and error handling"""
    def __init__(self, config: 'EnhancedConfig'):
        self.config = config
        self.cache = {}
        self.logger = logging.getLogger(__name__)
        self.rate_limiters = {
            'openai': RateLimiter(60, 60),    # 60 requests per minute
            'anthropic': RateLimiter(10, 1),   # 10 requests per second
            'vertex': RateLimiter(100, 60),    # 100 requests per minute
            'mistral': RateLimiter(50, 60)     # 50 requests per minute
        }
        self.metrics = defaultdict(dict)
        self.error_counts = defaultdict(int)
        self.clients = {}
        self.initialized = False
        self.session = None

    async def initialize(self) -> None:
        """Initialize LLM manager with proper client setup"""
        try:
            # Create aiohttp session for async requests
            self.session = aiohttp.ClientSession()

            # Initialize API clients
            if self.config.api_settings.vertex_ai_credentials:
                try:
                    credentials = service_account.Credentials.from_service_account_info(
                        self.config.api_settings.vertex_ai_credentials
                    )
                    vertexai.init(
                        project=self.config.api_settings.vertex_ai_project_id,
                        location=self.config.api_settings.vertex_ai_location,
                        credentials=credentials
                    )
                    self.logger.info("Vertex AI initialized successfully")
                except Exception as e:
                    self.logger.error(f"Vertex AI initialization failed: {e}")
                    raise

            # Initialize OpenAI client
            if self.config.api_settings.openai_api_key:
                try:
                    self.clients['openai'] = AsyncOpenAI(
                        api_key=self.config.api_settings.openai_api_key
                    )
                    self.logger.info("OpenAI client initialized successfully")
                except Exception as e:
                    self.logger.error(f"OpenAI client initialization failed: {e}")
                    raise

            # Initialize Anthropic client
            if self.config.api_settings.anthropic_api_key:
                try:
                    self.clients['anthropic'] = anthropic.AsyncAnthropicVertex()
                    self.logger.info("Anthropic client initialized successfully")
                except Exception as e:
                    self.logger.error(f"Anthropic client initialization failed: {e}")
                    raise

            self.initialized = True
            self.logger.info("LLM manager initialized successfully")

        except Exception as e:
            self.logger.error(f"LLM manager initialization failed: {e}")
            await self.cleanup()
            raise

    async def cleanup(self) -> None:
        """Cleanup manager resources"""
        try:
            # Close aiohttp session
            if self.session:
                await self.session.close()

            # Cleanup clients
            for client_name, client in self.clients.items():
                try:
                    if hasattr(client, 'close'):
                        await client.close()
                    self.logger.info(f"{client_name} client closed successfully")
                except Exception as e:
                    self.logger.error(f"Error closing {client_name} client: {e}")

            # Clear cache and metrics
            self.cache.clear()
            self.metrics.clear()
            self.error_counts.clear()

            self.initialized = False
            self.logger.info("LLM manager cleaned up successfully")

        except Exception as e:
            self.logger.error(f"LLM manager cleanup failed: {e}")
            raise

    async def generate_response(self,
                              model: str,
                              prompt: str,
                              max_tokens: Optional[int] = None,
                              **kwargs) -> Dict[str, Any]:
        """Generate LLM response with comprehensive handling"""
        if not self.initialized:
            raise RuntimeError("LLM manager not initialized")

        start_time = time.time()
        cache_key = self._generate_cache_key(model, prompt, max_tokens, kwargs)

        try:
            # Check cache
            if cached := self.cache.get(cache_key):
                self._update_metrics(model, 'cache_hit', time.time() - start_time)
                return cached

            # Get appropriate rate limiter
            provider = self._get_provider(model)
            await self.rate_limiters[provider].wait()

            # Generate response
            response = await self._route_to_provider(
                model, prompt, max_tokens, **kwargs)

            # Cache response
            self.cache[cache_key] = response

            # Update metrics
            self._update_metrics(model, 'success', time.time() - start_time)

            return response

        except Exception as e:
            self._handle_error(model, e)
            raise

    def _get_provider(self, model: str) -> str:
        """Determine the provider for a given model"""
        if "gpt" in model.lower():
            return 'openai'
        elif "claude" in model.lower():
            return 'anthropic'
        elif "gemini" in model.lower():
            return 'vertex'
        elif "mistral" in model.lower():
            return 'mistral'
        else:
            raise ValueError(f"Unsupported model: {model}")

    def _generate_cache_key(self, model: str, prompt: str, max_tokens: Optional[int], kwargs: Dict) -> str:
        """Generate a unique cache key for a request"""
        key_parts = [model, prompt, str(max_tokens), json.dumps(kwargs, sort_keys=True)]
        return hashlib.md5("".join(key_parts).encode()).hexdigest()

    def _update_metrics(self, model: str, status: str, duration: float):
        """Update LLM usage metrics"""
        self.metrics[model][status] = self.metrics[model].get(status, 0) + 1
        self.metrics[model]['avg_duration'] = (
            self.metrics[model].get('avg_duration', 0) + duration
        ) / self.metrics[model].get(status, 1)

    def _handle_error(self, model: str, error: Exception):
        """Handle LLM errors with logging and potential retries"""
        self.logger.error(f"LLM Error ({model}): {error}")
        self.error_counts[model] += 1

        
class ErrorHandler:
    """Handles system-wide error management and recovery"""
    def __init__(self):
        self.error_history = []
        self.recovery_strategies = {}
        self.metrics = defaultdict(Counter)
        self.logger = logging.getLogger(__name__)

    async def handle_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Handle errors with comprehensive recovery attempts."""
        error_id = f"error_{datetime.now().timestamp()}"
        
        try:
            # Log error details
            self.logger.error(f"Error {error_id}: {str(error)}")
            
            # Store error information
            self.error_history.append({
                'id': error_id,
                'error': str(error),
                'type': type(error).__name__,
                'context': context,
                'timestamp': datetime.now().isoformat()
            })
            
            # Update metrics
            self.metrics['errors'][type(error).__name__] += 1
            
            # Attempt recovery
            await self._attempt_recovery(error_id, error, context)
            
        except Exception as e:
            self.logger.error(f"Error handling failed: {str(e)}")
            raise

    async def _attempt_recovery(self, error_id: str, error: Exception, context: Dict[str, Any]) -> None:
        """Attempt to recover from error using appropriate strategy."""
        try:
            # Select recovery strategy
            strategy = self._select_recovery_strategy(error)
            
            # Execute recovery
            recovery_result = await self._execute_recovery(strategy, context)
            
            # Update recovery metrics
            self.metrics['recovery_attempts'][strategy] += 1
            if recovery_result.get('success'):
                self.metrics['successful_recoveries'][strategy] += 1
            
            # Log recovery attempt
            self.logger.info(f"Recovery attempt for error {error_id}: {recovery_result}")
            
        except Exception as e:
            self.logger.error(f"Recovery attempt failed: {str(e)}")
            self.metrics['failed_recoveries'] += 1

    async def initialize(self) -> None:
        """Initialize error handler with recovery strategies"""
        try:
            # Initialize recovery strategies
            await self._initialize_recovery_strategies()
            
            # Initialize error database
            await self._initialize_error_database()
            
            # Initialize monitoring
            await self._initialize_monitoring()
            
            self.initialized = True
            self.logger.info("Error handler initialized successfully")
        except Exception as e:
            self.logger.error(f"Error handler initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup error handler resources"""
        try:
            # Clear error database
            self.error_database.clear()
            
            # Clear history
            self.error_history.clear()
            self.recovery_history.clear()
            
            # Clear active recoveries
            self.active_recoveries.clear()
            
            # Clear cache
            self.cache.clear()
            
            self.initialized = False
            self.logger.info("Error handler cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Error handler cleanup failed: {e}")
            raise

    async def _initialize_recovery_strategies(self) -> None:
        """Initialize recovery strategies"""
        try:
            self.recovery_strategies = {
                'retry': self._retry_operation,
                'fallback': self._fallback_operation,
                'reset': self._reset_state,
                'compensate': self._compensating_action
            }
            self.logger.info("Recovery strategies initialized")
        except Exception as e:
            self.logger.error(f"Recovery strategies initialization failed: {e}")
            raise

    async def _initialize_error_database(self) -> None:
        """Initialize error database"""
        try:
            self.error_database = {}
            self.error_patterns = defaultdict(int)
            self.error_correlations = defaultdict(list)
            self.logger.info("Error database initialized")
        except Exception as e:
            self.logger.error(f"Error database initialization failed: {e}")
            raise

    async def _initialize_monitoring(self) -> None:
        """Initialize error monitoring system"""
        try:
            self.monitoring_config = {
                'alert_threshold': 5,
                'monitoring_interval': 60,
                'metrics_enabled': True
            }
            self.logger.info("Error monitoring initialized")
        except Exception as e:
            self.logger.error(f"Monitoring initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup error handler resources"""
        try:
            # Clear error database
            self.error_database.clear()
            
            # Clear history
            self.error_history.clear()
            self.recovery_history.clear()
            
            # Clear active recoveries
            self.active_recoveries.clear()
            
            # Clear cache
            self.cache.clear()
            
            self.initialized = False
            self.logger.info("Error handler cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Error handler cleanup failed: {e}")
            raise

    async def handle_error(self, error: Exception, context: Dict[str, Any]) -> Optional[Any]:
        """Handle errors with sophisticated recovery"""
        if not self.initialized:
            raise RuntimeError("Error handler not initialized")
            
        error_id = f"error_{datetime.now().timestamp()}"
        start_time = time.time()
        
        try:
            # Record error
            self._record_error(error_id, error, context)
            
            # Analyze error
            error_analysis = self._analyze_error(error)
            
            # Select recovery strategy
            strategy = await self._select_recovery_strategy(error_analysis)
            
            # Execute recovery
            recovery_result = await self._execute_recovery(strategy, context)
            
            # Update metrics
            self._update_recovery_metrics(error_id, start_time)
            
            return recovery_result
            
        except Exception as e:
            self.logger.error(f"Error handling failed: {e}")
            self.metrics["failed_recoveries"] += 1
            raise

    async def _retry_operation(self, context: Dict[str, Any]) -> Optional[Any]:
        """Retry failed operation with exponential backoff"""
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
                
                # Attempt operation
                result = await self._execute_operation(context)
                return result
                
            except Exception as e:
                self.logger.warning(f"Retry attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise

    async def _fallback_operation(self, context: Dict[str, Any]) -> Optional[Any]:
        """Execute fallback operation"""
        try:
            # Check for cached fallback
            cache_key = self._generate_cache_key(context)
            if cache_key in self.cache:
                return self.cache[cache_key]
            
            # Execute fallback logic
            fallback_result = await self._execute_fallback(context)
            
            # Cache successful fallback
            if fallback_result:
                self.cache[cache_key] = fallback_result
            
            return fallback_result
            
        except Exception as e:
            self.logger.error(f"Fallback operation failed: {e}")
            raise

    async def _reset_state(self, context: Dict[str, Any]) -> Optional[Any]:
        """Reset system state"""
        try:
            # Backup current state
            state_backup = self._backup_state(context)
            
            # Reset state
            await self._perform_reset(context)
            
            # Verify reset
            if await self._verify_reset(context):
                return True
            else:
                # Restore backup if verification fails
                await self._restore_state(state_backup)
                return False
                
        except Exception as e:
            self.logger.error(f"State reset failed: {e}")
            raise

    def _record_error(self, error_id: str, error: Exception, context: Dict[str, Any]):
        """Record error details"""
        self.error_database[error_id] = {
            "error": str(error),
            "type": type(error).__name__,
            "context": context,
            "timestamp": datetime.now().isoformat(),
            "status": "unresolved"
        }
        self.metrics["total_errors"] += 1

    def _analyze_error(self, error: Exception) -> Dict[str, Any]:
        """Analyze error and determine characteristics"""
        return {
            "type": type(error).__name__,
            "severity": self._determine_severity(error),
            "recoverable": self._is_recoverable(error),
            "timestamp": datetime.now().isoformat()
        }

    def _determine_severity(self, error: Exception) -> str:
        """Determine error severity"""
        if isinstance(error, (SystemError, MemoryError)):
            return "critical"
        elif isinstance(error, (ValueError, TypeError)):
            return "high"
        elif isinstance(error, (TimeoutError, ConnectionError)):
            return "medium"
        else:
            return "low"

    def _is_recoverable(self, error: Exception) -> bool:
        """Determine if error is recoverable"""
        return not isinstance(error, (SystemError, MemoryError))

    def _generate_cache_key(self, context: Dict[str, Any]) -> str:
        """Generate cache key from context"""
        return hashlib.md5(str(sorted(context.items())).encode()).hexdigest()

    def get_error_statistics(self) -> Dict[str, Any]:
        """Get error handling statistics"""
        return {
            "total_errors": self.metrics["total_errors"],
            "successful_recoveries": self.metrics["successful_recoveries"],
            "failed_recoveries": self.metrics["failed_recoveries"],
            "average_recovery_time": np.mean(self.metrics["recovery_times"])
                if self.metrics["recovery_times"] else 0,
            "error_patterns": dict(self.error_patterns)
        }
    async def _compensating_action(self, context: Dict[str, Any]) -> Optional[Any]:
        """Execute compensating action to handle errors"""
        try:
            # Record compensation attempt
            compensation_id = f"comp_{datetime.now().timestamp()}"
            
            # Execute compensation
            compensation_result = await self._execute_compensation(context)
            
            # Verify compensation
            if await self._verify_compensation(compensation_result):
                self.logger.info(f"Compensation {compensation_id} successful")
                return compensation_result
                
            self.logger.warning(f"Compensation {compensation_id} failed verification")
            return None
            
        except Exception as e:
            self.logger.error(f"Compensation action failed: {e}")
            raise

    async def _execute_compensation(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute specific compensation logic"""
        try:
            compensation_type = context.get('compensation_type', 'default')
            
            compensation_strategies = {
                'default': self._default_compensation,
                'rollback': self._rollback_compensation,
                'retry': self._retry_compensation,
                'alternate': self._alternate_compensation
            }
            
            if compensation_type not in compensation_strategies:
                raise ValueError(f"Unknown compensation type: {compensation_type}")
                
            return await compensation_strategies[compensation_type](context)
            
        except Exception as e:
            self.logger.error(f"Compensation execution failed: {e}")
            raise

    async def _verify_compensation(self, result: Dict[str, Any]) -> bool:
        """Verify compensation result"""
        try:
            if not result:
                return False
                
            required_fields = ['status', 'action_taken', 'verification']
            if not all(field in result for field in required_fields):
                return False
                
            if result['status'] != 'completed':
                return False
                
            if not result['verification']:
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Compensation verification failed: {e}")
            return False

    async def _default_compensation(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Default compensation strategy"""
        return {
            'status': 'completed',
            'action_taken': 'default_compensation',
            'verification': True,
            'timestamp': datetime.now().isoformat()
        }

    async def _rollback_compensation(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Rollback compensation strategy"""
        try:
            # Get rollback point
            rollback_point = context.get('rollback_point')
            if not rollback_point:
                raise ValueError("No rollback point specified")
                
            # Execute rollback
            await self._execute_rollback(rollback_point)
            
            return {
                'status': 'completed',
                'action_taken': 'rollback',
                'verification': True,
                'rollback_point': rollback_point,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Rollback compensation failed: {e}")
            raise

    async def _retry_compensation(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Retry compensation strategy"""
        try:
            max_retries = context.get('max_retries', 3)
            retry_delay = context.get('retry_delay', 1)
            
            for attempt in range(max_retries):
                try:
                    # Execute retry
                    result = await self._execute_retry(context)
                    
                    return {
                        'status': 'completed',
                        'action_taken': 'retry',
                        'verification': True,
                        'attempt': attempt + 1,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                except Exception as retry_error:
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(retry_delay * (2 ** attempt))
                    
        except Exception as e:
            self.logger.error(f"Retry compensation failed: {e}")
            raise

    async def _alternate_compensation(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Alternate path compensation strategy"""
        try:
            # Get alternate path
            alternate_path = context.get('alternate_path')
            if not alternate_path:
                raise ValueError("No alternate path specified")
                
            # Execute alternate path
            result = await self._execute_alternate_path(alternate_path, context)
            
            return {
                'status': 'completed',
                'action_taken': 'alternate_path',
                'verification': True,
                'alternate_path': alternate_path,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Alternate path compensation failed: {e}")
            raise

    async def _execute_rollback(self, rollback_point: str) -> None:
        """Execute rollback to specified point"""
        self.logger.info(f"Executing rollback to {rollback_point}")
        # Implement rollback logic here
        pass

    async def _execute_retry(self, context: Dict[str, Any]) -> Any:
        """Execute retry operation"""
        self.logger.info("Executing retry operation")
        # Implement retry logic here
        pass

    async def _execute_alternate_path(self, alternate_path: str, context: Dict[str, Any]) -> Any:
        """Execute alternate path"""
        self.logger.info(f"Executing alternate path: {alternate_path}")
        # Implement alternate path logic here
        pass

    async def _execute_operation(self, context: Dict[str, Any]) -> Any:
        """Execute operation with proper error handling"""
        self.logger.info("Executing operation")
        # Implement operation execution logic here
        pass

    async def _execute_fallback(self, context: Dict[str, Any]) -> Any:
        """Execute fallback operation"""
        self.logger.info("Executing fallback operation")
        # Implement fallback logic here
        pass

    async def _backup_state(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Backup current state"""
        self.logger.info("Backing up current state")
        # Implement state backup logic here
        return {}

    async def _perform_reset(self, context: Dict[str, Any]) -> None:
        """Perform state reset"""
        self.logger.info("Performing state reset")
        # Implement reset logic here
        pass

    async def _verify_reset(self, context: Dict[str, Any]) -> bool:
        """Verify state reset"""
        self.logger.info("Verifying state reset")
        # Implement reset verification logic here
        return True

    async def _restore_state(self, state_backup: Dict[str, Any]) -> None:
        """Restore state from backup"""
        self.logger.info("Restoring state from backup")
        # Implement state restoration logic here
        pass


class GraphManager:
    """Manages graph operations using NetworkX"""
    def __init__(self):
        self.graph = nx.DiGraph()
        self.logger = logging.getLogger(__name__)
        self.node_types = set()
        self.edge_types = set()
        self.metadata = defaultdict(dict)
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.initialized = False
        self.metrics = defaultdict(Counter)

    async def initialize(self) -> None:
        """Initialize graph manager components"""
        try:
            # Initialize graph components
            await self._initialize_graph_components()
            
            # Initialize indices
            await self._initialize_indices()
            
            # Initialize validation rules
            await self._initialize_validation_rules()
            
            self.initialized = True
            self.logger.info("Graph manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Graph manager initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup graph manager resources"""
        try:
            # Clear graph
            self.graph.clear()
            
            # Clear metadata
            self.metadata.clear()
            
            # Clear cache
            self.cache.clear()
            
            # Clear metrics
            self.metrics.clear()
            
            # Clear type sets
            self.node_types.clear()
            self.edge_types.clear()
            
            self.initialized = False
            self.logger.info("Graph manager cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Graph manager cleanup failed: {e}")
            raise
    async def _initialize_graph_components(self) -> None:
        """Initialize core graph components"""
        try:
            # Initialize graph structure
            self.graph = nx.DiGraph()
            
            # Initialize indices
            self.indices = {
                'node_label': {},
                'edge_type': {},
                'property': defaultdict(dict)
            }
            
            # Initialize constraints
            self.constraints = {
                'unique': set(),
                'required': set(),
                'type': {}
            }
            
            self.logger.info("Graph components initialized")
        except Exception as e:
            self.logger.error(f"Graph components initialization failed: {e}")
            raise

    async def _initialize_indices(self) -> None:
        """Initialize graph indices"""
        try:
            # Create indices for common properties
            self.indices['node_label']['type'] = defaultdict(set)
            self.indices['edge_type']['type'] = defaultdict(set)
            self.indices['property']['node'] = defaultdict(lambda: defaultdict(set))
            self.indices['property']['edge'] = defaultdict(lambda: defaultdict(set))
            
            self.logger.info("Graph indices initialized")
        except Exception as e:
            self.logger.error(f"Indices initialization failed: {e}")
            raise

    async def _initialize_validation_rules(self) -> None:
        """Initialize validation rules"""
        try:
            self.validation_rules = {
                'node': self._validate_node,
                'edge': self._validate_edge,
                'property': self._validate_property,
                'constraint': self._validate_constraint
            }
            self.logger.info("Validation rules initialized")
        except Exception as e:
            self.logger.error(f"Validation rules initialization failed: {e}")
            raise

    def add_node(self, node_id: str, attributes: Dict[str, Any]) -> None:
        """Add a node to the graph with validation"""
        if not self.initialized:
            raise RuntimeError("Graph manager not initialized")
            
        try:
            # Validate node attributes
            self._validate_node_attributes(attributes)
            
            # Add node to graph
            self.graph.add_node(node_id, **attributes)
            
            # Update indices
            self._update_node_indices(node_id, attributes)
            
            # Track node type
            node_type = attributes.get('type', 'default')
            self.node_types.add(node_type)
            
            # Store metadata
            self.metadata['nodes'][node_id] = {
                'created_at': datetime.now().isoformat(),
                'type': node_type,
                'attributes': attributes
            }
            
            self.logger.info(f"Node {node_id} added successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to add node: {e}")
            raise

    def add_edge(self, source_id: str, target_id: str, attributes: Dict[str, Any]) -> None:
        """Add an edge to the graph with validation"""
        if not self.initialized:
            raise RuntimeError("Graph manager not initialized")
            
        try:
            # Validate nodes exist
            if not all(self.graph.has_node(node) for node in [source_id, target_id]):
                raise ValueError("Source or target node does not exist")
                
            # Validate edge attributes
            self._validate_edge_attributes(attributes)
            
            # Add edge to graph
            self.graph.add_edge(source_id, target_id, **attributes)
            
            # Update indices
            self._update_edge_indices(source_id, target_id, attributes)
            
            # Track edge type
            edge_type = attributes.get('type', 'default')
            self.edge_types.add(edge_type)
            
            # Store metadata
            edge_key = f"{source_id}->{target_id}"
            self.metadata['edges'][edge_key] = {
                'created_at': datetime.now().isoformat(),
                'type': edge_type,
                'attributes': attributes
            }
            
            self.logger.info(f"Edge {edge_key} added successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to add edge: {e}")
            raise

    def _validate_node_attributes(self, attributes: Dict[str, Any]) -> None:
        """Validate node attributes"""
        try:
            # Check required attributes
            required_fields = ['type']
            if not all(field in attributes for field in required_fields):
                raise ValueError(f"Missing required attributes: {required_fields}")
                
            # Validate attribute types
            for key, value in attributes.items():
                if not self._validate_attribute_type(key, value):
                    raise ValueError(f"Invalid attribute type for {key}")
                    
        except Exception as e:
            self.logger.error(f"Node attribute validation failed: {e}")
            raise

    def _validate_edge_attributes(self, attributes: Dict[str, Any]) -> None:
        """Validate edge attributes"""
        try:
            # Check required attributes
            required_fields = ['type']
            if not all(field in attributes for field in required_fields):
                raise ValueError(f"Missing required attributes: {required_fields}")
                
            # Validate attribute types
            for key, value in attributes.items():
                if not self._validate_attribute_type(key, value):
                    raise ValueError(f"Invalid attribute type for {key}")
                    
        except Exception as e:
            self.logger.error(f"Edge attribute validation failed: {e}")
            raise

    def _validate_attribute_type(self, key: str, value: Any) -> bool:
        """Validate attribute type"""
        valid_types = {
            str: ['type', 'label', 'name', 'description'],
            int: ['weight', 'count', 'index'],
            float: ['score', 'probability', 'confidence'],
            bool: ['active', 'valid', 'enabled'],
            datetime: ['timestamp', 'created_at', 'updated_at']
        }
        
        for type_, keys in valid_types.items():
            if key in keys and not isinstance(value, type_):
                return False
        return True

    def _update_node_indices(self, node_id: str, attributes: Dict[str, Any]) -> None:
        """Update node indices"""
        try:
            # Update label index
            node_type = attributes.get('type', 'default')
            self.indices['node_label']['type'][node_type].add(node_id)
            
            # Update property indices
            for key, value in attributes.items():
                self.indices['property']['node'][key][value].add(node_id)
                
        except Exception as e:
            self.logger.error(f"Node index update failed: {e}")
            raise

    def _update_edge_indices(self, source_id: str, target_id: str, attributes: Dict[str, Any]) -> None:
        """Update edge indices"""
        try:
            # Update type index
            edge_type = attributes.get('type', 'default')
            edge_key = f"{source_id}->{target_id}"
            self.indices['edge_type']['type'][edge_type].add(edge_key)
            
            # Update property indices
            for key, value in attributes.items():
                self.indices['property']['edge'][key][value].add(edge_key)
                
        except Exception as e:
            self.logger.error(f"Edge index update failed: {e}")
            raise

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get node attributes"""
        if self.graph.has_node(node_id):
            return dict(self.graph.nodes[node_id])
        return None

    def get_edge(self, source_id: str, target_id: str) -> Optional[Dict[str, Any]]:
        """Get edge attributes"""
        if self.graph.has_edge(source_id, target_id):
            return dict(self.graph.edges[source_id, target_id])
        return None

    def get_neighbors(self, node_id: str) -> List[str]:
        """Get neighbors of a node"""
        if self.graph.has_node(node_id):
            return list(self.graph.neighbors(node_id))
        return []

    def get_graph_statistics(self) -> Dict[str, Any]:
        """Get graph statistics"""
        return {
            "node_count": self.graph.number_of_nodes(),
            "edge_count": self.graph.number_of_edges(),
            "node_types": len(self.node_types),
            "edge_types": len(self.edge_types),
            "density": nx.density(self.graph),
            "average_degree": sum(dict(self.graph.degree()).values()) / self.graph.number_of_nodes()
                if self.graph.number_of_nodes() > 0 else 0
        }

    async def _validate_node(self, node_data: Dict[str, Any]) -> bool:
        """Validate node data structure and content"""
        try:
            # Check required fields
            required_fields = ['id', 'type', 'properties']
            if not all(field in node_data for field in required_fields):
                self.logger.error(f"Missing required fields in node data: {required_fields}")
                return False

            # Validate ID format
            if not isinstance(node_data['id'], str) or not node_data['id']:
                self.logger.error("Invalid node ID format")
                return False

            # Validate type
            if not isinstance(node_data['type'], str) or not node_data['type']:
                self.logger.error("Invalid node type format")
                return False

            # Validate properties
            if not isinstance(node_data['properties'], dict):
                self.logger.error("Invalid properties format")
                return False

            # Validate property types
            for key, value in node_data['properties'].items():
                if not self._validate_property_type(key, value):
                    self.logger.error(f"Invalid property type for {key}")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Node validation failed: {e}")
            return False

    async def _validate_edge(self, edge_data: Dict[str, Any]) -> bool:
        """Validate edge data structure and content"""
        try:
            # Check required fields
            required_fields = ['source_id', 'target_id', 'type', 'properties']
            if not all(field in edge_data for field in required_fields):
                self.logger.error(f"Missing required fields in edge data: {required_fields}")
                return False

            # Validate source and target IDs
            if not all(isinstance(edge_data[field], str) and edge_data[field]
                      for field in ['source_id', 'target_id']):
                self.logger.error("Invalid source or target ID format")
                return False

            # Validate type
            if not isinstance(edge_data['type'], str) or not edge_data['type']:
                self.logger.error("Invalid edge type format")
                return False

            # Validate properties
            if not isinstance(edge_data['properties'], dict):
                self.logger.error("Invalid properties format")
                return False

            # Validate property types
            for key, value in edge_data['properties'].items():
                if not self._validate_property_type(key, value):
                    self.logger.error(f"Invalid property type for {key}")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Edge validation failed: {e}")
            return False

    async def _validate_property(self, property_data: Dict[str, Any]) -> bool:
        """Validate property data structure and content"""
        try:
            # Check required fields
            required_fields = ['key', 'value', 'type']
            if not all(field in property_data for field in required_fields):
                self.logger.error(f"Missing required fields in property data: {required_fields}")
                return False

            # Validate key
            if not isinstance(property_data['key'], str) or not property_data['key']:
                self.logger.error("Invalid property key format")
                return False

            # Validate type
            if not isinstance(property_data['type'], str) or not property_data['type']:
                self.logger.error("Invalid property type format")
                return False

            # Validate value based on type
            if not self._validate_property_value(property_data['value'], property_data['type']):
                self.logger.error("Invalid property value for specified type")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Property validation failed: {e}")
            return False

    async def _validate_constraint(self, constraint_data: Dict[str, Any]) -> bool:
        """Validate constraint data structure and content"""
        try:
            # Check required fields
            required_fields = ['type', 'target', 'properties']
            if not all(field in constraint_data for field in required_fields):
                self.logger.error(f"Missing required fields in constraint data: {required_fields}")
                return False

            # Validate constraint type
            valid_constraint_types = ['unique', 'required', 'range', 'regex']
            if constraint_data['type'] not in valid_constraint_types:
                self.logger.error(f"Invalid constraint type: {constraint_data['type']}")
                return False

            # Validate target
            valid_targets = ['node', 'edge', 'property']
            if constraint_data['target'] not in valid_targets:
                self.logger.error(f"Invalid constraint target: {constraint_data['target']}")
                return False

            # Validate properties based on constraint type
            if not self._validate_constraint_properties(
                constraint_data['type'],
                constraint_data['properties']
            ):
                self.logger.error("Invalid constraint properties")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Constraint validation failed: {e}")
            return False

    def _validate_property_type(self, key: str, value: Any) -> bool:
        """Validate property type"""
        try:
            valid_types = {
                str: ['name', 'description', 'label', 'type'],
                int: ['count', 'index', 'weight'],
                float: ['score', 'confidence', 'probability'],
                bool: ['active', 'valid', 'enabled'],
                list: ['tags', 'categories', 'aliases'],
                dict: ['metadata', 'config', 'settings']
            }

            for type_, valid_keys in valid_types.items():
                if key in valid_keys and not isinstance(value, type_):
                    return False
            return True

        except Exception as e:
            self.logger.error(f"Property type validation failed: {e}")
            return False

    def _validate_property_value(self, value: Any, property_type: str) -> bool:
        """Validate property value based on type"""
        try:
            type_validators = {
                'string': lambda x: isinstance(x, str),
                'integer': lambda x: isinstance(x, int),
                'float': lambda x: isinstance(x, float),
                'boolean': lambda x: isinstance(x, bool),
                'list': lambda x: isinstance(x, list),
                'dict': lambda x: isinstance(x, dict),
                'datetime': lambda x: isinstance(x, (datetime, str)) and self._validate_datetime(x)
            }

            validator = type_validators.get(property_type)
            if not validator:
                return False

            return validator(value)

        except Exception as e:
            self.logger.error(f"Property value validation failed: {e}")
            return False

    def _validate_constraint_properties(self, constraint_type: str, properties: Dict[str, Any]) -> bool:
        """Validate constraint properties based on type"""
        try:
            validators = {
                'unique': self._validate_unique_constraint,
                'required': self._validate_required_constraint,
                'range': self._validate_range_constraint,
                'regex': self._validate_regex_constraint
            }

            validator = validators.get(constraint_type)
            if not validator:
                return False

            return validator(properties)

        except Exception as e:
            self.logger.error(f"Constraint properties validation failed: {e}")
            return False

    def _validate_unique_constraint(self, properties: Dict[str, Any]) -> bool:
        """Validate unique constraint properties"""
        return isinstance(properties.get('property_name'), str)

    def _validate_required_constraint(self, properties: Dict[str, Any]) -> bool:
        """Validate required constraint properties"""
        return isinstance(properties.get('property_names'), list)

    def _validate_range_constraint(self, properties: Dict[str, Any]) -> bool:
        """Validate range constraint properties"""
        return (
            'min_value' in properties and
            'max_value' in properties and
            isinstance(properties['min_value'], (int, float)) and
            isinstance(properties['max_value'], (int, float)) and
            properties['min_value'] <= properties['max_value']
        )

    def _validate_regex_constraint(self, properties: Dict[str, Any]) -> bool:
        """Validate regex constraint properties"""
        try:
            pattern = properties.get('pattern')
            if not isinstance(pattern, str):
                return False
            re.compile(pattern)
            return True
        except re.error:
            return False

    def _validate_datetime(self, value: Union[datetime, str]) -> bool:
        """Validate datetime value"""
        if isinstance(value, datetime):
            return True
        try:
            datetime.fromisoformat(value)
            return True
        except (ValueError, TypeError):
            return False
class FeedbackLoop:
    """Feedback loop system for continuous learning and improvement"""
    def __init__(self):
        self.feedback_history = []
        self.adaptation_rules = {}
        self.learning_rate = 0.01
        self.logger = logging.getLogger(__name__)
        self.metrics = defaultdict(Counter)
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.initialized = False
        self.min_samples_for_adaptation = 5
        self.performance_metrics = defaultdict(list)
        self.learning_rules = {}

    async def initialize(self) -> None:
        """Initialize feedback loop system"""
        try:
            # Initialize feedback components
            await self._initialize_feedback_components()
            
            # Initialize learning rules
            await self._initialize_learning_rules()
            
            # Initialize adaptation rules
            await self._initialize_adaptation_rules()
            
            self.initialized = True
            self.logger.info("Feedback loop system initialized successfully")
        except Exception as e:
            self.logger.error(f"Feedback loop initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup feedback loop resources"""
        try:
            # Clear history
            self.feedback_history.clear()
            
            # Clear rules
            self.adaptation_rules.clear()
            self.learning_rules.clear()
            
            # Clear metrics
            self.metrics.clear()
            self.performance_metrics.clear()
            
            # Clear cache
            self.cache.clear()
            
            self.initialized = False
            self.logger.info("Feedback loop system cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Feedback loop cleanup failed: {e}")
            raise

    async def _initialize_feedback_components(self) -> None:
        """Initialize feedback system components"""
        try:
            # Initialize metrics tracking
            self.metrics = {
                'updates': Counter(),
                'adaptations': Counter(),
                'errors': Counter()
            }
            
            # Initialize performance tracking
            self.performance_metrics = defaultdict(list)
            
            # Initialize feedback history
            self.feedback_history = []
            
            self.logger.info("Feedback components initialized")
        except Exception as e:
            self.logger.error(f"Feedback components initialization failed: {e}")
            raise

    async def _initialize_learning_rules(self) -> None:
        """Initialize learning rules"""
        try:
            self.learning_rules = {
                'performance': self._update_performance_rules,
                'adaptation': self._update_adaptation_rules,
                'optimization': self._update_optimization_rules
            }
            self.logger.info("Learning rules initialized")
        except Exception as e:
            self.logger.error(f"Learning rules initialization failed: {e}")
            raise

    async def _initialize_adaptation_rules(self) -> None:
        """Initialize adaptation rules"""
        try:
            self.adaptation_rules = {
                'performance': self._adapt_performance,
                'resource': self._adapt_resources,
                'strategy': self._adapt_strategy
            }
            self.logger.info("Adaptation rules initialized")
        except Exception as e:
            self.logger.error(f"Adaptation rules initialization failed: {e}")
            raise

    async def process_feedback(self, feedback: Dict[str, Any]) -> None:
        """Process feedback and update adaptation rules"""
        if not self.initialized:
            raise RuntimeError("Feedback loop system not initialized")
            
        try:
            # Record feedback
            self.feedback_history.append({
                'feedback': feedback,
                'timestamp': datetime.now().isoformat()
            })
            
            # Update metrics
            self._update_metrics(feedback)
            
            # Update learning rules if enough samples
            if len(self.feedback_history) >= self.min_samples_for_adaptation:
                await self._update_learning_rules(feedback)
                
            # Apply adaptations
            await self._apply_adaptations(feedback)
            
        except Exception as e:
            self.logger.error(f"Feedback processing failed: {e}")
            raise

    async def _update_learning_rules(self, feedback: Dict[str, Any]) -> None:
        """Update learning rules based on feedback"""
        try:
            for rule_type, update_func in self.learning_rules.items():
                await update_func(feedback)
            self.metrics['updates']['learning_rules'] += 1
        except Exception as e:
            self.logger.error(f"Learning rules update failed: {e}")
            raise

    async def _apply_adaptations(self, feedback: Dict[str, Any]) -> None:
        """Apply adaptation rules based on feedback"""
        try:
            for rule_type, adapt_func in self.adaptation_rules.items():
                await adapt_func(feedback)
            self.metrics['adaptations']['applied'] += 1
        except Exception as e:
            self.logger.error(f"Adaptations application failed: {e}")
            raise

    async def _update_performance_rules(self, feedback: Dict[str, Any]) -> None:
        """Update performance-based learning rules"""
        try:
            performance_metrics = feedback.get('performance', {})
            for metric, value in performance_metrics.items():
                self.performance_metrics[metric].append(value)
        except Exception as e:
            self.logger.error(f"Performance rules update failed: {e}")
            raise

    async def _update_adaptation_rules(self, feedback: Dict[str, Any]) -> None:
        """Update adaptation rules based on feedback"""
        try:
            adaptation_metrics = feedback.get('adaptation', {})
            for metric, value in adaptation_metrics.items():
                if value > self.learning_rate:
                    self.adaptation_rules[metric] = {
                        'trend': value,
                        'adaptation': self._generate_adaptation_strategy(metric, value),
                        'timestamp': datetime.now().isoformat()
                    }
        except Exception as e:
            self.logger.error(f"Adaptation rules update failed: {e}")
            raise

    async def _update_optimization_rules(self, feedback: Dict[str, Any]) -> None:
        """Update optimization rules based on feedback"""
        try:
            optimization_metrics = feedback.get('optimization', {})
            for metric, value in optimization_metrics.items():
                self.learning_rules[metric] = {
                    'value': value,
                    'timestamp': datetime.now().isoformat()
                }
        except Exception as e:
            self.logger.error(f"Optimization rules update failed: {e}")
            raise

    async def _adapt_performance(self, feedback: Dict[str, Any]) -> None:
        """Adapt system based on performance feedback"""
        try:
            performance_data = feedback.get('performance', {})
            for metric, value in performance_data.items():
                if self._should_adapt(metric, value):
                    await self._apply_performance_adaptation(metric, value)
        except Exception as e:
            self.logger.error(f"Performance adaptation failed: {e}")
            raise

    async def _adapt_resources(self, feedback: Dict[str, Any]) -> None:
        """Adapt resource allocation based on feedback"""
        try:
            resource_data = feedback.get('resources', {})
            for resource, usage in resource_data.items():
                if self._should_adapt_resource(resource, usage):
                    await self._apply_resource_adaptation(resource, usage)
        except Exception as e:
            self.logger.error(f"Resource adaptation failed: {e}")
            raise

    async def _adapt_strategy(self, feedback: Dict[str, Any]) -> None:
        """Adapt strategy based on feedback"""
        try:
            strategy_data = feedback.get('strategy', {})
            for strategy, effectiveness in strategy_data.items():
                if self._should_adapt_strategy(strategy, effectiveness):
                    await self._apply_strategy_adaptation(strategy, effectiveness)
        except Exception as e:
            self.logger.error(f"Strategy adaptation failed: {e}")
            raise

    def _should_adapt(self, metric: str, value: float) -> bool:
        """Determine if adaptation is needed"""
        if not self.performance_metrics[metric]:
            return False
        avg_value = np.mean(self.performance_metrics[metric])
        return abs(value - avg_value) > self.learning_rate

    def _should_adapt_resource(self, resource: str, usage: float) -> bool:
        """Determine if resource adaptation is needed"""
        return usage > 0.8 or usage < 0.2

    def _should_adapt_strategy(self, strategy: str, effectiveness: float) -> bool:
        """Determine if strategy adaptation is needed"""
        return effectiveness < 0.5

    def _generate_adaptation_strategy(self, metric: str, trend: float) -> str:
        """Generate adaptation strategy based on metric and trend"""
        if trend > 0:
            return f"Increase resource allocation for {metric}"
        else:
            return f"Decrease resource allocation for {metric}"

    def _update_metrics(self, feedback: Dict[str, Any]) -> None:
        """Update system metrics based on feedback"""
        try:
            for category, values in feedback.items():
                if isinstance(values, dict):
                    for metric, value in values.items():
                        self.metrics[category][metric] += value
        except Exception as e:
            self.logger.error(f"Metrics update failed: {e}")
            raise

    def get_feedback_statistics(self) -> Dict[str, Any]:
        """Get feedback loop statistics"""
        return {
            "total_feedback": len(self.feedback_history),
            "adaptations": dict(self.metrics['adaptations']),
            "updates": dict(self.metrics['updates']),
            "errors": dict(self.metrics['errors']),
            "performance_metrics": {
                metric: np.mean(values) for metric, values in self.performance_metrics.items()
                if values
            }
        }
class ResourceManager:
    """Combined resource management and monitoring"""

    def __init__(self):
        self.resource_allocation = defaultdict(dict)
        self.resource_usage = defaultdict(list)
        self.allocation_history = []
        self.logger = logging.getLogger(__name__)
        self.monitoring = False
        self.thresholds = {
            "cpu_critical": 0.90,
            "cpu_warning": 0.80,
            "memory_critical": 0.90,
            "memory_warning": 0.80,
            "disk_critical": 0.95,
            "disk_warning": 0.85
        }

    async def initialize(self):
        """Initialize resource manager"""
        try:
            await self._initialize_resource_monitoring()
            await self._initialize_resource_pools()
            self.logger.info("Resource manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Resource manager initialization failed: {e}")
            raise

    async def _initialize_resource_monitoring(self):
        """Initialize resource monitoring"""
        try:
            self.monitoring = True
            self.current_stats = {
                "cpu_usage": 0.0,
                "memory_usage": 0.0,
                "disk_usage": 0.0,
                "network_usage": 0.0
            }
            self.monitoring_task = asyncio.create_task(
                self._monitor_resources())
        except Exception as e:
            self.logger.error(
                f"Resource monitoring initialization failed: {e}")
            raise

    async def _initialize_resource_pools(self):
        """Initialize resource pools"""
        try:
            self.resource_pools = {
                "cpu": {"total": psutil.cpu_count(), "allocated": 0},
                "memory": {"total": psutil.virtual_memory().total, "allocated": 0},
                "disk": {"total": psutil.disk_usage('/').total, "allocated": 0}
            }
        except Exception as e:
            self.logger.error(f"Resource pools initialization failed: {e}")
            raise

    async def _monitor_resources(self):
        """Monitor system resources"""
        while self.monitoring:
            try:
                self.current_stats.update({
                    "cpu_usage": psutil.cpu_percent() / 100,
                    "memory_usage": psutil.virtual_memory().percent / 100,
                    "disk_usage": psutil.disk_usage('/').percent / 100,
                    "network_usage": self._get_network_usage()
                })

                await self._check_thresholds()
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"Resource monitoring error: {e}")
                await asyncio.sleep(5)  # Back off on error

    def _get_network_usage(self) -> float:
        """Get network usage statistics"""
        try:
            net_io = psutil.net_io_counters()
            return (net_io.bytes_sent + net_io.bytes_recv) / 1024 / 1024  # MB
        except Exception as e:
            self.logger.error(f"Network usage check failed: {e}")
            return 0.0

    async def _check_thresholds(self):
        """Check resource usage against thresholds"""
        alerts = []
        for resource, usage in self.current_stats.items():
            critical_threshold = self.thresholds.get(f"{resource}_critical")
            warning_threshold = self.thresholds.get(f"{resource}_warning")

            if critical_threshold and usage >= critical_threshold:
                alerts.append({
                    "level": "CRITICAL",
                    "resource": resource,
                    "usage": usage,
                    "threshold": critical_threshold
                })
            elif warning_threshold and usage >= warning_threshold:
                alerts.append({
                    "level": "WARNING",
                    "resource": resource,
                    "usage": usage,
                    "threshold": warning_threshold
                })

        if alerts:
            await self._handle_alerts(alerts)

    async def _handle_alerts(self, alerts: List[Dict[str, Any]]):
        """Handle resource alerts"""
        for alert in alerts:
            self.logger.warning(f"Resource Alert: {alert}")
            # Implement alert handling logic here (e.g., scaling, notification)




class RateLimiter:
    """Enhanced Rate Limiter with metrics tracking"""
    def __init__(self, max_requests: int, time_window: int):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = asyncio.Lock()
        self.metrics = defaultdict(int)

    async def acquire(self):
        """Acquire rate limit token with metrics tracking"""
        async with self.lock:
            now = time.time()
            self.requests = [t for t in self.requests if t > now - self.time_window]
            
            if len(self.requests) >= self.max_requests:
                wait_time = self.requests[0] - (now - self.time_window)
                self.metrics['throttled_requests'] += 1
                await asyncio.sleep(wait_time)
            
            self.requests.append(now)
            self.metrics['total_requests'] += 1

    async def cleanup(self):
        """Cleanup rate limiter resources"""
        self.requests.clear()
        self.metrics.clear()

    def get_metrics(self) -> Dict[str, int]:
        """Get rate limiter metrics"""
        return dict(self.metrics)


# Cell 6.3 Execution Manager Agentic Workflow
import numpy as np
import networkx as nx
from typing import Dict, List, Any, Optional, Union, Tuple, Set, Callable, TypeVar, Protocol
import logging
import asyncio
import time
from datetime import datetime, timedelta
from collections import defaultdict, deque, Counter
import json
import os
import nest_asyncio
import base64
from tenacity import retry, stop_after_attempt, wait_exponential
import psutil
from google.cloud import bigquery, storage, exceptions
from google.oauth2 import service_account
from openai import AsyncOpenAI
import aiohttp
from dotenv import load_dotenv
from cachetools import TTLCache
from dataclasses import dataclass, field, asdict
import vertexai
from vertexai.generative_models import GenerativeModel
import anthropic
from neo4j import GraphDatabase
import hashlib
import matplotlib.pyplot as plt



# Initialize basic configuration
load_dotenv()
nest_asyncio.apply()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


NEO4J_URI = "neo4j+s://6fdaa9bb.databases.neo4j.io"

class ExecutionManager:
    """Manages workflow execution with comprehensive error handling"""
    def __init__(self, config: 'EnhancedConfig'):
        self.config = config
        self.active_workflows = {}
        self.logger = logging.getLogger(__name__)
        self.resource_manager = UnifiedResourceManager()
        self.evidence_store = EvidenceStore()
        self.checkpoint_manager = EnhancedCheckpointManager()
        self.initialized = False
        self.system_metrics = defaultdict(dict)

    async def initialize(self) -> None:
        """Initialize all system components"""
        try:
            # Initialize core components
            await self.resource_manager.initialize()
            await self.evidence_store.initialize()
            await self.checkpoint_manager.initialize()
            
            self.initialized = True
            self.logger.info("Execution manager initialized successfully")
        except Exception as e:
            self.logger.error(f"System initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup execution manager resources"""
        try:
            # Cleanup active workflows
            for workflow_id in list(self.active_workflows.keys()):
                await self._cleanup_workflow(workflow_id)
            
            # Cleanup components
            await self.resource_manager.cleanup()
            await self.evidence_store.cleanup()
            await self.checkpoint_manager.cleanup()
            
            # Clear collections
            self.active_workflows.clear()
            self.system_metrics.clear()
            
            self.initialized = False
            self.logger.info("Execution manager cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Execution manager cleanup failed: {e}")
            raise

    async def execute_workflow(self, workflow_config: WorkflowConfig) -> Dict[str, Any]:
        """Execute workflow with comprehensive monitoring and error handling"""
        if not self.initialized:
            raise RuntimeError("Execution manager not initialized")
            
        workflow_id = f"workflow_{datetime.now().timestamp()}"
        
        try:
            # Validate configuration
            self._validate_workflow_config(workflow_config)
            
            # Allocate resources
            resources = await self.resource_manager.allocate_resources(
                workflow_config.resource_requirements
            )
            
            # Execute steps
            results = []
            for step in workflow_config.steps:
                step_result = await self._execute_step(step, workflow_id)
                results.append(step_result)
                
                # Store checkpoint after each step
                await self.checkpoint_manager.save_checkpoint(
                    f"{workflow_id}_step_{len(results)}",
                    step_result
                )
            
            return {
                'workflow_id': workflow_id,
                'results': results,
                'metadata': self._generate_execution_metadata(workflow_id)
            }
            
        except Exception as e:
            self.logger.error(f"Workflow execution failed: {e}")
            await self._handle_workflow_error(workflow_id, e)
            raise
        finally:
            await self._cleanup_workflow(workflow_id, resources)

    async def _initialize_monitoring(self) -> None:
        """Initialize execution monitoring"""
        try:
            self.monitoring_config = {
                'metrics_enabled': True,
                'performance_tracking': True,
                'resource_monitoring': True
            }
            self.logger.info("Execution monitoring initialized")
        except Exception as e:
            self.logger.error(f"Monitoring initialization failed: {e}")
            raise

    def _validate_workflow_config(self, workflow_config: WorkflowConfig) -> None:
        """Validate workflow configuration"""
        if not workflow_config.steps:
            raise ValueError("Workflow configuration must have at least one step")

    async def _execute_step(self, step: Dict[str, Any], workflow_id: str) -> Dict[str, Any]:
        """Execute individual workflow step with error handling"""
        step_id = f"{workflow_id}_step_{step.get('id', str(time.time()))}"
        
        try:
            # Execute step
            result = await self._process_step(step)
            
            # Store evidence
            await self.evidence_store.store_evidence(
                step_id,
                result,
                {'workflow_id': workflow_id}
            )
            
            return {
                'step_id': step_id,
                'result': result,
                'status': 'completed',
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Step execution failed: {e}")
            await self._handle_step_error(step_id, e)
            raise

    async def _process_step(self, step: Dict[str, Any]) -> Any:
        """Process a single step in the workflow"""
        # Implement step processing logic
        pass

    

    async def execute_workflow(self, workflow_id: str, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute workflow with enhanced boss agent coordination."""
        if not self.initialized:
            raise RuntimeError("Workflow orchestrator not initialized")
            
        try:
            # Create execution plan through boss agent
            plan = await self.boss_agent.create_plan(tasks)
            
            # Execute through MoA layers
            results = await self._execute_through_layers(plan)
            
            # Store execution evidence
            await self.evidence_store.store_evidence(
                workflow_id,
                results,
                {'workflow_id': workflow_id}
            )
            
            return {
                'workflow_id': workflow_id,
                'results': results,
                'metadata': self._generate_execution_metadata(workflow_id)
            }
            
        except Exception as e:
            self.logger.error(f"Workflow execution failed: {str(e)}")
            await self._handle_workflow_error(workflow_id, e)
            raise

    async def _execute_through_layers(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute plan through MoA layers"""
        try:
            results = {}
            for layer_id in sorted(self.layer_agents.keys()):
                layer_results = []
                for agent in self.layer_agents[layer_id]:
                    result = await agent.process_task(plan)
                    layer_results.append(result)
                results[layer_id] = layer_results
            return results
        except Exception as e:
            self.logger.error(f"Layer execution failed: {str(e)}")
            raise
    async def _handle_step_error(self, step_id: str, error: Exception) -> None:
        """Handle step execution errors"""
        try:
            await self.evidence_store.store_evidence(
                f"error_{step_id}",
                {'error': str(error)},
                {'type': 'error'}
            )
            self.metrics["step_errors"] += 1
        except Exception as e:
            self.logger.error(f"Error handling failed: {e}")

    async def _cleanup_workflow(self, workflow_id: str, resources: Optional[Dict[str, Any]] = None) -> None:
        """Cleanup workflow resources"""
        try:
            if resources:
                await self.resource_manager.release_resources(resources)
            if workflow_id in self.active_workflows:
                del self.active_workflows[workflow_id]
        except Exception as e:
            self.logger.error(f"Workflow cleanup failed: {e}")

    def _generate_execution_metadata(self, workflow_id: str) -> Dict[str, Any]:
        """Generate workflow execution metadata"""
        return {
            'workflow_id': workflow_id,
            'start_time': datetime.now().isoformat(),
            'metrics': dict(self.metrics)
        }

# ------------------------------------
# WorkflowOrchestrator (your code)
# ------------------------------------
class WorkflowOrchestrator:
    """Enhanced workflow orchestrator with proper initialization tracking"""
    def __init__(self, config: 'EnhancedConfig'):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize state tracking
        self._initializing = False
        self.initialized = False
        
        # Initialize core components
        self.boss_agent = None
        self.layer_agents = defaultdict(list)
        self.evidence_store = None
        self.metrics = defaultdict(Counter)
        
        # Initialize communication components
        self.communication_system = None
        self.message_queue = asyncio.Queue()
        
        # Initialize task tracking
        self.active_tasks = {}
        self.task_history = []
        
        # Initialize performance tracking
        self.performance_metrics = defaultdict(list)
        
        # Initialize caching
        self.cache = TTLCache(maxsize=1000, ttl=3600)

    async def initialize(self) -> None:
        """Initialize workflow orchestrator with proper state tracking"""
        if self._initializing or self.initialized:
            return

        self._initializing = True
        try:
            # Initialize boss agent
            boss_config = self.config.get_boss_config()
            self.boss_agent = BossAgent(
                name="MainBoss",
                model_info=boss_config.to_dict(),
                config=self.config
            )
            await self.boss_agent.initialize()
            
            # Initialize layer agents
            await self._initialize_layer_agents()
            
            # Initialize evidence store
            self.evidence_store = EvidenceStore(self.config.evidence_store_config)
            await self.evidence_store.initialize()
            
            # Initialize communication system
            self.communication_system = CommunicationSystem()
            await self.communication_system.initialize()
            
            self.initialized = True
            self.logger.info("Workflow orchestrator initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Workflow orchestrator initialization failed: {e}")
            await self.cleanup()
            raise
        finally:
            self._initializing = False

    async def _initialize_layer_agents(self) -> None:
        """Initialize layer agents with proper error handling"""
        try:
            for layer_id in range(1, 5):  # Layers 1-4
                layer_config = self.config.get_layer_config(layer_id)
                if not layer_config:
                    continue
                
                self.layer_agents[layer_id] = []
                for agent_name in layer_config.agents:
                    agent = await self._create_layer_agent(agent_name, layer_id)
                    if agent:
                        self.layer_agents[layer_id].append(agent)
                        self.logger.info(f"Created agent {agent_name} for layer {layer_id}")

            self.logger.info(f"Initialized {sum(len(agents) for agents in self.layer_agents.values())} layer agents")
        except Exception as e:
            self.logger.error(f"Layer agent initialization failed: {e}")
            raise

    async def _create_layer_agent(self, agent_name: str, layer_id: int) -> Optional[BaseAgent]:
        """Create and initialize a layer agent"""
        try:
            model_config = self.config.get_model_config(agent_name)
            if not model_config:
                self.logger.warning(f"No model configuration found for agent {agent_name}")
                return None

            agent_class = self._get_agent_class(agent_name)
            agent = agent_class(
                name=f"{agent_name}_{layer_id}",
                model_info=model_config.to_dict(),
                config=self.config
            )
            await agent.initialize()
            return agent
        except Exception as e:
            self.logger.error(f"Agent creation failed for {agent_name}: {e}")
            return None

    def _get_agent_class(self, agent_type: str) -> Type[BaseAgent]:
        """Get the appropriate agent class based on type"""
        agent_classes = {
            'Layer1Agent': Layer1Agent,
            'Layer2Agent': Layer2Agent,
            'Layer3Agent': Layer3Agent,
            'Layer4Agent': Layer4Agent
        }
        
        if agent_type not in agent_classes:
            raise ValueError(f"Unknown agent type: {agent_type}")
        
        return agent_classes[agent_type]

    async def execute_workflow(self, workflow_id: str, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute workflow with comprehensive monitoring"""
        if not self.initialized:
            raise RuntimeError("Workflow orchestrator not initialized")
            
        try:
            # Create execution plan through boss agent
            plan = await self.boss_agent.create_plan(tasks)
            
            # Process through layers
            results = await self._process_through_layers(plan)
            
            # Store execution evidence
            await self.evidence_store.store_evidence(
                workflow_id,
                results,
                {'workflow_id': workflow_id}
            )
            
            return {
                'workflow_id': workflow_id,
                'results': results,
                'metadata': self._generate_execution_metadata(workflow_id)
            }
            
        except Exception as e:
            self.logger.error(f"Workflow execution failed: {str(e)}")
            await self._handle_workflow_error(workflow_id, e)
            raise

    async def cleanup(self) -> None:
        """Cleanup orchestrator resources"""
        try:
            # Cleanup boss agent
            if self.boss_agent:
                await self.boss_agent.cleanup()
            
            # Cleanup layer agents
            for agents in self.layer_agents.values():
                for agent in agents:
                    await agent.cleanup()
            
            # Cleanup evidence store
            if self.evidence_store:
                await self.evidence_store.cleanup()
            
            # Cleanup communication system
            if self.communication_system:
                await self.communication_system.cleanup()
            
            # Clear collections
            self.layer_agents.clear()
            self.active_tasks.clear()
            self.task_history.clear()
            self.cache.clear()
            
            self.initialized = False
            self._initializing = False
            
            self.logger.info("Workflow orchestrator cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Workflow orchestrator cleanup failed: {e}")
            raise

    def _generate_execution_metadata(self, workflow_id: str) -> Dict[str, Any]:
        """Generate workflow execution metadata"""
        return {
            'workflow_id': workflow_id,
            'timestamp': datetime.now().isoformat(),
            'agent_counts': {
                layer_id: len(agents)
                for layer_id, agents in self.layer_agents.items()
            },
            'metrics': dict(self.metrics)
        }

    async def _handle_workflow_error(self, workflow_id: str, error: Exception) -> None:
        """Handle workflow execution errors"""
        try:
            await self.evidence_store.store_evidence(
                f"error_{workflow_id}",
                {
                    'error': str(error),
                    'traceback': traceback.format_exc(),
                    'timestamp': datetime.now().isoformat()
                },
                {'type': 'workflow_error'}
            )
        except Exception as e:
            self.logger.error(f"Error handling failed: {e}")

    # Add example workflow method for testing
    async def run_example_workflow(self) -> None:
        """Run example workflow to demonstrate agent communication"""
        try:
            # Example task
            task_data = {
                'type': 'data_processing',
                'priority': 'high',
                'data': {'sample': 'data'}
            }

            # Boss agent task assignment
            await self.boss_agent.send_to_next_layer({
                'type': 'task_assignment',
                'target_layer': 2,
                'priority': 'high',
                'task': task_data
            })

            # Layer agent processing
            for layer_id, agents in self.layer_agents.items():
                for agent in agents:
                    result = await agent.process_task(task_data)
                    await agent.send_to_next_layer({
                        'type': 'processed_data',
                        'data': result
                    })

            # System update broadcast
            await self.boss_agent.broadcast_to_all_layers({
                'type': 'system_update',
                'update': 'New configuration loaded'
            })

        except Exception as e:
            self.logger.error(f"Example workflow failed: {e}")
            raise
    
    

# Import required libraries
import asyncio
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
import uuid
import json
from collections import defaultdict, Counter

# Message Types Enum
class MessageType(Enum):
    """Enumeration of message types for precise communication"""
    TASK_ASSIGNMENT = auto()
    TASK_RESULT = auto()
    CONTEXT_UPDATE = auto()
    ERROR_NOTIFICATION = auto()
    RESOURCE_REQUEST = auto()
    COLLABORATION_REQUEST = auto()

# Agent Message Class
@dataclass
class AgentMessage:
    """Comprehensive message structure for agent communication"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender: str = ''
    receiver: str = ''
    message_type: MessageType = MessageType.TASK_ASSIGNMENT
    content: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    context: Dict[str, Any] = field(default_factory=dict)
    priority: int = 1
    trace_id: Optional[str] = None

    def to_json(self) -> str:
        """Convert message to JSON for serialization"""
        return json.dumps({
            'id': self.id,
            'sender': self.sender,
            'receiver': self.receiver,
            'message_type': self.message_type.name,
            'content': self.content,
            'timestamp': self.timestamp.isoformat(),
            'context': self.context,
            'priority': self.priority,
            'trace_id': self.trace_id
        })

    @classmethod
    def from_json(cls, json_str: str) -> 'AgentMessage':
        """Create message from JSON string"""
        data = json.loads(json_str)
        message = cls(
            id=data['id'],
            sender=data['sender'],
            receiver=data['receiver'],
            message_type=MessageType[data['message_type']],
            content=data['content'],
            timestamp=datetime.fromisoformat(data['timestamp']),
            context=data['context'],
            priority=data['priority'],
            trace_id=data['trace_id']
        )
        return message
import logging
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from datetime import datetime
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class SessionManager:
    """Manages aiohttp client sessions"""
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(__name__)

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp client session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def cleanup(self) -> None:
        """Cleanup session resources"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.info("Session closed successfully")


from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum, auto


@dataclass
class Message:
    """Message class for communication between agents"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender: str = ''
    receiver: str = ''
    message_type: MessageType = MessageType.TASK_ASSIGNMENT
    content: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    context: Dict[str, Any] = field(default_factory=dict)
    priority: int = 1
    trace_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary format"""
        return {
            'id': self.id,
            'sender': self.sender,
            'receiver': self.receiver,
            'message_type': self.message_type.name,
            'content': self.content,
            'timestamp': self.timestamp.isoformat(),
            'context': self.context,
            'priority': self.priority,
            'trace_id': self.trace_id
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create message from dictionary"""
        return cls(
            id=data['id'],
            sender=data['sender'],
            receiver=data['receiver'],
            message_type=MessageType[data['message_type']],
            content=data['content'],
            timestamp=datetime.fromisoformat(data['timestamp']),
            context=data['context'],
            priority=data['priority'],
            trace_id=data['trace_id']
        )


# Context Manager Class
class ContextManager:
    """
    Manages context sharing and propagation between agents
    Supports versioning, inheritance, and intelligent context merging
    """
    def __init__(self, name="ContextManager"):
        self.context_store: Dict[str, Dict[str, Any]] = {}
        self.context_versions: Dict[str, int] = defaultdict(int)
        self.logger = logging.getLogger(name)

    def update_context(self, agent_name: str, context: Dict[str, Any]):
        """
        Update context for an agent with versioning
        
        Args:
            agent_name (str): Agent whose context is being updated
            context (Dict[str, Any]): New context information
        """
        current_version = self.context_versions[agent_name]
        new_version = current_version + 1
        
        self.context_store[agent_name] = {
            'version': new_version,
            'data': context,
            'timestamp': datetime.now()
        }
        self.context_versions[agent_name] = new_version

    def get_context(self, agent_name: str, version: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieve context for an agent
        
        Args:
            agent_name (str): Agent whose context is being retrieved
            version (Optional[int]): Specific version to retrieve
        
        Returns:
            Optional[Dict[str, Any]]: Context data or None
        """
        if agent_name not in self.context_store:
            return None
        
        if version is None or version == self.context_store[agent_name]['version']:
            return self.context_store[agent_name]['data']
        
        return None

    def merge_contexts(self, contexts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Intelligently merge multiple contexts
        
        Args:
            contexts (List[Dict[str, Any]]): Contexts to merge
        
        Returns:
            Dict[str, Any]: Merged context
        """
        merged_context = {}
        for context in contexts:
            for key, value in context.items():
                if key not in merged_context:
                    merged_context[key] = value
                elif isinstance(merged_context[key], dict) and isinstance(value, dict):
                    merged_context[key] = {**merged_context[key], **value}
                elif isinstance(merged_context[key], list) and isinstance(value, list):
                    merged_context[key].extend(value)
        return merged_context



class AgentCommunicationMixin:
    """Mixin class for agent communication capabilities"""
    def __init__(self, communication_system: CommunicationSystem):
        self.communication_system = communication_system
        self.session_manager = communication_system.session_manager
        self.logger = logging.getLogger(__name__)

    async def send_message(self, receiver: str, message_type: str, content: Any):
        """Send message to another agent"""
        try:
            await self.communication_system.send_message(
                self.name,
                receiver,
                {
                    'type': message_type,
                    'content': content,
                    'timestamp': datetime.now().isoformat()
                }
            )
        except Exception as e:
            self.logger.error(f"Message sending failed: {e}")
            raise

    async def receive_message(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """Receive message with timeout"""
        try:
            return await self.communication_system.receive_message(self.name, timeout)
        except Exception as e:
            self.logger.error(f"Message receiving failed: {e}")
            raise
    
class REWOOPlanningSystem:
    """Enhanced planning system with REWOO capabilities."""
    def __init__(self):
        self.evidence_store = EvidenceStore()
        self.context_cache = {}
        self.logger = logging.getLogger(__name__)

    async def initialize(self):
        """Initialize planning system."""
        try:
            await self.evidence_store.initialize()
            self.context_cache = {}
            self.logger.info("REWOO planning system initialized successfully")
        except Exception as e:
            self.logger.error(f"REWOO planning system initialization failed: {e}")
            raise

    async def _parse_task(self, task: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Parse task input into standardized format."""
        try:
            if isinstance(task, str):
                return {
                    "type": "general",
                    "description": task,
                    "requirements": [
                        {
                            "description": "Process and complete the task",
                            "requirements": [],
                            "dependencies": [],
                            "evidence_requirements": [
                                {
                                    "type": "execution_history",
                                    "description": "Previous similar task executions",
                                    "source": "evidence_store"
                                }
                            ]
                        }
                    ]
                }
            return task
        except Exception as e:
            self.logger.error(f"Task parsing failed: {e}")
            raise

    async def _analyze_requirements(self, task: Union[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze task requirements with proper type handling."""
        try:
            parsed_task = await self._parse_task(task)
            requirements = []
            
            if 'requirements' in parsed_task:
                for req in parsed_task['requirements']:
                    if isinstance(req, dict):
                        requirement = {
                            'description': req.get('description', ''),
                            'requirements': req.get('requirements', []),
                            'dependencies': req.get('dependencies', []),
                            'evidence_requirements': req.get('evidence_requirements', [])
                        }
                    else:
                        requirement = {
                            'description': str(req),
                            'requirements': [],
                            'dependencies': [],
                            'evidence_requirements': []
                        }
                    requirements.append(requirement)
            
            return requirements
        except Exception as e:
            self.logger.error(f"Requirements analysis failed: {e}")
            raise

    async def create_plan(self, task: Union[str, Dict[str, Any]], context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create REWOO plan with proper task parsing."""
        try:
            parsed_task = await self._parse_task(task)
            
            # Observe current state
            observation = await self._observe_world_state(context or {})
            
            # Retrieve relevant context
            retrieved_context = await self._retrieve_relevant_context(parsed_task, observation)
            
            # Generate plan steps
            plan_steps = await self._generate_plan_steps(parsed_task, retrieved_context)
            
            # Create evidence collection strategy
            evidence_strategy = await self._plan_evidence_collection(parsed_task)
            
            return {
                "task": parsed_task,
                "observation": observation,
                "context": retrieved_context,
                "steps": plan_steps,
                "evidence_strategy": evidence_strategy,
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "planner": "rewoo",
                    "version": "1.0"
                }
            }
        except Exception as e:
            self.logger.error(f"REWOO plan creation failed: {e}")
            raise

    async def _observe_world_state(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Observe current world state."""
        try:
            return {
                "timestamp": datetime.now().isoformat(),
                "context": context,
                "evidence": await self.evidence_store.get_available_evidence(),
                "recent_updates": await self.evidence_store.get_recent_additions(),
                "validation_status": await self.evidence_store.get_validation_status()
            }
        except Exception as e:
            self.logger.error(f"World state observation failed: {e}")
            raise

    async def _retrieve_relevant_context(self, task: Dict[str, Any], observation: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve relevant context for planning."""
        try:
            task_type = task.get('type', 'general')
            
            # Check cache first
            if task_type in self.context_cache:
                return self.context_cache[task_type]
            
            context = {
                'task_history': await self.evidence_store.get_task_history(task_type),
                'current_state': observation,
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'task_type': task_type
                }
            }
            
            # Cache the context
            self.context_cache[task_type] = context
            return context
        except Exception as e:
            self.logger.error(f"Context retrieval failed: {e}")
            raise

    async def _plan_evidence_collection(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Plan evidence collection strategy."""
        try:
            evidence_plan = {
                'required_evidence': [],
                'collection_strategy': {},
                'validation_rules': {}
            }
            
            # Identify required evidence
            for subtask in await self._decompose_task(task):
                for evidence_req in subtask['evidence_requirements']:
                    evidence_plan['required_evidence'].append({
                        'type': evidence_req['type'],
                        'description': evidence_req['description'],
                        'source': evidence_req.get('source', 'any'),
                        'validation_rules': evidence_req.get('validation_rules', {})
                    })
            
            # Define collection strategy
            evidence_plan['collection_strategy'] = {
                'parallel_collection': True,
                'max_retries': 3,
                'timeout': 30,
                'sources': ['knowledge_graph', 'external_apis', 'user_input']
            }
            
            # Define validation rules
            evidence_plan['validation_rules'] = {
                'completeness_threshold': 0.8,
                'freshness_threshold': 3600,  # 1 hour
                'consistency_check': True
            }
            
            return evidence_plan
        except Exception as e:
            self.logger.error(f"Evidence collection planning failed: {e}")
            raise

    async def _decompose_task(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Decompose task into subtasks."""
        try:
            requirements = await self._analyze_requirements(task)
            subtasks = []
            
            for req in requirements:
                subtask = {
                    'id': f"subtask_{len(subtasks)}",
                    'description': req['description'],
                    'requirements': req['requirements'],
                    'dependencies': req['dependencies'],
                    'evidence_requirements': req['evidence_requirements']
                }
                subtasks.append(subtask)
            
            return subtasks
        except Exception as e:
            self.logger.error(f"Task decomposition failed: {e}")
            raise
class REWOOCapabilities:
    """Enhanced REWOO capabilities for agents"""
    def __init__(self, model_info: Dict[str, Any], config: 'EnhancedConfig'):
        self.model_info = model_info
        self.config = config
        self.evidence_store = {}
        self.world_state = defaultdict(dict)
        self.planning_history = []
        self.logger = logging.getLogger(__name__)
        self.observation_store = None
        self.retrieval_cache = None
        self.monitoring = False
        
    # In code_cell6_1, update the _initialize_rewoo method:
    async def _initialize_rewoo(self):
        """Initialize REWOO system and related components"""
        try:
            # Pass self as config to REWOOSystem and PlanningSystem
            self.rewoo_system = REWOOSystem(self)
            self.evidence_store = EvidenceStore()
            self.planning_system = PlanningSystem(self)
        
            await self.evidence_store.initialize()
            await self.planning_system.initialize()
            await self.rewoo_system.initialize()

            self.rewoo_config = REWOOConfig(
                enabled=True,
                max_planning_steps=5,
                evidence_threshold=0.8,
                context_window=4096,
                planning_temperature=0.7
            )

            self._initialization_state['rewoo'] = True
            self.logger.info("REWOO system initialized successfully")
        except Exception as e:
            self.logger.error(f"REWOO system initialization failed: {e}")
            raise

    async def cleanup(self):
        """Cleanup REWOO resources"""
        try:
            # Stop monitoring
            self.monitoring = False
            
            # Clear caches
            if self.observation_store:
                self.observation_store.clear()
            if self.retrieval_cache:
                self.retrieval_cache.clear()
                
            # Clear evidence store
            self.evidence_store.clear()
            
            # Clear planning history
            self.planning_history.clear()
            
            # Clear world state
            self.world_state.clear()
            
            self.logger.info("REWOO capabilities cleaned up successfully")
        except Exception as e:
            self.logger.error(f"REWOO cleanup failed: {e}")
            raise

    async def observe(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Observe current world state and context"""
        observation = {
            'timestamp': datetime.now().isoformat(),
            'context': context,
            'world_state': self.world_state,
            'evidence': await self._collect_relevant_evidence(context)
        }
        return observation
        
    async def plan(self, task: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Generate execution plan with evidence"""
        observation = await self.observe(context)
        plan = {
            'task': task,
            'steps': await self._generate_steps(task, observation),
            'evidence': observation['evidence'],
            'timestamp': datetime.now().isoformat()
        }
        self.planning_history.append(plan)
        return plan
        
    async def replan(self, original_plan: Dict[str, Any], reason: str) -> Dict[str, Any]:
        """Generate new plan when original fails"""
        context = {
            'original_plan': original_plan,
            'failure_reason': reason,
            'history': self.planning_history
        }
        return await self.plan(original_plan['task'], context)

    async def _collect_relevant_evidence(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect relevant evidence for planning"""
        evidence = {}
        try:
            # Query evidence store based on context
            if 'task' in context:
                evidence['task_history'] = [
                    e for e in self.evidence_store.values()
                    if e.get('task') == context['task']
                ]
            
            # Get relevant world state
            evidence['world_state'] = {
                k: v for k, v in self.world_state.items()
                if self._is_relevant(k, context)
            }
            
            # Add retrieval results if available
            cache_key = self._generate_cache_key(context)
            if cache_key in self.retrieval_cache:
                evidence['retrieved'] = self.retrieval_cache[cache_key]
                
        except Exception as e:
            self.logger.error(f"Evidence collection failed: {e}")
            evidence['error'] = str(e)
            
        return evidence

    async def _generate_steps(self, task: str, observation: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate execution steps based on observation"""
        try:
            steps = []
            
            # Analyze task requirements
            requirements = self._analyze_requirements(task)
            
            # Generate steps based on requirements and evidence
            for req in requirements:
                step = {
                    'action': req['action'],
                    'inputs': req['inputs'],
                    'evidence': observation['evidence'].get(req['action'], {}),
                    'validation': req.get('validation', {})
                }
                steps.append(step)
                
            return steps
        except Exception as e:
            self.logger.error(f"Step generation failed: {e}")
            raise

    async def _create_rewoo_plan(self, task: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Create REWOO plan with proper task parsing."""
        try:
            parsed_task = await self._parse_task(task)
            
            # Observe current state
            observation = await self._observe_world_state(context)
            
            # Retrieve relevant context
            retrieved_context = await self._retrieve_relevant_context(parsed_task, observation)
            
            # Generate plan steps
            plan_steps = await self._generate_plan_steps(parsed_task, retrieved_context)
            
            # Create evidence collection strategy
            evidence_strategy = await self._plan_evidence_collection(parsed_task)
            
            return {
                "task": parsed_task,
                "observation": observation,
                "context": retrieved_context,
                "steps": plan_steps,
                "evidence_strategy": evidence_strategy,
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "planner": "rewoo",
                    "version": "1.0"
                }
            }
        except Exception as e:
            self.logger.error(f"REWOO plan creation failed: {e}")
            raise

    # Placeholder methods to prevent AttributeErrors
    async def _generate_subtasks(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate subtasks for decomposition"""
        return []

    async def _analyze_dependencies(self, task: Dict[str, Any]) -> List[str]:
        """Analyze dependencies among tasks"""
        return []

    async def _determine_execution_order(self, task: Dict[str, Any]) -> List[str]:
        """Determine the order of execution for tasks"""
        return []

    def _calculate_cpu_requirements(self, task: Dict[str, Any]) -> int:
        """Calculate CPU requirements"""
        return 2  # Example value

    def _calculate_memory_requirements(self, task: Dict[str, Any]) -> int:
        """Calculate memory requirements"""
        return 4096  # Example value in MB

    def _calculate_storage_requirements(self, task: Dict[str, Any]) -> int:
        """Calculate storage requirements"""
        return 100  # Example value in GB

    async def _determine_optimal_steps(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Determine optimal steps for execution"""
        return []

    async def _identify_parallel_tasks(self, task: Dict[str, Any]) -> List[str]:
        """Identify tasks that can be executed in parallel"""
        return []

    async def _calculate_critical_path(self, task: Dict[str, Any]) -> List[str]:
        """Calculate the critical path for task execution"""
        return []

    async def _generate_subtasks(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate subtasks based on task input"""
        return [task]

    async def _assess_complexity(self, components: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Assess the complexity of task components"""
        return {"complexity": "medium"}

    async def _estimate_resources(self, complexity: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate resources based on task complexity"""
        return {"cpu": 4, "memory": 8192, "storage": 200}

    async def _assess_risks(self, components: List[Dict[str, Any]], complexity: Dict[str, Any]) -> List[str]:
        """Assess risks associated with task components"""
        return ["low"]

    async def _process_evidence_item(self, value: Any, requirements: Dict[str, Any]) -> Any:
        """Process individual evidence items"""
        return value

    def _is_relevant(self, key: str, context: Dict[str, Any]) -> bool:
        """Check if a world state key is relevant to the context"""
        return True

    def _generate_cache_key(self, context: Dict[str, Any]) -> str:
        """Generate cache key from context"""
        return hashlib.md5(json.dumps(context, sort_keys=True).encode()).hexdigest()









           
class DataProcessingAgent(BaseAgent):
    """Agent specialized in data processing and transformation"""
    def __init__(self, name: str, model_info: Dict[str, str], config: 'Config', layer: int):
        super().__init__(name, model_info, config, layer)
        self.processed_tables_manager = ProcessedTablesManager(config)
        self.artifact_registry = config.artifact_registry
        self.schema_inventory = TableSchemaInventory(config)
        self.logger = logging.getLogger(__name__)
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.processed_tables = set()
        self.schema_inventory = TableSchemaInventory(config)
        
    async def initialize(self) -> None:
        """Initialize agent resources and validate requirements"""
        try:
            await self.processed_tables_manager.initialize()
            await self.schema_inventory.initialize()
            self.logger.info(f"{self.name} initialized successfully")
        except Exception as e:
            self.logger.error(f"Initialization failed: {e}")
            raise

    async def process_data(self, data: Any) -> Any:
        """Process data with comprehensive error handling and caching"""
        cache_key = hash(str(data))
        try:
            if cache_key in self.cache:
                self.logger.info("Returning cached result")
                return self.cache[cache_key]

            prompt = self._construct_processing_prompt(data)
            result = await self.process_input(prompt)
            
            # Cache successful result
            self.cache[cache_key] = result
            return result
        except Exception as e:
            self.logger.error(f"Data processing failed: {e}")
            raise

    async def process_data_async(self, task: str) -> None:
        """Process data asynchronously with proper communication"""
        try:
            self.logger.info(f"{self.name} processing: {task}")
            
            # Process data
            processed_data = await self.process_data({'task': task})
            
            # Store artifact
            artifact_id = self.artifact_registry.create_artifact(
                'processed_data',
                self.name,
                task[:50],  # Use truncated task as descriptor
                processed_data,
                {
                    'timestamp': datetime.now().isoformat(),
                    'task': task
                }
            )
            
            # Send completion message
            await self.communication_system.send_message_async(
                self.name,
                'BossAgent',
                'status',
                {
                    'status': 'completed',
                    'artifact_id': artifact_id
                }
            )
            
            # Forward to next agent if needed
            await self.communication_system.send_message_async(
                self.name,
                'InsightGenerationAgent',
                'processed_data',
                {
                    'artifact_id': artifact_id
                }
            )
        except Exception as e:
            self.logger.error(f"Async processing failed: {e}")
            raise

    async def handle_message_async(self, message: Dict[str, Any]) -> None:
        """Handle incoming messages asynchronously"""
        try:
            if message['type'] == 'task_assignment':
                task = message['content']
                self.logger.info(f"{self.name} received task: {task}")
                
                output = await self.process_input(task)
                await self.communication_system.send_message_async(
                    self.name,
                    'BossAgent',
                    'status',
                    {
                        'status': 'completed',
                        'output': output
                    }
                )
        except Exception as e:
            self.logger.error(f"Message handling failed: {e}")
            await self.communication_system.send_message_async(
                self.name,
                'BossAgent',
                'error',
                {
                    'error': str(e),
                    'task': message.get('content', 'Unknown task')
                }
            )

    async def execute_step_async(self, step: Dict[str, Any]) -> None:
        """Execute processing step asynchronously"""
        try:
            if step['tool'] == 'BigQueryClient':
                await self.fetch_data_from_bigquery_async()
            elif step['tool'] == 'DataStandardizer':
                await self.standardize_schemas_async(self.evidence.get('#E1'))
            elif step['tool'] == 'KnowledgeGraphCreator':
                await self.create_knowledge_graph_async(self.evidence.get('#E2'))
        except Exception as e:
            self.logger.error(f"Step execution failed: {e}")
            raise

    async def fetch_data_from_bigquery_async(self) -> pd.DataFrame:
        """Fetch data from BigQuery asynchronously"""
        try:
            self.logger.info(f"{self.name} fetching data from BigQuery")
            query = self._construct_bigquery_query()
            return await self._execute_bigquery_query(query)
        except Exception as e:
            self.logger.error(f"BigQuery fetch failed: {e}")
            raise

    async def standardize_schemas_async(self, data: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Standardize data schemas asynchronously"""
        if data is None:
            return None
            
        try:
            self.logger.info(f"{self.name} standardizing data schemas")
            return self.standardize_schema(data)
        except Exception as e:
            self.logger.error(f"Schema standardization failed: {e}")
            raise

    async def create_knowledge_graph_async(self, data: Optional[pd.DataFrame]) -> Optional[nx.DiGraph]:
        """Create knowledge graph from data asynchronously"""
        if data is None:
            return None
            
        try:
            self.logger.info(f"{self.name} creating knowledge graph")
            return await self._create_graph(data)
        except Exception as e:
            self.logger.error(f"Knowledge graph creation failed: {e}")
            raise

    def standardize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize DataFrame schema"""
        try:
            standardized_columns = {
                'customer_id': 'customer_id',
                'timestamp': 'event_timestamp',
                'event_type': 'event_type',
                'value': 'event_value',
            }

            # Rename columns
            df = df.rename(columns=standardized_columns)
            
            # Ensure required columns exist
            required_columns = ['customer_id', 'event_timestamp', 'event_type', 'event_value', 'source']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = np.nan
                    
            # Convert data types
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce')
            df['event_value'] = pd.to_numeric(df['event_value'], errors='coerce')
            
            return df[required_columns]
        except Exception as e:
            self.logger.error(f"Schema standardization failed: {e}")
            raise

    async def _execute_bigquery_query(self, query: str) -> pd.DataFrame:
        """Execute BigQuery query with proper error handling"""
        try:
            client = self.config.bigquery_client
            query_job = await asyncio.to_thread(client.query, query)
            results = await asyncio.to_thread(query_job.result)
            return results.to_dataframe()
        except Exception as e:
            self.logger.error(f"BigQuery query execution failed: {e}")
            raise

    def _construct_bigquery_query(self) -> str:
        """Construct BigQuery query with proper formatting"""
        return f"""
        SELECT *
        FROM `{self.config.project_id}.{self.config.dataset_id}.customer_interactions`
        WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        """

    async def _create_graph(self, df: pd.DataFrame) -> nx.DiGraph:
        """Create knowledge graph from DataFrame"""
        try:
            G = nx.DiGraph()
            # Implement graph creation logic
            return G
        except Exception as e:
            self.logger.error(f"Graph creation failed: {e}")
            raise

    def _construct_processing_prompt(self, data: Any) -> str:
        """Construct processing prompt with proper formatting"""
        return f"As a Data Processing Agent, analyze and process the following data: {data}"
    
    def _setup_configuration(self, config: Dict[str, Any]):
        """Setup agent configuration with enhanced validation"""
        try:
            # Initialize configuration manager if not exists
            if not hasattr(self, 'config_manager'):
                self.config_manager = ConfigurationManager()
            
            # Set required configuration keys
            self.config_manager.set_required_keys([
                'model_name',
                'max_tokens',
                'temperature',
                'e_commerce_settings'
            ])
            
            # Set default values
            self.config_manager.set_default_value('temperature', 0.7)
            self.config_manager.set_default_value('max_tokens', 4096)
            self.config_manager.set_default_value('e_commerce_settings', {
                'tracking_interval': 30,
                'customer_journey_steps': ['discovery', 'consideration', 'purchase', 'retention'],
                'metrics_tracking': ['conversion_rate', 'customer_lifetime_value', 'churn_rate']
            })
            
            # Validate and set configuration
            self.config_manager.validate_config(config)
            for key, value in config.items():
                self.config_manager.set_config(key, value)
                
            self.logger.info("Configuration setup completed successfully")
                
        except Exception as e:
            self.logger.error(f"Configuration setup failed: {e}")
            raise ConfigurationError(f"Failed to setup configuration: {str(e)}")

    def _generate_boss_agent_prompt(self, bigquery_data: str, business_objectives: str) -> str:
        """Generate the boss agent prompt template with e-commerce focus"""
        try:
            return f"""
            You are the Boss Agent, responsible for processing data from BigQuery within Google Cloud 
            and developing an ontology for e-commerce customer journey tracking. Your task is to analyze 
            the data, create a comprehensive ontology, and establish a knowledge graph for real-time 
            customer tracking and behavior prediction.

            Business Objectives:
            {business_objectives}

            Data Context:
            {bigquery_data}

            Please provide your analysis and recommendations following these steps:

            1. Data Processing:
               - Analyze the BigQuery data
               - Extract relevant features
               - Organize data into categories

            2. Ontology Development:
               - Define core entities
               - Establish relationships
               - Create hierarchies

            3. Knowledge Graph Construction:
               - Select appropriate graph database
               - Describe data population approach
               - Outline integration strategy

            4. Real-time Tracking and Prediction:
               - Propose data pipelines
               - Suggest ML models
               - Describe tracking mechanisms
            """
        except Exception as e:
            self.logger.error(f"Failed to generate prompt: {e}")
            raise

    def _extract_business_objectives(self, input_data: str) -> str:
        """Extract and format business objectives with validation"""
        try:
            # Parse input data
            parsed_data = json.loads(input_data) if isinstance(input_data, str) else input_data
            
            # Extract and validate objectives
            objectives = {
                'increase_conversion_rate': parsed_data.get('conversion_target', '15%'),
                'reduce_customer_churn': parsed_data.get('churn_target', '10%'),
                'increase_customer_lifetime_value': parsed_data.get('clv_target', '25%'),
                'timeframe': parsed_data.get('timeframe', '1 year')
            }
            
            # Add metadata
            objectives['metadata'] = {
                'timestamp': datetime.now().isoformat(),
                'source': parsed_data.get('source', 'user_input'),
                'confidence': parsed_data.get('confidence', 'high')
            }
            
            return json.dumps(objectives)
            
        except Exception as e:
            self.logger.error(f"Failed to extract business objectives: {e}")
            return json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })

    async def _create_layer_strategy(self, layer: int, master_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Create layer-specific strategy with e-commerce focus"""
        try:
            base_strategy = {
                'layer': layer,
                'priority': master_plan.get('priority', 'medium'),
                'timeout': master_plan.get('timeout', 3600),
                'retry_policy': master_plan.get('retry_policy', {'max_attempts': 3, 'backoff': 'exponential'}),
                'metrics': set()
            }
            
            # Layer-specific enhancements
            if layer == 1:  # Data Processing Layer
                base_strategy.update({
                    'task_type': 'data_processing',
                    'metrics': {'data_quality', 'processing_time', 'error_rate'},
                    'validation_rules': ['schema_validation', 'data_type_check', 'null_check']
                })
            elif layer == 2:  # Analysis Layer
                base_strategy.update({
                    'task_type': 'analysis',
                    'metrics': {'insight_quality', 'prediction_accuracy', 'model_performance'},
                    'models': ['customer_segmentation', 'churn_prediction', 'lifetime_value']
                })
            elif layer == 3:  # Knowledge Graph Layer
                base_strategy.update({
                    'task_type': 'knowledge_graph',
                    'metrics': {'graph_density', 'relationship_quality', 'update_frequency'},
                    'graph_operations': ['entity_extraction', 'relationship_mapping', 'hierarchy_building']
                })
            elif layer == 4:  # Action Layer
                base_strategy.update({
                    'task_type': 'action_generation',
                    'metrics': {'action_relevance', 'implementation_feasibility', 'expected_impact'},
                    'action_types': ['personalization', 'intervention', 'optimization']
                })
                
            return base_strategy
            
        except Exception as e:
            self.logger.error(f"Failed to create layer strategy: {e}")
            raise

    async def _assess_complexity(self, components: List[Dict[str, Any]]) -> Dict[str, float]:
        """Assess task complexity with detailed metrics"""
        try:
            complexity_scores = {
                'data_complexity': 0.0,
                'processing_complexity': 0.0,
                'analysis_complexity': 0.0,
                'integration_complexity': 0.0
            }
            
            for component in components:
                # Data complexity
                if 'data_size' in component:
                    complexity_scores['data_complexity'] += min(
                        float(component['data_size']) / 1e6,  # Size in MB
                        10.0  # Cap at 10
                    )
                
                # Processing complexity
                if 'operations' in component:
                    complexity_scores['processing_complexity'] += len(component['operations']) * 0.5
                
                # Analysis complexity
                if 'analysis_type' in component:
                    analysis_weights = {
                        'basic': 1.0,
                        'statistical': 2.0,
                        'machine_learning': 3.0,
                        'deep_learning': 4.0
                    }
                    complexity_scores['analysis_complexity'] += analysis_weights.get(
                        component['analysis_type'].lower(),
                        1.0
                    )
                
                # Integration complexity
                if 'integrations' in component:
                    complexity_scores['integration_complexity'] += len(component['integrations']) * 0.75
            
            # Normalize scores
            max_score = max(complexity_scores.values())
            if max_score > 0:
                complexity_scores = {
                    k: v / max_score * 10 for k, v in complexity_scores.items()
                }
            
            # Add overall complexity
            complexity_scores['overall'] = sum(complexity_scores.values()) / len(complexity_scores)
            
            return complexity_scores
            
        except Exception as e:
            self.logger.error(f"Complexity assessment failed: {e}")
            raise

    async def _estimate_resources(self, complexity: Dict[str, float]) -> Dict[str, Any]:
        """Estimate required resources based on complexity"""
        try:
            # Base resource requirements
            resources = {
                'compute': {
                    'cpu_cores': max(1, int(complexity['processing_complexity'] / 2)),
                    'memory_gb': max(2, int(complexity['data_complexity'] * 1.5)),
                    'gpu_required': complexity['analysis_complexity'] > 7.0
                },
                'storage': {
                    'temporary_gb': max(5, int(complexity['data_complexity'] * 2)),
                    'persistent_gb': max(10, int(complexity['data_complexity'] * 3))
                },
                'network': {
                    'bandwidth_mbps': max(100, int(complexity['integration_complexity'] * 50)),
                    'latency_ms': min(50, int(10 + complexity['integration_complexity'] * 2))
                },
                'time_estimate': {
                    'processing_minutes': max(5, int(complexity['overall'] * 10)),
                    'analysis_minutes': max(10, int(complexity['analysis_complexity'] * 15))
                }
            }
            
            # Add scaling factors
            resources['scaling'] = {
                'auto_scale': complexity['overall'] > 7.0,
                'max_replicas': max(3, int(complexity['overall'])),
                'scale_trigger': 'cpu_utilization_percentage',
                'scale_threshold': 70
            }
            
            # Add monitoring requirements
            resources['monitoring'] = {
                'metrics_interval_seconds': min(60, max(5, int(complexity['overall'] * 2))),
                'alert_threshold': 85,
                'required_metrics': [
                    'cpu_usage',
                    'memory_usage',
                    'network_latency',
                    'error_rate',
                    'processing_time'
                ]
            }
            
            return resources
            
        except Exception as e:
            self.logger.error(f"Resource estimation failed: {e}")
            raise

    def get_enhanced_metrics(self) -> Dict[str, Any]:
        """Get enhanced agent metrics including e-commerce specific metrics"""
        try:
            base_metrics = self.get_metrics()  # Get base metrics from parent
            
            # Add e-commerce specific metrics
            enhanced_metrics = {
                **base_metrics,
                'e_commerce_metrics': {
                    'conversion_tracking': {
                        'rate': self.metrics.get('conversion_rate', 0.0),
                        'trend': self._calculate_metric_trend('conversion_rate'),
                        'goal_progress': self._calculate_goal_progress('conversion_rate')
                    },
                    'customer_lifetime_value': {
                        'average': self.metrics.get('customer_lifetime_value', 0.0),
                        'trend': self._calculate_metric_trend('customer_lifetime_value'),
                        'segment_breakdown': self._get_clv_segments()
                    },
                    'churn_metrics': {
                        'rate': self.metrics.get('churn_rate', 0.0),
                        'prediction_accuracy': self.metrics.get('churn_prediction_accuracy', 0.0),
                        'risk_distribution': self._get_churn_risk_distribution()
                    }
                },
                'performance_metrics': {
                    'response_times': {
                        'avg_ms': self.metrics.get('avg_response_time', 0),
                        'p95_ms': self.metrics.get('p95_response_time', 0),
                        'p99_ms': self.metrics.get('p99_response_time', 0)
                    },
                    'resource_utilization': {
                        'cpu_percent': self.metrics.get('cpu_utilization', 0),
                        'memory_percent': self.metrics.get('memory_utilization', 0),
                        'network_usage': self.metrics.get('network_usage', 0)
                    }
                }
            }
            
            return enhanced_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to get enhanced metrics: {e}")
            return {'error': str(e)}

    def _calculate_metric_trend(self, metric_name: str, window_size: int = 7) -> Dict[str, Any]:
        """Calculate trend for a specific metric"""
        try:
            if not self.metrics.get(f'{metric_name}_history'):
                return {'trend': 'stable', 'change': 0.0}
                
            history = self.metrics[f'{metric_name}_history'][-window_size:]
            if len(history) < 2:
                return {'trend': 'insufficient_data', 'change': 0.0}
                
            change = (history[-1] - history[0]) / history[0] * 100
            
            return {
                'trend': 'increasing' if change > 5 else 'decreasing' if change < -5 else 'stable',
                'change': round(change, 2)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to calculate metric trend: {e}")
            return {'trend': 'error', 'change': 0.0}
    

class EnhancedWorker:
    """Enhanced worker with proper initialization and configuration."""
    def __init__(self, name: str, config: 'EnhancedConfig', tool_set: Dict[str, Callable]):
        self.name = name
        self.config = config
        self.tool_set = tool_set
        self.logger = logging.getLogger(__name__)
        self.metrics = defaultdict(list)
        
        # Get worker config
        worker_config = config.worker_configs[name]
        
        # Initialize cache with config values
        self.cache = TTLCache(
            maxsize=worker_config.cache_size,
            ttl=worker_config.timeout
        )
        self.initialized = False

    async def initialize(self) -> None:
        """Initialize worker."""
        try:
            # Initialize tools
            for tool_name, tool_factory in self.tool_set.items():
                tool = tool_factory()
                setattr(self, f"_{tool_name.lower()}", tool)

            self.initialized = True
            self.logger.info(f"Worker {self.name} initialized successfully")
        except Exception as e:
            self.logger.error(f"Worker {self.name} initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup worker resources."""
        try:
            self.cache.clear()
            self.initialized = False
            self.logger.info(f"Worker {self.name} cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Worker {self.name} cleanup failed: {e}")
            raise
    async def execute_task(self, instruction: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Base method for task execution"""
        execution_id = f"exec_{datetime.now().timestamp()}"
        try:
            result = await self._process_instruction(instruction, context)
            return {
                "execution_id": execution_id,
                "result": result,
                "metadata": {
                    "worker": self.name,
                    "timestamp": datetime.now().isoformat()
                }
            }
        except Exception as e:
            self.logger.error(f"Task execution failed: {e}")
            raise

    async def _process_instruction(self, instruction: str, context: Dict[str, Any]) -> Any:
        """Process a single instruction"""
        raise NotImplementedError("Subclasses must implement _process_instruction")

# Now define the enhanced worker with REWOO capabilities
class EnhancedWorkerWithREWOO(EnhancedWorker):
    """Worker enhanced with REWOO capabilities"""
    def __init__(self, name: str, config: EnhancedConfig, tool_set: Dict[str, Callable]):
        super().__init__(name, config, tool_set)
        self.evidence_store = EvidenceStore()
        self.planning_history = []
        self.continuous_learning = EnhancedContinuousLearning()

    async def execute_task(self, instruction: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute task with REWOO planning"""
        execution_id = f"exec_{datetime.now().timestamp()}"
        
        try:
            # Generate and validate plan
            plan = await self.generate_plan(instruction)
            if not await self.validate_plan(plan):
                plan = await self.replan(plan, "Initial plan validation failed")
            
            # Execute plan with evidence tracking
            result = await self._execute_plan_with_evidence(plan)
            
            # Update learning metrics
            await self.continuous_learning.update_metrics(execution_id, result)
            
            return {
                "execution_id": execution_id,
                "result": result,
                "evidence": await self.evidence_store.get_evidence(execution_id)
            }
            
        except Exception as e:
            self.logger.error(f"Task execution failed: {e}")
            raise

    async def generate_plan(self, instruction: str) -> Dict[str, Any]:
        """Generate REWOO plan with evidence tracking"""
        try:
            plan = await self._create_execution_plan(instruction)
            await self.evidence_store.store_evidence(
                "initial_plan",
                plan,
                {"timestamp": datetime.now().isoformat()}
            )
            return plan
        except Exception as e:
            self.logger.error(f"Plan generation failed: {e}")
            raise

    async def _execute_plan_with_evidence(self, plan: Dict[str, Any]) -> Any:
        """Execute plan with evidence tracking"""
        try:
            result = await self._execute_through_tools(plan)
            await self.evidence_store.store_evidence(
                "execution_result",
                result,
                {"timestamp": datetime.now().isoformat()}
            )
            return result
        except Exception as e:
            self.logger.error(f"Plan execution failed: {e}")
            raise

    async def _execute_through_tools(self, plan: Dict[str, Any]) -> Any:
        """Execute plan through available tools"""
        try:
            results = []
            for step in plan.get("steps", []):
                tool_name = step.get("tool")
                if tool_name in self.tool_set:
                    result = await self.tool_set[tool_name](step.get("input"))
                    results.append(result)
            return results
        except Exception as e:
            self.logger.error(f"Tool execution failed: {e}")
            raise

    async def validate_plan(self, plan: Dict[str, Any]) -> bool:
        """Validate generated plan"""
        try:
            if not plan or "steps" not in plan:
                return False
            return all(self._validate_step(step) for step in plan["steps"])
        except Exception as e:
            self.logger.error(f"Plan validation failed: {e}")
            return False

    def _validate_step(self, step: Dict[str, Any]) -> bool:
        """Validate a single plan step"""
        return (
            isinstance(step, dict)
            and "tool" in step
            and step["tool"] in self.tool_set
            and "input" in step
        )

    async def replan(self, original_plan: Dict[str, Any], reason: str) -> Dict[str, Any]:
        """Generate new plan when original plan fails"""
        try:
            self.planning_history.append({
                "original_plan": original_plan,
                "reason": reason,
                "timestamp": datetime.now().isoformat()
            })
            return await self._create_execution_plan(
                f"Replan needed: {reason}",
                context={"original_plan": original_plan}
            )
        except Exception as e:
            self.logger.error(f"Replanning failed: {e}")
            raise

    async def _create_execution_plan(self, instruction: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create execution plan for given instruction"""
        try:
            return {
                "instruction": instruction,
                "steps": await self._generate_steps(instruction, context or {}),
                "created_at": datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Execution plan creation failed: {e}")
            raise

    async def _generate_steps(self, instruction: str, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate execution steps"""
        # Implement step generation logic
        raise NotImplementedError("Subclasses must implement _generate_steps")
        
        
class EnhancedSolver:
    """Enhanced solver for synthesizing final solutions from plans and evidence"""
    def __init__(self, config: EnhancedConfig, model_info: Dict[str, Any]):
        self.config = config
        self.model_info = model_info
        self.solution_history = []
        self.performance_tracker = defaultdict(list)
        self.evidence_analyzer = EvidenceAnalyzer()
        self.quality_checker = QualityChecker()
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            "solutions_generated": 0,
            "solution_quality_scores": [],
            "processing_times": [],
            "evidence_usage": []
        }
        self.cache = TTLCache(maxsize=1000, ttl=3600)

    async def initialize(self):
        """Initialize solver components"""
        try:
            await self.evidence_analyzer.initialize()
            await self.quality_checker.initialize()
            self.logger.info("Solver initialized successfully")
        except Exception as e:
            self.logger.error(f"Solver initialization failed: {e}")
            raise

    async def solve(self,
                   task: str,
                   plans: List[Dict[str, Any]],
                   evidence: Dict[str, Any]) -> Dict[str, Any]:
        """Solve task with enhanced reasoning and validation"""
        solution_id = f"sol_{datetime.now().timestamp()}"
        start_time = time.time()
        
        try:
            # Validate inputs
            self._validate_inputs(task, plans, evidence)
            
            # Analyze evidence quality
            evidence_quality = await self.evidence_analyzer.analyze_evidence(evidence)
            
            # Generate solution with confidence score
            solution, confidence = await self._generate_solution(task, plans, evidence)
            
            # Validate solution
            validated_solution = await self._validate_solution(solution, confidence)
            
            # Record metrics
            self._record_solution_metrics(solution_id, confidence, evidence_quality)
            
            # Update processing time
            self.metrics["processing_times"].append(time.time() - start_time)
            
            return {
                "solution_id": solution_id,
                "solution": validated_solution,
                "confidence": confidence,
                "evidence_quality": evidence_quality,
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "model_used": self.model_info["model_name"],
                    "processing_time": time.time() - start_time
                }
            }
            
        except Exception as e:
            self.logger.error(f"Solution generation failed: {e}")
            raise

    async def _generate_solution(self,
                               task: str,
                               plans: List[Dict[str, Any]],
                               evidence: Dict[str, Any]) -> Tuple[Any, float]:
        """Generate solution with confidence score"""
        try:
            # Check cache
            cache_key = self._generate_cache_key(task, plans, evidence)
            if cache_key in self.cache:
                return self.cache[cache_key]

            # Analyze task requirements
            requirements = self._analyze_requirements(task)
            
            # Process evidence
            processed_evidence = await self._process_evidence(evidence, requirements)
            
            # Generate initial solution
            solution = await self._synthesize_solution(processed_evidence, plans)
            
            # Calculate confidence
            confidence = self._calculate_confidence(solution, processed_evidence)
            
            # Cache result
            self.cache[cache_key] = (solution, confidence)
            
            return solution, confidence
            
        except Exception as e:
            self.logger.error(f"Solution generation failed: {e}")
            raise

    async def _validate_solution(self,
                               solution: Any,
                               confidence: float) -> Any:
        """Validate generated solution"""
        try:
            # Check solution quality
            quality_score = await self.quality_checker.check_quality(solution)
            
            # Record quality score
            self.metrics["solution_quality_scores"].append(quality_score)
            
            # Validate if quality meets threshold
            if quality_score >= self.config.quality_threshold:
                return solution
            
            # Attempt improvement if quality is low
            if quality_score < self.config.quality_threshold:
                improved_solution = await self._improve_solution(solution)
                if improved_solution:
                    return improved_solution
            
            return solution
            
        except Exception as e:
            self.logger.error(f"Solution validation failed: {e}")
            raise

    async def _improve_solution(self, solution: Any) -> Optional[Any]:
        """Attempt to improve solution quality"""
        try:
            # Analyze current solution
            analysis = self._analyze_solution(solution)
            
            # Generate improvements
            improvements = await self._generate_improvements(analysis)
            
            # Apply improvements
            improved_solution = await self._apply_improvements(solution, improvements)
            
            # Validate improvements
            if await self._validate_improvements(improved_solution):
                return improved_solution
            
            return None
            
        except Exception as e:
            self.logger.error(f"Solution improvement failed: {e}")
            return None

    def _validate_inputs(self,
                        task: str,
                        plans: List[Dict[str, Any]],
                        evidence: Dict[str, Any]):
        """Validate input parameters"""
        if not task:
            raise ValueError("Task cannot be empty")
            
        if not plans:
            raise ValueError("Plans cannot be empty")
            
        if not evidence:
            raise ValueError("Evidence cannot be empty")
            
        # Validate plan structure
        for plan in plans:
            if not isinstance(plan, dict) or 'steps' not in plan:
                raise ValueError("Invalid plan structure")

    async def _process_evidence(self,
                              evidence: Dict[str, Any],
                              requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Process and structure evidence"""
        try:
            processed_evidence = {}
            
            # Process each evidence item
            for key, value in evidence.items():
                processed_value = await self._process_evidence_item(value, requirements)
                processed_evidence[key] = processed_value
                
            # Track evidence usage
            self.metrics["evidence_usage"].append(len(processed_evidence))
            
            return processed_evidence
            
        except Exception as e:
            self.logger.error(f"Evidence processing failed: {e}")
            raise

    async def _synthesize_solution(self,
                                 evidence: Dict[str, Any],
                                 plans: List[Dict[str, Any]]) -> Any:
        """Synthesize solution from evidence and plans"""
        try:
            # Combine evidence with plans
            synthesis_input = self._prepare_synthesis_input(evidence, plans)
            
            # Generate solution
            solution = await self._generate_synthesis(synthesis_input)
            
            # Update metrics
            self.metrics["solutions_generated"] += 1
            
            return solution
            
        except Exception as e:
            self.logger.error(f"Solution synthesis failed: {e}")
            raise

    def _calculate_confidence(self,
                            solution: Any,
                            evidence: Dict[str, Any]) -> float:
        """Calculate confidence score for solution"""
        try:
            # Calculate base confidence
            base_confidence = self._calculate_base_confidence(solution)
            
            # Adjust based on evidence quality
            evidence_factor = self._calculate_evidence_factor(evidence)
            
            # Calculate final confidence
            confidence = base_confidence * evidence_factor
            
            return min(1.0, max(0.0, confidence))
            
        except Exception as e:
            self.logger.error(f"Confidence calculation failed: {e}")
            return 0.0

    def _record_solution_metrics(self,
                               solution_id: str,
                               confidence: float,
                               evidence_quality: float):
        """Record solution metrics"""
        try:
            metrics = {
                "solution_id": solution_id,
                "confidence": confidence,
                "evidence_quality": evidence_quality,
                "timestamp": datetime.now().isoformat()
            }
            
            self.solution_history.append(metrics)
            
        except Exception as e:
            self.logger.error(f"Metrics recording failed: {e}")

    def _generate_cache_key(self,
                          task: str,
                          plans: List[Dict[str, Any]],
                          evidence: Dict[str, Any]) -> str:
        """Generate cache key for solution"""
        key_components = [
            task,
            str(sorted(str(p) for p in plans)),
            str(sorted(str(e) for e in evidence.items()))
        ]
        return hashlib.md5("_".join(key_components).encode()).hexdigest()

    def get_solver_statistics(self) -> Dict[str, Any]:
        """Get solver statistics"""
        return {
            "total_solutions": self.metrics["solutions_generated"],
            "average_quality": np.mean(self.metrics["solution_quality_scores"])
                if self.metrics["solution_quality_scores"] else 0,
            "average_processing_time": np.mean(self.metrics["processing_times"])
                if self.metrics["processing_times"] else 0,
            "average_evidence_usage": np.mean(self.metrics["evidence_usage"])
                if self.metrics["evidence_usage"] else 0
        }
    
from typing import Dict, Any, Type
from collections import defaultdict, Counter
from datetime import datetime
import logging
from abc import ABC, abstractmethod





class EnhancedMoASystem:
    """Enhanced MoA system with comprehensive agent management"""
    
    def __init__(self, config: 'EnhancedConfig'):
        """
        Initialize the Enhanced MoA system.
        
        Args:
            config (EnhancedConfig): System configuration object
        """
        self.config = config
        self.communication = CommunicationChannel()
        self.agents: Dict[str, BaseAgent] = {}
        self.agent_states: Dict[str, Dict[str, Any]] = {}
        self.evidence_store = EnhancedEvidenceStore()
        self.metrics = defaultdict(Counter)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    async def initialize(self) -> None:
        """Initialize MoA system with proper error handling"""
        try:
            # Initialize communication channel
            await self.communication.initialize()
            
            # Initialize evidence store
            await self.evidence_store.initialize()
            
            # Initialize agents
            await self._initialize_agents()
            
            self.logger.info("MoA system initialized successfully")
        except Exception as e:
            self.logger.error(f"MoA system initialization failed: {str(e)}")
            await self.cleanup()
            raise

    async def cleanup(self) -> None:
        """Cleanup system resources"""
        try:
            # Cleanup communication channel
            if hasattr(self, 'communication'):
                await self.communication.cleanup()
            
            # Cleanup evidence store
            if hasattr(self, 'evidence_store'):
                await self.evidence_store.cleanup()
            
            # Cleanup agents
            if hasattr(self, 'agents'):
                for agent in self.agents.values():
                    try:
                        await agent.cleanup()
                    except Exception as e:
                        self.logger.error(f"Error cleaning up agent {agent.name}: {str(e)}")
            
            self.logger.info("MoA system cleaned up successfully")
        except Exception as e:
            self.logger.error(f"MoA system cleanup failed: {str(e)}")
            raise

    async def _initialize_agents(self) -> None:
        """Initialize all agents with proper error handling"""
        try:
            agent_factory = AgentFactory(self.config)
            await agent_factory.initialize()
            
            for agent_config in self.config.agent_configs:
                try:
                    agent = await agent_factory.create_agent(
                        agent_config.type,
                        agent_config.model_config,
                        self.communication
                    )
                    self.agents[agent.name] = agent
                    self.agent_states[agent.name] = {
                        'status': 'active',
                        'initialized_at': datetime.now().isoformat()
                    }
                    self.logger.info(f"Initialized agent {agent.name}")
                except Exception as e:
                    self.logger.error(f"Failed to initialize agent {agent_config.name}: {str(e)}")
                    raise
            
            self.logger.info(f"Initialized {len(self.agents)} agents successfully")
        except Exception as e:
            self.logger.error(f"Agent initialization failed: {str(e)}")
            raise

    async def _create_agent(self, agent_config: AgentConfig) -> 'BaseAgent':
        """Create an agent instance with proper error handling"""
        try:
            agent_class = self._get_agent_class(agent_config.type)
            agent = agent_class(
                name=agent_config.name,
                model_info=agent_config.model_info,
                config=self.config,
                communication_channel=self.communication
            )
            return agent
        except Exception as e:
            self.logger.error(f"Agent creation failed for {agent_config.name}: {str(e)}")
            raise

    def _get_agent_class(self, agent_type: str) -> Type['BaseAgent']:
        """Get the appropriate agent class with validation"""
        agent_classes = {
            'BossAgent': BossAgent,
            'Layer1Agent': Layer1Agent,
            'Layer2Agent': Layer2Agent,
            'Layer3Agent': Layer3Agent,
            'Layer4Agent': Layer4Agent
        }
        
        if agent_type not in agent_classes:
            raise ValueError(f"Unknown agent type: {agent_type}")
            
        return agent_classes[agent_type]

        
class OptimizedMixtureOfAgents:
    """Optimized MoA implementation with unified systems"""
    def __init__(self, config: 'EnhancedConfig'):
        self.config = config
        self.communication = UnifiedCommunicationSystem()
        self.agents = {}
        self.layer_structure = {
            0: [("BossAgent", "claude-3-5-sonnet@20240620")],
            1: [("Layer1Agent", "gpt-4")],
            2: [("Layer2Agent", "claude-3-opus@20240229")],
            3: [("Layer3Agent", "mistral-large-2")],
            4: [("Layer4Agent", "gpt-0.1-preview")]
        }
        self.logger = logging.getLogger(__name__)

    async def initialize(self):
        try:
            await self._initialize_agents()
            await self.communication.initialize()
            self.logger.info("MoA system initialized successfully")
        except Exception as e:
            self.logger.error(f"MoA initialization failed: {e}")
            raise

    async def _initialize_agents(self):
        try:
            for layer_num, agents in self.layer_structure.items():
                for agent_name, model in agents:
                    self.agents[agent_name] = EnhancedBaseAgent(
                        name=agent_name,
                        model_info={"model_name": model},
                        config=self.config
                    )
                    await self.agents[agent_name].initialize()
        except Exception as e:
            self.logger.error(f"Agent initialization failed: {e}")
            raise

    async def process_task(self, task: str) -> Dict[str, Any]:
        task_id = f"task_{datetime.now().timestamp()}"
        try:
            boss_plan = await self._execute_boss_layer(task)
            result = await self._process_through_layers(boss_plan)
            
            return {
                "task_id": task_id,
                "result": result,
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "processing_path": self._get_processing_path()
                }
            }
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            raise

    async def _execute_boss_layer(self, task: str) -> Dict[str, Any]:
        try:
            boss_agent = self.agents["BossAgent"]
            return await boss_agent.process_task(task)
        except Exception as e:
            self.logger.error(f"Boss layer execution failed: {e}")
            raise

    async def _process_through_layers(self, initial_plan: Dict[str, Any]) -> Dict[str, Any]:
        current_result = initial_plan
        try:
            for layer_num in range(1, len(self.layer_structure)):
                layer_results = []
                for agent_name, _ in self.layer_structure[layer_num]:
                    agent = self.agents.get(agent_name)
                    if agent:
                        result = await agent.process_task(current_result)
                        layer_results.append(result)
                current_result = await self._aggregate_layer_results(layer_results, layer_num)
            return current_result
        except Exception as e:
            self.logger.error(f"Layer processing failed: {e}")
            raise

    async def _aggregate_layer_results(self, results: List[Dict[str, Any]], layer: int) -> Dict[str, Any]:
        try:
            # Implement layer-specific aggregation logic here
            return {
                "layer": layer,
                "aggregated_results": results,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Result aggregation failed: {e}")
            raise

    def _get_processing_path(self) -> List[str]:
        return [
            agent_name
            for layer in self.layer_structure.values()
            for agent_name, _ in layer
        ]



@dataclass
class WorkflowState:
    """State tracking for workflow execution"""
    workflow_id: str
    start_time: datetime = field(default_factory=datetime.now)
    status: str = "initialized"
    current_step: int = 0
    total_steps: int = 0
    results: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

import logging
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import defaultdict


    
class WorkflowOptimizer:
    """Optimizes workflow execution and task processing"""
    def __init__(self, config: EnhancedConfig):
        self.config = config
        self.workflow_history = []
        self.optimization_patterns = defaultdict(list)
        self.task_scheduler = TaskScheduler()
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            "workflows_optimized": 0,
            "optimization_gains": [],
            "execution_times": [],
            "resource_savings": []
        }
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.optimization_thresholds = {
            "execution_time": 5.0,
            "resource_usage": 0.8,
            "parallel_tasks": 5
        }

    async def initialize(self):
        """Initialize workflow optimizer"""
        try:
            await self.task_scheduler.initialize()
            self._initialize_optimization_patterns()
            self.logger.info("Workflow optimizer initialized successfully")
        except Exception as e:
            self.logger.error(f"Workflow optimizer initialization failed: {e}")
            raise

    async def optimize_workflow(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize workflow execution"""
        optimization_id = f"wf_opt_{datetime.now().timestamp()}"
        start_time = time.time()
        
        try:
            # Analyze workflow
            analysis = self._analyze_workflow(workflow)
            
            # Generate optimization plan
            optimization_plan = await self._generate_workflow_optimization(analysis)
            
            # Apply optimizations
            optimized_workflow = await self._apply_workflow_optimizations(
                workflow,
                optimization_plan
            )
            
            # Schedule optimized workflow
            scheduled_workflow = await self.task_scheduler.schedule_workflow(
                optimized_workflow
            )
            
            # Record optimization
            optimization_record = {
                "optimization_id": optimization_id,
                "original_workflow": workflow,
                "optimized_workflow": optimized_workflow,
                "optimization_plan": optimization_plan,
                "execution_time": time.time() - start_time
            }
            
            self.workflow_history.append(optimization_record)
            self._update_metrics(optimization_record)
            
            return scheduled_workflow
            
        except Exception as e:
            self.logger.error(f"Workflow optimization failed: {e}")
            raise

    def _analyze_workflow(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze workflow for optimization opportunities"""
        try:
            analysis = {
                "task_dependencies": self._analyze_task_dependencies(workflow),
                "resource_requirements": self._analyze_resource_requirements(workflow),
                "parallelization_opportunities": self._identify_parallel_tasks(workflow),
                "bottlenecks": self._identify_workflow_bottlenecks(workflow),
                "optimization_opportunities": []
            }
            
            # Identify optimization opportunities
            for bottleneck in analysis["bottlenecks"]:
                opportunity = self._generate_optimization_opportunity(bottleneck)
                if opportunity:
                    analysis["optimization_opportunities"].append(opportunity)
                    
            return analysis
            
        except Exception as e:
            self.logger.error(f"Workflow analysis failed: {e}")
            raise

    async def _generate_workflow_optimization(self,
                                           analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate workflow optimization plan"""
        try:
            optimization_plan = {
                "task_optimizations": [],
                "resource_optimizations": [],
                "parallel_execution_plan": None,
                "expected_improvements": {}
            }
            
            # Generate task optimizations
            for opportunity in analysis["optimization_opportunities"]:
                optimization = await self._generate_task_optimization(opportunity)
                if optimization:
                    optimization_plan["task_optimizations"].append(optimization)
                    
            # Generate resource optimizations
            resource_optimizations = self._generate_resource_optimizations(analysis)
            optimization_plan["resource_optimizations"] = resource_optimizations
            
            # Generate parallel execution plan
            if analysis["parallelization_opportunities"]:
                optimization_plan["parallel_execution_plan"] = \
                    self._generate_parallel_execution_plan(
                        analysis["parallelization_opportunities"]
                    )
                    
            return optimization_plan
            
        except Exception as e:
            self.logger.error(f"Optimization plan generation failed: {e}")
            raise


class EnhancedErrorHandler:
    def __init__(self):
        self.recovery_strategies = {
            'configuration': self._handle_config_error,
            'communication': self._handle_comm_error,
            'evidence': self._handle_evidence_error
        }
        self.state_manager = StateManager()

    async def handle_error(self, error_type: str, context: Dict[str, Any]) -> None:
        strategy = self.recovery_strategies.get(error_type)
        if strategy:
            await strategy(context)
            await self.state_manager.restore_last_valid_state()



    def _analyze_error(self, error: Exception) -> Dict[str, Any]:
        """Analyze error and determine characteristics"""
        return {
            'type': type(error).__name__,
            'message': str(error),
            'severity': self._determine_severity(error),
            'recoverable': self._is_recoverable(error),
            'timestamp': datetime.now().isoformat()
        }

    async def _execute_recovery(self, strategy: str, context: Dict[str, Any]) -> None:
        """Execute recovery strategy"""
        try:
            recovery_strategies = {
                'retry': self._retry_operation,
                'fallback': self._fallback_operation,
                'reset': self._reset_state,
                'compensate': self._compensating_action
            }
            
            if strategy not in recovery_strategies:
                raise ValueError(f"Unknown recovery strategy: {strategy}")
            
            await recovery_strategies[strategy](context)
            
        except Exception as e:
            self.logger.error(f"Recovery execution failed: {e}")
            raise
    

    async def _retry_operation(self, context: Dict[str, Any]) -> Optional[Any]:
        """Retry failed operation with exponential backoff"""
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                delay = base_delay * (2 ** attempt)
                await asyncio.sleep(delay)
                
                # Attempt operation
                result = await self._execute_operation(context)
                return result
                
            except Exception as e:
                self.logger.warning(f"Retry attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise

    async def _fallback_operation(self, context: Dict[str, Any]) -> Optional[Any]:
        """Execute fallback operation"""
        try:
            # Check for cached fallback
            cache_key = self._generate_cache_key(context)
            if cache_key in self.recovery_cache:
                return self.recovery_cache[cache_key]
            
            # Execute fallback logic
            fallback_result = await self._execute_fallback(context)
            
            # Cache successful fallback
            if fallback_result:
                self.recovery_cache[cache_key] = fallback_result
            
            return fallback_result
            
        except Exception as e:
            self.logger.error(f"Fallback operation failed: {e}")
            raise

    async def _reset_state(self, context: Dict[str, Any]) -> Optional[Any]:
        """Reset system state"""
        try:
            # Backup current state
            state_backup = self._backup_state(context)
            
            # Reset state
            await self._perform_reset(context)
            
            # Verify reset
            if await self._verify_reset(context):
                return True
            else:
                # Restore backup if verification fails
                await self._restore_state(state_backup)
                return False
                
        except Exception as e:
            self.logger.error(f"State reset failed: {e}")
            raise

    async def _compensating_action(self, context: Dict[str, Any]) -> Optional[Any]:
        """Execute compensating action"""
        try:
            # Record compensation attempt
            compensation_id = f"comp_{datetime.now().timestamp()}"
            
            # Execute compensation
            compensation_result = await self._execute_compensation(context)
            
            # Verify compensation
            if await self._verify_compensation(compensation_result):
                return compensation_result
            return None
            
        except Exception as e:
            self.logger.error(f"Compensation action failed: {e}")
            raise

    def _analyze_error(self, error: Exception) -> Dict[str, Any]:
        """Analyze error and determine characteristics"""
        try:
            error_type = type(error).__name__
            error_message = str(error)
            
            analysis = {
                "type": error_type,
                "message": error_message,
                "severity": self._determine_severity(error),
                "pattern": self._identify_error_pattern(error_message),
                "timestamp": datetime.now().isoformat()
            }
            
            # Update error patterns
            self.error_patterns[analysis["pattern"]] += 1
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analysis failed: {e}")
            raise

    async def _select_recovery_strategy(self, error_analysis: Dict[str, Any]) -> str:
        """Select appropriate recovery strategy"""
        try:
            # Check cached strategies
            cache_key = f"{error_analysis['type']}_{error_analysis['pattern']}"
            if cache_key in self.recovery_cache:
                return self.recovery_cache[cache_key]
            
            # Select strategy based on analysis
            if error_analysis["severity"] == "critical":
                return "compensate"
            elif error_analysis["severity"] == "high":
                return "reset"
            elif error_analysis["severity"] == "medium":
                return "fallback"
            else:
                return "retry"
                
        except Exception as e:
            self.logger.error(f"Strategy selection failed: {e}")
            raise

    def _determine_severity(self, error: Exception) -> str:
        """Determine error severity"""
        if isinstance(error, (SystemError, MemoryError)):
            return "critical"
        elif isinstance(error, (ValueError, TypeError)):
            return "high"
        elif isinstance(error, (TimeoutError, ConnectionError)):
            return "medium"
        else:
            return "low"

    def _identify_error_pattern(self, error_message: str) -> str:
        """Identify error pattern from message"""
        # Implement pattern matching logic
        return "generic_error"

    def _record_error(self, error_id: str, error: Exception, context: Dict[str, Any]):
        """Record error details"""
        self.error_database[error_id] = {
            "error": str(error),
            "type": type(error).__name__,
            "context": context,
            "timestamp": datetime.now().isoformat(),
            "status": "unresolved"
        }

    def _record_recovery_attempt(self,
                               error_id: str,
                               strategy: str,
                               result: Any):
        """Record recovery attempt details"""
        self.recovery_history.append({
            "error_id": error_id,
            "strategy": strategy,
            "result": "success" if result else "failure",
            "timestamp": datetime.now().isoformat()
        })

    def _update_recovery_metrics(self, error_id: str, start_time: float):
        """Update recovery metrics"""
        recovery_time = time.time() - start_time
        self.metrics["recovery_times"].append(recovery_time)

    def _cache_successful_recovery(self, strategy: str, context: Dict[str, Any]):
        """Cache successful recovery strategy"""
        cache_key = self._generate_cache_key(context)
        self.recovery_cache[cache_key] = strategy

    def _generate_cache_key(self, context: Dict[str, Any]) -> str:
        """Generate cache key from context"""
        return hashlib.md5(str(context).encode()).hexdigest()

    def get_error_statistics(self) -> Dict[str, Any]:
        """Get error handling statistics"""
        return {
            "total_errors": self.metrics["total_errors"],
            "successful_recoveries": self.metrics["successful_recoveries"],
            "failed_recoveries": self.metrics["failed_recoveries"],
            "average_recovery_time": np.mean(self.metrics["recovery_times"])
                if self.metrics["recovery_times"] else 0,
            "error_patterns": dict(self.error_patterns)
        }

class ErrorRecoveryManager:
    """Manages error recovery strategies"""
    def __init__(self, config: EnhancedConfig):
        self.config = config
        self.recovery_history = []
        self.active_recoveries = set()
        
    async def handle_error(self, error: Exception, context: Dict[str, Any]) -> Optional[Any]:
        """Handle errors with sophisticated recovery"""
        recovery_id = f"recovery_{datetime.now().timestamp()}"
        
        try:
            # Analyze error
            error_analysis = self._analyze_error(error)
            
            # Select recovery strategy
            strategy = await self._select_recovery_strategy(error_analysis)
            
            # Execute recovery
            result = await self._execute_recovery(strategy, context)
            
            # Record recovery attempt
            self._record_recovery(recovery_id, error, strategy, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error recovery failed: {e}")
            raise


class MetaLearningSystem:
    """Meta-learning system for optimization and model selection"""
    def __init__(self):
        self.learning_patterns = defaultdict(list)
        self.model_performance = defaultdict(lambda: {
            "accuracy": [],
            "response_time": [],
            "success_rate": [],
            "adaptation_rate": []
        })
        self.optimization_history = []
        self.learning_rules = {}
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            "patterns_identified": 0,
            "successful_optimizations": 0,
            "failed_optimizations": 0,
            "learning_efficiency": []
        }
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.pattern_matcher = PatternMatcher()
        self.strategy_optimizer = StrategyOptimizer()

    async def initialize(self):
        """Initialize meta-learning system"""
        try:
            await self.pattern_matcher.initialize()
            await self.strategy_optimizer.initialize()
            self._initialize_learning_rules()
            self.logger.info("Meta-learning system initialized successfully")
        except Exception as e:
            self.logger.error(f"Meta-learning initialization failed: {e}")
            raise

    async def update(self, model_name: str, metrics: Dict[str, float]):
        """Update meta-learning system with new performance data"""
        try:
            # Update performance metrics
            self._update_performance_metrics(model_name, metrics)
            
            # Identify learning patterns
            patterns = await self._identify_patterns(model_name)
            
            # Update learning rules
            await self._update_learning_rules(patterns)
            
            # Optimize strategies if needed
            if self._should_optimize(model_name):
                await self.optimize_strategies(model_name)
                
            # Record update
            self._record_update(model_name, metrics, patterns)
            
        except Exception as e:
            self.logger.error(f"Meta-learning update failed: {e}")
            raise

    async def optimize_strategies(self, model_name: str) -> Dict[str, Any]:
        """Optimize learning strategies for a model"""
        try:
            # Analyze current performance
            performance_analysis = self._analyze_performance(model_name)
            
            # Generate optimization strategies
            strategies = await self._generate_optimization_strategies(performance_analysis)
            
            # Evaluate strategies
            evaluated_strategies = await self._evaluate_strategies(strategies)
            
            # Select best strategy
            best_strategy = self._select_best_strategy(evaluated_strategies)
            
            # Apply optimization
            optimization_result = await self._apply_optimization(model_name, best_strategy)
            
            # Record optimization
            self._record_optimization(model_name, optimization_result)
            
            return optimization_result
            
        except Exception as e:
            self.logger.error(f"Strategy optimization failed: {e}")
            raise

    async def _identify_patterns(self, model_name: str) -> List[Dict[str, Any]]:
        """Identify learning patterns for a model"""
        try:
            # Get performance history
            performance_history = self.model_performance[model_name]
            
            # Extract patterns
            patterns = await self.pattern_matcher.find_patterns(performance_history)
            
            # Analyze pattern significance
            significant_patterns = self._analyze_pattern_significance(patterns)
            
            # Update metrics
            self.metrics["patterns_identified"] += len(significant_patterns)
            
            return significant_patterns
            
        except Exception as e:
            self.logger.error(f"Pattern identification failed: {e}")
            raise

    async def _update_learning_rules(self, patterns: List[Dict[str, Any]]):
        """Update learning rules based on identified patterns"""
        try:
            for pattern in patterns:
                # Generate rule from pattern
                rule = await self._generate_rule(pattern)
                
                # Validate rule
                if self._validate_rule(rule):
                    # Add or update rule
                    self.learning_rules[rule["id"]] = rule
                    
            # Prune obsolete rules
            self._prune_learning_rules()
            
        except Exception as e:
            self.logger.error(f"Learning rules update failed: {e}")
            raise

    def _analyze_performance(self, model_name: str) -> Dict[str, Any]:
        """Analyze model performance"""
        try:
            performance_data = self.model_performance[model_name]
            
            analysis = {
                "accuracy_trend": self._calculate_trend(performance_data["accuracy"]),
                "response_time_trend": self._calculate_trend(performance_data["response_time"]),
                "success_rate_trend": self._calculate_trend(performance_data["success_rate"]),
                "adaptation_rate_trend": self._calculate_trend(performance_data["adaptation_rate"]),
                "overall_performance": self._calculate_overall_performance(performance_data)
            }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Performance analysis failed: {e}")
            raise

    async def _generate_optimization_strategies(self,
                                             analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate optimization strategies based on performance analysis"""
        try:
            strategies = []
            
            # Generate strategies for each performance aspect
            for aspect, trend in analysis.items():
                if aspect != "overall_performance":
                    strategy = await self._generate_strategy(aspect, trend)
                    if strategy:
                        strategies.append(strategy)
                        
            # Generate combined strategies
            combined_strategies = await self._generate_combined_strategies(strategies)
            
            return strategies + combined_strategies
            
        except Exception as e:
            self.logger.error(f"Strategy generation failed: {e}")
            raise

    async def _evaluate_strategies(self,
                                 strategies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Evaluate potential optimization strategies"""
        try:
            evaluated_strategies = []
            
            for strategy in strategies:
                # Simulate strategy application
                simulation_result = await self._simulate_strategy(strategy)
                
                # Calculate expected improvement
                expected_improvement = self._calculate_expected_improvement(simulation_result)
                
                # Calculate implementation cost
                implementation_cost = self._calculate_implementation_cost(strategy)
                
                # Calculate overall score
                score = self._calculate_strategy_score(expected_improvement, implementation_cost)
                
                evaluated_strategies.append({
                    "strategy": strategy,
                    "expected_improvement": expected_improvement,
                    "implementation_cost": implementation_cost,
                    "score": score
                })
                
            return sorted(evaluated_strategies, key=lambda x: x["score"], reverse=True)
            
        except Exception as e:
            self.logger.error(f"Strategy evaluation failed: {e}")
            raise

    def _select_best_strategy(self,
                            evaluated_strategies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Select the best optimization strategy"""
        try:
            if not evaluated_strategies:
                raise ValueError("No strategies to select from")
                
            return evaluated_strategies[0]["strategy"]
            
        except Exception as e:
            self.logger.error(f"Strategy selection failed: {e}")
            raise

    async def _apply_optimization(self,
                                model_name: str,
                                strategy: Dict[str, Any]) -> Dict[str, Any]:
        """Apply selected optimization strategy"""
        try:
            # Prepare optimization
            optimization_params = self._prepare_optimization_params(strategy)
            
            # Apply optimization
            result = await self.strategy_optimizer.apply_strategy(
                model_name,
                optimization_params
            )
            
            # Validate result
            if self._validate_optimization_result(result):
                self.metrics["successful_optimizations"] += 1
            else:
                self.metrics["failed_optimizations"] += 1
                
            return result
            
        except Exception as e:
            self.logger.error(f"Optimization application failed: {e}")
            self.metrics["failed_optimizations"] += 1
            raise

    def get_meta_learning_statistics(self) -> Dict[str, Any]:
        """Get meta-learning system statistics"""
        return {
            "patterns_identified": self.metrics["patterns_identified"],
            "successful_optimizations": self.metrics["successful_optimizations"],
            "failed_optimizations": self.metrics["failed_optimizations"],
            "average_learning_efficiency": np.mean(self.metrics["learning_efficiency"])
                if self.metrics["learning_efficiency"] else 0
        }








class PatternAnalyzer:
    """Analyzes patterns in solutions and evidence"""
    def __init__(self):
        self.pattern_history = []
        self.pattern_frequencies = defaultdict(int)
        self.pattern_scores = {}
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            "patterns_analyzed": 0,
            "pattern_matches": 0,
            "processing_times": []
        }
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.nlp = None  # Will be initialized with spaCy

    async def initialize(self):
        """Initialize pattern analyzer"""
        try:
            import spacy
            self.nlp = spacy.load("en_core_web_sm")
            self._initialize_pattern_rules()
            self.logger.info("Pattern analyzer initialized successfully")
        except Exception as e:
            self.logger.error(f"Pattern analyzer initialization failed: {e}")
            raise

    async def analyze_patterns(self, content: Any) -> Dict[str, Any]:
        """Analyze patterns in content"""
        analysis_id = f"pattern_{datetime.now().timestamp()}"
        start_time = time.time()
        
        try:
            # Check cache
            cache_key = self._generate_cache_key(content)
            if cache_key in self.cache:
                return self.cache[cache_key]

            # Extract patterns
            structural_patterns = self._extract_structural_patterns(content)
            semantic_patterns = await self._extract_semantic_patterns(content)
            temporal_patterns = self._extract_temporal_patterns(content)
            
            # Analyze patterns
            pattern_analysis = {
                "analysis_id": analysis_id,
                "structural": structural_patterns,
                "semantic": semantic_patterns,
                "temporal": temporal_patterns,
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "processing_time": time.time() - start_time
                }
            }
            
            # Update metrics
            self._update_metrics(pattern_analysis)
            
            # Cache result
            self.cache[cache_key] = pattern_analysis
            
            return pattern_analysis
            
        except Exception as e:
            self.logger.error(f"Pattern analysis failed: {e}")
            raise

    def _extract_structural_patterns(self, content: Any) -> Dict[str, Any]:
        """Extract structural patterns from content"""
        try:
            patterns = {
                "repetition": self._find_repetition_patterns(content),
                "hierarchy": self._find_hierarchy_patterns(content),
                "sequence": self._find_sequence_patterns(content)
            }
            return patterns
        except Exception as e:
            self.logger.error(f"Structural pattern extraction failed: {e}")
            raise

    async def _extract_semantic_patterns(self, content: Any) -> Dict[str, Any]:
        """Extract semantic patterns from content"""
        try:
            doc = self.nlp(str(content))
            patterns = {
                "entities": self._extract_entities(doc),
                "relationships": self._extract_relationships(doc),
                "concepts": await self._extract_concepts(doc)
            }
            return patterns
        except Exception as e:
            self.logger.error(f"Semantic pattern extraction failed: {e}")
            raise

    def _extract_temporal_patterns(self, content: Any) -> Dict[str, Any]:
        """Extract temporal patterns from content"""
        try:
            patterns = {
                "sequence": self._find_temporal_sequence(content),
                "frequency": self._find_temporal_frequency(content),
                "duration": self._find_temporal_duration(content)
            }
            return patterns
        except Exception as e:
            self.logger.error(f"Temporal pattern extraction failed: {e}")
            raise

class SemanticValidator:
    """Validates semantic coherence and consistency"""
    def __init__(self):
        self.validation_history = []
        self.semantic_rules = {}
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            "validations_performed": 0,
            "validation_failures": 0,
            "processing_times": []
        }
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.nlp = None
        self.embedding_model = None

    async def initialize(self):
        """Initialize semantic validator"""
        try:
            import spacy
            import tensorflow_hub as hub
            
            self.nlp = spacy.load("en_core_web_sm")
            self.embedding_model = hub.load("https://tfhub.dev/google/universal-sentence-encoder/4")
            
            self._initialize_semantic_rules()
            self.logger.info("Semantic validator initialized successfully")
        except Exception as e:
            self.logger.error(f"Semantic validator initialization failed: {e}")
            raise

    async def check_coherence(self, content: Any) -> float:
        """Check semantic coherence"""
        validation_id = f"validation_{datetime.now().timestamp()}"
        start_time = time.time()
        
        try:
            # Check cache
            cache_key = self._generate_cache_key(content)
            if cache_key in self.cache:
                return self.cache[cache_key]

            # Perform coherence checks
            topic_coherence = await self._check_topic_coherence(content)
            argument_coherence = await self._check_argument_coherence(content)
            narrative_coherence = await self._check_narrative_coherence(content)
            
            # Calculate overall coherence
            coherence_score = np.mean([
                topic_coherence,
                argument_coherence,
                narrative_coherence
            ])
            
            # Record validation
            validation_result = {
                "validation_id": validation_id,
                "scores": {
                    "topic_coherence": topic_coherence,
                    "argument_coherence": argument_coherence,
                    "narrative_coherence": narrative_coherence,
                    "overall_coherence": coherence_score
                },
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "processing_time": time.time() - start_time
                }
            }
            
            # Update metrics
            self._update_metrics(validation_result)
            
            # Cache result
            self.cache[cache_key] = coherence_score
            
            return coherence_score
            
        except Exception as e:
            self.logger.error(f"Coherence check failed: {e}")
            self.metrics["validation_failures"] += 1
            raise

    async def _check_topic_coherence(self, content: Any) -> float:
        """Check topic coherence"""
        try:
            # Extract topics
            topics = self._extract_topics(content)
            
            # Calculate topic similarity
            topic_similarities = await self._calculate_topic_similarities(topics)
            
            # Calculate coherence score
            coherence_score = np.mean(topic_similarities) if topic_similarities else 0.0
            
            return min(1.0, max(0.0, coherence_score))
            
        except Exception as e:
            self.logger.error(f"Topic coherence check failed: {e}")
            return 0.0

    async def _check_argument_coherence(self, content: Any) -> float:
        """Check argument coherence"""
        try:
            # Extract arguments
            arguments = self._extract_arguments(content)
            
            # Check argument structure
            structure_score = self._check_argument_structure(arguments)
            
            # Check argument flow
            flow_score = await self._check_argument_flow(arguments)
            
            # Calculate coherence score
            coherence_score = 0.6 * structure_score + 0.4 * flow_score
            
            return min(1.0, max(0.0, coherence_score))
            
        except Exception as e:
            self.logger.error(f"Argument coherence check failed: {e}")
            return 0.0

    async def _check_narrative_coherence(self, content: Any) -> float:
        """Check narrative coherence"""
        try:
            # Extract narrative elements
            elements = self._extract_narrative_elements(content)
            
            # Check narrative structure
            structure_score = self._check_narrative_structure(elements)
            
            # Check narrative flow
            flow_score = await self._check_narrative_flow(elements)
            
            # Calculate coherence score
            coherence_score = 0.5 * structure_score + 0.5 * flow_score
            
            return min(1.0, max(0.0, coherence_score))
            
        except Exception as e:
            self.logger.error(f"Narrative coherence check failed: {e}")
            return 0.0

    def _generate_cache_key(self, content: Any) -> str:
        """Generate cache key for content"""
        return hashlib.md5(str(content).encode()).hexdigest()

    def get_validation_statistics(self) -> Dict[str, Any]:
        """Get validation statistics"""
        return {
            "total_validations": self.metrics["validations_performed"],
            "validation_failures": self.metrics["validation_failures"],
            "average_processing_time": np.mean(self.metrics["processing_times"])
                if self.metrics["processing_times"] else 0
        }


class ValidationFramework:
    """Framework for system validation and quality assurance"""
    def __init__(self):
        self.validation_metrics = defaultdict(dict)
        self.quality_thresholds = {
            'response_quality': 0.8,
            'performance_score': 0.7,
            'reliability_score': 0.9
        }
        
    async def validate_system(self) -> Dict[str, Any]:
        """Perform comprehensive system validation"""
        validation_tasks = [
            self._validate_moa_integration(),
            self._validate_performance(),
            self._validate_reliability(),
            self._validate_resource_usage()
        ]
        
        results = await asyncio.gather(*validation_tasks)
        return self._compile_validation_results(results)

class IntegrationTestSuite:
    """Comprehensive integration testing suite"""
    def __init__(self, config: EnhancedConfig):
        self.config = config
        self.test_results = defaultdict(list)
        self.system_metrics = SystemMetricsCollector()
        self.validation_framework = ValidationFramework()
        
    async def run_integration_tests(self) -> Dict[str, Any]:
        """Execute full integration test suite"""
        test_suites = {
            'moa_integration': self._test_moa_integration,
            'agent_communication': self._test_agent_communication,
            'error_handling': self._test_error_handling,
            'performance': self._test_performance,
            'resource_management': self._test_resource_management
        }
        
        results = {}
        for suite_name, test_func in test_suites.items():
            try:
                logger.info(f"Running test suite: {suite_name}")
                results[suite_name] = await test_func()
            except Exception as e:
                logger.error(f"Test suite {suite_name} failed: {e}")
                results[suite_name] = {'status': 'failed', 'error': str(e)}
                
        return await self._analyze_test_results(results)

class MetricsCollector:
    """Collects and manages system metrics"""
    def __init__(self):
        self.metrics_history = defaultdict(list)
        self.current_metrics = {}
        self.logger = logging.getLogger(__name__)

    async def collect_metrics(self) -> Dict[str, Any]:
        """Collect current system metrics"""
        try:
            metrics = {
                'cpu_usage': psutil.cpu_percent(),
                'memory_usage': psutil.virtual_memory().percent,
                'disk_usage': psutil.disk_usage('/').percent,
                'network_io': psutil.net_io_counters()._asdict()
            }
            
            self.current_metrics = metrics
            self._update_history(metrics)
            
            return metrics
        except Exception as e:
            self.logger.error(f"Metrics collection failed: {e}")
            raise

    def _update_history(self, metrics: Dict[str, Any]) -> None:
        """Update metrics history"""
        for metric, value in metrics.items():
            self.metrics_history[metric].append({
                'value': value,
                'timestamp': datetime.now().isoformat()
            })

    def get_metrics_history(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get historical metrics"""
        return dict(self.metrics_history)

    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return self.current_metrics
    
class EnhancedValidator:
    """Enhanced validation with proper type checking"""
    
    def validate_storage_config(self, config: Dict[str, Any]) -> bool:
        if not isinstance(config, dict):
            return False
        required_fields = {'type', 'compression', 'backup_enabled', 'path'}
        valid_types = {
            'type': str,
            'compression': bool,
            'backup_enabled': bool,
            'path': str
        }
        return (all(field in config for field in required_fields) and
                all(isinstance(config[field], valid_types[field])
                    for field in required_fields))

    def validate_cache_config(self, config: Dict[str, Any]) -> bool:
        if not isinstance(config, dict):
            return False
        required_fields = {'enabled', 'max_size', 'ttl', 'strategy'}
        valid_types = {
            'enabled': bool,
            'max_size': int,
            'ttl': int,
            'strategy': str
        }
        return (all(field in config for field in required_fields) and
                all(isinstance(config[field], valid_types[field])
                    for field in required_fields))

    def validate_indexing_config(self, config: Dict[str, Any]) -> bool:
        if not isinstance(config, dict):
            return False
        required_fields = {'enabled', 'fields', 'auto_update'}
        valid_types = {
            'enabled': bool,
            'fields': list,
            'auto_update': bool
        }
        return (all(field in config for field in required_fields) and
                all(isinstance(config[field], valid_types[field])
                    for field in required_fields))

    def validate_validation_config(self, config: Dict[str, Any]) -> bool:
        if not isinstance(config, dict):
            return False
        required_fields = {'required_fields', 'max_size', 'strict_mode'}
        valid_types = {
            'required_fields': list,
            'max_size': int,
            'strict_mode': bool
        }
        return (all(field in config for field in required_fields) and
                all(isinstance(config[field], valid_types[field])
                    for field in required_fields))
    
class EvidenceAnalyzer:
    """Analyzes and validates evidence for solution generation"""
    def __init__(self):
        self.analysis_history = []
        self.quality_metrics = defaultdict(list)
        self.validation_rules = {}
        self.evidence_patterns = defaultdict(int)
        self.logger = logging.getLogger(__name__)
        self.metrics = {
            "analyses_performed": 0,
            "validation_failures": 0,
            "quality_scores": [],
            "processing_times": []
        }
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.pattern_matcher = PatternMatcher()
        self.confidence_calculator = ConfidenceCalculator()

    async def initialize(self):
        """Initialize evidence analyzer"""
        try:
            self._initialize_validation_rules()
            await self.pattern_matcher.initialize()
            await self.confidence_calculator.initialize()
            self.logger.info("Evidence analyzer initialized successfully")
        except Exception as e:
            self.logger.error(f"Evidence analyzer initialization failed: {e}")
            raise

    async def analyze_evidence(self, evidence: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze evidence quality and relevance"""
        analysis_id = f"analysis_{datetime.now().timestamp()}"
        start_time = time.time()
        
        try:
            # Check cache
            cache_key = self._generate_cache_key(evidence)
            if cache_key in self.cache:
                return self.cache[cache_key]

            # Validate evidence structure
            self._validate_evidence_structure(evidence)
            
            # Analyze quality
            quality_scores = await self._analyze_quality(evidence)
            
            # Analyze relevance
            relevance_scores = await self._analyze_relevance(evidence)
            
            # Analyze consistency
            consistency_score = await self._analyze_consistency(evidence)
            
            # Calculate overall score
            overall_score = self._calculate_overall_score(
                quality_scores,
                relevance_scores,
                consistency_score
            )
            
            # Prepare analysis result
            analysis_result = {
                "analysis_id": analysis_id,
                "quality_scores": quality_scores,
                "relevance_scores": relevance_scores,
                "consistency_score": consistency_score,
                "overall_score": overall_score,
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "processing_time": time.time() - start_time
                }
            }
            
            # Update metrics
            self._update_metrics(analysis_result)
            
            # Cache result
            self.cache[cache_key] = analysis_result
            
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"Evidence analysis failed: {e}")
            self.metrics["validation_failures"] += 1
            raise

    async def _analyze_quality(self, evidence: Dict[str, Any]) -> Dict[str, float]:
        """Analyze evidence quality metrics"""
        try:
            quality_scores = {}
            
            for key, value in evidence.items():
                # Completeness check
                completeness = self._check_completeness(value)
                
                # Accuracy check
                accuracy = await self._check_accuracy(value)
                
                # Reliability check
                reliability = self._check_reliability(value)
                
                quality_scores[key] = {
                    "completeness": completeness,
                    "accuracy": accuracy,
                    "reliability": reliability,
                    "overall": np.mean([completeness, accuracy, reliability])
                }
                
            return quality_scores
            
        except Exception as e:
            self.logger.error(f"Quality analysis failed: {e}")
            raise

    async def _analyze_relevance(self, evidence: Dict[str, Any]) -> Dict[str, float]:
        """Analyze evidence relevance"""
        try:
            relevance_scores = {}
            
            for key, value in evidence.items():
                # Context relevance
                context_relevance = await self._check_context_relevance(value)
                
                # Time relevance
                time_relevance = self._check_time_relevance(value)
                
                # Source relevance
                source_relevance = self._check_source_relevance(value)
                
                relevance_scores[key] = {
                    "context": context_relevance,
                    "time": time_relevance,
                    "source": source_relevance,
                    "overall": np.mean([context_relevance, time_relevance, source_relevance])
                }
                
            return relevance_scores
            
        except Exception as e:
            self.logger.error(f"Relevance analysis failed: {e}")
            raise

    async def _analyze_consistency(self, evidence: Dict[str, Any]) -> float:
        """Analyze evidence consistency"""
        try:
            # Check internal consistency
            internal_consistency = self._check_internal_consistency(evidence)
            
            # Check cross-reference consistency
            cross_consistency = await self._check_cross_consistency(evidence)
            
            # Check temporal consistency
            temporal_consistency = self._check_temporal_consistency(evidence)
            
            return np.mean([
                internal_consistency,
                cross_consistency,
                temporal_consistency
            ])
            
        except Exception as e:
            self.logger.error(f"Consistency analysis failed: {e}")
            raise

    def _validate_evidence_structure(self, evidence: Dict[str, Any]):
        """Validate evidence structure"""
        if not isinstance(evidence, dict):
            raise ValueError("Evidence must be a dictionary")
            
        for key, value in evidence.items():
            if not self._validate_evidence_item(value):
                raise ValueError(f"Invalid evidence item: {key}")

    def _validate_evidence_item(self, item: Any) -> bool:
        """Validate individual evidence item"""
        try:
            # Check if item has required fields
            required_fields = ["content", "source", "timestamp"]
            if isinstance(item, dict):
                return all(field in item for field in required_fields)
            return False
        except Exception as e:
            self.logger.error(f"Evidence item validation failed: {e}")
            return False

    def _check_completeness(self, evidence_item: Dict[str, Any]) -> float:
        """Check evidence completeness"""
        try:
            required_fields = ["content", "source", "timestamp"]
            optional_fields = ["metadata", "confidence", "context"]
            
            # Calculate completeness score
            required_score = sum(1 for field in required_fields if field in evidence_item) / len(required_fields)
            optional_score = sum(1 for field in optional_fields if field in evidence_item) / len(optional_fields)
            
            return 0.7 * required_score + 0.3 * optional_score
            
        except Exception as e:
            self.logger.error(f"Completeness check failed: {e}")
            return 0.0

    async def _check_accuracy(self, evidence_item: Dict[str, Any]) -> float:
        """Check evidence accuracy"""
        try:
            # Source reliability weight
            source_weight = self._get_source_reliability_weight(evidence_item.get("source"))
            
            # Content validation weight
            content_weight = await self._validate_content(evidence_item.get("content"))
            
            # Confidence weight
            confidence_weight = evidence_item.get("confidence", 0.5)
            
            return np.mean([source_weight, content_weight, confidence_weight])
            
        except Exception as e:
            self.logger.error(f"Accuracy check failed: {e}")
            return 0.0

    def _check_reliability(self, evidence_item: Dict[str, Any]) -> float:
        """Check evidence reliability"""
        try:
            # Source verification
            source_score = self._verify_source(evidence_item.get("source"))
            
            # Timestamp verification
            time_score = self._verify_timestamp(evidence_item.get("timestamp"))
            
            # Context verification
            context_score = self._verify_context(evidence_item.get("context"))
            
            return np.mean([source_score, time_score, context_score])
            
        except Exception as e:
            self.logger.error(f"Reliability check failed: {e}")
            return 0.0

    def _calculate_overall_score(self,
                               quality_scores: Dict[str, float],
                               relevance_scores: Dict[str, float],
                               consistency_score: float) -> float:
        """Calculate overall evidence score"""
        try:
            # Calculate average quality score
            avg_quality = np.mean([scores["overall"] for scores in quality_scores.values()])
            
            # Calculate average relevance score
            avg_relevance = np.mean([scores["overall"] for scores in relevance_scores.values()])
            
            # Weighted combination
            weights = {
                "quality": 0.4,
                "relevance": 0.4,
                "consistency": 0.2
            }
            
            overall_score = (
                weights["quality"] * avg_quality +
                weights["relevance"] * avg_relevance +
                weights["consistency"] * consistency_score
            )
            
            return min(1.0, max(0.0, overall_score))
            
        except Exception as e:
            self.logger.error(f"Overall score calculation failed: {e}")
            return 0.0

    def _update_metrics(self, analysis_result: Dict[str, Any]):
        """Update analyzer metrics"""
        try:
            self.metrics["analyses_performed"] += 1
            self.metrics["quality_scores"].append(analysis_result["overall_score"])
            self.metrics["processing_times"].append(
                analysis_result["metadata"]["processing_time"]
            )
            
        except Exception as e:
            self.logger.error(f"Metrics update failed: {e}")

    def _generate_cache_key(self, evidence: Dict[str, Any]) -> str:
        """Generate cache key for evidence"""
        return hashlib.md5(str(sorted(evidence.items())).encode()).hexdigest()

    def get_analyzer_statistics(self) -> Dict[str, Any]:
        """Get analyzer statistics"""
        return {
            "total_analyses": self.metrics["analyses_performed"],
            "validation_failures": self.metrics["validation_failures"],
            "average_quality": np.mean(self.metrics["quality_scores"])
                if self.metrics["quality_scores"] else 0,
            "average_processing_time": np.mean(self.metrics["processing_times"])
                if self.metrics["processing_times"] else 0
        }

class IntegratedSystem:
    """Main system integrating all components"""
    def __init__(self):
        self.config = EnhancedConfig()
        self.logger = logging.getLogger(__name__)
        self.rewoo_system = None
        self.execution_manager = None
        self.resource_manager = None
        self.initialized = False
        self.system_metrics = defaultdict(dict)

    async def initialize(self):
        """Initialize all system components"""
        try:
            # Initialize configuration
            await self.config.initialize()

            # Initialize core components
            self.rewoo_system = REWOOSystem(self.config)
            self.execution_manager = ExecutionManager(self.config)
            self.resource_manager = UnifiedResourceManager()

            # Initialize all components
            await self._initialize_components()
            
            self.initialized = True
            self.logger.info("Integrated system initialized successfully")
        except Exception as e:
            self.logger.error(f"System initialization failed: {e}")
            raise

    async def _initialize_components(self):
        """Initialize all system components in proper order"""
        try:
            # Initialize resource management
            await self.resource_manager.initialize()
            
            # Initialize REWOO system
            await self.rewoo_system.initialize()
            
            # Initialize execution manager
            await self.execution_manager.initialize()
            
        except Exception as e:
            self.logger.error(f"Component initialization failed: {e}")
            raise

    async def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming request through the system"""
        request_id = f"request_{datetime.now().timestamp()}"
        
        try:
            # Validate request
            self._validate_request(request)
            
            # Create workflow configuration
            workflow_config = self._create_workflow_config(request)
            
            # Process through REWOO
            rewoo_result = await self.rewoo_system.process_task(request)
            
            # Execute workflow
            execution_result = await self.execution_manager.execute_workflow(workflow_config)
            
            # Combine and validate results
            final_result = await self._combine_results(rewoo_result, execution_result)
            
            return {
                'request_id': request_id,
                'result': final_result,
                'metadata': self._generate_request_metadata(request_id)
            }
            
        except Exception as e:
            self.logger.error(f"Request processing failed: {e}")
            await self._handle_request_error(request_id, e)
            raise

class SystemMetrics:
    """System-wide metrics tracking"""
    def __init__(self):
        self.metrics = defaultdict(lambda: defaultdict(list))
        self.start_time = datetime.now()

    def record_metric(self, category: str, name: str, value: Any):
        """Record a metric with timestamp"""
        self.metrics[category][name].append({
            'value': value,
            'timestamp': datetime.now().isoformat()
        })

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics"""
        return {
            category: {
                name: self._calculate_metric_summary(values)
                for name, values in metrics.items()
            }
            for category, metrics in self.metrics.items()
        }

class SystemHealth:
    """System health monitoring"""
    def __init__(self):
        self.health_checks = defaultdict(dict)
        self.alert_thresholds = {
            'cpu_usage': 0.8,
            'memory_usage': 0.8,
            'error_rate': 0.1
        }

    async def check_health(self) -> Dict[str, Any]:
        """Perform system health check"""
        return {
            'cpu_usage': psutil.cpu_percent() / 100,
            'memory_usage': psutil.virtual_memory().percent / 100,
            'disk_usage': psutil.disk_usage('/').percent / 100,
            'timestamp': datetime.now().isoformat()
        }

# Main Application
class EnhancedApplication:
    """Main application with improved initialization"""
    def __init__(self):
        self.config = None
        self.moa_system = None
        self.workflow_orchestrator = None
        self.health_monitor = None
        self.loop = asyncio.get_event_loop()

    async def initialize(self, config_path: str):
        """Initialize application with proper error handling"""
        try:
            # Initialize configuration
            self.config = EnhancedConfig()
            await self.config.initialize()
            
            # Initialize components
            self.moa_system = EnhancedMoASystem(self.config)
            self.workflow_orchestrator = WorkflowOrchestrator(self.config)
            self.health_monitor = SystemHealthMonitor(self.config)
            
            # Initialize components asynchronously
            await self.moa_system.initialize()
            await self.workflow_orchestrator.initialize()
            await self.health_monitor.initialize()
            
            logger.info("Application initialized successfully")
        except Exception as e:
            logger.error(f"Application initialization failed: {e}")
            raise
# Import necessary modules if not already imported
import asyncio
import nest_asyncio
import logging
from typing import Optional

# Apply nest_asyncio at the start to allow nested event loops in Jupyter
nest_asyncio.apply()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def initialize_application():
    """Initialize all application components"""
    try:
        # Initialize configuration
        config = await initialize_moa_config()
        
        # Initialize core components
        artifact_registry = ArtifactRegistry()
        await artifact_registry.initialize()
        
        communication_manager = CommunicationManager(config)
        await communication_manager.initialize()
        
        validation_manager = ValidationManager(config)
        await validation_manager.initialize()
        
        return {
            'config': config,
            'artifact_registry': artifact_registry,
            'communication_manager': communication_manager,
            'validation_manager': validation_manager
        }
    except Exception as e:
        logger.error(f"Application initialization failed: {e}")
        raise

def initialize_notebook_environment():
    """Initialize the notebook environment and return components"""
    try:
        # Initialize all components
        components = asyncio.run(initialize_application())
        
        # Extract commonly used components
        config = components['config']
        model_config = config.get_model_config('BossAgent')
        bigquery_client = config.get_client('bigquery')
        anthropic_client = config.get_client('anthropic')
        cache = config.enhanced_config.cache_manager
        
        logger.info("Notebook environment initialized successfully")
        
        return components, model_config, bigquery_client, anthropic_client, cache
    except Exception as e:
        logger.error(f"Notebook initialization failed: {e}")
        raise


# Add this before the main() function in code_cell_6_2
async def process_sample_tasks(orchestrator: WorkflowOrchestrator) -> Dict[str, Any]:
    """Process sample workflow tasks using the MoA framework."""
    try:
        workflow_tasks = [
            {
                "task": "Analyze customer journey data",
                "type": "analysis",
                "priority": 1,
                "metadata": {
                    "data_source": "bigquery",
                    "time_range": "last_30_days",
                    "requirements": ["customer_interactions", "purchase_history"]
                }
            },
            {
                "task": "Generate customer insights report",
                "type": "reporting",
                "priority": 2,
                "metadata": {
                    "format": "structured",
                    "sections": ["behavior_patterns", "conversion_analysis", "recommendations"]
                }
            }
        ]
        
        workflow_id = f"workflow_{datetime.now().timestamp()}"
        logger.info(f"Starting workflow {workflow_id}")
        
        # Ensure orchestrator is initialized
        if not orchestrator.initialized:
            await orchestrator.initialize()
        
        # Process tasks through MoA layers
        results = await orchestrator.execute_workflow(workflow_id, workflow_tasks)
        
        logger.info(f"Workflow {workflow_id} completed successfully")
        logger.debug(f"Workflow results: {results}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error processing sample tasks: {str(e)}")
        raise
# Main initialization function
async def initialize_moa_config() -> EnhancedConfig:
    """Initialize MOA configuration"""
    try:
        config = EnhancedConfig()
        await config.initialize()
        return config
    except Exception as e:
        logger.error(f"MOA configuration initialization failed: {e}")
        raise

# Main execution
async def main():
    """Main execution with proper error handling"""
    try:
        config = await initialize_moa_config()
        return {"status": "success", "config": config}
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return {"status": "error", "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {"status": "error", "error": str(e)}

if __name__ == "__main__":
    result = asyncio.run(main())
    if result["status"] == "error":
        print(f"Error: {result['error']}")
        sys.exit(1)
