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
