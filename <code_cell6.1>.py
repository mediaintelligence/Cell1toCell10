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
  
