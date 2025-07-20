#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Agent AI System - Production Ready
========================================

A comprehensive multi-agent system with REWOO capabilities, evidence tracking,
and layer-based processing architecture.

Author: AI Assistant
Version: 1.0.0
"""

import asyncio
import logging
import os
import json
import time
import hashlib
import uuid
import traceback
import statistics
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Type, Set
from collections import defaultdict, Counter, deque
from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod
import nest_asyncio

# External dependencies
import pandas as pd
import numpy as np
import networkx as nx
from cachetools import TTLCache
from dotenv import load_dotenv

# Apply nest_asyncio for Jupyter compatibility
nest_asyncio.apply()

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass

class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass

class InitializationError(Exception):
    """Custom exception for initialization errors"""
    pass

class ResourceError(Exception):
    """Custom exception for resource-related errors"""
    pass

class CommunicationError(Exception):
    """Exception raised for communication-related errors"""
    pass

# ============================================================================
# CONFIGURATION DATACLASSES
# ============================================================================

@dataclass
class ModelConfig:
    """Enhanced model configuration with validation"""
    model_name: str
    api_key: Optional[str] = None
    layer_id: int = 0
    version: str = "latest"
    max_tokens: int = 4000
    temperature: float = 0.7
    top_p: float = 1.0
    context_window: int = 8192
    pool_size: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.model_name:
            raise ValueError("model_name is required")
        if self.max_tokens <= 0:
            raise ValueError("max_tokens must be positive")
        if not 0 <= self.temperature <= 2:
            raise ValueError("temperature must be between 0 and 2")
        if not 0 <= self.top_p <= 1:
            raise ValueError("top_p must be between 0 and 1")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

@dataclass
class LayerConfig:
    """Layer configuration with validation"""
    layer_id: int
    agents: List[str] = field(default_factory=list)
    model_name: str = ""
    pool_size: int = 1
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate configuration"""
        if self.layer_id < 0:
            raise ValueError("layer_id must be non-negative")
        if self.pool_size <= 0:
            raise ValueError("pool_size must be positive")

@dataclass
class EvidenceStoreConfig:
    """Evidence store configuration"""
    storage_type: str = "memory"
    compression: bool = False
    backup_enabled: bool = False
    cache_size: int = 1000
    ttl: int = 3600
    validation_rules: Dict[str, Any] = field(default_factory=lambda: {
        'required_fields': ['content', 'source', 'timestamp'],
        'max_size': 1024 * 1024  # 1MB
    })
    
    def __post_init__(self):
        """Validate configuration"""
        valid_types = ['memory', 'disk', 'distributed']
        if self.storage_type not in valid_types:
            raise ValueError(f"storage_type must be one of {valid_types}")

@dataclass
class CommunicationConfig:
    """Communication system configuration"""
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

# ============================================================================
# EVIDENCE SYSTEM
# ============================================================================

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

class EvidenceStore:
    """Enhanced evidence store with proper configuration handling"""
    
    def __init__(self, config: Optional[EvidenceStoreConfig] = None):
        self.config = config or EvidenceStoreConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.initialized = False
        
        # Core storage
        self._storage: Dict[str, Any] = {}
        self._cache: Optional[TTLCache] = None
        self._index: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
        
        # Metrics and tracking
        self.metrics = defaultdict(Counter)
        self.recent_additions = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        
    async def initialize(self) -> None:
        """Initialize evidence store with proper validation"""
        try:
            # Initialize storage
            self._storage = {}
            
            # Initialize cache if enabled
            if self.config.cache_size > 0:
                self._cache = TTLCache(
                    maxsize=self.config.cache_size,
                    ttl=self.config.ttl
                )
            
            # Initialize index
            self._index = defaultdict(lambda: defaultdict(list))
            
            self.initialized = True
            self.logger.info("Evidence store initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Evidence store initialization failed: {e}")
            raise

    async def store_evidence(self, key: str, value: Any, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Store evidence with validation and indexing"""
        try:
            if not self.initialized:
                raise RuntimeError("Evidence store not initialized")
            
            # Validate evidence
            if not self._validate_evidence(value, metadata or {}):
                raise ValidationError("Evidence validation failed")
            
            # Create evidence record
            evidence_record = {
                'value': value,
                'metadata': metadata or {},
                'timestamp': datetime.now().isoformat(),
                'id': str(uuid.uuid4())
            }

            # Store evidence
            async with self._lock:
                self._storage[key] = evidence_record
                self._update_indices(key, evidence_record)
                self.recent_additions.append(key)
                self.metrics['stored_evidence'] += 1

            return evidence_record['id']

        except Exception as e:
            self.logger.error(f"Evidence storage failed: {e}")
            raise

    async def get_evidence(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve evidence by key"""
        try:
            if not self.initialized:
                raise RuntimeError("Evidence store not initialized")
            
            return self._storage.get(key)
            
        except Exception as e:
            self.logger.error(f"Evidence retrieval failed: {e}")
            return None

    def _validate_evidence(self, value: Any, metadata: Dict[str, Any]) -> bool:
        """Validate evidence content and metadata"""
        try:
            # Check required fields
            required_fields = self.config.validation_rules.get('required_fields', [])
            if not all(field in metadata for field in required_fields):
                return False

            # Check size limit
            content_size = len(str(value).encode('utf-8'))
            max_size = self.config.validation_rules.get('max_size', 1024 * 1024)
            if content_size > max_size:
                return False

            return True
            
        except Exception as e:
            self.logger.error(f"Evidence validation failed: {e}")
            return False

    def _update_indices(self, key: str, evidence_record: Dict[str, Any]) -> None:
        """Update evidence indices"""
        try:
            metadata = evidence_record['metadata']
            
            # Update type index
            if 'type' in metadata:
                self._index['type'][metadata['type']].append(key)
            
            # Update source index
            if 'source' in metadata:
                self._index['source'][metadata['source']].append(key)
            
            # Update timestamp index
            timestamp = evidence_record['timestamp']
            date_key = timestamp.split('T')[0]
            self._index['timestamp'][date_key].append(key)
            
        except Exception as e:
            self.logger.error(f"Index update failed: {e}")

    async def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of available evidence"""
        try:
            if not self.initialized:
                raise RuntimeError("Evidence store not initialized")

            return {
                'total_evidence': len(self._storage),
                'evidence_types': {
                    type_name: len(keys) 
                    for type_name, keys in self._index['type'].items()
                },
                'sources': {
                    source: len(keys) 
                    for source, keys in self._index['source'].items()
                },
                'recent_additions': len(self.recent_additions),
                'metrics': dict(self.metrics),
                'initialized': self.initialized
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get summary: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup evidence store resources"""
        try:
            self._storage.clear()
            self._index.clear()
            self.metrics.clear()
            self.recent_additions.clear()
            
            if self._cache:
                self._cache.clear()
            
            self.initialized = False
            self.logger.info("Evidence store cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Evidence store cleanup failed: {e}")
            raise

# ============================================================================
# COMMUNICATION SYSTEM
# ============================================================================

class CommunicationSystem:
    """Enhanced communication system with proper initialization"""
    
    def __init__(self, config: Optional[CommunicationConfig] = None):
        self.config = config or CommunicationConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.initialized = False
        
        # Core communication
        self.channels: Dict[str, asyncio.Queue] = {}
        self.subscribers: Dict[str, Set[str]] = defaultdict(set)
        self.message_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # State tracking
        self.channel_states: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self.agent_registrations: Dict[str, Dict[str, Any]] = {}
        
        # Metrics and synchronization
        self.metrics = defaultdict(Counter)
        self._lock = asyncio.Lock()
        
    async def initialize(self) -> None:
        """Initialize communication system with proper sequence"""
        try:
            # Initialize core channels
            await self._initialize_core_channels()
            
            # Initialize metrics
            self.metrics = defaultdict(Counter)
            
            self.initialized = True
            self.logger.info("Communication system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Communication system initialization failed: {e}")
            raise

    async def _initialize_core_channels(self) -> None:
        """Initialize core communication channels"""
        try:
            core_channels = [
                'system', 'coordination', 'task_assignment', 
                'result_collection', 'error_handling'
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

    async def create_channel(self, channel_name: str, buffer_size: int = 1000) -> asyncio.Queue:
        """Create a new communication channel"""
        try:
            async with self._lock:
                if channel_name not in self.channels:
                    self.channels[channel_name] = asyncio.Queue(maxsize=buffer_size)
                    self.channel_states[channel_name] = {
                        'active': True,
                        'message_count': 0,
                        'last_activity': datetime.now().isoformat()
                    }
                    self.metrics['channels_created'] += 1
                    self.logger.info(f"Channel {channel_name} created successfully")
                    
                return self.channels[channel_name]
                
        except Exception as e:
            self.logger.error(f"Channel creation failed: {e}")
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
                        await self.create_channel(channel)
                    self.subscribers[channel].add(agent_name)
                
                self.metrics['agent_registrations'] += 1
                self.logger.info(f"Agent {agent_name} registered successfully")
                
        except Exception as e:
            self.logger.error(f"Agent registration failed: {e}")
            raise

    async def send_message(self, sender: str, target: str, message: Dict[str, Any]) -> None:
        """Send message to target channel"""
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
            for channel in agent_channels:
                try:
                    message = await asyncio.wait_for(
                        self.channels[channel].get(),
                        timeout=timeout
                    )
                    self.metrics['messages_received'][agent_name] += 1
                    return message
                except asyncio.TimeoutError:
                    continue
                    
            return None
            
        except Exception as e:
            self.logger.error(f"Message reception failed: {e}")
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
                        
            # Clear collections
            self.channels.clear()
            self.subscribers.clear()
            self.message_history.clear()
            self.agent_registrations.clear()
            self.channel_states.clear()
            self.metrics.clear()
            
            self.initialized = False
            self.logger.info("Communication system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Communication system cleanup failed: {e}")
            raise

# ============================================================================
# PLANNING SYSTEM
# ============================================================================

class PlanningSystem:
    """Enhanced planning system with proper state management"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.initialized = False
        
        # Core planning
        self._plans: Dict[str, Dict[str, Any]] = {}
        self.planning_history: List[Dict[str, Any]] = []
        self.active_plans: Dict[str, Dict[str, Any]] = {}
        
        # Metrics and caching
        self.metrics = defaultdict(Counter)
        self.cache = TTLCache(maxsize=1000, ttl=3600)

    async def initialize(self) -> None:
        """Initialize planning system with proper error handling"""
        try:
            # Initialize core components
            self._plans = {}
            self.planning_history = []
            self.active_plans = {}
            
            # Initialize metrics
            self.metrics = defaultdict(Counter)
            
            self.initialized = True
            self.logger.info("Planning system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Planning system initialization failed: {e}")
            raise

    async def create_plan(self, task: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create execution plan with comprehensive monitoring"""
        if not self.initialized:
            raise RuntimeError("Planning system not initialized")
            
        plan_id = f"plan_{datetime.now().timestamp()}"
        
        try:
            # Create plan
            plan = await self._create_execution_plan(task, context)
            
            # Store plan
            self._plans[plan_id] = plan
            self.planning_history.append({
                'plan_id': plan_id,
                'task': task,
                'context': context,
                'timestamp': datetime.now().isoformat()
            })
            
            # Update metrics
            self.metrics['plans_created'] += 1
            
            return plan
            
        except Exception as e:
            self.logger.error(f"Plan creation failed: {e}")
            self.metrics['failed_plans'] += 1
            raise

    async def _create_execution_plan(self, task: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create detailed execution plan"""
        try:
            return {
                'id': str(uuid.uuid4()),
                'task': task,
                'context': context or {},
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
        """Generate execution steps for the task"""
        try:
            # Basic step generation logic
            steps = [
                {
                    'id': str(uuid.uuid4()),
                    'type': 'analysis',
                    'action': 'analyze_task',
                    'parameters': task,
                    'status': 'pending'
                },
                {
                    'id': str(uuid.uuid4()),
                    'type': 'processing',
                    'action': 'process_data',
                    'parameters': task,
                    'status': 'pending'
                },
                {
                    'id': str(uuid.uuid4()),
                    'type': 'validation',
                    'action': 'validate_results',
                    'parameters': task,
                    'status': 'pending'
                }
            ]
            return steps
            
        except Exception as e:
            self.logger.error(f"Step generation failed: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup planning system resources"""
        try:
            self._plans.clear()
            self.planning_history.clear()
            self.active_plans.clear()
            self.metrics.clear()
            self.cache.clear()
            
            self.initialized = False
            self.logger.info("Planning system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Planning system cleanup failed: {e}")
            raise

# ============================================================================
# REWOO SYSTEM
# ============================================================================

class REWOOSystem:
    """REWOO (Reason, Execute, Write, Observe, Output) System"""
    
    def __init__(self, config: Optional[REWOOConfig] = None):
        self.config = config or REWOOConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.initialized = False
        
        # Core components
        self.evidence_store: Optional[EvidenceStore] = None
        self.planning_system: Optional[PlanningSystem] = None
        
        # REWOO state
        self.planning_history: List[Dict[str, Any]] = []
        self.evidence_patterns: Dict[str, Any] = {}
        
        # Metrics and caching
        self.metrics = defaultdict(Counter)
        self.cache = TTLCache(maxsize=self.config.cache_size, ttl=3600)

    async def initialize(self) -> None:
        """Initialize REWOO system with proper component initialization"""
        try:
            # Initialize evidence store
            self.evidence_store = EvidenceStore()
            await self.evidence_store.initialize()
            
            # Initialize planning system
            self.planning_system = PlanningSystem()
            await self.planning_system.initialize()
            
            # Initialize metrics
            self.metrics = defaultdict(Counter)
            
            self.initialized = True
            self.logger.info("REWOO system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"REWOO system initialization failed: {e}")
            raise

    async def create_plan(self, task: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Create execution plan with REWOO methodology"""
        if not self.initialized:
            raise RuntimeError("REWOO system not initialized")
            
        try:
            # Normalize task input
            if isinstance(task, str):
                task = {'description': task, 'type': 'general'}
                
            # Observe current state
            observation = await self._observe_world_state(task)
            
            # Create plan using planning system
            plan = await self.planning_system.create_plan(task, {'observation': observation})
            
            # Store planning evidence
            await self.evidence_store.store_evidence(
                f"plan_{datetime.now().timestamp()}",
                {
                    'task': task,
                    'plan': plan,
                    'observation': observation
                },
                {'type': 'planning', 'system': 'rewoo'}
            )
            
            return plan
            
        except Exception as e:
            self.logger.error(f"Plan creation failed: {e}")
            raise

    async def _observe_world_state(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Observe current world state with proper error handling"""
        try:
            # Get current system state
            current_state = {
                'timestamp': datetime.now().isoformat(),
                'context': context,
                'system_metrics': self._get_system_metrics(),
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
            if self.evidence_store:
                return await self.evidence_store.get_summary()
            return {}
        except Exception as e:
            self.logger.error(f"Evidence summary retrieval failed: {e}")
            return {}

    def _get_system_metrics(self) -> Dict[str, Any]:
        """Get current system metrics"""
        try:
            import psutil
            return {
                'cpu_usage': psutil.cpu_percent(),
                'memory_usage': psutil.virtual_memory().percent,
                'timestamp': datetime.now().isoformat()
            }
        except ImportError:
            return {
                'cpu_usage': 0.0,
                'memory_usage': 0.0,
                'timestamp': datetime.now().isoformat()
            }

    async def cleanup(self) -> None:
        """Cleanup REWOO system resources"""
        try:
            if self.evidence_store:
                await self.evidence_store.cleanup()
                
            if self.planning_system:
                await self.planning_system.cleanup()
                
            self.planning_history.clear()
            self.evidence_patterns.clear()
            self.metrics.clear()
            self.cache.clear()
            
            self.initialized = False
            self.logger.info("REWOO system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"REWOO system cleanup failed: {e}")
            raise

# ============================================================================
# BASE AGENT CLASSES
# ============================================================================

class BaseAgent(ABC):
    """Base class for all agents with enhanced communication capabilities"""
    
    def __init__(self, name: str, model_info: Dict[str, Any], config: Dict[str, Any]):
        self.name = name
        self.model_info = model_info
        self.config = config
        self.layer_id = model_info.get('layer_id', 0)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}_{name}")
        self.initialized = False
        
        # Core components
        self.evidence_store: Optional[EvidenceStore] = None
        self.communication_system: Optional[CommunicationSystem] = None
        
        # State management
        self.state = defaultdict(dict)
        self.metrics = defaultdict(Counter)
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        
    async def initialize(self) -> None:
        """Initialize agent with proper error handling"""
        try:
            await self._initialize_core_components()
            await self._initialize_agent()
            
            self.initialized = True
            self.logger.info(f"Agent {self.name} initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Agent initialization failed: {e}")
            await self.cleanup()
            raise

    async def _initialize_core_components(self) -> None:
        """Initialize core agent components"""
        try:
            # Initialize evidence store
            self.evidence_store = EvidenceStore()
            await self.evidence_store.initialize()
            
            # Initialize metrics tracking
            self.metrics = defaultdict(Counter)
            
            self.logger.info("Core components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Core component initialization failed: {e}")
            raise

    @abstractmethod
    async def _initialize_agent(self) -> None:
        """Initialize agent-specific components. To be implemented by subclasses."""
        pass

    @abstractmethod
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task. To be implemented by subclasses."""
        pass

    async def cleanup(self) -> None:
        """Cleanup agent resources"""
        try:
            if self.evidence_store:
                await self.evidence_store.cleanup()
                
            self.state.clear()
            self.metrics.clear()
            self.cache.clear()
            
            self.initialized = False
            self.logger.info(f"Agent {self.name} cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Agent cleanup failed: {e}")
            raise

# ============================================================================
# LAYER AGENTS
# ============================================================================

class Layer1Agent(BaseAgent):
    """Data Integration & Processing Layer Agent"""
    
    async def _initialize_agent(self) -> None:
        """Initialize Layer1 agent-specific components"""
        self.data_processors = {
            'csv': self._process_csv_data,
            'json': self._process_json_data,
            'text': self._process_text_data
        }
        self.logger.info(f"Layer1 agent {self.name} components initialized")

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process data integration tasks"""
        try:
            data_type = task.get('data_type', 'text')
            processor = self.data_processors.get(data_type, self._process_text_data)
            
            result = await processor(task.get('data', ''))
            
            # Store evidence
            await self.evidence_store.store_evidence(
                f"layer1_{datetime.now().timestamp()}",
                result,
                {'processor': data_type, 'agent': self.name}
            )
            
            return {
                'processed_data': result,
                'metadata': {
                    'agent': self.name,
                    'layer': 1,
                    'processor': data_type,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            raise

    async def _process_csv_data(self, data: str) -> Dict[str, Any]:
        """Process CSV data"""
        try:
            # Simple CSV processing
            lines = data.strip().split('\n')
            if not lines:
                return {'rows': 0, 'data': []}
                
            headers = lines[0].split(',')
            rows = [line.split(',') for line in lines[1:]]
            
            return {
                'headers': headers,
                'rows': len(rows),
                'data': rows[:10],  # First 10 rows
                'total_rows': len(rows)
            }
        except Exception as e:
            self.logger.error(f"CSV processing failed: {e}")
            return {'error': str(e)}

    async def _process_json_data(self, data: str) -> Dict[str, Any]:
        """Process JSON data"""
        try:
            parsed_data = json.loads(data)
            return {
                'type': type(parsed_data).__name__,
                'keys': list(parsed_data.keys()) if isinstance(parsed_data, dict) else None,
                'length': len(parsed_data) if hasattr(parsed_data, '__len__') else None,
                'sample': str(parsed_data)[:200]
            }
        except Exception as e:
            self.logger.error(f"JSON processing failed: {e}")
            return {'error': str(e)}

    async def _process_text_data(self, data: str) -> Dict[str, Any]:
        """Process text data"""
        try:
            return {
                'length': len(data),
                'words': len(data.split()),
                'lines': len(data.split('\n')),
                'preview': data[:200]
            }
        except Exception as e:
            self.logger.error(f"Text processing failed: {e}")
            return {'error': str(e)}

class Layer2Agent(BaseAgent):
    """Knowledge Graph Construction & Analysis Layer Agent"""
    
    async def _initialize_agent(self) -> None:
        """Initialize Layer2 agent-specific components"""
        self.analyzers = {
            'pattern': self._analyze_patterns,
            'structure': self._analyze_structure,
            'relationships': self._analyze_relationships
        }
        self.logger.info(f"Layer2 agent {self.name} components initialized")

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process analysis tasks"""
        try:
            analysis_type = task.get('analysis_type', 'pattern')
            analyzer = self.analyzers.get(analysis_type, self._analyze_patterns)
            
            input_data = task.get('processed_data', task.get('data', {}))
            result = await analyzer(input_data)
            
            # Store evidence
            await self.evidence_store.store_evidence(
                f"layer2_{datetime.now().timestamp()}",
                result,
                {'analysis': analysis_type, 'agent': self.name}
            )
            
            return {
                'analysis_result': result,
                'metadata': {
                    'agent': self.name,
                    'layer': 2,
                    'analysis_type': analysis_type,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            raise

    async def _analyze_patterns(self, data: Any) -> Dict[str, Any]:
        """Analyze patterns in data"""
        try:
            if isinstance(data, dict):
                return {
                    'pattern_type': 'structured',
                    'key_count': len(data),
                    'nested_levels': self._count_nested_levels(data),
                    'data_types': self._analyze_data_types(data)
                }
            elif isinstance(data, str):
                return {
                    'pattern_type': 'text',
                    'length': len(data),
                    'unique_chars': len(set(data)),
                    'whitespace_ratio': data.count(' ') / len(data) if data else 0
                }
            else:
                return {
                    'pattern_type': 'unknown',
                    'type': type(data).__name__,
                    'length': len(data) if hasattr(data, '__len__') else None
                }
        except Exception as e:
            self.logger.error(f"Pattern analysis failed: {e}")
            return {'error': str(e)}

    async def _analyze_structure(self, data: Any) -> Dict[str, Any]:
        """Analyze data structure"""
        try:
            return {
                'structure_type': type(data).__name__,
                'complexity': self._calculate_complexity(data),
                'depth': self._calculate_depth(data)
            }
        except Exception as e:
            self.logger.error(f"Structure analysis failed: {e}")
            return {'error': str(e)}

    async def _analyze_relationships(self, data: Any) -> Dict[str, Any]:
        """Analyze relationships in data"""
        try:
            return {
                'relationship_count': self._count_relationships(data),
                'connection_strength': self._calculate_connection_strength(data)
            }
        except Exception as e:
            self.logger.error(f"Relationship analysis failed: {e}")
            return {'error': str(e)}

    def _count_nested_levels(self, data: Dict[str, Any], level: int = 0) -> int:
        """Count nested levels in dictionary"""
        if not isinstance(data, dict):
            return level
        return max([self._count_nested_levels(v, level + 1) if isinstance(v, dict) else level + 1 
                   for v in data.values()] + [level])

    def _analyze_data_types(self, data: Dict[str, Any]) -> Dict[str, int]:
        """Analyze data types in dictionary"""
        type_counts = defaultdict(int)
        for value in data.values():
            type_counts[type(value).__name__] += 1
        return dict(type_counts)

    def _calculate_complexity(self, data: Any) -> float:
        """Calculate data complexity score"""
        if isinstance(data, dict):
            return len(data) * 0.1
        elif isinstance(data, (list, tuple)):
            return len(data) * 0.05
        elif isinstance(data, str):
            return len(data) * 0.001
        return 1.0

    def _calculate_depth(self, data: Any) -> int:
        """Calculate data depth"""
        if isinstance(data, dict):
            return 1 + max([self._calculate_depth(v) for v in data.values()] + [0])
        elif isinstance(data, (list, tuple)) and data:
            return 1 + max([self._calculate_depth(item) for item in data] + [0])
        return 1

    def _count_relationships(self, data: Any) -> int:
        """Count relationships in data"""
        if isinstance(data, dict):
            return len(data)
        elif isinstance(data, (list, tuple)):
            return len(data)
        return 0

    def _calculate_connection_strength(self, data: Any) -> float:
        """Calculate connection strength"""
        relationships = self._count_relationships(data)
        return min(relationships / 10.0, 1.0)

class BossAgent(BaseAgent):
    """Enhanced boss agent with comprehensive planning and coordination capabilities"""
    
    def __init__(self, name: str, model_info: Dict[str, Any], config: Dict[str, Any]):
        super().__init__(name, model_info, config)
        
        # Layer management
        self.layer_agents: Dict[int, List[BaseAgent]] = defaultdict(list)
        self.active_tasks: Dict[str, Dict[str, Any]] = {}
        
        # Systems
        self.rewoo_system: Optional[REWOOSystem] = None
        self.planning_system: Optional[PlanningSystem] = None
        self.communication_system: Optional[CommunicationSystem] = None

    async def _initialize_agent(self) -> None:
        """Initialize boss agent-specific components"""
        try:
            # Initialize REWOO system
            self.rewoo_system = REWOOSystem()
            await self.rewoo_system.initialize()
            
            # Initialize planning system
            self.planning_system = PlanningSystem()
            await self.planning_system.initialize()
            
            # Initialize communication system
            self.communication_system = CommunicationSystem()
            await self.communication_system.initialize()
            
            # Initialize layer agents
            await self._initialize_layer_agents()
            
            self.logger.info("Boss agent components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Boss agent initialization failed: {e}")
            raise

    async def _initialize_layer_agents(self) -> None:
        """Initialize layer agents"""
        try:
            # Create Layer 1 agent
            layer1_config = ModelConfig(
                model_name="layer1_processor",
                layer_id=1,
                metadata={'role': 'data_processor'}
            )
            layer1_agent = Layer1Agent(
                name="Layer1_Processor",
                model_info=layer1_config.to_dict(),
                config=self.config
            )
            await layer1_agent.initialize()
            self.layer_agents[1].append(layer1_agent)
            
            # Create Layer 2 agent
            layer2_config = ModelConfig(
                model_name="layer2_analyzer",
                layer_id=2,
                metadata={'role': 'data_analyzer'}
            )
            layer2_agent = Layer2Agent(
                name="Layer2_Analyzer",
                model_info=layer2_config.to_dict(),
                config=self.config
            )
            await layer2_agent.initialize()
            self.layer_agents[2].append(layer2_agent)
            
            self.logger.info(f"Initialized {sum(len(agents) for agents in self.layer_agents.values())} layer agents")
            
        except Exception as e:
            self.logger.error(f"Layer agent initialization failed: {e}")
            raise

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task with comprehensive coordination"""
        if not self.initialized:
            raise RuntimeError(f"Boss agent {self.name} not initialized")
            
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
            results = await self._process_through_layers(task_id, task)
            
            # Store execution evidence
            await self.evidence_store.store_evidence(
                task_id,
                {
                    'task': task,
                    'plan': plan,
                    'results': results,
                    'metadata': self._generate_execution_metadata(task_id)
                },
                {'type': 'task_complete', 'agent': self.name}
            )
            
            return {
                'task_id': task_id,
                'results': results,
                'metadata': self._generate_execution_metadata(task_id)
            }
            
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            await self._handle_task_error(task_id, e)
            raise

    async def _process_through_layers(self, task_id: str, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task through layers with monitoring"""
        results = {}
        current_input = task
        
        try:
            for layer_id in sorted(self.layer_agents.keys()):
                layer_agents = self.layer_agents[layer_id]
                if not layer_agents:
                    continue
                
                # Process with first available agent in layer
                agent = layer_agents[0]
                layer_result = await agent.process_task(current_input)
                
                results[layer_id] = layer_result
                self.active_tasks[task_id]['layer_results'][layer_id] = layer_result
                
                # Prepare input for next layer
                current_input = {
                    'processed_data': layer_result.get('processed_data') or layer_result.get('analysis_result'),
                    'metadata': layer_result.get('metadata', {}),
                    'previous_layer': layer_id
                }
                
            return results
            
        except Exception as e:
            self.logger.error(f"Layer processing failed: {e}")
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
            
            # Store error evidence
            await self.evidence_store.store_evidence(
                f"error_{task_id}",
                {
                    'error': str(error),
                    'task_id': task_id,
                    'timestamp': datetime.now().isoformat()
                },
                {'type': 'error', 'agent': self.name}
            )
            
            self.metrics['task_errors'] += 1
            
        except Exception as e:
            self.logger.error(f"Error handling failed: {e}")

    def _generate_execution_metadata(self, task_id: str) -> Dict[str, Any]:
        """Generate execution metadata"""
        task_data = self.active_tasks.get(task_id, {})
        return {
            'task_id': task_id,
            'boss_agent': self.name,
            'start_time': task_data.get('start_time', datetime.now()).isoformat(),
            'layer_count': len(self.layer_agents),
            'active_agents': {
                layer_id: len(agents)
                for layer_id, agents in self.layer_agents.items()
            },
            'status': task_data.get('status', 'unknown')
        }

    async def cleanup(self) -> None:
        """Cleanup boss agent resources"""
        try:
            # Cleanup systems
            if self.rewoo_system:
                await self.rewoo_system.cleanup()
                
            if self.planning_system:
                await self.planning_system.cleanup()
                
            if self.communication_system:
                await self.communication_system.cleanup()
                
            # Cleanup layer agents
            for layer_agents in self.layer_agents.values():
                for agent in layer_agents:
                    await agent.cleanup()
            
            # Clear collections
            self.layer_agents.clear()
            self.active_tasks.clear()
            
            # Call parent cleanup
            await super().cleanup()
            
            self.logger.info(f"Boss agent {self.name} cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Boss agent cleanup failed: {e}")
            raise

# ============================================================================
# MAIN CONFIGURATION CLASS
# ============================================================================

class EnhancedConfig:
    """Streamlined configuration management system"""
    
    def __init__(self, 
                 configuration_profile: str = "minimal", 
                 validation_mode: str = "lenient"):
        """Initialize configuration system"""
        self.configuration_profile = configuration_profile
        self.validation_mode = validation_mode
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Initialize state
        self.initialized = False
        self._initializing = False
        
        # Configuration containers
        self.model_configs: Dict[str, ModelConfig] = {}
        self.layer_configs: Dict[int, LayerConfig] = {}
        self.evidence_store_config: Optional[EvidenceStoreConfig] = None
        self.communication_config: Optional[CommunicationConfig] = None
        self.rewoo_config: Optional[REWOOConfig] = None
        
        # System components
        self.components: Dict[str, Any] = {}
        
        # Metrics and state
        self.metrics = defaultdict(Counter)
        self.initialization_state = defaultdict(bool)
        
        # Load default configurations
        self._load_default_configs()
    
    def _load_default_configs(self) -> None:
        """Load default configurations"""
        try:
            # Evidence store config
            self.evidence_store_config = EvidenceStoreConfig()
            
            # Communication config
            self.communication_config = CommunicationConfig()
            
            # REWOO config
            self.rewoo_config = REWOOConfig()
            
            # Default model configurations
            self.model_configs = {
                'BossAgent': ModelConfig(
                    model_name="enhanced_boss_agent",
                    api_key=os.getenv("API_KEY"),
                    layer_id=0,
                    metadata={
                        "role": "coordinator",
                        "capabilities": ["planning", "delegation", "synthesis"],
                        "type": "boss"
                    }
                )
            }
            
            # Default layer configurations
            self.layer_configs = {
                0: LayerConfig(
                    layer_id=0,
                    agents=['BossAgent'],
                    model_name="enhanced_boss_agent",
                    metadata={'type': 'coordinator'}
                ),
                1: LayerConfig(
                    layer_id=1,
                    agents=['Layer1Agent'],
                    model_name="data_processor",
                    metadata={'type': 'processor'}
                ),
                2: LayerConfig(
                    layer_id=2,
                    agents=['Layer2Agent'],
                    model_name="data_analyzer",
                    metadata={'type': 'analyzer'}
                )
            }
            
            self.logger.info("Default configurations loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to load default configurations: {e}")
            raise ConfigurationError(f"Default configuration loading failed: {str(e)}")
    
    async def initialize(self) -> None:
        """Initialize the configuration system"""
        if self._initializing or self.initialized:
            return
        
        self._initializing = True
        
        try:
            # Initialize components in order
            await self._initialize_core_components()
            
            # Mark as initialized
            self.initialized = True
            self.logger.info("Configuration system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Configuration initialization failed: {e}")
            await self._cleanup_partial_initialization()
            raise InitializationError(f"Failed to initialize configuration: {str(e)}")
        finally:
            self._initializing = False
    
    async def _initialize_core_components(self) -> None:
        """Initialize core system components"""
        try:
            # Initialize evidence store
            if self.evidence_store_config:
                self.components['evidence_store'] = {
                    'config': self.evidence_store_config,
                    'initialized': True
                }
                self.initialization_state['evidence_store'] = True
            
            # Initialize communication system
            if self.communication_config:
                self.components['communication'] = {
                    'config': self.communication_config,
                    'initialized': True
                }
                self.initialization_state['communication'] = True
            
            # Initialize REWOO system
            if self.rewoo_config:
                self.components['rewoo'] = {
                    'config': self.rewoo_config,
                    'initialized': True
                }
                self.initialization_state['rewoo'] = True
            
            self.logger.info("Core components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Core component initialization failed: {e}")
            raise
    
    async def _cleanup_partial_initialization(self) -> None:
        """Cleanup partial initialization"""
        try:
            # Reset initialization state
            self.initialized = False
            self._initializing = False
            self.initialization_state.clear()
            
            # Clear components
            self.components.clear()
            
            self.logger.info("Partial initialization cleaned up")
            
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")
    
    # ========================================================================
    # PUBLIC API METHODS
    # ========================================================================
    
    def get_model_config(self, model_name: str) -> Optional[ModelConfig]:
        """Get model configuration by name"""
        return self.model_configs.get(model_name)
    
    def get_layer_config(self, layer_id: int) -> Optional[LayerConfig]:
        """Get layer configuration by ID"""
        return self.layer_configs.get(layer_id)
    
    def get_status(self) -> Dict[str, Any]:
        """Get current system status"""
        return {
            'initialized': self.initialized,
            'initializing': self._initializing,
            'profile': self.configuration_profile,
            'validation_mode': self.validation_mode,
            'components': {
                name: component.get('initialized', False)
                for name, component in self.components.items()
            },
            'model_configs_count': len(self.model_configs),
            'layer_configs_count': len(self.layer_configs)
        }
    
    async def cleanup(self) -> None:
        """Cleanup configuration system"""
        try:
            # Cleanup components
            for component_name, component in self.components.items():
                if hasattr(component, 'cleanup'):
                    await component.cleanup()
            
            # Clear state
            self.components.clear()
            self.initialization_state.clear()
            self.metrics.clear()
            
            self.initialized = False
            self.logger.info("Configuration system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")
            raise

# ============================================================================
# FACTORY FUNCTION
# ============================================================================

async def create_enhanced_config(
    profile: str = "minimal",
    validation_mode: str = "lenient"
) -> EnhancedConfig:
    """Factory function to create and initialize EnhancedConfig"""
    try:
        config = EnhancedConfig(
            configuration_profile=profile,
            validation_mode=validation_mode
        )
        await config.initialize()
        return config
    except Exception as e:
        logger.error(f"Failed to create enhanced config: {e}")
        raise

# ============================================================================
# SYSTEM ORCHESTRATOR
# ============================================================================

class MultiAgentSystem:
    """Main orchestrator for the multi-agent system"""
    
    def __init__(self, config: Optional[EnhancedConfig] = None):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.initialized = False
        
        # Core components
        self.boss_agent: Optional[BossAgent] = None
        self.evidence_store: Optional[EvidenceStore] = None
        self.communication_system: Optional[CommunicationSystem] = None
    
    async def initialize(self) -> None:
        """Initialize the multi-agent system"""
        try:
            # Initialize config if not provided
            if self.config is None:
                self.config = await create_enhanced_config()
            elif not self.config.initialized:
                await self.config.initialize()
            
            # Initialize evidence store
            self.evidence_store = EvidenceStore(self.config.evidence_store_config)
            await self.evidence_store.initialize()
            
            # Initialize communication system
            self.communication_system = CommunicationSystem(self.config.communication_config)
            await self.communication_system.initialize()
            
            # Initialize boss agent
            boss_config = self.config.get_model_config('BossAgent')
            if boss_config:
                self.boss_agent = BossAgent(
                    name="MainBoss",
                    model_info=boss_config.to_dict(),
                    config=self.config.__dict__
                )
                await self.boss_agent.initialize()
            
            self.initialized = True
            self.logger.info("Multi-agent system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"System initialization failed: {e}")
            await self.cleanup()
            raise
    
    async def process_task(self, task: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Process a task through the multi-agent system"""
        if not self.initialized:
            raise RuntimeError("System not initialized")
        
        try:
            # Normalize task input
            if isinstance(task, str):
                task = {'description': task, 'type': 'general'}
            
            # Process through boss agent
            result = await self.boss_agent.process_task(task)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Task processing failed: {e}")
            raise
    
    async def cleanup(self) -> None:
        """Cleanup system resources"""
        try:
            if self.boss_agent:
                await self.boss_agent.cleanup()
            
            if self.evidence_store:
                await self.evidence_store.cleanup()
            
            if self.communication_system:
                await self.communication_system.cleanup()
            
            if self.config:
                await self.config.cleanup()
            
            self.initialized = False
            self.logger.info("Multi-agent system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"System cleanup failed: {e}")
            raise

# ============================================================================
# USAGE EXAMPLE
# ============================================================================

async def main():
    """Example usage of the multi-agent system"""
    try:
        # Create and initialize system
        system = MultiAgentSystem()
        await system.initialize()
        
        # Process a sample task
        task = {
            'description': 'Analyze this sample data and provide insights',
            'data': 'name,age,city\nJohn,30,New York\nJane,25,Los Angeles\nBob,35,Chicago',
            'data_type': 'csv'
        }
        
        result = await system.process_task(task)
        
        print("Task Result:")
        print(json.dumps(result, indent=2, default=str))
        
        # Get system status
        status = system.config.get_status()
        print("\nSystem Status:")
        print(json.dumps(status, indent=2))
        
        # Cleanup
        await system.cleanup()
        
    except Exception as e:
        logger.error(f"Example failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
