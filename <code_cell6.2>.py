#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Complete Multi-Agent System - Production Ready Implementation
=============================================================

A comprehensive multi-agent system with REWOO capabilities, evidence tracking,
and layer-based processing architecture. This is the complete, production-ready
version that addresses all identified issues.

Author: AI Assistant
Version: 2.0.0
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
import psutil
import networkx as nx
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Type, Set, Callable
from collections import defaultdict, Counter, deque
from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod
from enum import Enum
import aiohttp
from cachetools import TTLCache
import nest_asyncio

# Apply nest_asyncio for Jupyter compatibility
nest_asyncio.apply()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class SystemError(Exception):
    """Base system error"""
    pass

class ConfigurationError(SystemError):
    """Custom exception for configuration errors"""
    pass

class ValidationError(SystemError):
    """Custom exception for validation errors"""
    pass

class InitializationError(SystemError):
    """Custom exception for initialization errors"""
    pass

class ResourceError(SystemError):
    """Custom exception for resource-related errors"""
    pass

class CommunicationError(SystemError):
    """Exception raised for communication-related errors"""
    pass

# ============================================================================
# CONFIGURATION SYSTEM
# ============================================================================

class ModelProvider(Enum):
    """Supported model providers"""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    VERTEX = "vertex"
    MISTRAL = "mistral"

@dataclass
class ModelConfig:
    """Enhanced model configuration with validation"""
    model_name: str
    provider: ModelProvider
    layer_id: Optional[int] = None
    max_tokens: int = 2048
    temperature: float = 0.7
    timeout: int = 30
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if self.temperature < 0 or self.temperature > 2:
            raise ValueError("Temperature must be between 0 and 2")
        if self.max_tokens <= 0:
            raise ValueError("Max tokens must be positive")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

@dataclass
class APISettings:
    """API configuration settings"""
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    vertex_ai_credentials: Optional[Dict[str, Any]] = None
    vertex_ai_project_id: Optional[str] = None
    vertex_ai_location: str = "us-central1"
    timeout: int = 30
    max_retries: int = 3
    
    def validate(self) -> bool:
        """Validate that at least one API is configured"""
        return any([
            self.openai_api_key,
            self.anthropic_api_key,
            self.vertex_ai_credentials
        ])

@dataclass
class EnhancedConfig:
    """Main system configuration"""
    api_settings: APISettings
    model_configs: Dict[str, ModelConfig] = field(default_factory=dict)
    layer_configs: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    pipeline_config: Dict[str, Any] = field(default_factory=dict)
    
    # Database settings
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"
    
    # System settings
    max_concurrent_tasks: int = 10
    cache_size: int = 1000
    cache_ttl: int = 3600
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.api_settings.validate():
            raise ConfigurationError("At least one API must be configured")
    
    def get_model_config(self, model_name: str) -> Optional[ModelConfig]:
        """Get model configuration by name"""
        return self.model_configs.get(model_name)
    
    def get_layer_config(self, layer_id: int) -> Optional[Dict[str, Any]]:
        """Get layer configuration by ID"""
        return self.layer_configs.get(layer_id)

# ============================================================================
# EVIDENCE SYSTEM
# ============================================================================

@dataclass
class Evidence:
    """Evidence data structure"""
    id: str
    source: str
    content: Any
    timestamp: float
    metadata: Dict[str, Any]
    confidence: float = 1.0
    tags: List[str] = field(default_factory=list)

class EvidenceStore:
    """Evidence storage and retrieval system with advanced capabilities"""
    
    def __init__(self):
        self.evidence: Dict[str, Evidence] = {}
        self.indices: Dict[str, set] = defaultdict(set)
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.initialized = False
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.metrics = defaultdict(Counter)
    
    async def initialize(self) -> None:
        """Initialize evidence store"""
        try:
            self.evidence.clear()
            self.indices.clear()
            self.cache.clear()
            self.metrics.clear()
            self.initialized = True
            self.logger.info("Evidence store initialized successfully")
        except Exception as e:
            self.logger.error(f"Evidence store initialization failed: {e}")
            raise InitializationError(f"Evidence store init failed: {e}")
    
    async def store_evidence(self, 
                           evidence_id: str, 
                           content: Any, 
                           metadata: Dict[str, Any],
                           source: str = "system",
                           confidence: float = 1.0,
                           tags: List[str] = None) -> None:
        """Store evidence with comprehensive indexing"""
        if not self.initialized:
            raise SystemError("Evidence store not initialized")
        
        try:
            evidence = Evidence(
                id=evidence_id,
                source=source,
                content=content,
                timestamp=datetime.now().timestamp(),
                metadata=metadata,
                confidence=confidence,
                tags=tags or []
            )
            
            self.evidence[evidence_id] = evidence
            
            # Create comprehensive indices
            self.indices[f"source:{source}"].add(evidence_id)
            self.indices[f"confidence:{int(confidence * 10)}"].add(evidence_id)
            
            for key in metadata.keys():
                self.indices[f"metadata:{key}"].add(evidence_id)
            
            for tag in evidence.tags:
                self.indices[f"tag:{tag}"].add(evidence_id)
            
            self.metrics['stored'] += 1
            self.logger.debug(f"Stored evidence {evidence_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to store evidence {evidence_id}: {e}")
            raise
    
    async def retrieve_evidence(self, evidence_id: str) -> Optional[Evidence]:
        """Retrieve evidence by ID with caching"""
        if evidence_id in self.cache:
            self.metrics['cache_hits'] += 1
            return self.cache[evidence_id]
        
        evidence = self.evidence.get(evidence_id)
        if evidence:
            self.cache[evidence_id] = evidence
            self.metrics['retrieved'] += 1
        
        return evidence
    
    async def search_evidence(self, 
                            source: Optional[str] = None,
                            metadata_key: Optional[str] = None,
                            tags: Optional[List[str]] = None,
                            min_confidence: float = 0.0) -> List[Evidence]:
        """Advanced evidence search with multiple criteria"""
        try:
            evidence_ids = set()
            
            # Build search criteria
            if source:
                evidence_ids.update(self.indices.get(f"source:{source}", set()))
            
            if metadata_key:
                evidence_ids.update(self.indices.get(f"metadata:{metadata_key}", set()))
            
            if tags:
                tag_ids = set()
                for tag in tags:
                    tag_ids.update(self.indices.get(f"tag:{tag}", set()))
                if evidence_ids:
                    evidence_ids.intersection_update(tag_ids)
                else:
                    evidence_ids = tag_ids
            
            if not evidence_ids and not any([source, metadata_key, tags]):
                evidence_ids = set(self.evidence.keys())
            
            # Filter by confidence
            results = []
            for eid in evidence_ids:
                if eid in self.evidence:
                    evidence = self.evidence[eid]
                    if evidence.confidence >= min_confidence:
                        results.append(evidence)
            
            self.metrics['searches'] += 1
            return sorted(results, key=lambda x: x.confidence, reverse=True)
            
        except Exception as e:
            self.logger.error(f"Evidence search failed: {e}")
            return []
    
    async def cleanup(self) -> None:
        """Cleanup evidence store"""
        try:
            self.evidence.clear()
            self.indices.clear()
            self.cache.clear()
            self.metrics.clear()
            self.initialized = False
            self.logger.info("Evidence store cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Evidence store cleanup failed: {e}")

# ============================================================================
# RESOURCE MANAGEMENT
# ============================================================================

@dataclass
class ResourceAllocation:
    """Resource allocation tracking"""
    resource_type: str
    amount: float
    allocated_at: datetime
    allocated_to: str
    metadata: Dict[str, Any]

class ImprovedResourceManager:
    """Enhanced resource manager with monitoring and optimization"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Resource tracking
        self.allocations: Dict[str, ResourceAllocation] = {}
        self.metrics_history: deque = deque(maxlen=1000)
        self.alerts: List[Dict[str, Any]] = []
        
        # Monitoring
        self.monitoring_active = False
        self.monitoring_task: Optional[asyncio.Task] = None
        self.monitoring_interval = 5.0
        
        # Thresholds
        self.thresholds = {
            'cpu_warning': 70.0,
            'cpu_critical': 85.0,
            'memory_warning': 75.0,
            'memory_critical': 90.0,
            'disk_warning': 80.0,
            'disk_critical': 95.0
        }
        
        # Resource pools
        self.resource_pools = {}
        self.allocated_resources = defaultdict(float)
        self.initialized = False
    
    async def initialize(self) -> None:
        """Initialize resource manager"""
        try:
            self._initialize_resource_pools()
            await self._start_monitoring()
            self.initialized = True
            self.logger.info("Resource manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Resource manager initialization failed: {e}")
            raise
    
    def _initialize_resource_pools(self) -> None:
        """Initialize resource pools based on system capacity"""
        try:
            self.resource_pools = {
                'cpu_cores': psutil.cpu_count(),
                'memory_gb': psutil.virtual_memory().total / (1024**3),
                'disk_gb': psutil.disk_usage('/').total / (1024**3)
            }
            self.logger.info(f"Resource pools initialized: {self.resource_pools}")
        except Exception as e:
            self.logger.error(f"Resource pool initialization failed: {e}")
            raise
    
    async def allocate_resources(self, 
                               request_id: str,
                               requirements: Dict[str, float],
                               requestor: str) -> Dict[str, bool]:
        """Allocate resources with validation"""
        if not self.initialized:
            raise RuntimeError("Resource manager not initialized")
        
        allocation_results = {}
        allocated_resources = []
        
        try:
            # Check availability
            for resource_type, amount in requirements.items():
                if not self._can_allocate(resource_type, amount):
                    # Rollback partial allocations
                    for alloc_id in allocated_resources:
                        await self.release_resources(alloc_id)
                    raise ResourceError(f"Insufficient {resource_type} available")
            
            # Allocate resources
            for resource_type, amount in requirements.items():
                allocation_id = f"{request_id}_{resource_type}"
                
                allocation = ResourceAllocation(
                    resource_type=resource_type,
                    amount=amount,
                    allocated_at=datetime.now(),
                    allocated_to=requestor,
                    metadata={'request_id': request_id}
                )
                
                self.allocations[allocation_id] = allocation
                self.allocated_resources[resource_type] += amount
                allocated_resources.append(allocation_id)
                allocation_results[resource_type] = True
                
                self.logger.debug(f"Allocated {amount} {resource_type} to {requestor}")
            
            return allocation_results
            
        except Exception as e:
            self.logger.error(f"Resource allocation failed: {e}")
            raise
    
    async def release_resources(self, allocation_id: str) -> bool:
        """Release allocated resources"""
        try:
            if allocation_id not in self.allocations:
                return False
            
            allocation = self.allocations[allocation_id]
            self.allocated_resources[allocation.resource_type] -= allocation.amount
            del self.allocations[allocation_id]
            
            self.logger.debug(f"Released {allocation.amount} {allocation.resource_type}")
            return True
            
        except Exception as e:
            self.logger.error(f"Resource release failed: {e}")
            return False
    
    def _can_allocate(self, resource_type: str, amount: float) -> bool:
        """Check if resource can be allocated"""
        total_available = self.resource_pools.get(resource_type, 0)
        currently_allocated = self.allocated_resources.get(resource_type, 0)
        return (total_available - currently_allocated) >= amount
    
    async def _start_monitoring(self) -> None:
        """Start resource monitoring"""
        if not self.monitoring_active:
            self.monitoring_active = True
            self.monitoring_task = asyncio.create_task(self._monitoring_loop())
    
    async def _monitoring_loop(self) -> None:
        """Resource monitoring loop"""
        while self.monitoring_active:
            try:
                metrics = await self._collect_metrics()
                self.metrics_history.append(metrics)
                await self._check_thresholds(metrics)
                await asyncio.sleep(self.monitoring_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(self.monitoring_interval)
    
    async def _collect_metrics(self) -> Dict[str, Any]:
        """Collect system metrics"""
        try:
            return {
                'cpu_percent': psutil.cpu_percent(interval=1),
                'memory_percent': psutil.virtual_memory().percent,
                'disk_percent': psutil.disk_usage('/').percent,
                'timestamp': datetime.now()
            }
        except Exception as e:
            self.logger.error(f"Metrics collection failed: {e}")
            return {'timestamp': datetime.now()}
    
    async def _check_thresholds(self, metrics: Dict[str, Any]) -> None:
        """Check metrics against thresholds"""
        alerts = []
        
        for metric_name in ['cpu', 'memory', 'disk']:
            value = metrics.get(f'{metric_name}_percent', 0)
            
            if value >= self.thresholds.get(f'{metric_name}_critical', 100):
                alerts.append({
                    'level': 'CRITICAL',
                    'resource': metric_name,
                    'value': value,
                    'threshold': self.thresholds[f'{metric_name}_critical']
                })
            elif value >= self.thresholds.get(f'{metric_name}_warning', 100):
                alerts.append({
                    'level': 'WARNING',
                    'resource': metric_name,
                    'value': value,
                    'threshold': self.thresholds[f'{metric_name}_warning']
                })
        
        if alerts:
            await self._handle_alerts(alerts)
    
    async def _handle_alerts(self, alerts: List[Dict[str, Any]]) -> None:
        """Handle resource alerts"""
        for alert in alerts:
            self.alerts.append({**alert, 'timestamp': datetime.now()})
            level = alert['level']
            resource = alert['resource']
            value = alert['value']
            
            log_msg = f"{level} Alert: {resource} at {value:.1f}%"
            
            if level == 'CRITICAL':
                self.logger.critical(log_msg)
            else:
                self.logger.warning(log_msg)
    
    async def cleanup(self) -> None:
        """Cleanup resource manager"""
        try:
            self.monitoring_active = False
            if self.monitoring_task:
                self.monitoring_task.cancel()
                try:
                    await self.monitoring_task
                except asyncio.CancelledError:
                    pass
            
            self.allocations.clear()
            self.allocated_resources.clear()
            self.metrics_history.clear()
            self.alerts.clear()
            self.initialized = False
            
            self.logger.info("Resource manager cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Resource manager cleanup failed: {e}")

# ============================================================================
# AGENT SYSTEM
# ============================================================================

class BaseAgent(ABC):
    """Enhanced base agent with comprehensive capabilities"""
    
    def __init__(self, name: str, model_info: Dict[str, Any], config: EnhancedConfig):
        self.name = name
        self.model_info = model_info
        self.config = config
        self.layer_id = model_info.get('layer_id', 0)
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{name}")
        self.initialized = False
        
        # Core components
        self.evidence_store: Optional[EvidenceStore] = None
        
        # State management
        self.state = defaultdict(dict)
        self.metrics = defaultdict(Counter)
        self.cache = TTLCache(maxsize=1000, ttl=3600)
        self.task_history = deque(maxlen=100)
    
    async def initialize(self) -> None:
        """Initialize agent with comprehensive setup"""
        try:
            await self._initialize_core_components()
            await self._initialize_agent()
            
            self.initialized = True
            self.logger.info(f"Agent {self.name} initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Agent initialization failed: {e}")
            await self.cleanup()
            raise InitializationError(f"Agent {self.name} init failed: {e}")
    
    async def _initialize_core_components(self) -> None:
        """Initialize core agent components"""
        try:
            # Initialize evidence store
            self.evidence_store = EvidenceStore()
            await self.evidence_store.initialize()
            
            # Initialize metrics
            self.metrics = defaultdict(Counter)
            
            # Initialize state
            self.state = defaultdict(dict)
            
            self.logger.debug("Core components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Core component initialization failed: {e}")
            raise
    
    @abstractmethod
    async def _initialize_agent(self) -> None:
        """Initialize agent-specific components"""
        pass
    
    @abstractmethod
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task"""
        pass
    
    async def _record_task_execution(self, task: Dict[str, Any], result: Dict[str, Any], 
                                   execution_time: float) -> None:
        """Record task execution for metrics and learning"""
        try:
            execution_record = {
                'task_id': task.get('id', f"task_{datetime.now().timestamp()}"),
                'task_type': task.get('type', 'unknown'),
                'execution_time': execution_time,
                'status': result.get('status', 'unknown'),
                'timestamp': datetime.now().isoformat(),
                'agent': self.name,
                'layer': self.layer_id
            }
            
            self.task_history.append(execution_record)
            
            # Store as evidence
            if self.evidence_store:
                await self.evidence_store.store_evidence(
                    evidence_id=f"execution_{execution_record['task_id']}",
                    content=execution_record,
                    metadata={'type': 'execution_record', 'agent': self.name},
                    source=self.name,
                    tags=['execution', 'performance']
                )
            
            # Update metrics
            self.metrics['tasks_processed'] += 1
            self.metrics['total_execution_time'] += execution_time
            
        except Exception as e:
            self.logger.error(f"Failed to record task execution: {e}")
    
    async def get_performance_metrics(self) -> Dict[str, Any]:
        """Get agent performance metrics"""
        try:
            total_tasks = self.metrics['tasks_processed']
            total_time = self.metrics['total_execution_time']
            
            return {
                'total_tasks': total_tasks,
                'total_execution_time': total_time,
                'average_execution_time': total_time / total_tasks if total_tasks > 0 else 0,
                'tasks_per_minute': total_tasks / (total_time / 60) if total_time > 0 else 0,
                'recent_tasks': len(self.task_history),
                'agent_name': self.name,
                'layer_id': self.layer_id
            }
        except Exception as e:
            self.logger.error(f"Failed to get performance metrics: {e}")
            return {}
    
    async def cleanup(self) -> None:
        """Cleanup agent resources"""
        try:
            if self.evidence_store:
                await self.evidence_store.cleanup()
                
            self.state.clear()
            self.metrics.clear()
            self.cache.clear()
            self.task_history.clear()
            
            self.initialized = False
            self.logger.info(f"Agent {self.name} cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Agent cleanup failed: {e}")

class Layer1Agent(BaseAgent):
    """Data Integration & Processing Layer Agent"""
    
    async def _initialize_agent(self) -> None:
        """Initialize Layer1 agent-specific components"""
        self.data_processors = {
            'csv': self._process_csv_data,
            'json': self._process_json_data,
            'text': self._process_text_data,
            'xml': self._process_xml_data
        }
        self.logger.info(f"Layer1 agent {self.name} components initialized")
    
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process data integration tasks"""
        start_time = time.time()
        
        try:
            data_type = task.get('data_type', 'text')
            processor = self.data_processors.get(data_type, self._process_text_data)
            
            result = await processor(task.get('data', ''))
            
            # Store evidence
            if self.evidence_store:
                await self.evidence_store.store_evidence(
                    f"layer1_{datetime.now().timestamp()}",
                    result,
                    {'processor': data_type, 'agent': self.name},
                    source=self.name,
                    tags=['data_processing', f'layer_{self.layer_id}']
                )
            
            execution_time = time.time() - start_time
            
            response = {
                'processed_data': result,
                'status': 'success',
                'metadata': {
                    'agent': self.name,
                    'layer': self.layer_id,
                    'processor': data_type,
                    'execution_time': execution_time,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            await self._record_task_execution(task, response, execution_time)
            return response
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Task processing failed: {e}")
            
            error_response = {
                'error': str(e),
                'status': 'failed',
                'metadata': {
                    'agent': self.name,
                    'layer': self.layer_id,
                    'execution_time': execution_time,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            await self._record_task_execution(task, error_response, execution_time)
            return error_response
    
    async def _process_csv_data(self, data: str) -> Dict[str, Any]:
        """Process CSV data with enhanced parsing"""
        try:
            lines = data.strip().split('\n')
            if not lines:
                return {'rows': [], 'columns': [], 'type': 'csv'}
            
            headers = [h.strip() for h in lines[0].split(',')]
            rows = []
            
            for line_num, line in enumerate(lines[1:], 2):
                try:
                    row_data = [cell.strip() for cell in line.split(',')]
                    if len(row_data) == len(headers):
                        rows.append(dict(zip(headers, row_data)))
                    else:
                        self.logger.warning(f"CSV line {line_num}: column count mismatch")
                except Exception as e:
                    self.logger.warning(f"CSV line {line_num} parsing error: {e}")
            
            return {
                'type': 'csv',
                'columns': headers,
                'rows': rows,
                'row_count': len(rows),
                'column_count': len(headers),
                'data_quality': {
                    'complete_rows': len(rows),
                    'total_lines': len(lines) - 1,
                    'success_rate': len(rows) / (len(lines) - 1) if len(lines) > 1 else 0
                }
            }
            
        except Exception as e:
            self.logger.error(f"CSV processing failed: {e}")
            return {'error': str(e), 'type': 'csv'}
    
    async def _process_json_data(self, data: str) -> Dict[str, Any]:
        """Process JSON data with structure analysis"""
        try:
            parsed_data = json.loads(data)
            structure = self._analyze_json_structure(parsed_data)
            
            return {
                'type': 'json',
                'data': parsed_data,
                'structure': structure,
                'size_bytes': len(data),
                'complexity_score': self._calculate_json_complexity(parsed_data)
            }
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON parsing failed: {e}")
            return {'error': f"JSON decode error: {e}", 'type': 'json'}
        except Exception as e:
            self.logger.error(f"JSON processing failed: {e}")
            return {'error': str(e), 'type': 'json'}
    
    async def _process_text_data(self, data: str) -> Dict[str, Any]:
        """Process text data with enhanced analysis"""
        try:
            lines = data.split('\n')
            words = data.split()
            
            return {
                'type': 'text',
                'content': data,
                'statistics': {
                    'character_count': len(data),
                    'word_count': len(words),
                    'line_count': len(lines),
                    'average_words_per_line': len(words) / len(lines) if lines else 0,
                    'average_characters_per_word': len(data) / len(words) if words else 0
                },
                'content_analysis': {
                    'language_detected': self._detect_language(data),
                    'readability_score': self._calculate_readability(data),
                    'sentiment_indication': self._basic_sentiment_analysis(data)
                }
            }
        except Exception as e:
            self.logger.error(f"Text processing failed: {e}")
            return {'error': str(e), 'type': 'text'}
    
    async def _process_xml_data(self, data: str) -> Dict[str, Any]:
        """Process XML data"""
        try:
            # Basic XML processing (would use xml.etree.ElementTree in production)
            return {
                'type': 'xml',
                'content': data,
                'size_bytes': len(data),
                'element_count': data.count('<'),
                'processing_note': 'Basic XML processing implemented'
            }
        except Exception as e:
            self.logger.error(f"XML processing failed: {e}")
            return {'error': str(e), 'type': 'xml'}
    
    def _analyze_json_structure(self, data: Any, max_depth: int = 3) -> Dict[str, Any]:
        """Analyze JSON structure recursively"""
        if max_depth <= 0:
            return {'type': type(data).__name__, 'truncated': True}
        
        if isinstance(data, dict):
            return {
                'type': 'object',
                'key_count': len(data),
                'keys': list(data.keys())[:10],  # Limit to first 10 keys
                'sample_properties': {
                    k: self._analyze_json_structure(v, max_depth - 1)
                    for k, v in list(data.items())[:5]  # Analyze first 5 properties
                }
            }
        elif isinstance(data, list):
            return {
                'type': 'array',
                'length': len(data),
                'sample_element': self._analyze_json_structure(data[0], max_depth - 1) if data else None,
                'element_types': list(set(type(item).__name__ for item in data[:10]))
            }
        else:
            return {
                'type': type(data).__name__,
                'value_preview': str(data)[:100] if len(str(data)) > 100 else str(data)
            }
    
    def _calculate_json_complexity(self, data: Any, depth: int = 0) -> float:
        """Calculate JSON complexity score"""
        if depth > 10:  # Prevent infinite recursion
            return 1.0
        
        if isinstance(data, dict):
            return 1.0 + sum(self._calculate_json_complexity(v, depth + 1) for v in data.values()) / len(data)
        elif isinstance(data, list):
            if data:
                return 0.5 + sum(self._calculate_json_complexity(item, depth + 1) for item in data[:5]) / min(len(data), 5)
            return 0.5
        else:
            return 0.1
    
    def _detect_language(self, text: str) -> str:
        """Basic language detection"""
        # Simple heuristic-based language detection
        if any(char in text for char in 'αβγδεζηθικλμνξοπρστυφχψω'):
            return 'greek'
        elif any(char in text for char in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'):
            return 'russian'
        elif any(char in text for char in '你我他她它们的是在有一个'):
            return 'chinese'
        else:
            return 'english'  # Default assumption
    
    def _calculate_readability(self, text: str) -> float:
        """Calculate basic readability score"""
        words = text.split()
        if not words:
            return 0.0
        
        # Simple readability based on average word length
        avg_word_length = sum(len(word) for word in words) / len(words)
        
        # Normalize to 0-1 scale (assuming 5 chars/word is average)
        return min(1.0, max(0.0, 1.0 - (avg_word_length - 5) / 10))
    
    def _basic_sentiment_analysis(self, text: str) -> str:
        """Basic sentiment analysis"""
        positive_words = ['good', 'great', 'excellent', 'amazing', 'wonderful', 'fantastic']
        negative_words = ['bad', 'terrible', 'awful', 'horrible', 'disgusting', 'hate']
        
        text_lower = text.lower()
        positive_count = sum(word in text_lower for word in positive_words)
        negative_count = sum(word in text_lower for word in negative_words)
        
        if positive_count > negative_count:
            return 'positive'
        elif negative_count > positive_count:
            return 'negative'
        else:
            return 'neutral'

class Layer2Agent(BaseAgent):
    """Analysis & Reasoning Layer Agent"""
    
    async def _initialize_agent(self) -> None:
        """Initialize Layer2 agent components"""
        self.analyzers = {
            'pattern': self._analyze_patterns,
            'trend': self._analyze_trends,
            'anomaly': self._detect_anomalies,
            'classification': self._classify_data,
            'correlation': self._find_correlations
        }
        self.analysis_cache = TTLCache(maxsize=500, ttl=1800)
        self.logger.info(f"Layer2 agent {self.name} components initialized")
    
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process analysis tasks"""
        start_time = time.time()
        
        try:
            analysis_type = task.get('analysis_type', 'pattern')
            analyzer = self.analyzers.get(analysis_type, self._analyze_patterns)
            
            # Check cache first
            cache_key = self._generate_cache_key(task)
            if cache_key in self.analysis_cache:
                cached_result = self.analysis_cache[cache_key]
                self.logger.debug(f"Using cached analysis result for {cache_key}")
                return cached_result
            
            result = await analyzer(task.get('data', {}))
            
            execution_time = time.time() - start_time
            
            response = {
                'analysis_result': result,
                'status': 'success',
                'metadata': {
                    'agent': self.name,
                    'layer': self.layer_id,
                    'analysis_type': analysis_type,
                    'execution_time': execution_time,
                    'timestamp': datetime.now().isoformat(),
                    'cached': False
                }
            }
            
            # Cache the result
            self.analysis_cache[cache_key] = response
            
            await self._record_task_execution(task, response, execution_time)
            return response
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Analysis task failed: {e}")
            
            error_response = {
                'error': str(e),
                'status': 'failed',
                'metadata': {
                    'agent': self.name,
                    'layer': self.layer_id,
                    'execution_time': execution_time,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            await self._record_task_execution(task, error_response, execution_time)
            return error_response
    
    def _generate_cache_key(self, task: Dict[str, Any]) -> str:
        """Generate cache key for analysis task"""
        relevant_data = {
            'analysis_type': task.get('analysis_type'),
            'data_hash': hashlib.md5(str(task.get('data', {})).encode()).hexdigest()
        }
        return hashlib.md5(json.dumps(relevant_data, sort_keys=True).encode()).hexdigest()
    
    async def _analyze_patterns(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze patterns in data"""
        try:
            patterns_found = []
            
            # Pattern analysis for different data types
            if data.get('type') == 'csv' and 'rows' in data:
                patterns_found.extend(self._find_csv_patterns(data['rows']))
            elif data.get('type') == 'json':
                patterns_found.extend(self._find_json_patterns(data.get('data', {})))
            elif data.get('type') == 'text':
                patterns_found.extend(self._find_text_patterns(data.get('content', '')))
            
            return {
                'patterns_found': patterns_found,
                'pattern_count': len(patterns_found),
                'confidence': self._calculate_pattern_confidence(patterns_found),
                'method': 'pattern_analysis'
            }
        except Exception as e:
            self.logger.error(f"Pattern analysis failed: {e}")
            return {'error': str(e), 'method': 'pattern_analysis'}
    
    async def _analyze_trends(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze trends in data"""
        try:
            trends = []
            
            if data.get('type') == 'csv' and 'rows' in data:
                trends = self._analyze_csv_trends(data['rows'])
            
            return {
                'trends': trends,
                'trend_count': len(trends),
                'overall_direction': self._determine_overall_direction(trends),
                'strength': self._calculate_trend_strength(trends)
            }
        except Exception as e:
            self.logger.error(f"Trend analysis failed: {e}")
            return {'error': str(e)}
    
    async def _detect_anomalies(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect anomalies in data"""
        try:
            anomalies = []
            
            if data.get('type') == 'csv' and 'rows' in data:
                anomalies = self._detect_csv_anomalies(data['rows'])
            
            return {
                'anomalies': anomalies,
                'anomaly_count': len(anomalies),
                'anomaly_score': len(anomalies) / max(1, len(data.get('rows', []))),
                'threshold': 0.05  # 5% anomaly threshold
            }
        except Exception as e:
            self.logger.error(f"Anomaly detection failed: {e}")
            return {'error': str(e)}
    
    async def _classify_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Classify data into categories"""
        try:
            classification = {
                'data_type': data.get('type', 'unknown'),
                'size_category': self._classify_size(data),
                'complexity_category': self._classify_complexity(data),
                'quality_category': self._classify_quality(data)
            }
            
            return {
                'classification': classification,
                'confidence': 0.8,  # Fixed confidence for basic classification
                'method': 'rule_based_classification'
            }
        except Exception as e:
            self.logger.error(f"Data classification failed: {e}")
            return {'error': str(e)}
    
    async def _find_correlations(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Find correlations in data"""
        try:
            correlations = []
            
            if data.get('type') == 'csv' and 'rows' in data:
                correlations = self._find_csv_correlations(data['rows'], data.get('columns', []))
            
            return {
                'correlations': correlations,
                'correlation_count': len(correlations),
                'significant_correlations': [c for c in correlations if abs(c.get('strength', 0)) > 0.5]
            }
        except Exception as e:
            self.logger.error(f"Correlation analysis failed: {e}")
            return {'error': str(e)}
    
    def _find_csv_patterns(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find patterns in CSV data"""
        patterns = []
        
        if not rows:
            return patterns
        
        # Find missing value patterns
        for column in rows[0].keys():
            missing_count = sum(1 for row in rows if not row.get(column))
            if missing_count > 0:
                patterns.append({
                    'type': 'missing_values',
                    'column': column,
                    'count': missing_count,
                    'percentage': missing_count / len(rows)
                })
        
        # Find duplicate patterns
        seen_rows = set()
        duplicates = 0
        for row in rows:
            row_tuple = tuple(sorted(row.items()))
            if row_tuple in seen_rows:
                duplicates += 1
            seen_rows.add(row_tuple)
        
        if duplicates > 0:
            patterns.append({
                'type': 'duplicate_rows',
                'count': duplicates,
                'percentage': duplicates / len(rows)
            })
        
        return patterns
    
    def _find_json_patterns(self, data: Any) -> List[Dict[str, Any]]:
        """Find patterns in JSON data"""
        patterns = []
        
        if isinstance(data, dict):
            # Find common key patterns
            patterns.append({
                'type': 'object_structure',
                'key_count': len(data),
                'has_nested_objects': any(isinstance(v, dict) for v in data.values()),
                'has_arrays': any(isinstance(v, list) for v in data.values())
            })
        elif isinstance(data, list):
            # Find array patterns
            if data:
                element_types = [type(item).__name__ for item in data]
                type_counts = Counter(element_types)
                patterns.append({
                    'type': 'array_structure',
                    'length': len(data),
                    'element_types': dict(type_counts),
                    'homogeneous': len(type_counts) == 1
                })
        
        return patterns
    
    def _find_text_patterns(self, text: str) -> List[Dict[str, Any]]:
        """Find patterns in text data"""
        patterns = []
        
        # Find common patterns
        words = text.split()
        if words:
            word_counts = Counter(words)
            most_common = word_counts.most_common(5)
            
            patterns.append({
                'type': 'word_frequency',
                'total_words': len(words),
                'unique_words': len(word_counts),
                'most_common_words': most_common,
                'vocabulary_ratio': len(word_counts) / len(words)
            })
        
        return patterns
    
    def _calculate_pattern_confidence(self, patterns: List[Dict[str, Any]]) -> float:
        """Calculate confidence score for patterns"""
        if not patterns:
            return 0.0
        
        # Simple confidence based on pattern count and types
        base_confidence = 0.5
        pattern_bonus = min(0.4, len(patterns) * 0.1)
        
        return base_confidence + pattern_bonus
    
    def _analyze_csv_trends(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze trends in CSV data"""
        trends = []
        
        if not rows:
            return trends
        
        # Analyze numeric columns for trends
        for column in rows[0].keys():
            numeric_values = []
            for row in rows:
                try:
                    value = float(row.get(column, 0))
                    numeric_values.append(value)
                except (ValueError, TypeError):
                    continue
            
            if len(numeric_values) >= 3:  # Need at least 3 points for trend
                trend_direction = self._calculate_trend_direction(numeric_values)
                trends.append({
                    'column': column,
                    'direction': trend_direction,
                    'start_value': numeric_values[0],
                    'end_value': numeric_values[-1],
                    'change_percentage': ((numeric_values[-1] - numeric_values[0]) / numeric_values[0] * 100) if numeric_values[0] != 0 else 0
                })
        
        return trends
    
    def _calculate_trend_direction(self, values: List[float]) -> str:
        """Calculate trend direction from numeric values"""
        if len(values) < 2:
            return 'stable'
        
        differences = [values[i+1] - values[i] for i in range(len(values) - 1)]
        avg_change = sum(differences) / len(differences)
        
        if avg_change > 0.1:
            return 'increasing'
        elif avg_change < -0.1:
            return 'decreasing'
        else:
            return 'stable'
    
    def _determine_overall_direction(self, trends: List[Dict[str, Any]]) -> str:
        """Determine overall trend direction"""
        if not trends:
            return 'unknown'
        
        directions = [t.get('direction', 'stable') for t in trends]
        direction_counts = Counter(directions)
        
        return direction_counts.most_common(1)[0][0]
    
    def _calculate_trend_strength(self, trends: List[Dict[str, Any]]) -> float:
        """Calculate overall trend strength"""
        if not trends:
            return 0.0
        
        strengths = []
        for trend in trends:
            change_pct = abs(trend.get('change_percentage', 0))
            strength = min(1.0, change_pct / 100)  # Normalize to 0-1
            strengths.append(strength)
        
        return sum(strengths) / len(strengths)
    
    def _detect_csv_anomalies(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detect anomalies in CSV data"""
        anomalies = []
        
        if not rows:
            return anomalies
        
        # Detect outliers in numeric columns
        for column in rows[0].keys():
            numeric_values = []
            for i, row in enumerate(rows):
                try:
                    value = float(row.get(column, 0))
                    numeric_values.append((i, value))
                except (ValueError, TypeError):
                    continue
            
            if len(numeric_values) >= 5:  # Need sufficient data for outlier detection
                values_only = [v[1] for v in numeric_values]
                mean_val = statistics.mean(values_only)
                stdev = statistics.stdev(values_only) if len(values_only) > 1 else 0
                
                if stdev > 0:
                    for row_idx, value in numeric_values:
                        z_score = abs((value - mean_val) / stdev)
                        if z_score > 2.5:  # 2.5 standard deviations
                            anomalies.append({
                                'type': 'outlier',
                                'column': column,
                                'row_index': row_idx,
                                'value': value,
                                'z_score': z_score
                            })
        
        return anomalies
    
    def _classify_size(self, data: Dict[str, Any]) -> str:
        """Classify data size"""
        if data.get('type') == 'csv':
            row_count = len(data.get('rows', []))
            if row_count < 100:
                return 'small'
            elif row_count < 10000:
                return 'medium'
            else:
                return 'large'
        elif data.get('type') in ['json', 'text']:
            size_bytes = data.get('size_bytes', 0)
            if size_bytes < 1024:
                return 'small'
            elif size_bytes < 1024 * 1024:
                return 'medium'
            else:
                return 'large'
        return 'unknown'
    
    def _classify_complexity(self, data: Dict[str, Any]) -> str:
        """Classify data complexity"""
        if data.get('type') == 'csv':
            column_count = len(data.get('columns', []))
            if column_count < 5:
                return 'simple'
            elif column_count < 20:
                return 'moderate'
            else:
                return 'complex'
        elif data.get('type') == 'json':
            complexity_score = data.get('complexity_score', 0)
            if complexity_score < 2:
                return 'simple'
            elif complexity_score < 5:
                return 'moderate'
            else:
                return 'complex'
        return 'unknown'
    
    def _classify_quality(self, data: Dict[str, Any]) -> str:
        """Classify data quality"""
        if data.get('type') == 'csv':
            quality_info = data.get('data_quality', {})
            success_rate = quality_info.get('success_rate', 1.0)
            if success_rate > 0.95:
                return 'high'
            elif success_rate > 0.8:
                return 'medium'
            else:
                return 'low'
        return 'unknown'
    
    def _find_csv_correlations(self, rows: List[Dict[str, Any]], columns: List[str]) -> List[Dict[str, Any]]:
        """Find correlations between CSV columns"""
        correlations = []
        
        # Get numeric columns
        numeric_columns = []
        for column in columns:
            numeric_values = []
            for row in rows:
                try:
                    value = float(row.get(column, 0))
                    numeric_values.append(value)
                except (ValueError, TypeError):
                    continue
            
            if len(numeric_values) == len(rows):  # All values are numeric
                numeric_columns.append((column, numeric_values))
        
        # Calculate correlations between numeric columns
        for i in range(len(numeric_columns)):
            for j in range(i + 1, len(numeric_columns)):
                col1_name, col1_values = numeric_columns[i]
                col2_name, col2_values = numeric_columns[j]
                
                correlation = self._calculate_correlation(col1_values, col2_values)
                
                correlations.append({
                    'column1': col1_name,
                    'column2': col2_name,
                    'strength': correlation,
                    'type': 'pearson'
                })
        
        return correlations
    
    def _calculate_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation coefficient"""
        if len(x) != len(y) or len(x) < 2:
            return 0.0
        
        try:
            mean_x = statistics.mean(x)
            mean_y = statistics.mean(y)
            
            numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(len(x)))
            
            sum_sq_x = sum((x[i] - mean_x) ** 2 for i in range(len(x)))
            sum_sq_y = sum((y[i] - mean_y) ** 2 for i in range(len(y)))
            
            denominator = (sum_sq_x * sum_sq_y) ** 0.5
            
            if denominator == 0:
                return 0.0
            
            return numerator / denominator
            
        except Exception:
            return 0.0

class Layer3Agent(BaseAgent):
    """Synthesis & Integration Layer Agent"""
    
    async def _initialize_agent(self) -> None:
        """Initialize Layer3 agent components"""
        self.synthesizers = {
            'merge': self._merge_results,
            'summarize': self._summarize_results,
            'prioritize': self._prioritize_results,
            'aggregate': self._aggregate_results,
            'consensus': self._build_consensus
        }
        self.synthesis_cache = TTLCache(maxsize=300, ttl=2400)  # 40 minute cache
        self.logger.info(f"Layer3 agent {self.name} components initialized")
    
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process synthesis tasks"""
        start_time = time.time()
        
        try:
            synthesis_type = task.get('synthesis_type', 'merge')
            synthesizer = self.synthesizers.get(synthesis_type, self._merge_results)
            
            result = await synthesizer(task.get('inputs', []))
            
            execution_time = time.time() - start_time
            
            response = {
                'synthesis_result': result,
                'status': 'success',
                'metadata': {
                    'agent': self.name,
                    'layer': self.layer_id,
                    'synthesis_type': synthesis_type,
                    'input_count': len(task.get('inputs', [])),
                    'execution_time': execution_time,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            await self._record_task_execution(task, response, execution_time)
            return response
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Synthesis task failed: {e}")
            
            error_response = {
                'error': str(e),
                'status': 'failed',
                'metadata': {
                    'agent': self.name,
                    'layer': self.layer_id,
                    'execution_time': execution_time,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            await self._record_task_execution(task, error_response, execution_time)
            return error_response
    
    async def _merge_results(self, inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge multiple results intelligently"""
        try:
            merged_data = {
                'source_count': len(inputs),
                'merged_timestamp': datetime.now().isoformat(),
                'data_sources': [],
                'consolidated_results': {}
            }
            
            # Categorize inputs by type
            categorized_inputs = defaultdict(list)
            for input_data in inputs:
                data_type = input_data.get('metadata', {}).get('layer', 'unknown')
                categorized_inputs[data_type].append(input_data)
                
                merged_data['data_sources'].append({
                    'agent': input_data.get('metadata', {}).get('agent', 'unknown'),
                    'layer': data_type,
                    'timestamp': input_data.get('metadata', {}).get('timestamp')
                })
            
            # Merge by category
            for category, category_inputs in categorized_inputs.items():
                if category == 1:  # Layer 1 - Data processing
                    merged_data['consolidated_results']['processed_data'] = self._merge_processed_data(category_inputs)
                elif category == 2:  # Layer 2 - Analysis
                    merged_data['consolidated_results']['analysis_results'] = self._merge_analysis_results(category_inputs)
                else:
                    merged_data['consolidated_results'][f'layer_{category}'] = category_inputs
            
            return {
                'merged_data': merged_data,
                'merge_quality': self._assess_merge_quality(inputs),
                'method': 'intelligent_merge'
            }
            
        except Exception as e:
            self.logger.error(f"Merge operation failed: {e}")
            return {'error': str(e), 'method': 'merge'}
    
    async def _summarize_results(self, inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create comprehensive summary of results"""
        try:
            summary = {
                'total_inputs': len(inputs),
                'summary_timestamp': datetime.now().isoformat(),
                'key_findings': [],
                'data_overview': {},
                'quality_assessment': {}
            }
            
            # Extract key findings
            for input_data in inputs:
                if 'analysis_result' in input_data:
                    analysis = input_data['analysis_result']
                    if isinstance(analysis, dict):
                        summary['key_findings'].extend(self._extract_key_findings(analysis))
                
                if 'processed_data' in input_data:
                    processed = input_data['processed_data']
                    if isinstance(processed, dict):
                        self._update_data_overview(summary['data_overview'], processed)
            
            # Assess overall quality
            summary['quality_assessment'] = self._assess_overall_quality(inputs)
            
            # Generate executive summary
            summary['executive_summary'] = self._generate_executive_summary(summary)
            
            return {
                'summary': summary,
                'confidence': self._calculate_summary_confidence(summary),
                'method': 'comprehensive_summary'
            }
            
        except Exception as e:
            self.logger.error(f"Summarization failed: {e}")
            return {'error': str(e), 'method': 'summarize'}
    
    async def _prioritize_results(self, inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Prioritize results based on importance and quality"""
        try:
            prioritized_results = []
            
            for i, input_data in enumerate(inputs):
                priority_score = self._calculate_priority_score(input_data)
                
                prioritized_results.append({
                    'original_index': i,
                    'priority_score': priority_score,
                    'data': input_data,
                    'priority_factors': self._get_priority_factors(input_data)
                })
            
            # Sort by priority score (highest first)
            prioritized_results.sort(key=lambda x: x['priority_score'], reverse=True)
            
            return {
                'prioritized_list': prioritized_results,
                'top_priority': prioritized_results[0] if prioritized_results else None,
                'priority_distribution': self._analyze_priority_distribution(prioritized_results),
                'method': 'multi_factor_prioritization'
            }
            
        except Exception as e:
            self.logger.error(f"Prioritization failed: {e}")
            return {'error': str(e), 'method': 'prioritize'}
    
    async def _aggregate_results(self, inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate results with statistical analysis"""
        try:
            aggregation = {
                'statistical_summary': {},
                'trend_analysis': {},
                'quality_metrics': {},
                'data_distribution': {}
            }
            
            # Collect numeric metrics
            numeric_metrics = defaultdict(list)
            for input_data in inputs:
                self._extract_numeric_metrics(input_data, numeric_metrics)
            
            # Calculate statistics
            for metric_name, values in numeric_metrics.items():
                if values:
                    aggregation['statistical_summary'][metric_name] = {
                        'count': len(values),
                        'mean': statistics.mean(values),
                        'median': statistics.median(values),
                        'std_dev': statistics.stdev(values) if len(values) > 1 else 0,
                        'min': min(values),
                        'max': max(values)
                    }
            
            # Analyze trends
            aggregation['trend_analysis'] = self._analyze_aggregated_trends(numeric_metrics)
            
            # Calculate quality metrics
            aggregation['quality_metrics'] = self._calculate_aggregated_quality(inputs)
            
            return {
                'aggregation': aggregation,
                'confidence': self._calculate_aggregation_confidence(aggregation),
                'method': 'statistical_aggregation'
            }
            
        except Exception as e:
            self.logger.error(f"Aggregation failed: {e}")
            return {'error': str(e), 'method': 'aggregate'}
    
    async def _build_consensus(self, inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build consensus from multiple inputs"""
        try:
            consensus = {
                'agreement_level': 0.0,
                'consensus_points': [],
                'disagreement_points': [],
                'confidence_weighted_result': {}
            }
            
            # Extract comparable elements
            comparable_elements = self._extract_comparable_elements(inputs)
            
            # Find agreement and disagreement
            for element_key, element_values in comparable_elements.items():
                agreement_score = self._calculate_agreement_score(element_values)
                
                if agreement_score > 0.7:  # High agreement
                    consensus_value = self._calculate_consensus_value(element_values)
                    consensus['consensus_points'].append({
                        'element': element_key,
                        'consensus_value': consensus_value,
                        'agreement_score': agreement_score
                    })
                else:  # Disagreement
                    consensus['disagreement_points'].append({
                        'element': element_key,
                        'values': element_values,
                        'agreement_score': agreement_score
                    })
            
            # Calculate overall agreement
            if comparable_elements:
                total_agreement = sum(
                    self._calculate_agreement_score(values) 
                    for values in comparable_elements.values()
                )
                consensus['agreement_level'] = total_agreement / len(comparable_elements)
            
            return {
                'consensus': consensus,
                'method': 'weighted_consensus_building'
            }
            
        except Exception as e:
            self.logger.error(f"Consensus building failed: {e}")
            return {'error': str(e), 'method': 'consensus'}
    
    # Helper methods for synthesis operations
    
    def _merge_processed_data(self, inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge processed data from Layer 1"""
        merged = {
            'total_data_points': 0,
            'data_types': set(),
            'combined_statistics': {}
        }
        
        for input_data in inputs:
            processed = input_data.get('processed_data', {})
            if isinstance(processed, dict):
                merged['data_types'].add(processed.get('type', 'unknown'))
                
                if processed.get('type') == 'csv':
                    merged['total_data_points'] += processed.get('row_count', 0)
                elif processed.get('type') == 'text':
                    merged['total_data_points'] += processed.get('statistics', {}).get('word_count', 0)
        
        merged['data_types'] = list(merged['data_types'])
        return merged
    
    def _merge_analysis_results(self, inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge analysis results from Layer 2"""
        merged = {
            'analysis_types': set(),
            'combined_findings': [],
            'overall_confidence': 0.0
        }
        
        confidences = []
        
        for input_data in inputs:
            analysis = input_data.get('analysis_result', {})
            if isinstance(analysis, dict):
                # Extract analysis type
                method = analysis.get('method', 'unknown')
                merged['analysis_types'].add(method)
                
                # Extract findings
                if 'patterns_found' in analysis:
                    merged['combined_findings'].extend(analysis['patterns_found'])
                if 'trends' in analysis:
                    merged['combined_findings'].extend(analysis['trends'])
                if 'anomalies' in analysis:
                    merged['combined_findings'].extend(analysis['anomalies'])
                
                # Track confidence
                if 'confidence' in analysis:
                    confidences.append(analysis['confidence'])
        
        merged['analysis_types'] = list(merged['analysis_types'])
        
        if confidences:
            merged['overall_confidence'] = statistics.mean(confidences)
        
        return merged
    
    def _assess_merge_quality(self, inputs: List[Dict[str, Any]]) -> float:
        """Assess the quality of merge operation"""
        if not inputs:
            return 0.0
        
        quality_factors = []
        
        # Factor 1: Input diversity
        agents = set(inp.get('metadata', {}).get('agent', '') for inp in inputs)
        diversity_score = min(1.0, len(agents) / 3)  # Normalize by expected max agents
        quality_factors.append(diversity_score)
        
        # Factor 2: Data completeness
        complete_inputs = sum(1 for inp in inputs if not inp.get('error'))
        completeness_score = complete_inputs / len(inputs)
        quality_factors.append(completeness_score)
        
        # Factor 3: Temporal consistency
        timestamps = [inp.get('metadata', {}).get('timestamp') for inp in inputs if inp.get('metadata', {}).get('timestamp')]
        if timestamps:
            # Check if all timestamps are within reasonable time window (1 hour)
            timestamps_dt = [datetime.fromisoformat(ts.replace('Z', '+00:00')) for ts in timestamps]
            time_span = max(timestamps_dt) - min(timestamps_dt)
            temporal_score = 1.0 if time_span.total_seconds() < 3600 else 0.5
            quality_factors.append(temporal_score)
        
        return statistics.mean(quality_factors) if quality_factors else 0.0
    
    def _extract_key_findings(self, analysis: Dict[str, Any]) -> List[str]:
        """Extract key findings from analysis"""
        findings = []
        
        if 'patterns_found' in analysis:
            patterns = analysis['patterns_found']
            if patterns:
                findings.append(f"Found {len(patterns)} data patterns")
        
        if 'trends' in analysis:
            trends = analysis['trends']
            if trends:
                directions = [t.get('direction', 'unknown') for t in trends]
                most_common_direction = Counter(directions).most_common(1)[0][0]
                findings.append(f"Primary trend direction: {most_common_direction}")
        
        if 'anomalies' in analysis:
            anomalies = analysis['anomalies']
            if anomalies:
                findings.append(f"Detected {len(anomalies)} anomalies")
        
        return findings
    
    def _update_data_overview(self, overview: Dict[str, Any], processed_data: Dict[str, Any]) -> None:
        """Update data overview with processed data"""
        data_type = processed_data.get('type', 'unknown')
        
        if data_type not in overview:
            overview[data_type] = {
                'count': 0,
                'total_size': 0,
                'features': set()
            }
        
        overview[data_type]['count'] += 1
        
        if data_type == 'csv':
            overview[data_type]['total_size'] += processed_data.get('row_count', 0)
            if 'columns' in processed_data:
                overview[data_type]['features'].update(processed_data['columns'])
        elif data_type == 'text':
            overview[data_type]['total_size'] += processed_data.get('statistics', {}).get('character_count', 0)
        
        # Convert sets to lists for JSON serialization
        for data_type_key in overview:
            if 'features' in overview[data_type_key] and isinstance(overview[data_type_key]['features'], set):
                overview[data_type_key]['features'] = list(overview[data_type_key]['features'])
    
    def _assess_overall_quality(self, inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Assess overall quality of inputs"""
        if not inputs:
            return {'overall_score': 0.0}
        
        successful_inputs = sum(1 for inp in inputs if not inp.get('error'))
        success_rate = successful_inputs / len(inputs)
        
        # Assess execution times
        execution_times = [
            inp.get('metadata', {}).get('execution_time', 0) 
            for inp in inputs 
            if inp.get('metadata', {}).get('execution_time')
        ]
        
        avg_execution_time = statistics.mean(execution_times) if execution_times else 0
        
        return {
            'overall_score': (success_rate + (1.0 if avg_execution_time < 1.0 else 0.5)) / 2,
            'success_rate': success_rate,
            'average_execution_time': avg_execution_time,
            'total_inputs': len(inputs),
            'successful_inputs': successful_inputs
        }
    
    def _generate_executive_summary(self, summary: Dict[str, Any]) -> str:
        """Generate executive summary"""
        total_inputs = summary.get('total_inputs', 0)
        key_findings_count = len(summary.get('key_findings', []))
        quality_score = summary.get('quality_assessment', {}).get('overall_score', 0)
        
        return (f"Processed {total_inputs} inputs with {key_findings_count} key findings. "
                f"Overall quality score: {quality_score:.2f}. "
                f"Data processing completed successfully with comprehensive analysis coverage.")
    
    def _calculate_summary_confidence(self, summary: Dict[str, Any]) -> float:
        """Calculate confidence in summary"""
        factors = []
        
        # Factor 1: Number of inputs
        input_count = summary.get('total_inputs', 0)
        input_factor = min(1.0, input_count / 5)  # Normalize by expected 5 inputs
        factors.append(input_factor)
        
        # Factor 2: Quality assessment
        quality_score = summary.get('quality_assessment', {}).get('overall_score', 0)
        factors.append(quality_score)
        
        # Factor 3: Key findings availability
        findings_count = len(summary.get('key_findings', []))
        findings_factor = min(1.0, findings_count / 3)  # Normalize by expected 3 findings
        factors.append(findings_factor)
        
        return statistics.mean(factors) if factors else 0.0
    
    def _calculate_priority_score(self, input_data: Dict[str, Any]) -> float:
        """Calculate priority score for input"""
        score = 0.0
        
        # Factor 1: Execution success
        if not input_data.get('error'):
            score += 0.3
        
        # Factor 2: Data quality
        if 'processed_data' in input_data:
            processed = input_data['processed_data']
            if isinstance(processed, dict) and not processed.get('error'):
                score += 0.3
        
        # Factor 3: Analysis confidence
        if 'analysis_result' in input_data:
            analysis = input_data['analysis_result']
            if isinstance(analysis, dict):
                confidence = analysis.get('confidence', 0)
                score += confidence * 0.2
        
        # Factor 4: Recency
        timestamp_str = input_data.get('metadata', {}).get('timestamp')
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                age_minutes = (datetime.now(timestamp.tzinfo) - timestamp).total_seconds() / 60
                recency_score = max(0, 1 - (age_minutes / 60))  # Full score if < 1 hour old
                score += recency_score * 0.2
            except (ValueError, TypeError):
                pass
        
        return score
    
    def _get_priority_factors(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get factors that influenced priority score"""
        factors = {
            'has_error': bool(input_data.get('error')),
            'has_processed_data': 'processed_data' in input_data,
            'has_analysis': 'analysis_result' in input_data,
            'agent': input_data.get('metadata', {}).get('agent'),
            'layer': input_data.get('metadata', {}).get('layer')
        }
        
        if 'analysis_result' in input_data:
            analysis = input_data['analysis_result']
            if isinstance(analysis, dict):
                factors['analysis_confidence'] = analysis.get('confidence', 0)
        
        return factors
    
    def _analyze_priority_distribution(self, prioritized_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze distribution of priority scores"""
        if not prioritized_results:
            return {}
        
        scores = [result['priority_score'] for result in prioritized_results]
        
        return {
            'mean_score': statistics.mean(scores),
            'median_score': statistics.median(scores),
            'score_range': max(scores) - min(scores),
            'high_priority_count': sum(1 for score in scores if score > 0.7),
            'medium_priority_count': sum(1 for score in scores if 0.3 <= score <= 0.7),
            'low_priority_count': sum(1 for score in scores if score < 0.3)
        }
    
    def _extract_numeric_metrics(self, input_data: Dict[str, Any], metrics: Dict[str, List[float]]) -> None:
        """Extract numeric metrics from input data"""
        # Extract execution time
        exec_time = input_data.get('metadata', {}).get('execution_time')
        if exec_time:
            metrics['execution_time'].append(exec_time)
        
        # Extract confidence scores
        if 'analysis_result' in input_data:
            analysis = input_data['analysis_result']
            if isinstance(analysis, dict):
                confidence = analysis.get('confidence')
                if confidence is not None:
                    metrics['confidence'].append(confidence)
        
        # Extract data size metrics
        if 'processed_data' in input_data:
            processed = input_data['processed_data']
            if isinstance(processed, dict):
                if processed.get('type') == 'csv':
                    row_count = processed.get('row_count')
                    if row_count:
                        metrics['csv_row_count'].append(row_count)
                elif processed.get('type') == 'text':
                    word_count = processed.get('statistics', {}).get('word_count')
                    if word_count:
                        metrics['text_word_count'].append(word_count)
    
    def _analyze_aggregated_trends(self, numeric_metrics: Dict[str, List[float]]) -> Dict[str, Any]:
        """Analyze trends in aggregated metrics"""
        trends = {}
        
        for metric_name, values in numeric_metrics.items():
            if len(values) >= 3:  # Need at least 3 points for trend
                # Simple trend analysis
                first_half = values[:len(values)//2]
                second_half = values[len(values)//2:]
                
                if first_half and second_half:
                    first_avg = statistics.mean(first_half)
                    second_avg = statistics.mean(second_half)
                    
                    if second_avg > first_avg * 1.1:
                        direction = 'increasing'
                    elif second_avg < first_avg * 0.9:
                        direction = 'decreasing'
                    else:
                        direction = 'stable'
                    
                    trends[metric_name] = {
                        'direction': direction,
                        'first_half_avg': first_avg,
                        'second_half_avg': second_avg,
                        'change_percentage': ((second_avg - first_avg) / first_avg * 100) if first_avg != 0 else 0
                    }
        
        return trends
    
    def _calculate_aggregated_quality(self, inputs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate quality metrics for aggregated data"""
        if not inputs:
            return {}
        
        # Count successful vs failed operations
        successful = sum(1 for inp in inputs if not inp.get('error'))
        total = len(inputs)
        
        # Calculate average execution time
        exec_times = [
            inp.get('metadata', {}).get('execution_time', 0) 
            for inp in inputs 
            if inp.get('metadata', {}).get('execution_time')
        ]
        avg_exec_time = statistics.mean(exec_times) if exec_times else 0
        
        # Calculate data completeness
        complete_data = sum(
            1 for inp in inputs 
            if ('processed_data' in inp or 'analysis_result' in inp) and not inp.get('error')
        )
        
        return {
            'success_rate': successful / total,
            'average_execution_time': avg_exec_time,
            'data_completeness': complete_data / total,
            'overall_quality_score': (successful / total + (complete_data / total)) / 2
        }
    
    def _calculate_aggregation_confidence(self, aggregation: Dict[str, Any]) -> float:
        """Calculate confidence in aggregation results"""
        factors = []
        
        # Factor 1: Statistical coverage
        stats = aggregation.get('statistical_summary', {})
        if stats:
            avg_count = statistics.mean([s.get('count', 0) for s in stats.values()])
            coverage_factor = min(1.0, avg_count / 5)  # Normalize by expected 5 data points
            factors.append(coverage_factor)
        
        # Factor 2: Quality metrics
        quality = aggregation.get('quality_metrics', {})
        quality_score = quality.get('overall_quality_score', 0)
        factors.append(quality_score)
        
        # Factor 3: Trend consistency
        trends = aggregation.get('trend_analysis', {})
        if trends:
            # Check for consistent trends
            directions = [t.get('direction', 'unknown') for t in trends.values()]
            direction_consistency = 1.0 if len(set(directions)) <= 2 else 0.5
            factors.append(direction_consistency)
        
        return statistics.mean(factors) if factors else 0.5
    
    def _extract_comparable_elements(self, inputs: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        """Extract comparable elements from inputs for consensus building"""
        comparable = defaultdict(list)
        
        for input_data in inputs:
            # Extract confidence scores
            if 'analysis_result' in input_data:
                analysis = input_data['analysis_result']
                if isinstance(analysis, dict) and 'confidence' in analysis:
                    comparable['confidence'].append(analysis['confidence'])
            
            # Extract execution success
            comparable['success'].append(not bool(input_data.get('error')))
            
            # Extract agent performance
            exec_time = input_data.get('metadata', {}).get('execution_time')
            if exec_time:
                comparable['execution_time'].append(exec_time)
        
        return dict(comparable)
    
    def _calculate_agreement_score(self, values: List[Any]) -> float:
        """Calculate agreement score for a set of values"""
        if not values:
            return 0.0
        
        if len(values) == 1:
            return 1.0
        
        # For numeric values
        if all(isinstance(v, (int, float)) for v in values):
            mean_val = statistics.mean(values)
            if mean_val == 0:
                return 1.0 if all(v == 0 for v in values) else 0.0
            
            # Calculate coefficient of variation
            std_dev = statistics.stdev(values) if len(values) > 1 else 0
            cv = std_dev / mean_val if mean_val != 0 else float('inf')
            
            # High agreement if coefficient of variation is low
            return max(0.0, 1.0 - cv)
        
        # For boolean or categorical values
        value_counts = Counter(values)
        most_common_count = value_counts.most_common(1)[0][1]
        return most_common_count / len(values)
    
    def _calculate_consensus_value(self, values: List[Any]) -> Any:
        """Calculate consensus value from a list of values"""
        if not values:
            return None
        
        # For numeric values, use mean
        if all(isinstance(v, (int, float)) for v in values):
            return statistics.mean(values)
        
        # For categorical values, use most common
        value_counts = Counter(values)
        return value_counts.most_common(1)[0][0]

class Layer4Agent(BaseAgent):
    """Output & Response Layer Agent"""
    
    async def _initialize_agent(self) -> None:
        """Initialize Layer4 agent components"""
        self.formatters = {
            'json': self._format_json,
            'text': self._format_text,
            'report': self._format_report,
            'markdown': self._format_markdown,
            'summary': self._format_summary
        }
        self.output_cache = TTLCache(maxsize=200, ttl=1800)  # 30 minute cache
        self.logger.info(f"Layer4 agent {self.name} components initialized")
    
    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process formatting tasks"""
        start_time = time.time()
        
        try:
            format_type = task.get('format_type', 'json')
            formatter = self.formatters.get(format_type, self._format_json)
            
            result = await formatter(task.get('data', {}))
            
            execution_time = time.time() - start_time
            
            response = {
                'formatted_output': result,
                'status': 'success',
                'metadata': {
                    'agent': self.name,
                    'layer': self.layer_id,
                    'format_type': format_type,
                    'output_length': len(str(result)),
                    'execution_time': execution_time,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            await self._record_task_execution(task, response, execution_time)
            return response
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Formatting task failed: {e}")
            
            error_response = {
                'error': str(e),
                'status': 'failed',
                'metadata': {
                    'agent': self.name,
                    'layer': self.layer_id,
                    'execution_time': execution_time,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
            await self._record_task_execution(task, error_response, execution_time)
            return error_response
    
    async def _format_json(self, data: Dict[str, Any]) -> str:
        """Format as JSON with enhanced structure"""
        try:
            formatted_data = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'format_version': '2.0',
                    'data_type': self._identify_data_type(data)
                },
                'content': data,
                'statistics': self._generate_data_statistics(data)
            }
            
            return json.dumps(formatted_data, indent=2, ensure_ascii=False, default=str)
            
        except Exception as e:
            self.logger.error(f"JSON formatting failed: {e}")
            return json.dumps({'error': str(e), 'original_data': str(data)}, indent=2)
    
    async def _format_text(self, data: Dict[str, Any]) -> str:
        """Format as human-readable text"""
        try:
            text_parts = []
            text_parts.append(f"Data Analysis Report - Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            text_parts.append("=" * 60)
            text_parts.append("")
            
            # Add data overview
            data_type = self._identify_data_type(data)
            text_parts.append(f"Data Type: {data_type}")
            text_parts.append("")
            
            # Format main content
            text_parts.extend(self._format_content_as_text(data))
            
            # Add statistics
            text_parts.append("")
            text_parts.append("Statistics:")
            text_parts.append("-" * 20)
            stats = self._generate_data_statistics(data)
            for key, value in stats.items():
                text_parts.append(f"{key}: {value}")
            
            return "\n".join(text_parts)
            
        except Exception as e:
            self.logger.error(f"Text formatting failed: {e}")
            return f"Error formatting data: {e}\n\nOriginal data: {str(data)}"
    
    async def _format_report(self, data: Dict[str, Any]) -> str:
        """Format as comprehensive report"""
        try:
            report_sections = []
            
            # Executive Summary
            report_sections.append("# Executive Summary")
            report_sections.append(self._generate_executive_summary(data))
            report_sections.append("")
            
            # Data Overview
            report_sections.append("# Data Overview")
            report_sections.append(self._generate_data_overview(data))
            report_sections.append("")
            
            # Key Findings
            report_sections.append("# Key Findings")
            findings = self._extract_key_findings_for_report(data)
            for i, finding in enumerate(findings, 1):
                report_sections.append(f"{i}. {finding}")
            report_sections.append("")
            
            # Detailed Analysis
            report_sections.append("# Detailed Analysis")
            report_sections.extend(self._generate_detailed_analysis(data))
            report_sections.append("")
            
            # Recommendations
            report_sections.append("# Recommendations")
            recommendations = self._generate_recommendations(data)
            for i, rec in enumerate(recommendations, 1):
                report_sections.append(f"{i}. {rec}")
            report_sections.append("")
            
            # Appendix
            report_sections.append("# Appendix")
            report_sections.append("## Technical Details")
            report_sections.append(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report_sections.append(f"Data processed: {self._count_data_elements(data)} elements")
            
            return "\n".join(report_sections)
            
        except Exception as e:
            self.logger.error(f"Report formatting failed: {e}")
            return f"# Error Report\n\nFailed to generate report: {e}"
    
    async def _format_markdown(self, data: Dict[str, Any]) -> str:
        """Format as Markdown document"""
        try:
            md_sections = []
            
            # Title
            md_sections.append("# Multi-Agent System Analysis Results")
            md_sections.append("")
            md_sections.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}")
            md_sections.append("")
            
            # Quick Stats
            stats = self._generate_data_statistics(data)
            md_sections.append("## Quick Statistics")
            md_sections.append("")
            for key, value in stats.items():
                md_sections.append(f"- **{key}:** {value}")
            md_sections.append("")
            
            # Main Content
            md_sections.append("## Analysis Results")
            md_sections.append("")
            md_sections.extend(self._format_content_as_markdown(data))
            
            # Data Quality
            md_sections.append("")
            md_sections.append("## Data Quality Assessment")
            md_sections.append("")
            quality_info = self._assess_data_quality(data)
            for metric, score in quality_info.items():
                # Create progress bar using markdown
                progress = "█" * int(score * 10) + "░" * (10 - int(score * 10))
                md_sections.append(f"- **{metric}:** {progress} ({score:.1%})")
            
            return "\n".join(md_sections)
            
        except Exception as e:
            self.logger.error(f"Markdown formatting failed: {e}")
            return f"# Error\n\nMarkdown formatting failed: {e}"
    
    async def _format_summary(self, data: Dict[str, Any]) -> str:
        """Format as concise summary"""
        try:
            summary_parts = []
            
            # Header
            summary_parts.append("🔍 Analysis Summary")
            summary_parts.append("=" * 50)
            
            # Key metrics
            stats = self._generate_data_statistics(data)
            summary_parts.append(f"📊 Processed {stats.get('element_count', 0)} data elements")
            summary_parts.append(f"⏱️ Execution time: {stats.get('total_execution_time', 0):.2f}s")
            summary_parts.append(f"✅ Success rate: {stats.get('success_rate', 0):.1%}")
            
            # Top findings
            findings = self._extract_key_findings_for_report(data)[:3]  # Top 3
            if findings:
                summary_parts.append("")
                summary_parts.append("🎯 Top Findings:")
                for finding in findings:
                    summary_parts.append(f"  • {finding}")
            
            # Quality assessment
            quality = self._assess_data_quality(data)
            avg_quality = statistics.mean(quality.values()) if quality else 0
            quality_emoji = "🟢" if avg_quality > 0.8 else "🟡" if avg_quality > 0.6 else "🔴"
            summary_parts.append(f"\n{quality_emoji} Overall Quality: {avg_quality:.1%}")
            
            return "\n".join(summary_parts)
            
        except Exception as e:
            self.logger.error(f"Summary formatting failed: {e}")
            return f"❌ Summary generation failed: {e}"
    
    # Helper methods for formatting
    
    def _identify_data_type(self, data: Dict[str, Any]) -> str:
        """Identify the primary data type"""
        if 'synthesis_result' in data:
            return 'synthesis'
        elif 'analysis_result' in data:
            return 'analysis'
        elif 'processed_data' in data:
            return 'processed'
        elif isinstance(data, dict) and data.get('type'):
            return data['type']
        else:
            return 'mixed'
    
    def _generate_data_statistics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate statistics about the data"""
        stats = {
            'element_count': self._count_data_elements(data),
            'data_size_bytes': len(json.dumps(data, default=str)),
            'nested_levels': self._count_nesting_levels(data),
            'timestamp': datetime.now().isoformat()
        }
        
        # Add execution time if available
        if isinstance(data, dict) and 'metadata' in data:
            exec_time = data['metadata'].get('execution_time')
            if exec_time:
                stats['total_execution_time'] = exec_time
        
        # Calculate success rate
        if self._has_synthesis_data(data):
            stats['success_rate'] = self._calculate_success_rate(data)
        
        return stats
    
    def _count_data_elements(self, data: Any, level: int = 0) -> int:
        """Count data elements recursively"""
        if level > 10:  # Prevent infinite recursion
            return 1
        
        if isinstance(data, dict):
            return sum(self._count_data_elements(value, level + 1) for value in data.values())
        elif isinstance(data, list):
            return sum(self._count_data_elements(item, level + 1) for item in data)
        else:
            return 1
    
    def _count_nesting_levels(self, data: Any, current_level: int = 0) -> int:
        """Count maximum nesting levels"""
        if current_level > 20:  # Prevent infinite recursion
            return current_level
        
        if isinstance(data, dict):
            if not data:
                return current_level
            return max(self._count_nesting_levels(value, current_level + 1) for value in data.values())
        elif isinstance(data, list):
            if not data:
                return current_level
            return max(self._count_nesting_levels(item, current_level + 1) for item in data)
        else:
            return current_level
    
    def _format_content_as_text(self, data: Dict[str, Any]) -> List[str]:
        """Format content as readable text lines"""
        lines = []
        
        if 'synthesis_result' in data:
            synthesis = data['synthesis_result']
            if isinstance(synthesis, dict):
                lines.append("Synthesis Results:")
                lines.extend(self._format_synthesis_as_text(synthesis))
        
        if 'analysis_result' in data:
            analysis = data['analysis_result']
            if isinstance(analysis, dict):
                lines.append("Analysis Results:")
                lines.extend(self._format_analysis_as_text(analysis))
        
        if 'processed_data' in data:
            processed = data['processed_data']
            if isinstance(processed, dict):
                lines.append("Processed Data:")
                lines.extend(self._format_processed_as_text(processed))
        
        return lines
    
    def _format_synthesis_as_text(self, synthesis: Dict[str, Any]) -> List[str]:
        """Format synthesis results as text"""
        lines = []
        
        if 'merged_data' in synthesis:
            merged = synthesis['merged_data']
            lines.append(f"  - Merged {merged.get('source_count', 0)} data sources")
            
        if 'summary' in synthesis:
            summary = synthesis['summary']
            lines.append(f"  - Generated summary with {len(summary.get('key_findings', []))} key findings")
            
        if 'consensus' in synthesis:
            consensus = synthesis['consensus']
            lines.append(f"  - Built consensus with {consensus.get('agreement_level', 0):.1%} agreement")
        
        return lines
    
    def _format_analysis_as_text(self, analysis: Dict[str, Any]) -> List[str]:
        """Format analysis results as text"""
        lines = []
        
        if 'patterns_found' in analysis:
            patterns = analysis['patterns_found']
            lines.append(f"  - Found {len(patterns)} patterns")
            
        if 'trends' in analysis:
            trends = analysis['trends']
            lines.append(f"  - Identified {len(trends)} trends")
            
        if 'anomalies' in analysis:
            anomalies = analysis['anomalies']
            lines.append(f"  - Detected {len(anomalies)} anomalies")
            
        if 'confidence' in analysis:
            confidence = analysis['confidence']
            lines.append(f"  - Analysis confidence: {confidence:.1%}")
        
        return lines
    
    def _format_processed_as_text(self, processed: Dict[str, Any]) -> List[str]:
        """Format processed data as text"""
        lines = []
        
        data_type = processed.get('type', 'unknown')
        lines.append(f"  - Data type: {data_type}")
        
        if data_type == 'csv':
            lines.append(f"  - Rows: {processed.get('row_count', 0)}")
            lines.append(f"  - Columns: {len(processed.get('columns', []))}")
        elif data_type == 'text':
            stats = processed.get('statistics', {})
            lines.append(f"  - Words: {stats.get('word_count', 0)}")
            lines.append(f"  - Characters: {stats.get('character_count', 0)}")
        
        return lines
    
    def _format_content_as_markdown(self, data: Dict[str, Any]) -> List[str]:
        """Format content as Markdown"""
        lines = []
        
        if 'synthesis_result' in data:
            lines.append("### 🔗 Synthesis Results")
            lines.append("")
            synthesis = data['synthesis_result']
            if isinstance(synthesis, dict):
                lines.extend(self._format_synthesis_as_markdown(synthesis))
                lines.append("")
        
        if 'analysis_result' in data:
            lines.append("### 📊 Analysis Results")
            lines.append("")
            analysis = data['analysis_result']
            if isinstance(analysis, dict):
                lines.extend(self._format_analysis_as_markdown(analysis))
                lines.append("")
        
        if 'processed_data' in data:
            lines.append("### 🗂️ Processed Data")
            lines.append("")
            processed = data['processed_data']
            if isinstance(processed, dict):
                lines.extend(self._format_processed_as_markdown(processed))
                lines.append("")
        
        return lines
    
    def _format_synthesis_as_markdown(self, synthesis: Dict[str, Any]) -> List[str]:
        """Format synthesis results as Markdown"""
        lines = []
        
        if 'merged_data' in synthesis:
            merged = synthesis['merged_data']
            lines.append(f"**Data Integration:** Merged {merged.get('source_count', 0)} sources")
            
        if 'summary' in synthesis:
            summary = synthesis['summary']
            findings_count = len(summary.get('key_findings', []))
            lines.append(f"**Summary Generation:** {findings_count} key findings identified")
            
        if 'consensus' in synthesis:
            consensus = synthesis['consensus']
            agreement = consensus.get('agreement_level', 0)
            lines.append(f"**Consensus Building:** {agreement:.1%} agreement level achieved")
        
        return lines
    
    def _format_analysis_as_markdown(self, analysis: Dict[str, Any]) -> List[str]:
        """Format analysis results as Markdown"""
        lines = []
        
        if 'patterns_found' in analysis:
            patterns = analysis['patterns_found']
            lines.append(f"**Pattern Detection:** {len(patterns)} patterns identified")
            
        if 'trends' in analysis:
            trends = analysis['trends']
            lines.append(f"**Trend Analysis:** {len(trends)} trends discovered")
            
        if 'anomalies' in analysis:
            anomalies = analysis['anomalies']
            lines.append(f"**Anomaly Detection:** {len(anomalies)} anomalies found")
            
        if 'confidence' in analysis:
            confidence = analysis['confidence']
            confidence_bar = "█" * int(confidence * 10) + "░" * (10 - int(confidence * 10))
            lines.append(f"**Confidence:** {confidence_bar} {confidence:.1%}")
        
        return lines
    
    def _format_processed_as_markdown(self, processed: Dict[str, Any]) -> List[str]:
        """Format processed data as Markdown"""
        lines = []
        
        data_type = processed.get('type', 'unknown')
        lines.append(f"**Data Type:** {data_type.upper()}")
        
        if data_type == 'csv':
            rows = processed.get('row_count', 0)
            cols = len(processed.get('columns', []))
            lines.append(f"**Dimensions:** {rows} rows × {cols} columns")
            
            quality = processed.get('data_quality', {})
            if quality:
                success_rate = quality.get('success_rate', 1)
                lines.append(f"**Quality:** {success_rate:.1%} success rate")
                
        elif data_type == 'text':
            stats = processed.get('statistics', {})
            word_count = stats.get('word_count', 0)
            char_count = stats.get('character_count', 0)
            lines.append(f"**Content:** {word_count:,} words, {char_count:,} characters")
            
            content_analysis = processed.get('content_analysis', {})
            if content_analysis:
                sentiment = content_analysis.get('sentiment_indication', 'neutral')
                lines.append(f"**Sentiment:** {sentiment}")
        
        return lines
    
    def _generate_executive_summary(self, data: Dict[str, Any]) -> str:
        """Generate executive summary"""
        try:
            elements = self._count_data_elements(data)
            data_type = self._identify_data_type(data)
            
            summary = f"This report presents the analysis of {elements} data elements "
            summary += f"processed through a multi-agent system. The primary data type is {data_type}. "
            
            # Add specific insights based on data type
            if 'synthesis_result' in data:
                synthesis = data['synthesis_result']
                if isinstance(synthesis, dict) and 'merged_data' in synthesis:
                    source_count = synthesis['merged_data'].get('source_count', 0)
                    summary += f"Data from {source_count} sources has been successfully integrated and analyzed. "
            
            success_rate = self._calculate_success_rate(data)
            summary += f"The analysis achieved a {success_rate:.1%} success rate with comprehensive coverage of all input data."
            
            return summary
            
        except Exception as e:
            return f"Executive summary generation failed: {e}"
    
    def _generate_data_overview(self, data: Dict[str, Any]) -> str:
        """Generate data overview section"""
        try:
            overview_parts = []
            
            # Basic metrics
            stats = self._generate_data_statistics(data)
            overview_parts.append(f"Total data elements processed: {stats['element_count']}")
            overview_parts.append(f"Data size: {stats['data_size_bytes']:,} bytes")
            overview_parts.append(f"Maximum nesting level: {stats['nested_levels']}")
            
            # Timing information
            if 'total_execution_time' in stats:
                overview_parts.append(f"Total execution time: {stats['total_execution_time']:.2f} seconds")
            
            # Data composition
            data_types = self._analyze_data_composition(data)
            if data_types:
                overview_parts.append("\nData composition:")
                for dtype, count in data_types.items():
                    overview_parts.append(f"  - {dtype}: {count} instances")
            
            return "\n".join(overview_parts)
            
        except Exception as e:
            return f"Data overview generation failed: {e}"
    
    def _extract_key_findings_for_report(self, data: Dict[str, Any]) -> List[str]:
        """Extract key findings for report"""
        findings = []
        
        try:
            # Extract from synthesis results
            if 'synthesis_result' in data:
                synthesis = data['synthesis_result']
                if isinstance(synthesis, dict):
                    if 'summary' in synthesis:
                        summary = synthesis['summary']
                        if isinstance(summary, dict):
                            key_findings = summary.get('key_findings', [])
                            findings.extend(key_findings[:3])  # Top 3 findings
            
            # Extract from analysis results
            if 'analysis_result' in data:
                analysis = data['analysis_result']
                if isinstance(analysis, dict):
                    if 'patterns_found' in analysis:
                        patterns = analysis['patterns_found']
                        if patterns:
                            findings.append(f"Identified {len(patterns)} distinct data patterns")
                    
                    if 'trends' in analysis:
                        trends = analysis['trends']
                        if trends:
                            directions = [t.get('direction', 'unknown') for t in trends if isinstance(t, dict)]
                            if directions:
                                most_common = Counter(directions).most_common(1)[0][0]
                                findings.append(f"Primary trend direction: {most_common}")
            
            # Add quality findings
            success_rate = self._calculate_success_rate(data)
            if success_rate < 0.8:
                findings.append(f"Data quality concern: {success_rate:.1%} success rate indicates potential issues")
            elif success_rate > 0.95:
                findings.append(f"High data quality: {success_rate:.1%} success rate achieved")
            
            return findings[:5]  # Limit to top 5 findings
            
        except Exception as e:
            return [f"Finding extraction failed: {e}"]
    
    def _generate_detailed_analysis(self, data: Dict[str, Any]) -> List[str]:
        """Generate detailed analysis section"""
        lines = []
        
        try:
            # Analyze each major component
            if 'synthesis_result' in data:
                lines.append("## Synthesis Analysis")
                lines.extend(self._analyze_synthesis_details(data['synthesis_result']))
                lines.append("")
            
            if 'analysis_result' in data:
                lines.append("## Analysis Details")
                lines.extend(self._analyze_analysis_details(data['analysis_result']))
                lines.append("")
            
            if 'processed_data' in data:
                lines.append("## Data Processing Details")
                lines.extend(self._analyze_processing_details(data['processed_data']))
                lines.append("")
            
            # Performance analysis
            lines.append("## Performance Analysis")
            lines.extend(self._analyze_performance_details(data))
            
        except Exception as e:
            lines.append(f"Detailed analysis generation failed: {e}")
        
        return lines
    
    def _generate_recommendations(self, data: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on analysis"""
        recommendations = []
        
        try:
            # Quality-based recommendations
            success_rate = self._calculate_success_rate(data)
            if success_rate < 0.9:
                recommendations.append("Consider implementing additional data validation steps to improve success rate")
            
            # Performance-based recommendations
            stats = self._generate_data_statistics(data)
            if stats.get('total_execution_time', 0) > 5.0:
                recommendations.append("Optimize processing pipeline to reduce execution time")
            
            # Data-specific recommendations
            if self._has_large_dataset(data):
                recommendations.append("Consider implementing batch processing for large datasets")
            
            if self._has_quality_issues(data):
                recommendations.append("Implement data quality checks and cleaning procedures")
            
            # Default recommendations
            if not recommendations:
                recommendations.extend([
                    "Continue monitoring system performance and data quality",
                    "Consider expanding analysis capabilities based on current success",
                    "Implement regular system health checks and maintenance"
                ])
                
        except Exception as e:
            recommendations.append(f"Recommendation generation failed: {e}")
        
        return recommendations
    
    def _has_synthesis_data(self, data: Dict[str, Any]) -> bool:
        """Check if data contains synthesis information"""
        return 'synthesis_result' in data or 'analysis_result' in data
    
    def _calculate_success_rate(self, data: Dict[str, Any]) -> float:
        """Calculate overall success rate"""
        try:
            # Look for error indicators in nested data
            error_count = 0
            total_count = 0
            
            def count_errors(obj, depth=0):
                nonlocal error_count, total_count
                if depth > 10:  # Prevent infinite recursion
                    return
                
                if isinstance(obj, dict):
                    total_count += 1
                    if obj.get('error') or obj.get('status') == 'failed':
                        error_count += 1
                    
                    for value in obj.values():
                        count_errors(value, depth + 1)
                elif isinstance(obj, list):
                    for item in obj:
                        count_errors(item, depth + 1)
            
            count_errors(data)
            
            if total_count == 0:
                return 1.0
            
            return (total_count - error_count) / total_count
            
        except Exception:
            return 0.5  # Default moderate success rate
    
    def _analyze_data_composition(self, data: Dict[str, Any]) -> Dict[str, int]:
        """Analyze composition of data types"""
        composition = defaultdict(int)
        
        def analyze_obj(obj, depth=0):
            if depth > 10:  # Prevent infinite recursion
                return
            
            if isinstance(obj, dict):
                # Check for specific data type indicators
                if 'type' in obj:
                    composition[obj['type']] += 1
                elif 'synthesis_result' in obj:
                    composition['synthesis'] += 1
                elif 'analysis_result' in obj:
                    composition['analysis'] += 1
                elif 'processed_data' in obj:
                    composition['processed'] += 1
                else:
                    composition['object'] += 1
                
                for value in obj.values():
                    analyze_obj(value, depth + 1)
            elif isinstance(obj, list):
                composition['array'] += 1
                for item in obj:
                    analyze_obj(item, depth + 1)
        
        analyze_obj(data)
        return dict(composition)
    
    def _assess_data_quality(self, data: Dict[str, Any]) -> Dict[str, float]:
        """Assess various aspects of data quality"""
        quality_metrics = {}
        
        # Completeness
        quality_metrics['completeness'] = self._assess_completeness(data)
        
        # Accuracy (based on success rate)
        quality_metrics['accuracy'] = self._calculate_success_rate(data)
        
        # Timeliness (based on execution time)
        stats = self._generate_data_statistics(data)
        exec_time = stats.get('total_execution_time', 0)
        quality_metrics['timeliness'] = max(0, 1.0 - (exec_time / 10.0))  # Normalize by 10 seconds
        
        # Consistency
        quality_metrics['consistency'] = self._assess_consistency(data)
        
        return quality_metrics
    
    def _assess_completeness(self, data: Dict[str, Any]) -> float:
        """Assess data completeness"""
        try:
            # Count non-null, non-empty values
            total_fields = 0
            complete_fields = 0
            
            def count_fields(obj, depth=0):
                nonlocal total_fields, complete_fields
                if depth > 10:
                    return
                
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        total_fields += 1
                        if value is not None and value != '' and value != []:
                            complete_fields += 1
                        
                        if isinstance(value, (dict, list)):
                            count_fields(value, depth + 1)
                elif isinstance(obj, list):
                    for item in obj:
                        count_fields(item, depth + 1)
            
            count_fields(data)
            
            return complete_fields / total_fields if total_fields > 0 else 0.0
            
        except Exception:
            return 0.5
    
    def _assess_consistency(self, data: Dict[str, Any]) -> float:
        """Assess data consistency"""
        try:
            # Look for consistent patterns in data structure
            structure_consistency = 0.0
            
            # Check if similar objects have similar structures
            if isinstance(data, dict):
                # Simple heuristic: if no errors, assume good consistency
                if not any('error' in str(value).lower() for value in data.values()):
                    structure_consistency = 0.8
                else:
                    structure_consistency = 0.4
            
            return structure_consistency
            
        except Exception:
            return 0.5
    
    def _analyze_synthesis_details(self, synthesis: Dict[str, Any]) -> List[str]:
        """Analyze synthesis details"""
        lines = []
        
        if isinstance(synthesis, dict):
            if 'merged_data' in synthesis:
                merged = synthesis['merged_data']
                lines.append(f"Successfully merged {merged.get('source_count', 0)} data sources")
                
            if 'summary' in synthesis:
                summary = synthesis['summary']
                if isinstance(summary, dict):
                    lines.append(f"Generated summary with {len(summary.get('key_findings', []))} findings")
                    
                    quality = summary.get('quality_assessment', {})
                    if quality:
                        overall_score = quality.get('overall_score', 0)
                        lines.append(f"Overall quality score: {overall_score:.2f}")
        
        return lines
    
    def _analyze_analysis_details(self, analysis: Dict[str, Any]) -> List[str]:
        """Analyze analysis details"""
        lines = []
        
        if isinstance(analysis, dict):
            method = analysis.get('method', 'unknown')
            lines.append(f"Analysis method: {method}")
            
            confidence = analysis.get('confidence', 0)
            lines.append(f"Confidence level: {confidence:.1%}")
            
            if 'patterns_found' in analysis:
                patterns = analysis['patterns_found']
                lines.append(f"Pattern analysis: {len(patterns)} patterns detected")
                
            if 'trends' in analysis:
                trends = analysis['trends']
                lines.append(f"Trend analysis: {len(trends)} trends identified")
        
        return lines
    
    def _analyze_processing_details(self, processed: Dict[str, Any]) -> List[str]:
        """Analyze processing details"""
        lines = []
        
        if isinstance(processed, dict):
            data_type = processed.get('type', 'unknown')
            lines.append(f"Data type: {data_type}")
            
            if data_type == 'csv':
                lines.append(f"Processed {processed.get('row_count', 0)} rows")
                lines.append(f"Columns: {len(processed.get('columns', []))}")
                
                quality = processed.get('data_quality', {})
                if quality:
                    success_rate = quality.get('success_rate', 0)
                    lines.append(f"Processing success rate: {success_rate:.1%}")
                    
            elif data_type == 'text':
                stats = processed.get('statistics', {})
                lines.append(f"Word count: {stats.get('word_count', 0):,}")
                lines.append(f"Character count: {stats.get('character_count', 0):,}")
        
        return lines
    
    def _analyze_performance_details(self, data: Dict[str, Any]) -> List[str]:
        """Analyze performance details"""
        lines = []
        
        stats = self._generate_data_statistics(data)
        
        lines.append(f"Total execution time: {stats.get('total_execution_time', 0):.2f} seconds")
        lines.append(f"Data processing efficiency: {stats['element_count'] / max(1, stats.get('total_execution_time', 1)):.0f} elements/second")
        lines.append(f"Memory efficiency: {stats['data_size_bytes'] / 1024:.1f} KB processed")
        
        success_rate = self._calculate_success_rate(data)
        lines.append(f"Overall success rate: {success_rate:.1%}")
        
        return lines
    
    def _has_large_dataset(self, data: Dict[str, Any]) -> bool:
        """Check if dataset is considered large"""
        stats = self._generate_data_statistics(data)
        return stats['element_count'] > 1000 or stats['data_size_bytes'] > 1024 * 1024  # 1MB
    
    def _has_quality_issues(self, data: Dict[str, Any]) -> bool:
        """Check if there are data quality issues"""
        success_rate = self._calculate_success_rate(data)
        return success_rate < 0.85

# ============================================================================
# AGENT COORDINATION SYSTEM
# ============================================================================

class AgentCoordinator:
    """Advanced agent coordination system with comprehensive management"""
    
    def __init__(self, config: EnhancedConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.initialized = False
        
        # Agent management
        self.active_agents: Dict[int, List[BaseAgent]] = defaultdict(list)
        self.task_queue = asyncio.Queue()
        self.coordination_state = {}
        
        # Performance tracking
        self.metrics = defaultdict(Counter)
        self.execution_history = deque(maxlen=1000)
        
        # Caching and optimization
        self.result_cache = TTLCache(maxsize=1000, ttl=3600)
        self.optimization_strategies = {}
    
    async def initialize(self) -> None:
        """Initialize coordination system"""
        try:
            # Initialize agent layers
            await self._initialize_agent_layers()
            
            # Initialize coordination strategies
            await self._initialize_coordination_strategies()
            
            # Initialize performance monitoring
            await self._initialize_performance_monitoring()
            
            self.initialized = True
            self.logger.info("Agent coordination system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Coordination system initialization failed: {e}")
            await self.cleanup()
            raise InitializationError(f"Coordination init failed: {e}")
    
    async def _initialize_agent_layers(self) -> None:
        """Initialize agents for each layer"""
        try:
            layer_configs = {
                1: {'agent_class': Layer1Agent, 'count': 1},
                2: {'agent_class': Layer2Agent, 'count': 1},
                3: {'agent_class': Layer3Agent, 'count': 1},
                4: {'agent_class': Layer4Agent, 'count': 1}
            }
            
            for layer_id, layer_config in layer_configs.items():
                agent_class = layer_config['agent_class']
                agent_count = layer_config['count']
                
                for i in range(agent_count):
                    agent_name = f"Layer{layer_id}_Agent_{i}"
                    model_info = {'layer_id': layer_id, 'agent_index': i}
                    
                    agent = agent_class(agent_name, model_info, self.config)
                    await agent.initialize()
                    
                    self.active_agents[layer_id].append(agent)
                    self.logger.info(f"Initialized {agent_name}")
            
            total_agents = sum(len(agents) for agents in self.active_agents.values())
            self.logger.info(f"Initialized {total_agents} agents across {len(self.active_agents)} layers")
            
        except Exception as e:
            self.logger.error(f"Agent layer initialization failed: {e}")
            raise
    
    async def _initialize_coordination_strategies(self) -> None:
        """Initialize coordination strategies"""
        try:
            self.coordination_strategies = {
                'sequential': self._execute_sequential_coordination,
                'parallel': self._execute_parallel_coordination,
                'adaptive': self._execute_adaptive_coordination
            }
            
            # Default optimization strategies
            self.optimization_strategies = {
                'cache_results': True,
                'parallel_layer_execution': False,  # Sequential by default
                'adaptive_timeout': True,
                'load_balancing': True
            }
            
            self.logger.info("Coordination strategies initialized")
            
        except Exception as e:
            self.logger.error(f"Coordination strategies initialization failed: {e}")
            raise
    
    async def _initialize_performance_monitoring(self) -> None:
        """Initialize performance monitoring"""
        try:
            self.performance_metrics = {
                'total_tasks': 0,
                'successful_tasks': 0,
                'failed_tasks': 0,
                'average_execution_time': 0.0,
                'cache_hit_rate': 0.0
            }
            
            self.logger.info("Performance monitoring initialized")
            
        except Exception as e:
            self.logger.error(f"Performance monitoring initialization failed: {e}")
            raise
    
    async def coordinate_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Coordinate task execution through agent layers"""
        if not self.initialized:
            raise RuntimeError("Coordination system not initialized")
        
        task_id = task.get('id', f"task_{datetime.now().timestamp()}")
        start_time = time.time()
        
        try:
            self.logger.info(f"Starting coordination for task {task_id}")
            
            # Check cache first
            cache_key = self._generate_cache_key(task)
            if cache_key in self.result_cache:
                cached_result = self.result_cache[cache_key]
                self.metrics['cache_hits'] += 1
                self.logger.info(f"Returning cached result for task {task_id}")
                return cached_result
            
            # Initialize task state
            self.coordination_state[task_id] = {
                'status': 'processing',
                'start_time': start_time,
                'layer_results': {},
                'current_layer': 1
            }
            
            # Execute coordination strategy
            strategy = self._select_coordination_strategy(task)
            result = await strategy(task_id, task)
            
            # Cache successful results
            if result.get('status') == 'success':
                self.result_cache[cache_key] = result
            
            # Update performance metrics
            execution_time = time.time() - start_time
            await self._update_performance_metrics(task_id, result, execution_time)
            
            # Cleanup task state
            self.coordination_state[task_id]['status'] = 'completed'
            self.coordination_state[task_id]['end_time'] = time.time()
            
            self.logger.info(f"Task {task_id} completed in {execution_time:.2f}s")
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Task coordination failed for {task_id}: {e}")
            
            # Update failure metrics
            self.metrics['failed_tasks'] += 1
            
            # Cleanup and return error
            if task_id in self.coordination_state:
                self.coordination_state[task_id]['status'] = 'failed'
                self.coordination_state[task_id]['error'] = str(e)
            
            return {
                'task_id': task_id,
                'status': 'failed',
                'error': str(e),
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat()
            }
    
    def _generate_cache_key(self, task: Dict[str, Any]) -> str:
        """Generate cache key for task"""
        # Create key based on task content, excluding timestamps
        task_copy = task.

