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
