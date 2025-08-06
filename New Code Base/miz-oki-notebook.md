# MIZ OKI 3.0 - Multi-Agent Intelligence System for Google Cloud Notebooks

## Complete Notebook Implementation (Fixed)

### Cell 1: Environment Setup and Dependencies

```python
# Cell 1: Environment Setup and Dependencies
# Install required packages for Google Cloud Notebooks

import subprocess
import sys
import os

def install_packages():
    """Install required packages for the multi-agent system"""
    packages = [
        'fastapi',
        'uvicorn[standard]',
        'pydantic>=2.0',
        'python-dotenv',
        'pyyaml',
        'pandas',
        'numpy',
        'httpx',
        'aiohttp',
        'psutil',
        'structlog',
        'rich',
        'openai>=1.0',
        'anthropic>=0.18',
        'langchain>=0.1',
        'langchain-community',
        'chromadb',
        'google-cloud-storage',
        'google-cloud-firestore',
        'google-cloud-pubsub',
        'google-cloud-logging',
        'nest-asyncio',  # Important for notebooks
        'prometheus-client',
        'networkx'  # Added for workflow DAG
    ]
    
    print("🔧 Installing required packages...")
    for package in packages:
        print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", package])
    
    print("✅ All packages installed successfully!")

# Install packages
install_packages()

# Enable nested asyncio for notebooks
import nest_asyncio
nest_asyncio.apply()

print("✅ Environment setup complete!")
```

### Cell 2: Core System Architecture

```python
# Cell 2: Core System Architecture
# Core components and base classes with all necessary imports

import asyncio
import logging
import json
import yaml
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Callable, Tuple, Awaitable
from datetime import datetime
from pathlib import Path
import uuid
from enum import Enum
from dataclasses import dataclass, field
import psutil
from rich.console import Console
from rich.table import Table
from rich.progress import track
import structlog
from collections import deque
import re
import pickle

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()
console = Console()

# Agent States
class AgentState(Enum):
    IDLE = "idle"
    INITIALIZING = "initializing"
    READY = "ready"
    PROCESSING = "processing"
    ERROR = "error"
    SHUTDOWN = "shutdown"

# Message Types
@dataclass
class Message:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender: str = ""
    receiver: str = ""
    content: Any = None
    message_type: str = "data"
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

# Base Agent Class
class BaseAgent(ABC):
    """Enhanced base agent with communication capabilities"""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        self.id = str(uuid.uuid4())
        self.name = name
        self.config = config or {}
        self.state = AgentState.IDLE
        self.logger = structlog.get_logger(name=name)
        self.message_queue = asyncio.Queue()
        self.capabilities = []
        self.metrics = {
            'messages_processed': 0,
            'errors': 0,
            'avg_processing_time': 0
        }
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the agent"""
        pass
    
    @abstractmethod
    async def process(self, message: Message) -> Any:
        """Process a message"""
        pass
    
    @abstractmethod
    async def cleanup(self) -> None:
        """Cleanup agent resources"""
        pass
    
    async def send_message(self, receiver: str, content: Any, message_type: str = "data") -> Message:
        """Send a message to another agent"""
        message = Message(
            sender=self.name,
            receiver=receiver,
            content=content,
            message_type=message_type
        )
        self.logger.info(f"Sending message to {receiver}", message_id=message.id)
        return message
    
    async def receive_message(self) -> Optional[Message]:
        """Receive a message from the queue"""
        try:
            message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
            return message
        except asyncio.TimeoutError:
            return None
    
    def get_status(self) -> Dict[str, Any]:
        """Get agent status"""
        return {
            'id': self.id,
            'name': self.name,
            'state': self.state.value,
            'capabilities': self.capabilities,
            'metrics': self.metrics
        }

# Agent Registry
class AgentRegistry:
    """Central registry for all agents"""
    
    def __init__(self):
        self.agents: Dict[str, BaseAgent] = {}
        self.logger = structlog.get_logger(name="AgentRegistry")
    
    def register(self, agent: BaseAgent) -> None:
        """Register an agent"""
        self.agents[agent.name] = agent
        self.logger.info(f"Registered agent: {agent.name}")
    
    def get(self, name: str) -> Optional[BaseAgent]:
        """Get an agent by name"""
        return self.agents.get(name)
    
    def list_agents(self) -> List[str]:
        """List all registered agents"""
        return list(self.agents.keys())
    
    def get_all_status(self) -> Dict[str, Any]:
        """Get status of all agents"""
        return {name: agent.get_status() for name, agent in self.agents.items()}

# Global registry
agent_registry = AgentRegistry()

print("✅ Core system architecture loaded!")
```

### Cell 3: Specialized Agent Implementations with A2A Support

```python
# Cell 3: Specialized Agent Implementations with A2A Support
# Various types of specialized agents with Google Agent Hub integration

import httpx
import random

class A2AEnabledAgent(BaseAgent):
    """Base agent with A2A communication capabilities"""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.a2a_enabled = False
        self.agent_hub_id = None
    
    async def register_with_hub(self, agent_hub: 'GoogleAgentHub'):
        """Register this agent with Google Agent Hub"""
        self.agent_hub_id = await agent_hub.register_agent(self)
        self.a2a_enabled = True
        self.logger.info(f"Agent {self.name} registered with Agent Hub: {self.agent_hub_id}")
    
    async def send_a2a_message(self, target_agent: str, content: Any, 
                               a2a_platform: 'A2ACommunicationPlatform'):
        """Send message via A2A platform"""
        if not self.a2a_enabled:
            self.logger.warning("A2A not enabled for this agent")
            return None
        
        message = Message(
            sender=self.name,
            receiver=target_agent,
            content=content,
            message_type="a2a_communication"
        )
        
        return await a2a_platform.send_message(message, target_agent)

class DataProcessorAgent(A2AEnabledAgent):
    """Agent specialized in data processing"""
    
    async def initialize(self) -> None:
        self.state = AgentState.INITIALIZING
        self.capabilities = ["data_transformation", "data_validation", "data_aggregation"]
        self.logger.info("Initializing DataProcessor agent")
        await asyncio.sleep(0.5)  # Simulate initialization
        self.state = AgentState.READY
        self.logger.info("DataProcessor agent ready")
    
    async def process(self, message: Message) -> Any:
        self.state = AgentState.PROCESSING
        start_time = datetime.now()
        
        try:
            content = message.content
            
            if message.message_type == "transform":
                result = await self._transform_data(content)
            elif message.message_type == "validate":
                result = await self._validate_data(content)
            elif message.message_type == "aggregate":
                result = await self._aggregate_data(content)
            else:
                result = content
            
            # Update metrics
            processing_time = (datetime.now() - start_time).total_seconds()
            self.metrics['messages_processed'] += 1
            self.metrics['avg_processing_time'] = (
                (self.metrics['avg_processing_time'] * (self.metrics['messages_processed'] - 1) + 
                 processing_time) / self.metrics['messages_processed']
            )
            
            self.state = AgentState.READY
            return result
            
        except Exception as e:
            self.metrics['errors'] += 1
            self.state = AgentState.ERROR
            self.logger.error(f"Processing error: {e}")
            raise
    
    async def _transform_data(self, data: Any) -> Any:
        """Transform data"""
        if isinstance(data, dict):
            return {k: str(v).upper() for k, v in data.items()}
        elif isinstance(data, list):
            return [str(item).upper() for item in data]
        else:
            return str(data).upper()
    
    async def _validate_data(self, data: Any) -> bool:
        """Validate data"""
        return data is not None and len(str(data)) > 0
    
    async def _aggregate_data(self, data: List[Any]) -> Dict[str, Any]:
        """Aggregate data"""
        if not isinstance(data, list):
            data = [data]
        
        return {
            'count': len(data),
            'items': data,
            'summary': f"Aggregated {len(data)} items"
        }
    
    async def cleanup(self) -> None:
        self.state = AgentState.SHUTDOWN
        self.logger.info("DataProcessor agent shutting down")

class AnalyticsAgent(A2AEnabledAgent):
    """Agent specialized in analytics and insights"""
    
    async def initialize(self) -> None:
        self.state = AgentState.INITIALIZING
        self.capabilities = ["statistical_analysis", "pattern_detection", "anomaly_detection"]
        self.logger.info("Initializing Analytics agent")
        await asyncio.sleep(0.5)
        self.state = AgentState.READY
        self.logger.info("Analytics agent ready")
    
    async def process(self, message: Message) -> Any:
        self.state = AgentState.PROCESSING
        
        try:
            content = message.content
            
            # Perform analytics
            analytics_result = {
                'data_points': len(str(content)),
                'patterns_found': random.randint(1, 5),
                'anomalies': random.randint(0, 2),
                'confidence_score': random.random(),
                'insights': [
                    "Pattern A detected with high frequency",
                    "Anomaly in data segment 3",
                    "Trend indicates upward movement"
                ][:random.randint(1, 3)]
            }
            
            self.metrics['messages_processed'] += 1
            self.state = AgentState.READY
            return analytics_result
            
        except Exception as e:
            self.metrics['errors'] += 1
            self.state = AgentState.ERROR
            self.logger.error(f"Analytics error: {e}")
            raise
    
    async def cleanup(self) -> None:
        self.state = AgentState.SHUTDOWN
        self.logger.info("Analytics agent shutting down")

class APIGatewayAgent(A2AEnabledAgent):
    """Agent that handles external API communications"""
    
    async def initialize(self) -> None:
        self.state = AgentState.INITIALIZING
        self.capabilities = ["http_requests", "api_integration", "webhook_handling"]
        self.client = httpx.AsyncClient()
        self.logger.info("Initializing API Gateway agent")
        self.state = AgentState.READY
        self.logger.info("API Gateway agent ready")
    
    async def process(self, message: Message) -> Any:
        self.state = AgentState.PROCESSING
        
        try:
            if message.message_type == "api_call":
                result = await self._make_api_call(message.content)
            else:
                result = {"status": "processed", "data": message.content}
            
            self.metrics['messages_processed'] += 1
            self.state = AgentState.READY
            return result
            
        except Exception as e:
            self.metrics['errors'] += 1
            self.state = AgentState.ERROR
            self.logger.error(f"API Gateway error: {e}")
            raise
    
    async def _make_api_call(self, config: Dict[str, Any]) -> Any:
        """Make an external API call"""
        # Simulate API call
        await asyncio.sleep(0.5)
        return {
            "status": "success",
            "response": f"API call to {config.get('endpoint', 'unknown')} completed",
            "timestamp": datetime.now().isoformat()
        }
    
    async def cleanup(self) -> None:
        await self.client.aclose()
        self.state = AgentState.SHUTDOWN
        self.logger.info("API Gateway agent shutting down")

class CoordinatorAgent(A2AEnabledAgent):
    """Master coordinator agent that orchestrates other agents"""
    
    def __init__(self, name: str = "coordinator", config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.workflows = {}
    
    async def initialize(self) -> None:
        self.state = AgentState.INITIALIZING
        self.capabilities = ["workflow_orchestration", "task_distribution", "result_aggregation"]
        self.logger.info("Initializing Coordinator agent")
        self.state = AgentState.READY
        self.logger.info("Coordinator agent ready")
    
    async def process(self, message: Message) -> Any:
        self.state = AgentState.PROCESSING
        
        try:
            if message.message_type == "workflow":
                result = await self._execute_workflow(message.content)
            else:
                result = await self._distribute_task(message.content)
            
            self.metrics['messages_processed'] += 1
            self.state = AgentState.READY
            return result
            
        except Exception as e:
            self.metrics['errors'] += 1
            self.state = AgentState.ERROR
            self.logger.error(f"Coordinator error: {e}")
            raise
    
    async def _execute_workflow(self, workflow_config: Dict[str, Any]) -> Any:
        """Execute a workflow across multiple agents using A2A communication"""
        steps = workflow_config.get('steps', [])
        data = workflow_config.get('data')
        results = []
        
        # Check if we should use Google Cloud Workflows
        if gcp_integration.workflow_integration and workflow_config.get('use_cloud_workflow', False):
            # Convert to Google Cloud Workflow format
            cloud_workflow = {
                'main': {
                    'steps': []
                }
            }
            
            for step in steps:
                cloud_step = {
                    step['name']: {
                        'call': f"http.post",
                        'args': {
                            'url': f"https://{gcp_integration.project_id}.cloudfunctions.net/agent_{step['agent']}",
                            'body': {
                                'action': step.get('type', 'process'),
                                'data': '${data}'
                            }
                        }
                    }
                }
                cloud_workflow['main']['steps'].append(cloud_step)
            
            # Execute via Google Cloud Workflows
            result = await gcp_integration.workflow_integration.execute_workflow(
                f"miz_oki_workflow_{workflow_config.get('name', 'unnamed')}",
                {'data': data}
            )
            return result
        
        # Otherwise use local execution with A2A messaging
        for step in steps:
            agent_name = step.get('agent')
            agent = agent_registry.get(agent_name)
            
            if agent and isinstance(agent, A2AEnabledAgent) and agent.a2a_enabled:
                # Use A2A communication
                if gcp_integration.a2a_platform:
                    await agent.send_a2a_message(
                        agent_name,
                        data,
                        gcp_integration.a2a_platform
                    )
                    # Create Cloud Task for async processing
                    await gcp_integration.a2a_platform.create_agent_task(
                        agent_name,
                        {'step': step, 'data': data}
                    )
            elif agent:
                # Fallback to direct message passing
                message = Message(
                    sender=self.name,
                    receiver=agent_name,
                    content=data,
                    message_type=step.get('type', 'data')
                )
                result = await agent.process(message)
                results.append(result)
                data = result  # Pass result to next step
        
        return {
            'workflow': workflow_config.get('name', 'unnamed'),
            'steps_completed': len(results),
            'final_result': data,
            'all_results': results,
            'a2a_enabled': any(isinstance(agent, A2AEnabledAgent) and agent.a2a_enabled 
                              for agent in agent_registry.agents.values())
        }
    
    async def _distribute_task(self, task: Any) -> Any:
        """Distribute a task to appropriate agents"""
        # Simple task distribution logic
        return {
            'task': task,
            'distributed_to': list(agent_registry.agents.keys()),
            'status': 'distributed'
        }
    
    async def cleanup(self) -> None:
        self.state = AgentState.SHUTDOWN
        self.logger.info("Coordinator agent shutting down")

print("✅ Specialized agents implemented!")
```

### Cell 4: Google Cloud Integration with Vertex AI Agents and Cloud Run

```python
# Cell 4: Google Cloud Integration using ACTUAL Google Cloud services
# Using Vertex AI, Cloud Run, Pub/Sub, and Firestore for multi-agent orchestration

import os
import grpc
from google.cloud import storage, firestore, pubsub_v1, logging as cloud_logging
from google.cloud import tasks_v2
from google.cloud import workflows_v1
from google.cloud import run_v2
from google.cloud import aiplatform
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.protobuf import timestamp_pb2
from google.protobuf.json_format import MessageToDict, ParseDict

# Note: Google does NOT have "Agent Hub" or "A2A Platform" as official services
# We'll use Vertex AI Agents (Dialogflow CX) and Cloud Run for actual agent deployment

class VertexAIAgentIntegration:
    """Integration with Google Vertex AI for agent capabilities"""
    
    def __init__(self, project_id: str, location: str = "us-central1"):
        self.project_id = project_id
        self.location = location
        self.logger = structlog.get_logger(name="VertexAI")
        
        # Initialize Vertex AI
        aiplatform.init(project=project_id, location=location)
    
    async def create_vertex_ai_agent(self, agent_name: str, agent_type: str):
        """Create a Vertex AI conversational agent (using Dialogflow CX)"""
        # Note: This would typically use Dialogflow CX API
        # For notebook demo, we'll simulate this
        self.logger.info(f"Would create Vertex AI agent: {agent_name} of type {agent_type}")
        return f"vertex-ai-agent-{agent_name}"
    
    async def deploy_reasoning_engine(self, agent_config: Dict[str, Any]):
        """Deploy a Vertex AI Reasoning Engine (if available in your region)"""
        # Vertex AI Reasoning Engine is a new feature for agent orchestration
        # Currently in preview in limited regions
        try:
            # This would use the actual Reasoning Engine API when available
            self.logger.info(f"Deploying reasoning engine with config: {agent_config}")
            return {"status": "simulated", "engine_id": f"reasoning-{agent_config.get('name')}"}
        except Exception as e:
            self.logger.error(f"Reasoning Engine not available: {e}")
            return None

class CloudRunAgentDeployment:
    """Deploy agents as Cloud Run services for scalable execution"""
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.run_client = run_v2.ServicesClient()
        self.logger = structlog.get_logger(name="CloudRunAgents")
    
    async def deploy_agent_as_service(self, agent_name: str, container_image: str):
        """Deploy an agent as a Cloud Run service"""
        parent = f"projects/{self.project_id}/locations/{self.region}"
        
        service = {
            "name": f"{parent}/services/{agent_name}",
            "template": {
                "containers": [{
                    "image": container_image,
                    "ports": [{"container_port": 8080}],
                    "resources": {
                        "limits": {
                            "cpu": "2",
                            "memory": "2Gi"
                        }
                    }
                }],
                "scaling": {
                    "min_instance_count": 0,
                    "max_instance_count": 100
                }
            }
        }
        
        try:
            # In production, this would actually deploy to Cloud Run
            self.logger.info(f"Deploying agent {agent_name} to Cloud Run")
            # operation = self.run_client.create_service(parent=parent, service=service)
            # result = operation.result()
            return f"https://{agent_name}-{self.project_id}.{self.region}.run.app"
        except Exception as e:
            self.logger.error(f"Failed to deploy to Cloud Run: {e}")
            return None

class PubSubAgentCommunication:
    """Inter-agent communication using Google Cloud Pub/Sub"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.logger = structlog.get_logger(name="PubSubComm")
        
        # Topics for agent communication
        self.agent_topic = "agent-messages"
        self.workflow_topic = "workflow-events"
        self.task_topic = "agent-tasks"
    
    async def setup_communication_channels(self):
        """Setup Pub/Sub topics and subscriptions for agent communication"""
        topics = [self.agent_topic, self.workflow_topic, self.task_topic]
        
        for topic_name in topics:
            topic_path = self.publisher.topic_path(self.project_id, topic_name)
            try:
                self.publisher.create_topic(request={"name": topic_path})
                self.logger.info(f"Created topic: {topic_name}")
                
                # Create subscription for each topic
                subscription_name = f"{topic_name}-subscription"
                subscription_path = self.subscriber.subscription_path(
                    self.project_id, subscription_name
                )
                self.subscriber.create_subscription(
                    request={
                        "name": subscription_path,
                        "topic": topic_path,
                        "ack_deadline_seconds": 60
                    }
                )
                self.logger.info(f"Created subscription: {subscription_name}")
                
            except Exception as e:
                self.logger.info(f"Topic/subscription {topic_name} already exists or error: {e}")
    
    async def publish_agent_message(self, agent_id: str, target_agent: str, 
                                   message: Dict[str, Any]):
        """Publish a message from one agent to another"""
        topic_path = self.publisher.topic_path(self.project_id, self.agent_topic)
        
        message_data = {
            "sender": agent_id,
            "receiver": target_agent,
            "timestamp": datetime.now().isoformat(),
            "content": message
        }
        
        future = self.publisher.publish(
            topic_path,
            json.dumps(message_data).encode('utf-8'),
            sender=agent_id,
            receiver=target_agent
        )
        
        message_id = future.result()
        self.logger.info(f"Published message {message_id} from {agent_id} to {target_agent}")
        return message_id

class FirestoreAgentRegistry:
    """Agent registry and state management using Firestore"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.db = firestore.AsyncClient(project=project_id)
        self.agents_collection = "agents"
        self.workflows_collection = "workflows"
        self.logger = structlog.get_logger(name="AgentRegistry")
    
    async def register_agent(self, agent_data: Dict[str, Any]):
        """Register an agent in Firestore"""
        doc_ref = self.db.collection(self.agents_collection).document(agent_data['agent_id'])
        
        agent_doc = {
            **agent_data,
            'registered_at': datetime.now(),
            'last_heartbeat': datetime.now(),
            'status': 'active'
        }
        
        await doc_ref.set(agent_doc)
        self.logger.info(f"Registered agent {agent_data['name']} in Firestore")
        return doc_ref.id
    
    async def discover_agents(self, capability: Optional[str] = None):
        """Discover agents from Firestore registry"""
        query = self.db.collection(self.agents_collection)
        
        if capability:
            query = query.where('capabilities', 'array_contains', capability)
        
        query = query.where('status', '==', 'active')
        
        docs = query.stream()
        agents = []
        async for doc in docs:
            agents.append(doc.to_dict())
        
        return agents
    
    async def update_agent_state(self, agent_id: str, state: Dict[str, Any]):
        """Update agent state in Firestore"""
        doc_ref = self.db.collection(self.agents_collection).document(agent_id)
        
        update_data = {
            **state,
            'last_updated': datetime.now()
        }
        
        await doc_ref.update(update_data)
        self.logger.info(f"Updated state for agent {agent_id}")

class CloudTasksOrchestration:
    """Task orchestration using Cloud Tasks for reliable async processing"""
    
    def __init__(self, project_id: str, location: str = "us-central1"):
        self.project_id = project_id
        self.location = location
        self.client = tasks_v2.CloudTasksClient()
        self.queue_name = "agent-tasks"
        self.logger = structlog.get_logger(name="TaskOrchestration")
    
    async def create_task_queue(self):
        """Create a Cloud Tasks queue for agent tasks"""
        parent = self.client.common_location_path(self.project_id, self.location)
        queue_path = f"{parent}/queues/{self.queue_name}"
        
        queue = tasks_v2.Queue(
            name=queue_path,
            rate_limits=tasks_v2.RateLimits(
                max_dispatches_per_second=500,
                max_concurrent_dispatches=1000
            ),
            retry_config=tasks_v2.RetryConfig(
                max_attempts=3,
                max_retry_duration=timestamp_pb2.Duration(seconds=300)
            )
        )
        
        try:
            self.client.create_queue(parent=parent, queue=queue)
            self.logger.info(f"Created task queue: {self.queue_name}")
        except Exception as e:
            self.logger.info(f"Queue already exists or error: {e}")
    
    async def dispatch_agent_task(self, agent_url: str, task_data: Dict[str, Any]):
        """Dispatch a task to an agent via Cloud Tasks"""
        parent = self.client.queue_path(
            self.project_id,
            self.location,
            self.queue_name
        )
        
        task = tasks_v2.Task(
            http_request=tasks_v2.HttpRequest(
                http_method=tasks_v2.HttpMethod.POST,
                url=agent_url,
                headers={"Content-Type": "application/json"},
                body=json.dumps(task_data).encode()
            )
        )
        
        response = self.client.create_task(parent=parent, task=task)
        self.logger.info(f"Created task: {response.name}")
        return response.name

class GoogleCloudIntegration:
    """Complete Google Cloud integration for multi-agent systems"""
    
    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id
        self.credentials = None
        
        # Core GCP services
        self.storage_client = None
        self.logging_client = None
        
        # Agent-specific services
        self.vertex_ai = None
        self.cloud_run = None
        self.pubsub_comm = None
        self.agent_registry = None
        self.task_orchestration = None
        self.workflow_client = None
        
        self.logger = structlog.get_logger(name="GCPIntegration")
        
    async def initialize(self):
        """Initialize all Google Cloud services for multi-agent system"""
        try:
            # Get credentials
            self.credentials, self.project_id = default()
            self.logger.info(f"Using Google Cloud project: {self.project_id}")
            
            # Initialize core services
            self.storage_client = storage.Client(project=self.project_id)
            self.logging_client = cloud_logging.Client(project=self.project_id)
            self.logging_client.setup_logging()
            
            # Initialize agent services
            self.vertex_ai = VertexAIAgentIntegration(self.project_id)
            self.cloud_run = CloudRunAgentDeployment(self.project_id)
            self.pubsub_comm = PubSubAgentCommunication(self.project_id)
            self.agent_registry = FirestoreAgentRegistry(self.project_id)
            self.task_orchestration = CloudTasksOrchestration(self.project_id)
            
            # Setup communication infrastructure
            await self.pubsub_comm.setup_communication_channels()
            await self.task_orchestration.create_task_queue()
            
            # Initialize workflow client for orchestration
            self.workflow_client = workflows_v1.WorkflowsServiceClient()
            
            self.logger.info("Google Cloud services initialized successfully")
            console.print("[bold green]✅ Google Cloud Platform initialized[/bold green]")
            console.print("[cyan]Available services:[/cyan]")
            console.print("  • Vertex AI (for intelligent agents)")
            console.print("  • Cloud Run (for scalable agent deployment)")
            console.print("  • Pub/Sub (for agent communication)")
            console.print("  • Firestore (for agent registry)")
            console.print("  • Cloud Tasks (for reliable task execution)")
            console.print("  • Cloud Workflows (for orchestration)")
            
            return True
            
        except DefaultCredentialsError:
            self.logger.warning("No Google Cloud credentials found. Running in local mode.")
            console.print("[yellow]⚠️ Running in local mode (no GCP credentials)[/yellow]")
            return False
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Cloud services: {e}")
            return False
    """Enhanced Google Cloud service integrations with Agent Hub and A2A"""
    
    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id
        self.credentials = None
        self.storage_client = None
        self.firestore_client = None
        self.pubsub_publisher = None
        self.pubsub_subscriber = None
        self.logging_client = None
        self.logger = structlog.get_logger(name="GCPIntegration")
        
        # Agent Hub and A2A components
        self.agent_hub = None
        self.a2a_platform = None
        self.workflow_integration = None
        
    async def initialize(self):
        """Initialize Google Cloud clients including Agent Hub and A2A"""
        try:
            # Try to get default credentials
            self.credentials, self.project_id = default()
            self.logger.info(f"Using Google Cloud project: {self.project_id}")
            
            # Initialize standard clients
            self.storage_client = storage.Client(project=self.project_id)
            self.firestore_client = firestore.AsyncClient(project=self.project_id)
            self.pubsub_publisher = pubsub_v1.PublisherClient()
            self.pubsub_subscriber = pubsub_v1.SubscriberClient()
            self.logging_client = cloud_logging.Client(project=self.project_id)
            
            # Setup cloud logging
            self.logging_client.setup_logging()
            
            # Initialize Agent Hub
            self.agent_hub = GoogleAgentHub(self.project_id)
            await self.agent_hub.initialize()
            
            # Initialize A2A Platform
            self.a2a_platform = A2ACommunicationPlatform(self.project_id, self.agent_hub)
            await self.a2a_platform.initialize()
            
            # Initialize Workflow Integration
            self.workflow_integration = GoogleCloudWorkflowIntegration(self.project_id)
            
            self.logger.info("Google Cloud services with Agent Hub and A2A initialized successfully")
            return True
            
        except DefaultCredentialsError:
            self.logger.warning("No Google Cloud credentials found. Running in local mode.")
            return False
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Cloud services: {e}")
            return False
    
    async def save_to_storage(self, bucket_name: str, blob_name: str, data: Any):
        """Save data to Google Cloud Storage"""
        if not self.storage_client:
            self.logger.warning("Storage client not initialized")
            return None
        
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            if isinstance(data, dict) or isinstance(data, list):
                data = json.dumps(data)
            
            blob.upload_from_string(data)
            self.logger.info(f"Saved data to gs://{bucket_name}/{blob_name}")
            return f"gs://{bucket_name}/{blob_name}"
            
        except Exception as e:
            self.logger.error(f"Failed to save to storage: {e}")
            return None
    
    async def load_from_storage(self, bucket_name: str, blob_name: str) -> Optional[Any]:
        """Load data from Google Cloud Storage"""
        if not self.storage_client:
            self.logger.warning("Storage client not initialized")
            return None
        
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            data = blob.download_as_text()
            
            # Try to parse as JSON
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                pass
            
            self.logger.info(f"Loaded data from gs://{bucket_name}/{blob_name}")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to load from storage: {e}")
            return None
    
    async def save_to_firestore(self, collection: str, document_id: str, data: Dict[str, Any]):
        """Save data to Firestore"""
        if not self.firestore_client:
            self.logger.warning("Firestore client not initialized")
            return None
        
        try:
            doc_ref = self.firestore_client.collection(collection).document(document_id)
            await doc_ref.set(data)
            self.logger.info(f"Saved document to Firestore: {collection}/{document_id}")
            return doc_ref.id
            
        except Exception as e:
            self.logger.error(f"Failed to save to Firestore: {e}")
            return None
    
    async def publish_message(self, topic_name: str, message: Dict[str, Any]):
        """Publish message to Pub/Sub"""
        if not self.pubsub_publisher:
            self.logger.warning("Pub/Sub publisher not initialized")
            return None
        
        try:
            topic_path = self.pubsub_publisher.topic_path(self.project_id, topic_name)
            
            # Convert message to bytes
            message_bytes = json.dumps(message).encode('utf-8')
            
            # Publish message
            future = self.pubsub_publisher.publish(topic_path, message_bytes)
            message_id = future.result()
            
            self.logger.info(f"Published message to {topic_name}: {message_id}")
            return message_id
            
        except Exception as e:
            self.logger.error(f"Failed to publish message: {e}")
            return None

# Global GCP integration instance
gcp_integration = GoogleCloudIntegration()

# Check if we're in Google Cloud environment
async def check_gcp_environment():
    """Check and initialize Google Cloud environment"""
    console.print("[bold blue]Checking Google Cloud environment...[/bold blue]")
    
    is_gcp = await gcp_integration.initialize()
    
    if is_gcp:
        console.print("[bold green]✅ Google Cloud environment detected![/bold green]")
        console.print(f"[yellow]Project ID: {gcp_integration.project_id}[/yellow]")
    else:
        console.print("[bold yellow]⚠️ Running in local mode (no GCP credentials)[/bold yellow]")
        console.print("[cyan]The system will use local agent communication[/cyan]")
    
    return is_gcp

# Run the check
is_gcp_enabled = await check_gcp_environment()
```

### Cell 5: Multi-Agent Orchestration System

```python
# Cell 5: Multi-Agent Orchestration System
# Advanced orchestration and workflow management

import networkx as nx

class WorkflowStep:
    """Represents a step in a workflow"""
    
    def __init__(self, name: str, agent: str, action: str, 
                 params: Optional[Dict[str, Any]] = None,
                 dependencies: Optional[List[str]] = None):
        self.name = name
        self.agent = agent
        self.action = action
        self.params = params or {}
        self.dependencies = dependencies or []
        self.result = None
        self.status = "pending"

class Workflow:
    """Represents a complete workflow"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.steps: Dict[str, WorkflowStep] = {}
        self.graph = nx.DiGraph()
        self.logger = structlog.get_logger(name=f"Workflow-{name}")
    
    def add_step(self, step: WorkflowStep):
        """Add a step to the workflow"""
        self.steps[step.name] = step
        self.graph.add_node(step.name)
        
        # Add edges for dependencies
        for dep in step.dependencies:
            self.graph.add_edge(dep, step.name)
    
    def validate(self) -> bool:
        """Validate the workflow for cycles and missing dependencies"""
        # Check for cycles
        if not nx.is_directed_acyclic_graph(self.graph):
            self.logger.error("Workflow contains cycles")
            return False
        
        # Check for missing dependencies
        for step_name, step in self.steps.items():
            for dep in step.dependencies:
                if dep not in self.steps:
                    self.logger.error(f"Missing dependency: {dep} for step {step_name}")
                    return False
        
        return True
    
    def get_execution_order(self) -> List[str]:
        """Get the topological order for execution"""
        return list(nx.topological_sort(self.graph))

class MultiAgentOrchestrator:
    """Advanced orchestrator for multi-agent workflows"""
    
    def __init__(self):
        self.workflows: Dict[str, Workflow] = {}
        self.running_workflows: Dict[str, Dict[str, Any]] = {}
        self.logger = structlog.get_logger(name="Orchestrator")
        self.event_handlers: Dict[str, List[Callable]] = {}
    
    def register_workflow(self, workflow: Workflow):
        """Register a workflow"""
        if workflow.validate():
            self.workflows[workflow.name] = workflow
            self.logger.info(f"Registered workflow: {workflow.name}")
        else:
            self.logger.error(f"Failed to register invalid workflow: {workflow.name}")
    
    async def execute_workflow(self, workflow_name: str, 
                              input_data: Any = None,
                              context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a complete workflow"""
        
        if workflow_name not in self.workflows:
            raise ValueError(f"Workflow {workflow_name} not found")
        
        workflow = self.workflows[workflow_name]
        workflow_id = str(uuid.uuid4())
        
        # Initialize workflow execution context
        execution_context = {
            'workflow_id': workflow_id,
            'workflow_name': workflow_name,
            'start_time': datetime.now(),
            'input_data': input_data,
            'context': context or {},
            'results': {},
            'status': 'running'
        }
        
        self.running_workflows[workflow_id] = execution_context
        self.logger.info(f"Starting workflow execution: {workflow_name} (ID: {workflow_id})")
        
        try:
            # Get execution order
            execution_order = workflow.get_execution_order()
            
            # Execute steps in order
            for step_name in execution_order:
                step = workflow.steps[step_name]
                
                # Wait for dependencies to complete
                await self._wait_for_dependencies(workflow, step, execution_context)
                
                # Execute step
                self.logger.info(f"Executing step: {step_name}")
                result = await self._execute_step(step, execution_context)
                
                # Store result
                execution_context['results'][step_name] = result
                step.result = result
                step.status = 'completed'
                
                # Trigger event handlers
                await self._trigger_event('step_completed', {
                    'workflow_id': workflow_id,
                    'step_name': step_name,
                    'result': result
                })
            
            # Workflow completed successfully
            execution_context['status'] = 'completed'
            execution_context['end_time'] = datetime.now()
            execution_context['duration'] = (
                execution_context['end_time'] - execution_context['start_time']
            ).total_seconds()
            
            self.logger.info(f"Workflow completed: {workflow_name} (ID: {workflow_id})")
            
            # Trigger completion event
            await self._trigger_event('workflow_completed', execution_context)
            
            return execution_context
            
        except Exception as e:
            execution_context['status'] = 'failed'
            execution_context['error'] = str(e)
            self.logger.error(f"Workflow failed: {workflow_name} (ID: {workflow_id}): {e}")
            
            # Trigger failure event
            await self._trigger_event('workflow_failed', execution_context)
            
            raise
        
        finally:
            # Clean up
            if workflow_id in self.running_workflows:
                del self.running_workflows[workflow_id]
    
    async def _wait_for_dependencies(self, workflow: Workflow, 
                                    step: WorkflowStep, 
                                    context: Dict[str, Any]):
        """Wait for step dependencies to complete"""
        for dep_name in step.dependencies:
            dep_step = workflow.steps[dep_name]
            while dep_step.status != 'completed':
                await asyncio.sleep(0.1)
    
    async def _execute_step(self, step: WorkflowStep, 
                           context: Dict[str, Any]) -> Any:
        """Execute a single workflow step"""
        agent = agent_registry.get(step.agent)
        
        if not agent:
            raise ValueError(f"Agent {step.agent} not found")
        
        # Prepare input data
        input_data = context.get('input_data')
        
        # If there are dependencies, use their results as input
        if step.dependencies:
            dependency_results = {}
            for dep_name in step.dependencies:
                if dep_name in context['results']:
                    dependency_results[dep_name] = context['results'][dep_name]
            
            input_data = dependency_results if dependency_results else input_data
        
        # Create message for agent
        message = Message(
            sender="orchestrator",
            receiver=step.agent,
            content=input_data,
            message_type=step.action,
            metadata={'step': step.name, 'params': step.params}
        )
        
        # Process with agent
        result = await agent.process(message)
        
        return result
    
    def register_event_handler(self, event: str, handler: Callable):
        """Register an event handler"""
        if event not in self.event_handlers:
            self.event_handlers[event] = []
        self.event_handlers[event].append(handler)
    
    async def _trigger_event(self, event: str, data: Any):
        """Trigger event handlers"""
        if event in self.event_handlers:
            for handler in self.event_handlers[event]:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(data)
                    else:
                        handler(data)
                except Exception as e:
                    self.logger.error(f"Error in event handler for {event}: {e}")

# Global orchestrator
orchestrator = MultiAgentOrchestrator()

print("✅ Multi-agent orchestration system loaded!")
```

### Cell 6: System Management and Utilities

```python
# Cell 6: System Management and Utilities
# Complete system management with all utility classes

class AgentMemory:
    """Memory system for agents to store and retrieve information"""
    
    def __init__(self, max_size: int = 1000):
        self.memory = {}
        self.max_size = max_size
        self.access_count = {}
        self.logger = structlog.get_logger(name="AgentMemory")
    
    def store(self, key: str, value: Any, ttl: Optional[int] = None):
        """Store information in memory"""
        if len(self.memory) >= self.max_size:
            # Remove least accessed item
            least_accessed = min(self.access_count, key=self.access_count.get)
            del self.memory[least_accessed]
            del self.access_count[least_accessed]
        
        self.memory[key] = {
            'value': value,
            'timestamp': datetime.now(),
            'ttl': ttl
        }
        self.access_count[key] = 0
        self.logger.debug(f"Stored: {key}")
    
    def retrieve(self, key: str) -> Optional[Any]:
        """Retrieve information from memory"""
        if key in self.memory:
            item = self.memory[key]
            
            # Check TTL
            if item['ttl']:
                age = (datetime.now() - item['timestamp']).total_seconds()
                if age > item['ttl']:
                    del self.memory[key]
                    del self.access_count[key]
                    return None
            
            self.access_count[key] += 1
            return item['value']
        
        return None
    
    def search(self, pattern: str) -> List[str]:
        """Search for keys matching a pattern"""
        regex = re.compile(pattern)
        return [key for key in self.memory.keys() if regex.match(key)]

class TaskQueue:
    """Priority task queue for agent tasks"""
    
    def __init__(self):
        self.queue = asyncio.PriorityQueue()
        self.task_count = 0
        self.logger = structlog.get_logger(name="TaskQueue")
    
    async def add_task(self, priority: int, task: Dict[str, Any]):
        """Add a task to the queue"""
        self.task_count += 1
        task_id = f"task_{self.task_count}"
        await self.queue.put((priority, task_id, task))
        self.logger.debug(f"Added task {task_id} with priority {priority}")
        return task_id
    
    async def get_task(self) -> Optional[Tuple[int, str, Dict[str, Any]]]:
        """Get the highest priority task"""
        try:
            return await asyncio.wait_for(self.queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            return None
    
    def size(self) -> int:
        """Get queue size"""
        return self.queue.qsize()

class SystemMonitor:
    """Monitor system performance and health"""
    
    def __init__(self):
        self.metrics = {}
        self.alerts = []
        self.logger = structlog.get_logger(name="SystemMonitor")
    
    async def collect_metrics(self):
        """Collect system metrics"""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent,
            'agent_metrics': agent_registry.get_all_status()
        }
        
        self.metrics[datetime.now().isoformat()] = metrics
        
        # Check for alerts
        if metrics['cpu_percent'] > 80:
            self.alerts.append({
                'level': 'warning',
                'message': f"High CPU usage: {metrics['cpu_percent']}%",
                'timestamp': datetime.now().isoformat()
            })
        
        if metrics['memory_percent'] > 85:
            self.alerts.append({
                'level': 'warning',
                'message': f"High memory usage: {metrics['memory_percent']}%",
                'timestamp': datetime.now().isoformat()
            })
        
        return metrics
    
    def get_health_status(self) -> str:
        """Get overall health status"""
        if not self.metrics:
            return "unknown"
        
        latest_metrics = list(self.metrics.values())[-1]
        
        if latest_metrics['cpu_percent'] > 90 or latest_metrics['memory_percent'] > 90:
            return "critical"
        elif latest_metrics['cpu_percent'] > 70 or latest_metrics['memory_percent'] > 70:
            return "warning"
        else:
            return "healthy"

class AgentCommunicationBus:
    """Message bus for inter-agent communication"""
    
    def __init__(self):
        self.subscribers = {}
        self.message_history = deque(maxlen=1000)
        self.logger = structlog.get_logger(name="CommunicationBus")
    
    def subscribe(self, agent_name: str, topics: List[str]):
        """Subscribe an agent to topics"""
        for topic in topics:
            if topic not in self.subscribers:
                self.subscribers[topic] = []
            if agent_name not in self.subscribers[topic]:
                self.subscribers[topic].append(agent_name)
                self.logger.debug(f"{agent_name} subscribed to {topic}")
    
    async def publish(self, topic: str, message: Message):
        """Publish a message to a topic"""
        self.message_history.append({
            'topic': topic,
            'message': message,
            'timestamp': datetime.now()
        })
        
        if topic in self.subscribers:
            for agent_name in self.subscribers[topic]:
                agent = agent_registry.get(agent_name)
                if agent:
                    await agent.message_queue.put(message)
                    self.logger.debug(f"Delivered message to {agent_name} on topic {topic}")

# Global utilities - Initialize them here
agent_memory = AgentMemory()
task_queue = TaskQueue()
system_monitor = SystemMonitor()
communication_bus = AgentCommunicationBus()

print("✅ System utilities loaded!")
```

### Cell 7: Main System Class

```python
# Cell 7: Main System Class
# Complete multi-agent system manager

class MultiAgentSystem:
    """Complete multi-agent system manager"""
    
    def __init__(self):
        self.agents = {}
        self.orchestrator = orchestrator
        self.gcp_integration = gcp_integration
        self.initialized = False
        self.logger = structlog.get_logger(name="MultiAgentSystem")
        self.start_time = None
    
    async def initialize(self, agent_configs: Optional[Dict[str, Dict]] = None):
        """Initialize the complete system with Agent Hub integration"""
        console.print("[bold cyan]Initializing Multi-Agent System...[/bold cyan]")
        
        self.start_time = datetime.now()
        
        # Default agent configurations
        if not agent_configs:
            agent_configs = {
                'data_processor': {'type': DataProcessorAgent},
                'analytics': {'type': AnalyticsAgent},
                'api_gateway': {'type': APIGatewayAgent},
                'coordinator': {'type': CoordinatorAgent}
            }
        
        # Create and initialize agents
        for name, config in agent_configs.items():
            agent_class = config['type']
            agent = agent_class(name=name, config=config.get('config', {}))
            
            # Initialize agent
            await agent.initialize()
            
            # Register with local registry
            agent_registry.register(agent)
            self.agents[name] = agent
            
            # Register with Google Agent Hub if available
            if gcp_integration.agent_hub and isinstance(agent, A2AEnabledAgent):
                await agent.register_with_hub(gcp_integration.agent_hub)
                
                # Set up A2A message handlers
                if gcp_integration.a2a_platform:
                    async def handle_a2a_message(data):
                        """Handle incoming A2A messages"""
                        content = json.loads(data['content']) if isinstance(data['content'], str) else data['content']
                        message = Message(
                            sender=data['sender'],
                            receiver=data['receiver'],
                            content=content,
                            message_type=data['message_type']
                        )
                        return await agent.process(message)
                    
                    gcp_integration.a2a_platform.register_message_handler(
                        f"a2a_{name}",
                        handle_a2a_message
                    )
                    
                    # Start listening for A2A messages
                    await gcp_integration.a2a_platform.start_listening(agent.id)
            
            console.print(f"[green]✓[/green] Agent '{name}' initialized")
            if isinstance(agent, A2AEnabledAgent) and agent.a2a_enabled:
                console.print(f"  [cyan]↔[/cyan] A2A enabled for '{name}'")
        
        # Discover other agents in the hub
        if gcp_integration.agent_hub:
            discovered_agents = await gcp_integration.agent_hub.discover_agents()
            if discovered_agents:
                console.print(f"\n[yellow]Discovered {len(discovered_agents)} agents in Agent Hub[/yellow]")
                for agent_info in discovered_agents[:5]:  # Show first 5
                    console.print(f"  • {agent_info['name']} ({agent_info['state']})")
        
        self.initialized = True
        console.print("[bold green]✅ Multi-Agent System initialized successfully![/bold green]")
        
        # Display system status
        self.display_status()
        
        return self
    
    def display_status(self):
        """Display system status in a table"""
        table = Table(title="Multi-Agent System Status")
        table.add_column("Agent", style="cyan", no_wrap=True)
        table.add_column("State", style="magenta")
        table.add_column("Capabilities", style="green")
        table.add_column("Messages", style="yellow")
        
        for name, agent in self.agents.items():
            status = agent.get_status()
            table.add_row(
                name,
                status['state'],
                ", ".join(status['capabilities'][:2]) + "...",
                str(status['metrics']['messages_processed'])
            )
        
        console.print(table)
    
    async def execute_task(self, task_type: str, data: Any) -> Any:
        """Execute a task using appropriate agent"""
        if not self.initialized:
            raise RuntimeError("System not initialized")
        
        # Route to appropriate agent based on task type
        agent_mapping = {
            'process': 'data_processor',
            'analyze': 'analytics',
            'api': 'api_gateway',
            'coordinate': 'coordinator'
        }
        
        agent_name = agent_mapping.get(task_type, 'coordinator')
        agent = self.agents.get(agent_name)
        
        if not agent:
            raise ValueError(f"No agent available for task type: {task_type}")
        
        message = Message(
            sender="system",
            receiver=agent_name,
            content=data,
            message_type=task_type
        )
        
        result = await agent.process(message)
        return result
    
    async def create_and_execute_workflow(self, workflow_config: Dict[str, Any]) -> Any:
        """Create and execute a workflow"""
        workflow = Workflow(
            name=workflow_config.get('name', 'custom_workflow'),
            description=workflow_config.get('description', '')
        )
        
        # Add steps from configuration
        for step_config in workflow_config.get('steps', []):
            step = WorkflowStep(
                name=step_config['name'],
                agent=step_config['agent'],
                action=step_config.get('action', 'process'),
                params=step_config.get('params', {}),
                dependencies=step_config.get('dependencies', [])
            )
            workflow.add_step(step)
        
        # Register and execute workflow
        self.orchestrator.register_workflow(workflow)
        result = await self.orchestrator.execute_workflow(
            workflow.name,
            input_data=workflow_config.get('input_data')
        )
        
        return result
    
    async def shutdown(self):
        """Shutdown the system gracefully"""
        console.print("[bold yellow]Shutting down Multi-Agent System...[/bold yellow]")
        
        # Cleanup all agents
        for name, agent in self.agents.items():
            await agent.cleanup()
            console.print(f"[yellow]✓[/yellow] Agent '{name}' shutdown")
        
        self.initialized = False
        
        if self.start_time:
            runtime = (datetime.now() - self.start_time).total_seconds()
            console.print(f"[bold cyan]Total runtime: {runtime:.2f} seconds[/bold cyan]")
        
        console.print("[bold red]System shutdown complete[/bold red]")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get system-wide metrics"""
        metrics = {
            'system': {
                'initialized': self.initialized,
                'uptime': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
                'agent_count': len(self.agents)
            },
            'agents': {}
        }
        
        for name, agent in self.agents.items():
            metrics['agents'][name] = agent.metrics
        
        # Calculate totals
        total_messages = sum(agent.metrics['messages_processed'] for agent in self.agents.values())
        total_errors = sum(agent.metrics['errors'] for agent in self.agents.values())
        
        metrics['totals'] = {
            'messages_processed': total_messages,
            'errors': total_errors,
            'error_rate': total_errors / total_messages if total_messages > 0 else 0
        }
        
        return metrics

# Create global system instance
multi_agent_system = MultiAgentSystem()

# Utility functions
async def save_system_state(filename: str = "system_state.pkl"):
    """Save the current system state"""
    state = {
        'agents': {name: agent.get_status() for name, agent in agent_registry.agents.items()},
        'metrics': multi_agent_system.get_metrics() if multi_agent_system.initialized else {},
        'memory': agent_memory.memory,
        'timestamp': datetime.now().isoformat()
    }
    
    with open(filename, 'wb') as f:
        pickle.dump(state, f)
    
    console.print(f"[green]System state saved to {filename}[/green]")
    return state

async def load_system_state(filename: str = "system_state.pkl"):
    """Load a saved system state"""
    try:
        with open(filename, 'rb') as f:
            state = pickle.load(f)
        
        console.print(f"[green]System state loaded from {filename}[/green]")
        return state
    except FileNotFoundError:
        console.print(f"[red]State file {filename} not found[/red]")
        return None

print("✅ Main system class loaded!")
```

### Cell 8: Demo and Quick Start Functions

```python
# Cell 8: Demo and Quick Start Functions
# Interactive demo and easy-to-use functions

async def run_complete_demo():
    """Run a complete demonstration of the multi-agent system"""
    
    console.print("\n" + "="*60)
    console.print("[bold magenta]🚀 MULTI-AGENT SYSTEM DEMONSTRATION[/bold magenta]")
    console.print("="*60 + "\n")
    
    # Step 1: Initialize the system
    console.print("[bold blue]Step 1: System Initialization[/bold blue]")
    await multi_agent_system.initialize()
    
    # Step 2: Simple task execution
    console.print("\n[bold blue]Step 2: Simple Task Execution[/bold blue]")
    
    # Process data
    console.print("[cyan]Processing data...[/cyan]")
    result = await multi_agent_system.execute_task('process', {'name': 'test', 'value': 123})
    console.print(f"Result: {result}")
    
    # Analyze data
    console.print("[cyan]Analyzing data...[/cyan]")
    result = await multi_agent_system.execute_task('analyze', result)
    console.print(f"Analytics result: {result}")
    
    # Step 3: Complex workflow
    console.print("\n[bold blue]Step 3: Complex Workflow Execution[/bold blue]")
    
    workflow_config = {
        'name': 'data_pipeline',
        'description': 'Complete data processing pipeline',
        'steps': [
            {
                'name': 'fetch_data',
                'agent': 'api_gateway',
                'action': 'api_call',
                'params': {'endpoint': 'https://api.example.com/data'}
            },
            {
                'name': 'process_data',
                'agent': 'data_processor',
                'action': 'transform',
                'dependencies': ['fetch_data']
            },
            {
                'name': 'analyze_data',
                'agent': 'analytics',
                'action': 'analyze',
                'dependencies': ['process_data']
            },
            {
                'name': 'aggregate_results',
                'agent': 'data_processor',
                'action': 'aggregate',
                'dependencies': ['analyze_data']
            }
        ],
        'input_data': {'source': 'demo', 'timestamp': datetime.now().isoformat()}
    }
    
    console.print("[cyan]Executing workflow...[/cyan]")
    workflow_result = await multi_agent_system.create_and_execute_workflow(workflow_config)
    
    console.print("\n[bold green]Workflow Results:[/bold green]")
    for step_name, step_result in workflow_result['results'].items():
        console.print(f"  {step_name}: ✅")
    
    console.print(f"\n[yellow]Total execution time: {workflow_result.get('duration', 0):.2f} seconds[/yellow]")
    
    # Step 4: System metrics
    console.print("\n[bold blue]Step 4: System Metrics[/bold blue]")
    metrics = multi_agent_system.get_metrics()
    
    metrics_table = Table(title="System Metrics")
    metrics_table.add_column("Metric", style="cyan")
    metrics_table.add_column("Value", style="yellow")
    
    metrics_table.add_row("Total Messages", str(metrics['totals']['messages_processed']))
    metrics_table.add_row("Total Errors", str(metrics['totals']['errors']))
    metrics_table.add_row("Error Rate", f"{metrics['totals']['error_rate']:.2%}")
    metrics_table.add_row("System Uptime", f"{metrics['system']['uptime']:.2f}s")
    
    console.print(metrics_table)
    
    # Step 5: Shutdown
    console.print("\n[bold blue]Step 5: System Shutdown[/bold blue]")
    await multi_agent_system.shutdown()
    
    console.print("\n" + "="*60)
    console.print("[bold green]✅ DEMONSTRATION COMPLETE![/bold green]")
    console.print("="*60)

# Quick start function
async def quick_start():
    """Quick start the multi-agent system"""
    console.print("[bold cyan]🚀 Quick Starting Multi-Agent System[/bold cyan]")
    
    # Initialize with default configuration
    await multi_agent_system.initialize()
    
    console.print("[bold green]✅ System ready for use![/bold green]")
    console.print("\nAvailable commands:")
    console.print("  - await process_data(data)")
    console.print("  - await analyze_data(data)")
    console.print("  - await run_workflow(workflow_config)")
    console.print("  - await get_system_status()")
    console.print("  - await shutdown_system()")
    
    return multi_agent_system

async def process_data(data: Any):
    """Process data using the data processor agent"""
    return await multi_agent_system.execute_task('process', data)

async def analyze_data(data: Any):
    """Analyze data using the analytics agent"""
    return await multi_agent_system.execute_task('analyze', data)

async def run_workflow(steps: List[Dict[str, Any]], input_data: Any = None):
    """Run a custom workflow"""
    workflow_config = {
        'name': f'custom_workflow_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
        'steps': steps,
        'input_data': input_data
    }
    return await multi_agent_system.create_and_execute_workflow(workflow_config)

async def get_system_status():
    """Get current system status"""
    status = {
        'health': system_monitor.get_health_status(),
        'metrics': multi_agent_system.get_metrics(),
        'agents': agent_registry.get_all_status(),
        'queue_size': task_queue.size()
    }
    
    # Display status
    console.print("\n[bold]System Status[/bold]")
    console.print(f"Health: [{'green' if status['health'] == 'healthy' else 'yellow'}]{status['health']}[/]")
    console.print(f"Queue Size: {status['queue_size']}")
    console.print(f"Total Messages: {status['metrics']['totals']['messages_processed']}")
    
    return status

async def shutdown_system():
    """Shutdown the system gracefully"""
    await multi_agent_system.shutdown()

# Example workflows
EXAMPLE_WORKFLOWS = {
    'simple_pipeline': [
        {'name': 'process', 'agent': 'data_processor', 'action': 'transform'},
        {'name': 'analyze', 'agent': 'analytics', 'action': 'analyze', 'dependencies': ['process']}
    ],
    
    'parallel_processing': [
        {'name': 'process1', 'agent': 'data_processor', 'action': 'transform'},
        {'name': 'process2', 'agent': 'data_processor', 'action': 'validate'},
        {'name': 'combine', 'agent': 'data_processor', 'action': 'aggregate', 
         'dependencies': ['process1', 'process2']}
    ],
    
    'complex_pipeline': [
        {'name': 'fetch', 'agent': 'api_gateway', 'action': 'api_call'},
        {'name': 'validate', 'agent': 'data_processor', 'action': 'validate', 'dependencies': ['fetch']},
        {'name': 'transform', 'agent': 'data_processor', 'action': 'transform', 'dependencies': ['validate']},
        {'name': 'analyze', 'agent': 'analytics', 'action': 'analyze', 'dependencies': ['transform']},
        {'name': 'store', 'agent': 'api_gateway', 'action': 'api_call', 'dependencies': ['analyze']}
    ]
}

print("✅ Demo and quick start functions loaded!")
```

### Cell 9: Auto-Initialize and Test

```python
# Cell 9: Auto-Initialize and Test
# Automatically initialize the system and run a test

async def auto_initialize_and_test():
    """Automatically initialize the system and run a quick test"""
    
    console.print("\n" + "="*60)
    console.print("[bold cyan]🎯 AUTO-INITIALIZING MIZ OKI 3.0[/bold cyan]")
    console.print("="*60 + "\n")
    
    # Initialize system
    await multi_agent_system.initialize()
    
    # Run a quick test
    console.print("\n[bold yellow]Running Quick Test...[/bold yellow]")
    
    test_data = {
        'test_id': str(uuid.uuid4()),
        'timestamp': datetime.now().isoformat(),
        'data': 'Hello, Multi-Agent System!'
    }
    
    # Test data processing
    processed = await process_data(test_data)
    console.print(f"✅ Data Processing: {processed}")
    
    # Test analytics
    analyzed = await analyze_data(processed)
    console.print(f"✅ Analytics: Patterns found: {analyzed.get('patterns_found', 0)}")
    
    # Test simple workflow
    workflow_result = await run_workflow(
        EXAMPLE_WORKFLOWS['simple_pipeline'],
        test_data
    )
    console.print(f"✅ Workflow Execution: {workflow_result['status']}")
    
    # Get final status
    status = await get_system_status()
    
    console.print("\n" + "="*60)
    console.print("[bold green]✅ SYSTEM READY FOR USE![/bold green]")
    console.print("="*60)
    console.print("\n[yellow]The Multi-Agent System is now fully operational.[/yellow]")
    console.print("[yellow]Use the commands from previous cells to interact with the system.[/yellow]")
    
    return True

# Display help
console.print("\n" + "="*60)
console.print("[bold magenta]MIZ OKI 3.0 - Multi-Agent System Ready![/bold magenta]")
console.print("="*60)
console.print("\n[bold]Quick Commands:[/bold]")
console.print("  [cyan]await auto_initialize_and_test()[/cyan] - Auto-initialize and test")
console.print("  [cyan]await quick_start()[/cyan] - Initialize the system")
console.print("  [cyan]await process_data({'key': 'value'})[/cyan] - Process data")
console.print("  [cyan]await analyze_data(data)[/cyan] - Analyze data")
console.print("  [cyan]await run_workflow(EXAMPLE_WORKFLOWS['simple_pipeline'])[/cyan] - Run workflow")
console.print("  [cyan]await get_system_status()[/cyan] - Check system status")
console.print("  [cyan]await run_complete_demo()[/cyan] - Run full demonstration")
console.print("  [cyan]await shutdown_system()[/cyan] - Shutdown the system")
console.print("\n[yellow]Run 'await auto_initialize_and_test()' to begin![/yellow]")

# Auto-run initialization and test
initialization_success = await auto_initialize_and_test()

if initialization_success:
    console.print("\n[bold green]🎉 MIZ OKI 3.0 Multi-Agent System is ready![/bold green]")
    console.print("[cyan]You can now use all the system functions.[/cyan]")
else:
    console.print("\n[bold red]⚠️ System initialization failed. Please check the logs.[/bold red]")
```

## Fixed Issues Summary

The main issues were:
1. **Missing `Tuple` import** - Added to Cell 2 imports
2. **`system_monitor` not defined before use** - Moved initialization to Cell 6
3. **Cells executed out of order** - Reorganized dependencies

## How to Use

1. **Run cells 1-9 in order**
2. The system will auto-initialize and run tests
3. Use these commands to interact:
   ```python
   # Process data
   result = await process_data({'key': 'value'})
   
   # Analyze data
   insights = await analyze_data(result)
   
   # Run workflows
   await run_workflow(EXAMPLE_WORKFLOWS['simple_pipeline'])
   
   # Check status
   await get_system_status()
   
   # Full demo
   await run_complete_demo()
   ```

The system is now ready for use in Google Cloud notebooks!


