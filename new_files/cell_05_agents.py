# Cell 05: Agent System
# This cell contains agent system components and management

import os
import sys
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseAgent(ABC):
    """Base agent class."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        self.name = name
        self.config = config or {}
        self.initialized = False
        self.logger = logging.getLogger(f"agent.{name}")
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the agent."""
        pass
    
    @abstractmethod
    async def process(self, input_data: Any) -> Any:
        """Process input data."""
        pass
    
    @abstractmethod
    async def cleanup(self) -> None:
        """Cleanup agent resources."""
        pass

class AgentManager:
    """Agent management system."""
    
    def __init__(self):
        self.agents = {}
        self.active_agents = set()
    
    def register_agent(self, agent: BaseAgent) -> None:
        """Register an agent."""
        self.agents[agent.name] = agent
        logger.info(f"Registered agent: {agent.name}")
    
    def get_agent(self, name: str) -> Optional[BaseAgent]:
        """Get an agent by name."""
        return self.agents.get(name)
    
    async def initialize_agent(self, name: str) -> None:
        """Initialize a specific agent."""
        agent = self.get_agent(name)
        if agent:
            await agent.initialize()
            self.active_agents.add(name)
            logger.info(f"Initialized agent: {name}")
        else:
            logger.error(f"Agent not found: {name}")
    
    async def initialize_all_agents(self) -> None:
        """Initialize all registered agents."""
        for agent in self.agents.values():
            await agent.initialize()
            self.active_agents.add(agent.name)
        logger.info("All agents initialized")
    
    async def process_with_agent(self, agent_name: str, input_data: Any) -> Any:
        """Process data with a specific agent."""
        agent = self.get_agent(agent_name)
        if agent and agent_name in self.active_agents:
            return await agent.process(input_data)
        else:
            logger.error(f"Agent not available: {agent_name}")
            return None
    
    async def cleanup_agent(self, name: str) -> None:
        """Cleanup a specific agent."""
        agent = self.get_agent(name)
        if agent:
            await agent.cleanup()
            self.active_agents.discard(name)
            logger.info(f"Cleaned up agent: {name}")
    
    async def cleanup_all_agents(self) -> None:
        """Cleanup all agents."""
        for agent in self.agents.values():
            await agent.cleanup()
        self.active_agents.clear()
        logger.info("All agents cleaned up")

class SimpleAgent(BaseAgent):
    """Simple agent implementation."""
    
    async def initialize(self) -> None:
        """Initialize the simple agent."""
        self.initialized = True
        self.logger.info("Simple agent initialized")
    
    async def process(self, input_data: Any) -> Any:
        """Process input data."""
        if not self.initialized:
            self.logger.error("Agent not initialized")
            return None
        
        # Simple processing logic
        if isinstance(input_data, str):
            return f"Processed: {input_data}"
        elif isinstance(input_data, dict):
            return {k: f"Processed: {v}" for k, v in input_data.items()}
        else:
            return f"Processed: {input_data}"
    
    async def cleanup(self) -> None:
        """Cleanup agent resources."""
        self.initialized = False
        self.logger.info("Simple agent cleaned up")

# Global agent manager
agent_manager = AgentManager()

if __name__ == "__main__":
    # Example usage
    async def main():
        # Create and register agents
        agent1 = SimpleAgent("agent1", {"type": "simple"})
        agent2 = SimpleAgent("agent2", {"type": "simple"})
        
        agent_manager.register_agent(agent1)
        agent_manager.register_agent(agent2)
        
        # Initialize agents
        await agent_manager.initialize_all_agents()
        
        # Process data
        result1 = await agent_manager.process_with_agent("agent1", "test data")
        result2 = await agent_manager.process_with_agent("agent2", {"key": "value"})
        
        logger.info(f"Result 1: {result1}")
        logger.info(f"Result 2: {result2}")
        
        # Cleanup
        await agent_manager.cleanup_all_agents()
    
    asyncio.run(main()) 