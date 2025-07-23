# Cell 08: System Initialization
# This cell handles system initialization and startup

import os
import sys
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

logger = logging.getLogger(__name__)

class SystemInitializer:
    """System initialization manager."""
    
    def __init__(self):
        self.initialized_components = set()
        self.initialization_order = []
        self.dependencies = {}
    
    def register_component(self, name: str, init_function, dependencies: List[str] = None) -> None:
        """Register a component for initialization."""
        self.initialization_order.append(name)
        self.dependencies[name] = dependencies or []
        logger.info(f"Registered component for initialization: {name}")
    
    async def initialize_system(self) -> bool:
        """Initialize the entire system."""
        logger.info("Starting system initialization...")
        
        try:
            # Initialize components in dependency order
            for component_name in self.initialization_order:
                await self._initialize_component(component_name)
            
            logger.info("System initialization completed successfully")
            return True
        except Exception as e:
            logger.error(f"System initialization failed: {e}")
            return False
    
    async def _initialize_component(self, component_name: str) -> None:
        """Initialize a specific component."""
        if component_name in self.initialized_components:
            logger.info(f"Component already initialized: {component_name}")
            return
        
        # Check dependencies
        dependencies = self.dependencies.get(component_name, [])
        for dep in dependencies:
            if dep not in self.initialized_components:
                logger.info(f"Initializing dependency: {dep}")
                await self._initialize_component(dep)
        
        # Initialize component
        logger.info(f"Initializing component: {component_name}")
        self.initialized_components.add(component_name)
        logger.info(f"Component initialized: {component_name}")

class EnvironmentSetup:
    """Environment setup utilities."""
    
    @staticmethod
    def setup_logging() -> None:
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('logs/system.log')
            ]
        )
        logger.info("Logging setup completed")
    
    @staticmethod
    def setup_directories() -> None:
        """Create necessary directories."""
        directories = [
            "logs",
            "data",
            "models",
            "outputs",
            "temp",
            "config"
        ]
        
        for directory in directories:
            Path(directory).mkdir(exist_ok=True)
            logger.info(f"Created directory: {directory}")
    
    @staticmethod
    def setup_environment_variables() -> None:
        """Setup environment variables."""
        env_vars = {
            "PYTHONPATH": str(Path(__file__).parent),
            "LOG_LEVEL": "INFO",
            "DEBUG": "False"
        }
        
        for key, value in env_vars.items():
            os.environ.setdefault(key, value)
            logger.info(f"Set environment variable: {key}={value}")

class ComponentManager:
    """Component management system."""
    
    def __init__(self):
        self.components = {}
        self.initialized = False
    
    def register_component(self, name: str, component: Any) -> None:
        """Register a component."""
        self.components[name] = component
        logger.info(f"Registered component: {name}")
    
    def get_component(self, name: str) -> Optional[Any]:
        """Get a component by name."""
        return self.components.get(name)
    
    async def initialize_components(self) -> None:
        """Initialize all registered components."""
        logger.info("Initializing components...")
        
        for name, component in self.components.items():
            if hasattr(component, 'initialize'):
                if asyncio.iscoroutinefunction(component.initialize):
                    await component.initialize()
                else:
                    component.initialize()
                logger.info(f"Initialized component: {name}")
        
        self.initialized = True
        logger.info("All components initialized")

class StartupManager:
    """Main startup manager."""
    
    def __init__(self):
        self.system_initializer = SystemInitializer()
        self.environment_setup = EnvironmentSetup()
        self.component_manager = ComponentManager()
        self.startup_complete = False
    
    async def startup(self) -> bool:
        """Perform system startup."""
        logger.info("Starting system startup process...")
        
        try:
            # Setup environment
            self.environment_setup.setup_logging()
            self.environment_setup.setup_directories()
            self.environment_setup.setup_environment_variables()
            
            # Initialize system
            success = await self.system_initializer.initialize_system()
            if not success:
                return False
            
            # Initialize components
            await self.component_manager.initialize_components()
            
            self.startup_complete = True
            logger.info("System startup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"System startup failed: {e}")
            return False
    
    def is_startup_complete(self) -> bool:
        """Check if startup is complete."""
        return self.startup_complete

# Global instances
system_initializer = SystemInitializer()
environment_setup = EnvironmentSetup()
component_manager = ComponentManager()
startup_manager = StartupManager()

if __name__ == "__main__":
    async def main():
        # Perform system startup
        success = await startup_manager.startup()
        
        if success:
            logger.info("System is ready")
        else:
            logger.error("System startup failed")
    
    asyncio.run(main()) 