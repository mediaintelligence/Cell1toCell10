# Cell 07: Orchestration System
# This cell contains orchestration and coordination components

import os
import sys
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

logger = logging.getLogger(__name__)

class Orchestrator:
    """Main orchestration system."""
    
    def __init__(self):
        self.components = {}
        self.tasks = {}
        self.running = False
    
    def register_component(self, name: str, component: Any) -> None:
        """Register a component with the orchestrator."""
        self.components[name] = component
        logger.info(f"Registered component: {name}")
    
    def get_component(self, name: str) -> Optional[Any]:
        """Get a component by name."""
        return self.components.get(name)
    
    async def start(self) -> None:
        """Start the orchestrator."""
        self.running = True
        logger.info("Orchestrator started")
    
    async def stop(self) -> None:
        """Stop the orchestrator."""
        self.running = False
        logger.info("Orchestrator stopped")
    
    async def orchestrate_task(self, task_name: str, task_config: Dict[str, Any]) -> Any:
        """Orchestrate a specific task."""
        if not self.running:
            logger.error("Orchestrator not running")
            return None
        
        logger.info(f"Orchestrating task: {task_name}")
        
        # Extract task configuration
        steps = task_config.get('steps', [])
        input_data = task_config.get('input_data')
        
        result = input_data
        for step in steps:
            step_name = step.get('name')
            step_type = step.get('type')
            step_config = step.get('config', {})
            
            if step_type == 'component':
                component_name = step_config.get('component')
                component = self.get_component(component_name)
                if component:
                    result = await self._execute_component_step(component, result, step_config)
                else:
                    logger.error(f"Component not found: {component_name}")
                    return None
            elif step_type == 'transform':
                result = await self._execute_transform_step(result, step_config)
            elif step_type == 'validate':
                result = await self._execute_validate_step(result, step_config)
        
        return result
    
    async def _execute_component_step(self, component: Any, data: Any, config: Dict[str, Any]) -> Any:
        """Execute a component step."""
        method_name = config.get('method', 'process')
        
        if hasattr(component, method_name):
            method = getattr(component, method_name)
            if asyncio.iscoroutinefunction(method):
                return await method(data)
            else:
                return method(data)
        else:
            logger.error(f"Method not found: {method_name}")
            return data
    
    async def _execute_transform_step(self, data: Any, config: Dict[str, Any]) -> Any:
        """Execute a transform step."""
        transform_type = config.get('transform_type', 'identity')
        
        if transform_type == 'uppercase' and isinstance(data, str):
            return data.upper()
        elif transform_type == 'lowercase' and isinstance(data, str):
            return data.lower()
        elif transform_type == 'reverse' and isinstance(data, str):
            return data[::-1]
        else:
            return data
    
    async def _execute_validate_step(self, data: Any, config: Dict[str, Any]) -> Any:
        """Execute a validate step."""
        validation_type = config.get('validation_type', 'basic')
        
        if validation_type == 'not_empty':
            if not data:
                raise ValueError("Data is empty")
        elif validation_type == 'is_string':
            if not isinstance(data, str):
                raise TypeError("Data is not a string")
        
        return data

class TaskScheduler:
    """Task scheduling system."""
    
    def __init__(self):
        self.scheduled_tasks = {}
        self.running = False
    
    def schedule_task(self, task_name: str, task_config: Dict[str, Any], 
                     schedule_time: Optional[datetime] = None) -> None:
        """Schedule a task for execution."""
        self.scheduled_tasks[task_name] = {
            'config': task_config,
            'schedule_time': schedule_time,
            'status': 'scheduled'
        }
        logger.info(f"Scheduled task: {task_name}")
    
    async def execute_scheduled_tasks(self, orchestrator: Orchestrator) -> None:
        """Execute all scheduled tasks."""
        current_time = datetime.now()
        
        for task_name, task_info in self.scheduled_tasks.items():
            if task_info['status'] == 'scheduled':
                schedule_time = task_info['schedule_time']
                
                if schedule_time is None or current_time >= schedule_time:
                    try:
                        result = await orchestrator.orchestrate_task(task_name, task_info['config'])
                        task_info['status'] = 'completed'
                        task_info['result'] = result
                        logger.info(f"Completed task: {task_name}")
                    except Exception as e:
                        task_info['status'] = 'failed'
                        task_info['error'] = str(e)
                        logger.error(f"Failed task {task_name}: {e}")
    
    def get_task_status(self, task_name: str) -> Optional[Dict[str, Any]]:
        """Get task status."""
        return self.scheduled_tasks.get(task_name)

class SystemMonitor:
    """System monitoring and health checks."""
    
    def __init__(self):
        self.metrics = {}
        self.health_checks = {}
    
    def register_health_check(self, name: str, check_function) -> None:
        """Register a health check function."""
        self.health_checks[name] = check_function
        logger.info(f"Registered health check: {name}")
    
    async def run_health_checks(self) -> Dict[str, Any]:
        """Run all health checks."""
        results = {}
        
        for name, check_function in self.health_checks.items():
            try:
                if asyncio.iscoroutinefunction(check_function):
                    result = await check_function()
                else:
                    result = check_function()
                results[name] = {'status': 'healthy', 'result': result}
            except Exception as e:
                results[name] = {'status': 'unhealthy', 'error': str(e)}
        
        return results
    
    def update_metric(self, name: str, value: Any) -> None:
        """Update a metric."""
        self.metrics[name] = {
            'value': value,
            'timestamp': datetime.now()
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all metrics."""
        return self.metrics

# Global instances
orchestrator = Orchestrator()
task_scheduler = TaskScheduler()
system_monitor = SystemMonitor()

if __name__ == "__main__":
    async def main():
        # Example orchestration
        await orchestrator.start()
        
        # Register components
        orchestrator.register_component("processor", lambda x: f"Processed: {x}")
        
        # Schedule and execute task
        task_config = {
            'steps': [
                {'type': 'component', 'name': 'process', 'config': {'component': 'processor'}},
                {'type': 'transform', 'name': 'uppercase', 'config': {'transform_type': 'uppercase'}}
            ],
            'input_data': "hello world"
        }
        
        result = await orchestrator.orchestrate_task("test_task", task_config)
        logger.info(f"Orchestration result: {result}")
        
        await orchestrator.stop()
    
    asyncio.run(main()) 