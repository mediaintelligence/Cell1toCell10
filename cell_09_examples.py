# Cell 09: Example Usages and Tests
# This cell contains example usages and test cases for the system

import logging
import asyncio

logger = logging.getLogger(__name__)

async def example_agent_usage(agent_manager):
    logger.info("Running example agent usage...")
    agent = agent_manager.get_agent("agent1")
    if agent:
        await agent.initialize()
        result = await agent.process("example input")
        logger.info(f"Agent result: {result}")
        await agent.cleanup()
    else:
        logger.error("Agent 'agent1' not found")

async def example_workflow_usage(workflow_engine):
    logger.info("Running example workflow usage...")
    workflow_steps = [
        {'type': 'process', 'config': {'processor_type': 'text'}},
        {'type': 'transform', 'config': {'transform_type': 'uppercase'}},
        {'type': 'validate', 'config': {'validation_type': 'not_empty'}}
    ]
    workflow_engine.register_workflow("example_workflow", workflow_steps)
    result = await workflow_engine.execute_workflow("example_workflow", "test input")
    logger.info(f"Workflow result: {result}")

async def example_orchestration(orchestrator):
    logger.info("Running example orchestration...")
    await orchestrator.start()
    orchestrator.register_component("processor", lambda x: f"Processed: {x}")
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

if __name__ == "__main__":
    # Import global instances from other cells
    from cell_05_agents import agent_manager
    from cell_06_specialized import workflow_engine
    from cell_07_orchestration import orchestrator

    async def main():
        await example_agent_usage(agent_manager)
        await example_workflow_usage(workflow_engine)
        await example_orchestration(orchestrator)
    
    asyncio.run(main()) 