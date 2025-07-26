# Cell 01: Setup and Initialization
# This cell handles the basic setup and initialization of the system

import os
import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Environment setup
def setup_environment():
    """Initialize the environment and basic configurations."""
    logger.info("Setting up environment...")
    
    # Create necessary directories
    directories = [
        "logs",
        "data",
        "models",
        "outputs",
        "temp"
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        logger.info(f"Created directory: {directory}")
    
    # Set environment variables
    os.environ.setdefault("PYTHONPATH", str(project_root))
    os.environ.setdefault("LOG_LEVEL", "INFO")
    
    logger.info("Environment setup completed")

if __name__ == "__main__":
    setup_environment() 