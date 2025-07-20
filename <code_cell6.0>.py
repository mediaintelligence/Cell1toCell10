# Enhanced Configuration Management System - Refactored
import logging
import asyncio
import os
import json
from typing import Dict, List, Any, Optional, Union, Type
from dataclasses import dataclass, field
from collections import defaultdict, Counter
from abc import ABC, abstractmethod
import traceback

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

# ============================================================================
# CORE CONFIGURATION DATACLASSES
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
        return {
            'model_name': self.model_name,
            'api_key': self.api_key,
            'layer_id': self.layer_id,
            'version': self.version,
            'max_tokens': self.max_tokens,
            'temperature': self.temperature,
            'top_p': self.top_p,
            'context_window': self.context_window,
            'pool_size': self.pool_size,
            'metadata': self.metadata
        }

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

# ============================================================================
# CONFIGURATION VALIDATOR
# ============================================================================

class ConfigurationValidator:
    """Centralized configuration validation"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def validate_model_config(self, config: ModelConfig) -> bool:
        """Validate model configuration"""
        try:
            # Check required fields
            if not config.model_name:
                self.logger.error("Model name is required")
                return False
            
            # Check API key for external models
            external_models = ['claude', 'gpt', 'mistral']
            if any(model in config.model_name.lower() for model in external_models):
                if not config.api_key:
                    self.logger.warning(f"API key missing for external model: {config.model_name}")
            
            # Validate metadata
            if 'role' not in config.metadata:
                self.logger.warning("Role not specified in metadata")
            
            return True
        except Exception as e:
            self.logger.error(f"Model configuration validation failed: {e}")
            return False
    
    def validate_layer_config(self, config: LayerConfig) -> bool:
        """Validate layer configuration"""
        try:
            if not config.agents:
                self.logger.warning(f"No agents specified for layer {config.layer_id}")
            
            return True
        except Exception as e:
            self.logger.error(f"Layer configuration validation failed: {e}")
            return False
    
    def validate_evidence_store_config(self, config: EvidenceStoreConfig) -> bool:
        """Validate evidence store configuration"""
        try:
            required_fields = config.validation_rules.get('required_fields', [])
            if not required_fields:
                self.logger.warning("No required fields specified for evidence store")
            
            return True
        except Exception as e:
            self.logger.error(f"Evidence store configuration validation failed: {e}")
            return False

# ============================================================================
# MAIN CONFIGURATION CLASS
# ============================================================================

class EnhancedConfig:
    """Streamlined configuration management system"""
    
    def __init__(self, 
                 configuration_profile: str = "minimal", 
                 validation_mode: str = "lenient"):
        """Initialize configuration system
        
        Args:
            configuration_profile: Profile determining feature set
            validation_mode: Validation strictness level
        """
        self.configuration_profile = configuration_profile
        self.validation_mode = validation_mode
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Initialize state
        self.initialized = False
        self._initializing = False
        
        # Initialize validator
        self.validator = ConfigurationValidator()
        
        # Configuration containers
        self.model_configs: Dict[str, ModelConfig] = {}
        self.layer_configs: Dict[int, LayerConfig] = {}
        self.evidence_store_config: Optional[EvidenceStoreConfig] = None
        self.communication_config: Optional[CommunicationConfig] = None
        
        # System components (will be initialized later)
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
            
            # Default model configurations
            self.model_configs = {
                'BossAgent': ModelConfig(
                    model_name="claude-3-5-sonnet@20240620",
                    api_key=os.getenv("ANTHROPIC_API_KEY"),
                    layer_id=0,
                    metadata={
                        "role": "coordinator",
                        "capabilities": ["planning", "delegation", "synthesis"],
                        "type": "boss"
                    }
                ),
                'Layer1Agent': ModelConfig(
                    model_name="o1-2024-12-17",
                    api_key=os.getenv("OPENAI_API_KEY"),
                    layer_id=1,
                    context_window=200000,
                    max_tokens=100000,
                    metadata={
                        "role": "processor",
                        "capabilities": ["data_analysis", "transformation"],
                        "type": "worker"
                    }
                ),
                'Layer2Agent': ModelConfig(
                    model_name="gemini-2.0-flash-exp",
                    layer_id=2,
                    context_window=65536,
                    metadata={
                        "role": "reasoner",
                        "capabilities": ["complex_reasoning", "decision_making"],
                        "type": "worker"
                    }
                )
            }
            
            # Default layer configurations
            self.layer_configs = {
                0: LayerConfig(
                    layer_id=0,
                    agents=['BossAgent'],
                    model_name="claude-3-5-sonnet@20240620",
                    metadata={'type': 'coordinator'}
                ),
                1: LayerConfig(
                    layer_id=1,
                    agents=['Layer1Agent'],
                    model_name="o1-2024-12-17",
                    metadata={'type': 'processor'}
                ),
                2: LayerConfig(
                    layer_id=2,
                    agents=['Layer2Agent'],
                    model_name="gemini-2.0-flash-exp",
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
            # Validate configurations
            await self._validate_configurations()
            
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
    
    async def _validate_configurations(self) -> None:
        """Validate all configurations"""
        try:
            # Validate model configurations
            for name, config in self.model_configs.items():
                if not self.validator.validate_model_config(config):
                    raise ValidationError(f"Invalid model configuration: {name}")
            
            # Validate layer configurations
            for layer_id, config in self.layer_configs.items():
                if not self.validator.validate_layer_config(config):
                    raise ValidationError(f"Invalid layer configuration: {layer_id}")
            
            # Validate evidence store configuration
            if self.evidence_store_config:
                if not self.validator.validate_evidence_store_config(self.evidence_store_config):
                    raise ValidationError("Invalid evidence store configuration")
            
            self.logger.info("Configuration validation completed successfully")
            
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            raise
    
    async def _initialize_core_components(self) -> None:
        """Initialize core system components"""
        try:
            # Initialize evidence store
            if self.evidence_store_config:
                # Placeholder for actual evidence store initialization
                self.components['evidence_store'] = {
                    'config': self.evidence_store_config,
                    'initialized': True
                }
                self.initialization_state['evidence_store'] = True
            
            # Initialize communication system
            if self.communication_config:
                # Placeholder for actual communication system initialization
                self.components['communication'] = {
                    'config': self.communication_config,
                    'initialized': True
                }
                self.initialization_state['communication'] = True
            
            # Initialize agent factory
            self.components['agent_factory'] = {
                'model_configs': self.model_configs,
                'layer_configs': self.layer_configs,
                'initialized': True
            }
            self.initialization_state['agent_factory'] = True
            
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
    
    def add_model_config(self, name: str, config: ModelConfig) -> None:
        """Add or update model configuration"""
        if not self.validator.validate_model_config(config):
            raise ValidationError(f"Invalid model configuration: {name}")
        
        self.model_configs[name] = config
        self.logger.info(f"Added model configuration: {name}")
    
    def add_layer_config(self, layer_id: int, config: LayerConfig) -> None:
        """Add or update layer configuration"""
        if not self.validator.validate_layer_config(config):
            raise ValidationError(f"Invalid layer configuration: {layer_id}")
        
        self.layer_configs[layer_id] = config
        self.logger.info(f"Added layer configuration: {layer_id}")
    
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
# USAGE EXAMPLE
# ============================================================================

async def main():
    """Example usage of the enhanced configuration system"""
    try:
        # Create and initialize configuration
        config = await create_enhanced_config(profile="full", validation_mode="strict")
        
        # Get status
        status = config.get_status()
        print(f"Configuration Status: {status}")
        
        # Get model configuration
        boss_config = config.get_model_config('BossAgent')
        if boss_config:
            print(f"Boss Agent Config: {boss_config.to_dict()}")
        
        # Cleanup
        await config.cleanup()
        
    except Exception as e:
        logger.error(f"Example failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
    # END OF FINAL UNIFIED CLASS
    # ============================
