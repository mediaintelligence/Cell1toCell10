# Cell 02: Imports and Dependencies
# This cell handles all necessary imports and dependency management

import os
import sys
import logging
from pathlib import Path

# Standard library imports
import json
import yaml
import asyncio
import datetime
from typing import Dict, List, Any, Optional, Union

# Data processing imports
import pandas as pd
import numpy as np

# AI/ML imports
try:
    import torch
    import transformers
    from transformers import AutoTokenizer, AutoModel
except ImportError:
    logging.warning("PyTorch and transformers not available")

# LangChain imports
try:
    from langchain.llms.base import LLM
    from langchain.chat_models import ChatOpenAI, ChatAnthropic
    from langchain.embeddings import OpenAIEmbeddings, HuggingFaceEmbeddings
    from langchain.vectorstores import Chroma, Pinecone
    from langchain.document_loaders import TextLoader, PDFLoader
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    from langchain.chains import LLMChain, ConversationChain
    from langchain.prompts import PromptTemplate
    from langchain.memory import ConversationBufferMemory
except ImportError:
    logging.warning("LangChain not available")

# Web and API imports
try:
    import requests
    import aiohttp
    from fastapi import FastAPI
    from pydantic import BaseModel
except ImportError:
    logging.warning("Web/API libraries not available")

# Database imports
try:
    import sqlite3
    import psycopg2
    from sqlalchemy import create_engine
except ImportError:
    logging.warning("Database libraries not available")

# Utility imports
import hashlib
import uuid
import pickle
import base64

logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if all required dependencies are available."""
    logger.info("Checking dependencies...")
    
    required_packages = [
        "pandas",
        "numpy",
        "requests"
    ]
    
    optional_packages = [
        "torch",
        "transformers",
        "langchain",
        "fastapi",
        "sqlalchemy"
    ]
    
    missing_required = []
    missing_optional = []
    
    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"✓ {package}")
        except ImportError:
            missing_required.append(package)
            logger.error(f"✗ {package} (required)")
    
    for package in optional_packages:
        try:
            __import__(package)
            logger.info(f"✓ {package} (optional)")
        except ImportError:
            missing_optional.append(package)
            logger.warning(f"✗ {package} (optional)")
    
    if missing_required:
        raise ImportError(f"Missing required packages: {missing_required}")
    
    logger.info("Dependency check completed")

if __name__ == "__main__":
    check_dependencies() 