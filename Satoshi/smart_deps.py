#!/usr/bin/env python3
"""
Smart dependency management for Satoshi
Handles optional dependencies gracefully
"""

import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from .satoshi_config file"""
    config = {}
    config_file = Path(__file__).parent / '.satoshi_config'
    
    if config_file.exists():
        try:
            with open(config_file, encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        config[key] = value.lower() == 'true' if value.lower() in ('true', 'false') else value
        except (OSError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to read config file {config_file}: {e}. Using default configuration.")
    return config

def should_show_warnings():
    """Check if optional dependency warnings should be shown"""
    config = load_config()
    return config.get('SHOW_OPTIONAL_WARNINGS', True)

def suppress_optional_warnings():
    """Suppress optional dependency warnings in production"""
    if not should_show_warnings():
        import warnings
        warnings.filterwarnings('ignore', message='.*not installed.*')

# Initialize smart warnings
suppress_optional_warnings()
