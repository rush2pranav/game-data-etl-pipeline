
"""
Valorant Game Data ETL Pipeline
-----------------------------------
A production-style ETL (Extract - Transform - Load) pipeline that:
  1. EXTRACTS game data from the Valorant API
  2. TRANSFORMS raw JSON into clean, analysis ready tables
  3. LOADS results into a SQLite database
  4. Runs on a configurable schedule (For Default, I am setting it for every 6 hours)

Proudly designed to run standalone or inside a Docker container :) (still learning so forgive all the rookue mistakes)
"""

import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
import schedule

# CONFIGURATION SECTION

def load_config():
    """Load pipeline configuration from JSON file."""
    config_paths = [
        '/app/config/pipeline_config.json',       # this is docker path
        'config/pipeline_config.json',             # this is the local path
    ]
    for path in config_paths:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    raise FileNotFoundError("pipeline_config.json not found")


def setup_logging(config):
    """Configure logging to both file and console."""
    log_cfg = config.get('logging', {})
    level = getattr(logging, log_cfg.get('level', 'INFO'))

    # this is for determining the log file path
    log_file = log_cfg.get('log_file', 'data/etl_pipeline.log')
    if not os.path.exists('/app') and 'local_log_file' in log_cfg:
        log_file = log_cfg['local_log_file']

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('etl_pipeline')