
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

# EXTRACTION WHERE WE WILL BE PULLING THE DATA FROM THE VALORANT API

class Extractor:
    """Handles all API communication and raw data extraction"""

    def __init__(self, config):
        self.base_url = config['api']['base_url']
        self.language = config['api']['language']
        self.delay = config['api']['request_delay_seconds']
        self.timeout = config['api']['timeout_seconds']
        self.logger = logging.getLogger('etl_pipeline.extract')

    def fetch_endpoint(self, endpoint):
        """Fetch a single API endpoint with error handling and retry"""
        url = f"{self.base_url}/{endpoint}"
        params = {"language": self.language}

        for attempt in range(3):
            try:
                self.logger.info(f"Fetching: {endpoint} (attempt {attempt + 1})")
                resp = requests.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                data = resp.json()

                if data.get("status") == 200:
                    records = data.get("data", [])
                    self.logger.info(f"  -> Retrieved {len(records)} records from {endpoint}")
                    return records
                else:
                    self.logger.warning(f"  -> API returned status {data.get('status')}")
                    return []

            except requests.Timeout:
                self.logger.warning(f"  -> Timeout on {endpoint}, retrying...")
                time.sleep(2 ** attempt)
            except requests.RequestException as e:
                self.logger.error(f"  -> Request failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    return []

        return []

    def extract_all(self, endpoints):
        """Extract data from all configured endpoints"""
        raw_data = {}
        for endpoint in endpoints:
            raw_data[endpoint] = self.fetch_endpoint(endpoint)
            time.sleep(self.delay)
        return raw_data
    
   # CLEANING AND STRUCTURING THE RAW DATA

class Transformer:
    """Transforms raw API JSON into clean data frames"""

    def __init__(self):
        self.logger = logging.getLogger('etl_pipeline.transform')

    def transform_all(self, raw_data):
        """Transform all extracted data into data frames"""
        transformed = {}

        if 'agents' in raw_data:
            transformed['agents'] = self._transform_agents(raw_data['agents'])
            transformed['abilities'] = self._transform_abilities(raw_data['agents'])

        if 'weapons' in raw_data:
            transformed['weapons'] = self._transform_weapons(raw_data['weapons'])
            transformed['weapon_damage'] = self._transform_damage_ranges(raw_data['weapons'])

        if 'maps' in raw_data:
            transformed['maps'] = self._transform_maps(raw_data['maps'])

        if 'gamemodes' in raw_data:
            transformed['gamemodes'] = self._transform_gamemodes(raw_data['gamemodes'])

        for name, df in transformed.items():
            self.logger.info(f"  Transformed: {name} -> {len(df)} rows, {len(df.columns)} columns")

        return transformed

    def _transform_agents(self, raw):
        agents = []
        for a in raw:
            if not a.get('isPlayableCharacter', False):
                continue
            agents.append({
                'uuid': a.get('uuid', ''),
                'name': a.get('displayName', ''),
                'role': a.get('role', {}).get('displayName', 'Unknown') if a.get('role') else 'Unknown',
                'description': (a.get('description', '') or '')[:500],
                'icon_url': a.get('displayIcon', ''),
            })
        return pd.DataFrame(agents)

    def _transform_abilities(self, raw):
        abilities = []
        for a in raw:
            if not a.get('isPlayableCharacter', False):
                continue
            name = a.get('displayName', '')
            role = a.get('role', {}).get('displayName', 'Unknown') if a.get('role') else 'Unknown'
            for ab in a.get('abilities', []):
                abilities.append({
                    'agent_name': name,
                    'agent_role': role,
                    'slot': ab.get('slot', ''),
                    'ability_name': ab.get('displayName', ''),
                    'description': (ab.get('description', '') or '')[:500],
                })
        return pd.DataFrame(abilities)

    def _transform_weapons(self, raw):
        weapons = []
        for w in raw:
            stats = w.get('weaponStats') or {}
            shop = w.get('shopData') or {}
            weapons.append({
                'uuid': w.get('uuid', ''),
                'name': w.get('displayName', ''),
                'category': (w.get('category', '') or '').replace('EEquippableCategory::', ''),
                'cost': shop.get('cost', 0),
                'fire_rate': stats.get('fireRate', 0),
                'magazine_size': stats.get('magazineSize', 0),
                'reload_time': stats.get('reloadTimeSeconds', 0),
                'equip_time': stats.get('equipTimeSeconds', 0),
                'first_bullet_accuracy': stats.get('firstBulletAccuracy', 0),
                'wall_penetration': stats.get('wallPenetration', ''),
                'icon_url': w.get('displayIcon', ''),
            })
        return pd.DataFrame(weapons)

    def _transform_damage_ranges(self, raw):
        ranges = []
        for w in raw:
            stats = w.get('weaponStats') or {}
            for i, dr in enumerate(stats.get('damageRanges', []) or []):
                ranges.append({
                    'weapon_name': w.get('displayName', ''),
                    'range_index': i,
                    'range_start': dr.get('rangeStartMeters', 0),
                    'range_end': dr.get('rangeEndMeters', 0),
                    'head_damage': dr.get('headDamage', 0),
                    'body_damage': dr.get('bodyDamage', 0),
                    'leg_damage': dr.get('legDamage', 0),
                })
        return pd.DataFrame(ranges)

    def _transform_maps(self, raw):
        maps = []
        for m in raw:
            callouts = m.get('callouts') or []
            maps.append({
                'uuid': m.get('uuid', ''),
                'name': m.get('displayName', ''),
                'coordinates': m.get('coordinates', ''),
                'num_callouts': len(callouts),
                'splash_url': m.get('splash', ''),
            })
        return pd.DataFrame(maps)

    def _transform_gamemodes(self, raw):
        modes = []
        for mode in raw:
            modes.append({
                'uuid': mode.get('uuid', ''),
                'name': mode.get('displayName', ''),
                'duration': mode.get('duration', ''),
                'allows_timeouts': mode.get('allowsMatchTimeouts', False),
            })
        return pd.DataFrame(modes)