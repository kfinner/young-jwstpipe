import os
from pathlib import Path

import yaml


CONFIG_ENV_VAR = "YOUNG_PIPELINE_CONFIG"


def get_config_path(default_path="config.yaml"):
    """Return the active pipeline config path, allowing an environment override."""
    env_path = os.environ.get(CONFIG_ENV_VAR)
    if env_path:
        return Path(env_path).expanduser().resolve()

    return Path(default_path).expanduser().resolve()


def load_config(default_path="config.yaml"):
    """Load the active pipeline config and return both the config and its path."""
    config_path = get_config_path(default_path)
    with config_path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}

    return config, config_path
