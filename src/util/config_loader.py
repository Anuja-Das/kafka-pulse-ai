# Loads application configuration from YAML and environment variables.

import os
import re
import yaml
from pathlib import Path
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parents[2]

ENV_PATH = project_root / ".env"
YAML_PATH = project_root / "resources" / "application.yml"

# Load environment variables from .env
load_dotenv(dotenv_path=ENV_PATH)

def _load_config():
    """
    Loads application.yml and replaces ${ENV_VAR} placeholders
    using values from .env or system environment.
    """
    with open(YAML_PATH, "r") as file:
        content = file.read()

    # Replace ${VAR} placeholders
    pattern = re.compile(r"\$\{([^}]+)}")
    content = pattern.sub(lambda m: os.environ[m.group(1)], content)

    return yaml.safe_load(content)

# Load config only once and expose as module-level variable
config = _load_config()
