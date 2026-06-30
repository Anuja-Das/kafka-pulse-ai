# Flat constants consumed by agent.py and kafka_mcp_server.py.
# Source of truth is resources/application.yml with ${ENV_VAR} substitution from .env.
from util.config_loader import config as _yml

KAFKA_BROKER     = _yml["kafka_broker"]
API_KEY          = _yml["api_key"]
AZURE_ENDPOINT   = _yml["azure_endpoint"]
API_VERSION      = _yml["api_version"]
AZURE_DEPLOYMENT = _yml["azure_deployment"]
