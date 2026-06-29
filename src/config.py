import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = "localhost:9092"

API_KEY = os.getenv("API_KEY")
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
API_VERSION = os.getenv("API_VERSION", "2024-02-01")
AZURE_DEPLOYMENT = os.getenv("AZURE_DEPLOYMENT", "gpt-4o")
