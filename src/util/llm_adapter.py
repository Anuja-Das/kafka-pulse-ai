from openai import AzureOpenAI
from src.util.config_loader import config

client = AzureOpenAI(
    api_key=config.get("api_key"),
    azure_endpoint=config.get("azure_endpoint"),
    api_version=config.get("api_version")
)

def call_llm(messages):
    response = client.chat.completions.create(
        model=config.get("azure_deployment"),
        messages=messages,
        response_format={"type": "json_object"}
    )
    return response.choices[0].message.content
