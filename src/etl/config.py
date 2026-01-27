from typing import Any, Dict


def load_config() -> Dict[str, Any]:
    """
    Placeholder config loader.

    Later this will come from:
    - Snowflake config table
    - YAML file
    - Environment variables
    - UI editor (web app)
    """
    return {
        "pipeline_name": "example_pipeline",
        "source_type": "api",
        "load_type": "full",
        "enabled": True,
    }
