import logging
import os
from pathlib import Path
from pydantic import BaseModel
from strictyaml import YAML, load

logger = logging.getLogger(__name__)

# Project Directories
PROJECT_PATH = os.getcwd()
CONFIG_FILE_PATH = "config.yaml"


class SalesForceConfig(BaseModel):
    """SalesForce config"""

    salesforce_domain: str
    subdomain: str


class OktaConfig(BaseModel):
    """Okta config"""

    okta_domain: str
    private_key: str
    client_id: str
    scopes: str
    ciam_api_url: str
    locale: str


class Config(BaseModel):
    """Master config object."""

    sfmc_config: SalesForceConfig
    okta_config: OktaConfig


def find_config_file() -> Path:
    """Locate the configuration file."""
    if CONFIG_FILE_PATH in os.listdir(PROJECT_PATH):
        return os.path.join(PROJECT_PATH, CONFIG_FILE_PATH)
    raise Exception(f"Config not found at {CONFIG_FILE_PATH!r}")


def fetch_config_from_yaml(cfg_path: Path = None) -> YAML:
    """Parse YAML containing the package configuration."""
    if not cfg_path:
        cfg_path = find_config_file()

    if cfg_path:
        with open(cfg_path, "r") as conf_file:
            parsed_config = load(conf_file.read())
            return parsed_config
    raise OSError(f"Did not find config file at path: {cfg_path}")


def create_and_validate_config(parsed_config: YAML = None) -> Config:
    """Run validation on config values."""
    if parsed_config is None:
        parsed_config = fetch_config_from_yaml()

    # specify the data attribute from the strictyaml YAML type.
    _config = Config(
        sfmc_config=SalesForceConfig(**parsed_config.data),
        okta_config=OktaConfig(**parsed_config.data),
    )
    logger.info("Config file read!.")

    return _config


config = create_and_validate_config()
