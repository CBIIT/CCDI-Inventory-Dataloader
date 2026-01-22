"""
Pytest configuration and shared fixtures for CCDI Portal ETL tests.
"""
import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest


@pytest.fixture
def project_root_path():
    """Return the project root directory path."""
    return Path(__file__).parent.parent


@pytest.fixture
def config_dir(project_root_path):
    """Return the config directory path."""
    return project_root_path / "config"


@pytest.fixture
def data_dir(project_root_path):
    """Return the data directory path."""
    return project_root_path / "data"


@pytest.fixture
def tmp_dir(project_root_path):
    """Return the tmp directory path."""
    return project_root_path / "tmp"


@pytest.fixture(autouse=True)
def reset_env_vars(monkeypatch):
    """Reset environment variables before each test."""
    # Save original values
    env_backup = {}
    test_env_vars = [
        'BENTO_LOG_LEVEL',
        'BENTO_NO_LOG',
        'BENTO_LOG_FILE_PREFIX',
        'BENTO_LOG_FOLDER',
        'APP_NAME',
    ]
    
    for var in test_env_vars:
        env_backup[var] = os.environ.get(var)
        monkeypatch.delenv(var, raising=False)
    
    yield
    
    # Restore original values after test
    for var, value in env_backup.items():
        if value is not None:
            monkeypatch.setenv(var, value)

