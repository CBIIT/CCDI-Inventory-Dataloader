"""
Unit tests for bento.common.utils module.
"""
import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from bento.common.utils import get_logger, get_uuid


class TestGetLogger:
    """Test cases for get_logger function."""

    def test_get_logger_returns_logger(self):
        """Test that get_logger returns a logger instance."""
        logger = get_logger('test_logger')
        assert logger is not None
        assert logger.name == 'test_logger'

    @patch.dict(os.environ, {}, clear=True)
    def test_get_logger_no_log_env_var(self):
        """Test logger behavior when BENTO_NO_LOG is set."""
        with patch.dict(os.environ, {'BENTO_NO_LOG': '1'}):
            logger = get_logger('test_logger')
            assert logger is not None

    @patch.dict(os.environ, {'BENTO_LOG_LEVEL': 'ERROR'})
    def test_get_logger_custom_log_level(self):
        """Test logger with custom log level from environment."""
        logger = get_logger('test_logger')
        assert logger.level <= 40  # ERROR level


class TestGetUuid:
    """Test cases for get_uuid function."""

    def test_get_uuid_returns_string(self):
        """Test that get_uuid returns a string."""
        uuid = get_uuid('test.domain.com', 'file', 'test_signature')
        assert isinstance(uuid, str)
        assert len(uuid) > 0

    def test_get_uuid_deterministic(self):
        """Test that get_uuid returns the same UUID for same inputs."""
        domain = 'test.domain.com'
        node_type = 'file'
        signature = 'test_signature'
        
        uuid1 = get_uuid(domain, node_type, signature)
        uuid2 = get_uuid(domain, node_type, signature)
        
        assert uuid1 == uuid2

    def test_get_uuid_different_signatures(self):
        """Test that different signatures produce different UUIDs."""
        domain = 'test.domain.com'
        node_type = 'file'
        
        uuid1 = get_uuid(domain, node_type, 'signature1')
        uuid2 = get_uuid(domain, node_type, 'signature2')
        
        assert uuid1 != uuid2

    def test_get_uuid_different_domains(self):
        """Test that different domains produce different UUIDs."""
        node_type = 'file'
        signature = 'test_signature'
        
        uuid1 = get_uuid('domain1.com', node_type, signature)
        uuid2 = get_uuid('domain2.com', node_type, signature)
        
        assert uuid1 != uuid2

