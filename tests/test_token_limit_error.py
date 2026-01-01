"""Test token limit error detection and handling."""
import pytest
from services.llm_client import TokenLimitError


def test_token_limit_error_creation():
    """Test TokenLimitError can be created and contains expected attributes."""
    error = TokenLimitError("Test message", {"code": "context_length_exceeded"})
    assert str(error) == "Test message"
    assert error.details == {"code": "context_length_exceeded"}


def test_token_limit_error_default_message():
    """Test TokenLimitError has appropriate default message."""
    error = TokenLimitError()
    assert "Token limit exceeded" in str(error)
    assert error.details == {}


def test_token_limit_error_is_exception():
    """Test TokenLimitError is a proper exception."""
    error = TokenLimitError("Token limit exceeded")
    assert isinstance(error, Exception)
    with pytest.raises(TokenLimitError):
        raise error
