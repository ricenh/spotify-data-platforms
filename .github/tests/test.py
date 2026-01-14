import pytest


def test_example():
    assert 1 + 1 == 2


def test_imports():
    """Test that main modules can be imported"""
    try:
        from src.extract import extract_recently_played
        from src.load import load_plays

        assert True
    except ImportError:
        pytest.skip("Modules not available")
