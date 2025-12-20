"""Jinja2 environment for publisher outputs."""
from jinja2 import Environment, FileSystemLoader


def build_environment() -> Environment:
    """Return a Jinja2 environment rooted at templates/."""
    return Environment(loader=FileSystemLoader("templates"))


env = build_environment()

__all__ = ["env", "build_environment"]
