# Agent Instructions

Use `uv` for Python environment and package management in this repository.

## Required conventions

- Do not use `pip install ...`.
- Do not use bare `python ...` for project tasks.
- Use `uv run ...` for running commands.
- Use `uv add ...` for adding dependencies.
- Use `uv lock` after dependency changes.
- Use `uv sync` to install/update the environment from lockfile.

## Common commands

- Install/sync deps: `uv sync`
- Add runtime dependency: `uv add <package>`
- Add dev dependency: `uv add --dev <package>`
- Refresh lockfile: `uv lock`
- Run tests: `uv run pytest`
- Run linter: `uv run ruff check .`
- Run app: `uv run python main.py run`
