# Contributing to tse_tick

Thank you for your interest in contributing.

## Development Setup

```bash
git clone https://github.com/tse-tick/tse_tick.git
cd tse_tick
pip install -e ".[dev]"
```

## Code Style

This project uses:
- **black** for formatting (`black tse_tick/`)
- **flake8** for linting (`flake8 tse_tick/`)
- **mypy** for type checking (`mypy tse_tick/`)

Line length is 100 characters.

## Running Tests

```bash
pytest tests/ -v
```

## Pull Request Guidelines

- One feature or fix per PR
- Include tests for new functionality
- Run `black`, `flake8`, and `mypy` before submitting
- Update CHANGELOG.md with a summary of your change

## Reporting Issues

Open an issue on GitHub with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Python version and OS
