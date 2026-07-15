# Contributing to tse_tick

Thank you for your interest in contributing.

## Development Setup

```bash
git clone https://github.com/tse-tick/tse_tick.git
cd tse_tick
pip install -e ".[query,dev]"
```

The `[query]` extra is **required to run the test suite**, not optional: it pulls in DuckDB, and
the query tests import `tse_tick.query` directly. Installing only `[dev]` leaves the suite unable
to collect.

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

## Adding a name-translation mapping

The yfinance / Polygon / ccxt → `tse_tick` tables live in
`tse_tick/data/translations.json` (no Python). To add or change a mapping, edit that
file: under the source, add the external name → our name to `functions` or `arguments`
(a list value means several of our names map to one external call; `translate()` returns
the first). Run `pytest tests/test_translate_data.py` and open a PR. End users can
override without editing the package by pointing `TSE_TICK_TRANSLATIONS` at a JSON file
of the same shape.

## Reporting Issues

Open an issue on GitHub with:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Python version and OS
