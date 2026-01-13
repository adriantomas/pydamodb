# Contributing to PydamoDB

Thank you for your interest in contributing to PydamoDB!
This document outlines the guidelines to help you make the best possible contribution.

## Getting Started

1. Fork the repository and create your branch from `main`.

1. Clone your fork and `cd` into the project directory.

1. Install [uv](https://docs.astral.sh/uv/#installation) (if not already installed).

1. Install dependencies:

   ```bash
   uv sync
   ```

1. Activate the virtual environment:

   ```bash
   source .venv/bin/activate
   ```

1. Run tests to verify your setup:

   ```bash
   pytest
   ```

1. Install the pre-commit hooks:

   ```bash
   pre-commit install
   ```

## How to Contribute

1. Write clear, well-tested code:
   - Add or update unit and integration tests for new features or bug fixes.
   - Ensure all tests pass locally before submitting a pull request.
1. Document your changes:
   - Update or add docstrings as needed.
   - If you add or change public APIs, update the relevant documentation.
1. Follow code style guidelines:
   - Use [PEP 8](https://www.python.org/dev/peps/pep-0008/) as the base style guide.
   - Use type annotations where possible.
   - Run `mypy` and `ruff` to check types and formatting.
1. Submit a pull request:
   - Provide a clear description of your changes and the motivation behind them.
   - Reference any related issues or discussions.
   - Be responsive to feedback and requested changes.

## Reporting Issues

When reporting bugs, include a minimal reproducible example and details about your environment.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/3/0/code_of_conduct/). Please be respectful and considerate in all interactions.

## Getting Help

If you have questions, open a [discussion](https://github.com/adriantomas/pydamodb/discussions) or reach out via issues.

______________________________________________________________________

*Happy coding!*
