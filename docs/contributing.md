# Contributing

Thank you for your interest in contributing to Concurry! This document provides guidelines and instructions for contributing.

## Code of Conduct

Please read and follow our [Code of Conduct](../CODE_OF_CONDUCT.md).

## Development Setup

### Prerequisites

- Python 3.10 or higher
- Git

### Setting Up Development Environment

1. **Fork and clone the repository:**

```bash
git clone https://github.com/YOUR_USERNAME/concurry.git
cd concurry
```

2. **Create a virtual environment:**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install development dependencies:**

```bash
pip install -e .[dev]
```

4. **Install documentation dependencies (optional):**

```bash
pip install -e .[docs]
```

## Running Tests

Run the test suite with pytest:

```bash
pytest
```

Run tests with coverage:

```bash
pytest --cov=concurry --cov-report=html
```

## Code Style

We use several tools to maintain code quality:

### Black (Code Formatting)

Format your code with Black:

```bash
black src/ tests/
```

### Flake8 (Linting)

Check for linting issues:

```bash
flake8 src/ tests/
```

### Ruff (Fast Linting and Formatting)

We also support Ruff for faster linting:

```bash
ruff check src/ tests/
ruff format src/ tests/
```

### MyPy (Type Checking)

Run type checking:

```bash
mypy src/
```

## Documentation

### Building Documentation Locally

To build and view documentation locally:

```bash
# Install docs dependencies
pip install -e .[docs]

# Serve documentation locally
mkdocs serve
```

Then open http://127.0.0.1:8000 in your browser.

### Writing Documentation

- Use Google-style docstrings for all public APIs
- Include type hints in function signatures
- Provide examples in docstrings where appropriate
- Update relevant `.md` files in the `docs/` directory

Example docstring format:

```python
def example_function(param1: str, param2: int = 0) -> bool:
    """Brief description of the function.
    
    Longer description providing more context about what the function
    does and how it should be used.
    
    Args:
        param1: Description of param1
        param2: Description of param2 (default: 0)
    
    Returns:
        Description of return value
    
    Raises:
        ValueError: When param2 is negative
    
    Example:
        ```python
        result = example_function("test", param2=42)
        print(result)
        ```
    """
    pass
```

## Pull Request Process

1. **Create a new branch:**

```bash
git checkout -b feature/your-feature-name
```

2. **Make your changes:**
   - Write clear, concise commit messages
   - Add tests for new functionality
   - Update documentation as needed

3. **Ensure all tests pass:**

```bash
pytest
black src/ tests/
flake8 src/ tests/
mypy src/
```

4. **Push your changes:**

```bash
git push origin feature/your-feature-name
```

5. **Create a pull request:**
   - Provide a clear description of the changes
   - Reference any related issues
   - Ensure CI/CD checks pass

## Pull Request Guidelines

- **Keep PRs focused:** One feature or fix per PR
- **Write tests:** All new code should have corresponding tests
- **Update docs:** Document new features and API changes
- **Follow conventions:** Maintain consistency with existing code
- **Be responsive:** Address review comments promptly

## Reporting Issues

When reporting issues, please include:

- Python version
- Concurry version
- Operating system
- Minimal code example to reproduce the issue
- Expected behavior
- Actual behavior
- Full error traceback (if applicable)

## Feature Requests

We welcome feature requests! Please:

- Check if the feature has already been requested
- Provide a clear use case
- Explain why the feature would be valuable
- Consider submitting a PR if you can implement it

## Areas for Contribution

Looking for ways to contribute? Consider:

- **Documentation:** Improve examples, fix typos, add tutorials
- **Tests:** Increase test coverage, add edge cases
- **Performance:** Optimize existing code, add benchmarks
- **Features:** Implement requested features
- **Bug fixes:** Fix reported issues

## Questions?

If you have questions about contributing:

- Open a [Discussion](https://github.com/adivekar-utexas/concurry/discussions)
- Review existing [Issues](https://github.com/adivekar-utexas/concurry/issues)
- Check the [Documentation](https://adivekar-utexas.github.io/concurry/)

Thank you for contributing to Concurry!

