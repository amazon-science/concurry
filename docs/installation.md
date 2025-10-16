# Installation

## Requirements

Concurry requires Python 3.10 or higher.

## Basic Installation

Install Concurry using pip:

```bash
pip install concurry
```

This installs the core library with support for:
- Threading and multiprocessing futures
- Asyncio futures
- Progress bars

## Optional Dependencies

### Ray Support

To use Concurry with Ray for distributed computing:

```bash
pip install concurry[ray]
```

This includes:
- `ray>=2.0.0`
- `cloudpickle>=2.0.0`

### Development Dependencies

For development and testing:

```bash
pip install concurry[dev]
```

This includes:
- `pytest>=6.0`
- `pytest-cov>=2.0`
- `black>=21.0`
- `flake8>=3.8`
- `mypy>=0.900`

### All Dependencies

To install everything:

```bash
pip install concurry[all]
```

## Verify Installation

Verify your installation by running:

```python
import concurry
from concurry.core.future import BaseFuture, wrap_future
from concurry.utils.progress import ProgressBar

print("Concurry installed successfully!")
```

## From Source

To install from source:

```bash
# Clone the repository
git clone https://github.com/amazon-science/concurry.git
cd concurry

# Install in development mode
pip install -e .

# Or with all dependencies
pip install -e .[all]
```

## Next Steps

- [Getting Started](user-guide/getting-started.md) - Learn the basics of Concurry
- [Workers Guide](user-guide/workers.md) - Build stateful concurrent operations
- [Worker Pools Guide](user-guide/pools.md) - Scale with worker pools

