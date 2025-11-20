# Workers

Workers are the fundamental building blocks of Concurry. They implement the **Actor Pattern**, allowing you to run stateful operations concurrently across different execution backends (Threads, Processes, Ray clusters) with a single, unified API.

## The Problem: Concurrency is Hard

In standard Python, running code concurrently requires learning completely different libraries:
*   `threading` for I/O bound tasks.
*   `multiprocessing` for CPU bound tasks (to bypass the GIL).
*   `asyncio` for high-concurrency I/O.
*   `ray` for distributed computing.

Each has a different API, different queue mechanisms, and different error handling. Refactoring from threads to processes often means rewriting your entire concurrency logic.

## The Solution: The Concurry Worker

A `Worker` is a class that wraps your logic and abstracts away the execution backend. You write your class **once**, and Concurry runs it anywhere.

### Basic Usage

1.  **Decorate**: Use `@worker` on your class.
2.  **Initialize**: Call `.init()`.
3.  **Override**: Use `.options()` to change configuration.

```python
from concurry import worker
import os

@worker(mode="process")
class IdentityWorker:
    def __init__(self, name: str):
        self.name = name
        
    def who_am_i(self) -> str:
        # Return process ID to prove isolation
        return f"I am {self.name} running in PID {os.getpid()}"

# Initialize the worker (starts the background process)
w = IdentityWorker.init(name="Worker-1")

# Call method (returns a Future)
future = w.who_am_i()

# Get result
print(future.result())

# Cleanup
w.stop()
```

!!! tip "Classes vs Functions"
    `@worker` is for **Classes** (stateful actors). 
    If you just want to parallelize a simple function, use the [`@task` decorator](task-decorator.md) instead.


### Switch Mode
You can override the mode at runtime!
```python
# This creates the new worker instead in Thread mode (instead of Process)
thread_worker = IdentityWorker.options(mode="thread").init(name="Worker-Thread")
print(thread_worker.who_am_i().result())
thread_worker.stop()
```

## Modes: Decision Matrix

Which execution mode should you use? Use this matrix to decide.

| Mode | Best For... | How it works | Overhead | Limitations |
| :--- | :--- | :--- | :--- | :--- |
| **`thread`** | **I/O Bound tasks** (API calls, DB queries, File I/O) | Runs in a Python Thread. Shares memory space. | Low (~1ms) | Limited by GIL (Global Interpreter Lock). Not for CPU heavy work. |
| **`process`** | **CPU Bound tasks** (Math, Image Processing, ML) | Runs in a separate OS Process. Bypasses GIL. | Medium (~20ms) | Arguments/Results must be picklable. Higher memory usage. |
| **`asyncio`** | **High-Concurrency I/O** (Web scraping, Chatbots) | Runs on an AsyncIO event loop. | Low (~10ms) | Best with `async def` methods. Single-core only. |
| **`ray`** | **Distributed Computing** (Scaling across nodes) | Runs as a Ray Actor. | Variable | Requires `ray` installed. Setup overhead. |
| **`sync`** | **Debugging / Testing** | Runs in the main thread (blocking). | None | No concurrency. |

### Lifecycle of a Worker

Understanding the worker lifecycle helps you manage resources effectively.

```text
1. Definition      @worker class MyWorker: ...
       |
2. Configuration   MyWorker.options(mode='thread', ...)
       |
3. Initialization  .init(args)
       |           [Spawns Thread/Process/Actor] -> [Calls __init__]
       |
4. Active State    <-- User calls methods (Non-blocking)
       |           --> Worker processes queue
       |
5. Shutdown        .stop() or Context Manager exit
                   [Cleans up resources] -> [Terminates Thread/Process]
```

## Validation & Type Safety

Concurry supports robust validation. You can choose your level of rigor.

### Option 1: `morphic.Typed` (Recommended)
Best for full lifecycle hooks and strong typing.

```python
from concurry import worker
from morphic import Typed
from pydantic import Field

@worker(mode="thread")
class RobustWorker(Typed):
    # Fields are validated at initialization
    api_key: str = Field(min_length=10)
    retries: int = Field(default=3, ge=0)

    def post_initialize(self):
        print(f"Worker ready with {self.retries} retries")

# Validates inputs before spawning worker
w = RobustWorker.init(api_key="secret_key_123")
```

### Option 2: `pydantic.BaseModel`
Excellent if you already use Pydantic models.

```python
from concurry import worker
from pydantic import BaseModel

@worker(mode="process")
class PydanticWorker(BaseModel):
    name: str
    score: float

w = PydanticWorker.init(name="AI", score=0.99)
```

### Option 3: `@validate` Decorator
Lightweight method-level validation.

```python
from concurry import worker
from morphic import validate

@worker(mode="thread")
class SimpleWorker:
    @validate
    def calculate(self, x: int, y: int) -> int:
        return x + y

w = SimpleWorker.init()
# Strings automatically coerced to int!
result = w.calculate("5", "10").result()  # Returns 15
```

## Composition: Workers within Workers

You can build complex systems by nesting workers. This allows for hierarchical architecture.

```python
@worker(mode="thread")
class DatabaseWorker:
    def query(self, sql): ...

@worker(mode="thread")
class APIWorker:
    def __init__(self):
        # This worker owns a private database worker
        self.db = DatabaseWorker.init()
    
    def process_request(self, user_id):
        # Delegate to internal worker
        # Future unwrapping handles the result automatically!
        user_data = self.db.query(f"SELECT * FROM users WHERE id={user_id}")
        return f"Processed {user_data.result()}"
        
    def stop(self):
        # Clean up child worker
        self.db.stop()
```

## TaskWorker: For Simple Functions

If you don't need state (class members), use `TaskWorker` to run standalone functions.

!!! tip "Pro Tip: The @task Decorator"
    For even simpler usage, use the [`@task` decorator](task-decorator.md) to parallelize functions in one line!

```python
from concurry import TaskWorker

def heavy_computation(x):
    return x ** x

# Create a generic executor
w = TaskWorker.options(mode="process").init()

# Submit arbitrary functions
future = w.submit(heavy_computation, 100)
```

## Manual Subclassing (Advanced)

While the `@worker` decorator is the preferred way to create workers, you can also inherit from the `Worker` class directly. This is useful if you have **other decorators** that interfere with `@worker` or if you are migrating legacy code.

```python
from concurry import Worker

# Legacy style: Explicit inheritance
class LegacyWorker(Worker):
    def run(self):
        return "Done"

# Usage is identical
w = LegacyWorker.options(mode="thread").init()
```

**When to use inheritance:**
*   You use a class decorator that modifies the class structure in a way `@worker` doesn't support.
*   You need explicit MRO (Method Resolution Order) control.
*   You are extending an existing Worker class.

## Best Practices

1.  **Use Context Managers**: Always use `with` to ensure workers are stopped.
    ```python
    @worker(mode="thread")
    class MyWorker: ...

    with MyWorker.init() as w:
        w.do_work()
    # Automatically stopped here
    ```
2.  **Don't Share Mutable State**: In `process` mode, memory is copied, not shared. Changes to `self.x` in one worker won't appear in another.
3.  **Handle Cleanup**: If you define `__del__`, make sure it's exception-safe.
4.  **Start Simple**: Start with `mode="sync"` for debugging, then switch to `thread` or `process`.
