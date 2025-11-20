# Recipe: High-Throughput Structured Extraction

**Build a production-grade worker that extracts structured data from thousands of text documents.**

## The Problem: "ETL at Scale is Painful"

Imagine you have the [IMDB Dataset](https://huggingface.co/datasets/imdb): 50,000 movie reviews. You need to convert this unstructured text into a SQL database with `sentiment`, `score`, and `summary` columns.

If you loop through them sequentially:
1.  It takes **10+ hours** (at ~1.5s per call).
2.  You hit API **rate limits** immediately.
3.  Some calls fail with **invalid JSON**.
4.  Network glitches crash your entire script.

## The Solution: The "Concurry Extractor"

We will build a robust async worker that:
*   **Runs 100+ calls concurrently** (Estimated Time: <15 minutes).
*   **Respects Rate Limits** for calls AND tokens.
*   **Validates JSON** automatically using Pydantic.
*   **Retries** only on failure (network or validation).

---

## Prerequisites

```bash
pip install concurry litellm instructor pydantic datasets
export OPENROUTER_API_KEY="sk-or-v1-..."
```

## The Implementation

### 1. The Data Schema
First, define the "shape" of your data. This serves as both your validation contract and your database schema.

```python
from pydantic import BaseModel, Field
from typing import List, Literal

class MovieReviewAnalysis(BaseModel):
    """Structured extraction from a movie review."""
    sentiment: Literal["positive", "negative", "mixed"] = Field(
        ..., description="Overall sentiment of the review"
    )
    score: int = Field(
        ..., ge=1, le=10, description="Rating from 1 to 10"
    )
    key_actors: List[str] = Field(
        ..., description="List of actors mentioned (or implied)"
    )
    one_line_summary: str = Field(
        ..., description="Concise summary of the reviewer's opinion"
    )
```

### 2. The Worker
This worker encapsulates the extraction logic, token estimation, and API interaction.

```python
from typing import Any, Dict, List, Type, TypeVar
from concurry import worker, async_gather, LimitSet, RateLimit, CallLimit
import litellm
import os
import asyncio
from functools import partial
from instructor.utils import extract_json_from_codeblock

# Suppress verbose logs from dependencies
litellm.suppress_debug_info = True

# Generic type for Pydantic models
T = TypeVar("T", bound=BaseModel)

def retry_until_parses(result, BaseModelClass: Type[T], **kw) -> bool:
    """
    Predicate: Returns True only if the result can be parsed into the model.
    Used by Concurry to automatically retry malformed LLM responses.
    """
    try:
        # Handle cases where result is a dict (from our worker) or string
        text = result["response"] if isinstance(result, dict) else str(result)
        
        # 1. Extract JSON candidate (handles markdown fences)
        json_str = extract_json_from_codeblock(text)
        # 2. Validate against schema
        BaseModelClass.model_validate_json(json_str)
        return True
    except:
        return False

def estimate_tokens(text: str) -> int:
    """Conservative estimate: ~3 chars per token."""
    return len(text) // 3

@worker(mode="asyncio")
class JsonExtractor(BaseModel):
    model_name: str
    api_key: str
    temperature: float
    max_tokens: int
    timeout: float = 120.0

    async def extract(self, text: str) -> Dict[str, Any]:
        """
        Extracts structured data from a single text input.
        
        Features:
        1. Estimates tokens upfront to reserve capacity.
        2. Acquires limits (blocking if quota exceeded).
        3. Calls LLM with timeout.
        4. Updates limits with actual usage.
        """
        
        # 1. ESTIMATE (5x multiplier for safety against JSON overhead)
        estimated_input = int(estimate_tokens(text) * 5.0) + 100
        
        # 2. ACQUIRE (Blocks here if limits are full)
        requested = {
            "input_tokens": estimated_input, 
            "output_tokens": self.max_tokens,
            "call_count": 1
        }
        
        with self.limits.acquire(requested=requested) as acq:
            try:
                # 3. EXECUTE
                response = await asyncio.wait_for(
                    litellm.acompletion(
                        model=self.model_name,
                        messages=[{
                            "role": "user", 
                            "content": f"Extract this review into JSON: {text}"
                        }],
                        api_key=self.api_key,
                        max_tokens=self.max_tokens,
                        temperature=self.temperature,
                    ),
                    timeout=self.timeout
                )
                
                # 4. UPDATE (Refunds unused tokens)
                acq.update(usage={
                    "input_tokens": response.usage.prompt_tokens,
                    "output_tokens": response.usage.completion_tokens,
                    "call_count": 1
                })
                
                return {
                    "response": response.choices[0].message.content,
                    "usage": response.usage
                }
                
            except Exception as e:
                # On failure, we still consumed input tokens!
                acq.update(usage={"input_tokens": estimated_input, "output_tokens": 0})
                raise e

    async def process_batch(self, texts: List[str]) -> List[str]:
        """Process a batch of texts concurrently."""
        tasks = [self.extract(text) for text in texts]
        # progress=True gives us a nice progress bar!
        results = await async_gather(tasks, progress=True)
        return [r["response"] for r in results]
```

### 3. Running the Pipeline
Now we load the IMDB dataset and process a batch.

```python
from datasets import load_dataset

# 1. Load Data (The Problem Source)
print("Loading IMDB dataset...")
dataset = load_dataset("imdb", split="train[:100]") # Taking 100 for demo
reviews = [x["text"] for x in dataset]

# 2. Configure Worker (The Engine)
extractor = JsonExtractor.options(
    # === Rate Limits ===
    limits=LimitSet(
        limits=[
            # 500 calls/min (Provider Limit)
            CallLimit(window_seconds=60, capacity=500),
            # 10M input tokens/min (Cost Control)
            RateLimit(key="input_tokens", window_seconds=60, capacity=10_000_000),
            # 1M output tokens/min (Cost Control)
            RateLimit(key="output_tokens", window_seconds=60, capacity=1_000_000),
        ],
        mode="asyncio"
    ),
    
    # === Reliability ===
    # If validation fails, retry up to 3 times (4 calls total).
    num_retries={"extract": 3, "*": 0},
    retry_until={
        "extract": partial(retry_until_parses, BaseModelClass=MovieReviewAnalysis),
        "*": None
    }

).init(
    model_name="openrouter/meta-llama/llama-3.1-8b-instruct",
    api_key=os.getenv("OPENROUTER_API_KEY"),
    max_tokens=512,
    temperature=0.9,  # Setting temperature close to 0 will make LLM calls deterministic.
)

# 3. Execute (The "LLM Magic")
print(f"Extracting {len(reviews)} reviews concurrently...")
raw_results = extractor.process_batch(reviews).result()

# 4. Validate & Save (The Result)
print("\nResults:")
for i, raw_json in enumerate(raw_results):
    # This is guaranteed to succeed because of retry_until!
    analysis = MovieReviewAnalysis.model_validate_json(
        extract_json_from_codeblock(raw_json)
    )
    print(f"{i+1}. [{analysis.sentiment.upper()}] {analysis.one_line_summary} (Score: {analysis.score})")

extractor.stop()
```

## Why this Pattern works

| Feature | Why it matters |
| :--- | :--- |
| **`asyncio` Mode** | **Speed**: Instead of waiting 1.5s per review sequentially, we wait 1.5s for *upto 500 reviews in parallel.* |
| **`retry_until`** | **Self-Healing**: If the LLM outputs "I can't do that" or an invalid json, Concurry catches it, waits, and tries again. You get clean data or an exception, never garbage. |
| **`LimitSet`** | **Safety**: We explicitly track `input_tokens` and `output_tokens` because they cost money and hit quotas. The worker pauses automatically when the quota for that window is empty, and resumes when sufficient time has passed. |

## Exercises For You
*   **Scale Up**: Change `mode="ray"` to run this across a cluster of 100 machines.
*   **Add Database**: In `process_batch`, insert `MovieReviewAnalysis` objects directly into Postgres.
