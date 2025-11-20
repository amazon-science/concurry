# Gallery & Recipes

**Real-world patterns for solving common concurrency problems.**

This gallery contains complete, copy-pasteable examples of production-grade systems built with Concurry.

## Recipes

| Recipe | Features Used | Best For... |
| :--- | :--- | :--- |
| [**Robust LLM Pipeline**](llm-with-structured-parsing.md) | `asyncio` mode, `RateLimit`, `retry_until`, `LimitSet` | Building AI applications that need to respect API quotas, validate JSON output, and retry failures automatically. |
| **Web Crawler** (Coming Soon) | `thread` mode, `RateLimit`, `gather` | Scraping thousands of pages while respecting `robots.txt` and server load. |
| **Data Processing Pipeline** (Coming Soon) | `process` mode, `TaskWorker`, `map` | CPU-intensive ETL jobs that need to bypass the GIL. |
| **Distributed Training** (Coming Soon) | `ray` mode, `Worker`, `GPU` | Scaling ML training across a cluster of machines. |

---

## Contributing

Have a cool pattern? [Open a PR](https://github.com/amazon-science/concurry) to add it to the gallery!
