# Recipe: Multi-Account AWS Bedrock with Load Balancing

**Distribute thousands of LLM calls across multiple AWS accounts and regions, with per-endpoint rate limiting.**

## The Problem: "One Account Isn't Enough"

You need to run 10,000 Claude calls on AWS Bedrock. But a single account in a single region gives you maybe 250 requests per minute. At that rate, you're looking at 40 minutes of wall time — and that's *if* you never hit a throttle.

In practice:
1.  A single account/region has **hard RPM quotas** you can't raise quickly.
2.  Different regions have **different limits** (e.g. eu-west-3 might only allow 50 RPM).
3.  You need **IAM role chaining** to access accounts, and each region needs its own client.
4.  If you naively round-robin without tracking limits, you'll get **throttled and waste retries**.

## The Solution: LimitPool + Multi-Account Worker

We'll build a system that:
*   **Spreads load across N accounts × M regions** using a `LimitPool`.
*   **Enforces per-endpoint RPM and token limits** so you never get throttled.
*   **Automatically selects** the next available account/region via round-robin.
*   **Supports text-only and vision calls** with a single `invoke()` method.

---

## Prerequisites

```bash
pip install concurry boto3 pydantic
```

You'll also need IAM roles configured for cross-account Bedrock access (see below).

## Background: Why IAM Role Chaining?

AWS Bedrock quotas are per-account, per-region. To multiply your throughput, you call Bedrock from *multiple* AWS accounts. But your code runs under a single identity (your laptop credentials, an EC2 instance profile, etc.). To make API calls *as if* you were in another account, you use **STS AssumeRole**.

Here's the flow:

```
Your credentials (Account A)
    │
    ▼  STS AssumeRole
Base Role (Account A)          ← optional "hop" role in your own account
    │
    ▼  STS AssumeRole
Target Role (Account B)        ← has Bedrock permissions in Account B
    │
    ▼  bedrock-runtime client
Bedrock endpoint (Account B, us-east-1)
```

**Why two hops (role chaining)?** In many orgs, your personal credentials can't directly assume roles in other accounts. Instead, you first assume a "base role" in your own account that *is* trusted by the target accounts. This is standard cross-account access — the base role's trust policy allows your identity, and the target role's trust policy allows the base role.

If your setup is simpler (direct assume into target accounts), just set `base_role_arn = None` and the worker will skip the first hop.

Each target account + region combination is a separate Bedrock endpoint with its own RPM quota. The worker creates one `boto3` client per unique (region, role_arn, base_role_arn) tuple and caches it for reuse.

## The Implementation

### 1. Define Your Account Configs

Each config entry represents one account + region endpoint with its own rate limits.

```python
from concurry import Worker, LimitSet, LimitPool, RateLimit, CallLimit, RateLimitAlgorithm
from typing import Optional, Dict, List

# Optional: base role for role-chaining (set to None if not needed)
BASE_ROLE_ARN = "arn:aws:iam::123456789012:role/MyBaseRole"

ACCOUNT_IDS = [
    "111111111111",
    "222222222222",
    "333333333333",
]

# Build configs programmatically — each account gets 4 endpoints
bedrock_configs = []
for account_id in ACCOUNT_IDS:
    role_arn = f"arn:aws:iam::{account_id}:role/BedrockAccessRole"
    for region, prefix, rpm in [
        ("us-east-1", "us", 125),
        ("eu-west-3", "eu", 125),
        ("ap-south-1", "apac", 125),
        ("us-east-1", "global", 250),  # "global" prefix, higher RPM
    ]:
        bedrock_configs.append({
            "region": region,
            "account_id": account_id,
            "model_id": f"{prefix}.anthropic.claude-opus-4-5-20251101-v1:0",
            "role_arn": role_arn,
            "base_role_arn": BASE_ROLE_ARN,
            "rpm": rpm,
        })

print(f"Total endpoints: {len(bedrock_configs)}")
# 3 accounts × 4 regions = 12 endpoints
```

### 2. Create the LimitPool

Each endpoint gets its own `LimitSet` with RPM and token limits. The `LimitPool` wraps them all and distributes calls via round-robin.

```python
limitsets = []
for config in bedrock_configs:
    limitset = LimitSet(
        limits=[
            # Per-endpoint RPM (from config)
            CallLimit(
                window_seconds=60,
                algorithm=RateLimitAlgorithm.SlidingWindow,
                capacity=config["rpm"],
            ),
            # Per-endpoint token budget
            RateLimit(
                key="total_tokens",
                window_seconds=60,
                algorithm=RateLimitAlgorithm.TokenBucket,
                capacity=50_000,
            ),
        ],
        shared=True,
        mode="thread",
        config=config,  # Metadata travels with the LimitSet
    )
    limitsets.append(limitset)

limit_pool = LimitPool(
    limit_sets=limitsets,
    load_balancing="round_robin",
    worker_index=0,
)
```

The key insight: each `LimitSet` carries a `config` dict. When the worker acquires a limit, it reads `config` to know *which* account/region/model to call. The pool handles selection; the worker just reads the config it gets.

### 3. The Worker

```python
import boto3
import json
from botocore.config import Config as BotoConfig

class BedrockWorker(Worker):
    """Worker for calling Claude on AWS Bedrock across multiple accounts."""

    def __init__(self):
        self._clients = {}

    def _get_client(self, region: str, role_arn: str, base_role_arn: Optional[str] = None):
        """Get or create a Bedrock client for the given region and role.
        
        Uses STS AssumeRole to get temporary credentials for the target account.
        If base_role_arn is provided, chains through it first (see "Why IAM Role
        Chaining?" above).
        
        Clients are cached by (region, role_arn, base_role_arn) so we only
        assume roles once per unique endpoint.
        """
        key = (region, role_arn, base_role_arn)
        if key not in self._clients:
            import boto3

            if base_role_arn:
                # Step 1: Assume base role in your own account
                sts = boto3.client("sts")
                base_creds = sts.assume_role(
                    RoleArn=base_role_arn,
                    RoleSessionName="bedrock-base",
                )["Credentials"]
                # Step 2: From base role, assume target role in remote account
                sts2 = boto3.client(
                    "sts",
                    aws_access_key_id=base_creds["AccessKeyId"],
                    aws_secret_access_key=base_creds["SecretAccessKey"],
                    aws_session_token=base_creds["SessionToken"],
                )
                target_creds = sts2.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName="bedrock-target",
                )["Credentials"]
            else:
                # Single hop: assume target role directly
                sts = boto3.client("sts")
                target_creds = sts.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName="bedrock-target",
                )["Credentials"]

            self._clients[key] = boto3.client(
                service_name="bedrock-runtime",
                region_name=region,
                aws_access_key_id=target_creds["AccessKeyId"],
                aws_secret_access_key=target_creds["SecretAccessKey"],
                aws_session_token=target_creds["SessionToken"],
                config=BotoConfig(read_timeout=600),
            )
        return self._clients[key]

    def invoke(
        self,
        *,
        prompt: str,
        image_base64: Optional[str] = None,
        max_tokens: int = 4096,
        uid: Optional[str] = None,
        est_input_tokens: int = 10_000,
        temperature: float = 0.1,
        system: Optional[str] = None,
    ) -> dict:
        """Invoke Claude with optional image, respecting per-endpoint limits.

        Args:
            prompt: Text prompt
            image_base64: Optional base64-encoded image (PNG)
            max_tokens: Maximum output tokens
            uid: Optional tracking identifier
            est_input_tokens: Estimated input tokens for rate limiting
            temperature: Sampling temperature
            system: Optional system prompt
        """
        with self.limits.acquire(
            requested={"total_tokens": est_input_tokens + max_tokens}
        ) as acq:
            # The pool selected an endpoint — read its config
            config = acq.config
            region = config["region"]
            model_id = config["model_id"]
            role_arn = config["role_arn"]
            base_role_arn = config.get("base_role_arn")

            client = self._get_client(region, role_arn, base_role_arn)

            # Build content: text-only or text + image
            content = []
            if image_base64 is not None:
                content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": image_base64,
                    }
                })
            content.append({"type": "text", "text": prompt})

            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "temperature": temperature,
                "messages": [{"role": "user", "content": content}],
            }
            if system is not None:
                request_body["system"] = system

            try:
                response = client.invoke_model(
                    modelId=model_id,
                    body=json.dumps(request_body),
                )
                response_body = json.loads(response["body"].read())
                usage = response_body.get("usage", {})
                input_tokens = usage["input_tokens"]
                output_tokens = usage["output_tokens"]

                # Update with actual usage (refunds over-estimated tokens)
                acq.update(usage={
                    "total_tokens": input_tokens + output_tokens,
                })

                return {
                    "uid": uid,
                    "text": response_body["content"][0]["text"],
                    "region": region,
                    "account_id": config["account_id"],
                    "model_id": model_id,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "stop_reason": response_body.get("stop_reason"),
                }
            except Exception as e:
                acq.update(usage={"total_tokens": est_input_tokens + max_tokens})
                print(f"[{uid}] Failed: {e}. Config= {acq.config}")
                raise e
```

### 4. Running It

```python
from concurry import wait

# Initialize with 350 threads, retries, and the multi-account limit pool
claude = BedrockWorker.options(
    mode="thread",
    max_workers=350,
    limits=limit_pool,
    num_retries=5,
    retry_wait=1.0,
    retry_jitter=0.3,
).init()

# --- Single call ---
result = claude.invoke(
    prompt="What is the capital of France?",
    max_tokens=100,
).result()
print(result["text"])
# => "The capital of France is Paris."
print(f"Routed to: {result['account_id']} / {result['region']}")

# --- Batch of calls ---
prompts = [f"Summarize topic #{i}" for i in range(500)]

futs = [
    claude.invoke(prompt=p, max_tokens=200, uid=str(i))
    for i, p in enumerate(prompts)
]

# Wait with a progress bar
_ = wait(futs, progress=True)

# Collect results
results = [f.result() for f in futs if f.done() and not f.exception()]
errors = [f for f in futs if f.done() and f.exception()]
print(f"Completed: {len(results)}, Failed: {len(errors)}")
```

## How It Works

```
                        ┌─────────────────────────────────┐
                        │          LimitPool               │
                        │       (round-robin)              │
                        └──────────┬──────────────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                     ▼
    ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
    │ LimitSet #1      │ │ LimitSet #2      │ │ LimitSet #N      │
    │ acct=111, us-e-1 │ │ acct=111, eu-w-3 │ │ acct=333, global │
    │ rpm=125          │ │ rpm=125          │ │ rpm=250          │
    │ tokens=50k/min   │ │ tokens=50k/min   │ │ tokens=50k/min   │
    │ config={...}     │ │ config={...}     │ │ config={...}     │
    └──────────────────┘ └──────────────────┘ └──────────────────┘
```

When `invoke()` calls `self.limits.acquire(...)`:

1.  The `LimitPool` picks the next `LimitSet` via round-robin.
2.  That `LimitSet` checks if RPM and token budgets have capacity.
3.  If yes, it reserves the tokens and returns an acquisition with `config` attached.
4.  If no, it blocks until the sliding window frees up capacity.
5.  The worker reads `config` to know which account/region/model to call.
6.  After the call, `acq.update()` refunds any over-estimated tokens.

## Why This Pattern Works

| Feature | Why it matters |
| :--- | :--- |
| **`LimitPool`** | **Throughput**: Instead of 250 RPM from one endpoint, you get `N_accounts × M_regions × per_endpoint_rpm`. With 7 accounts × 4 regions × ~125 RPM, that's ~3,500 RPM. |
| **Per-endpoint `LimitSet`** | **No throttling**: Each endpoint tracks its own RPM and token budget independently. You never accidentally send 300 requests to a 125 RPM endpoint. |
| **`config` on LimitSet** | **Zero routing logic**: The worker doesn't pick an account — the pool does. The worker just reads `acq.config` and calls the right endpoint. Adding a new account is one dict entry. |
| **Token tracking with `update()`** | **Cost control**: You estimate tokens upfront, then refund the difference. This prevents over-reserving capacity and keeps throughput high. |
| **`mode="thread"`** | **Simplicity**: Bedrock calls are I/O-bound. Threads give you concurrency without async complexity. 350 threads can keep 12 endpoints saturated. |

## Exercises For You
*   **Add `tpm` per config**: Some endpoints have token-per-minute limits. Add a `tpm` field to configs and create a second `RateLimit` in the `LimitSet` when present.
*   **Switch to Ray**: Change `mode="ray"` and `shared=True` on `LimitSet` to distribute across a cluster while still respecting per-endpoint limits via `RaySharedLimitSet`.
*   **Add vision**: Pass `image_base64` to `invoke()` for multimodal calls — the same rate limiting applies.
