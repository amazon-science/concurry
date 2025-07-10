"""Tests for concurry.core.config module."""

import pytest

from concurry.core.config import (
    ExecutionMode,
    ExecutorConfig,
    RateLimitAlgorithm,
    RateLimitConfig,
    RetryConfig,
)


class TestExecutorConfig:
    def test_basic_creation(self):
        """Test basic ExecutorConfig creation with defaults."""
        config = ExecutorConfig()

        assert config.mode == ExecutionMode.Auto
        assert config.max_workers is None
        assert config.timeout is None
        assert config.rate_limit is None
        assert config.retry_config is None

    def test_full_config_with_objects(self):
        """Test complete configuration using config objects."""
        rate_limit = RateLimitConfig.per_minute(100)
        retry_config = RetryConfig(max_retries=3)

        config = ExecutorConfig(
            mode=ExecutionMode.Threads,
            max_workers=4,
            timeout=30.0,
            rate_limit=rate_limit,
            retry_config=retry_config,
        )

        assert config.mode == ExecutionMode.Threads
        assert config.max_workers == 4
        assert config.timeout == 30.0
        assert config.rate_limit.max_calls == 100
        assert config.rate_limit.time_window == 60.0
        assert config.retry_config.max_retries == 3

    def test_dict_conversion_in_constructor(self):
        """Test automatic dict-to-config conversion in constructor."""
        config = ExecutorConfig(
            mode="threads",
            max_workers=2,
            rate_limit={"max_calls": 50, "time_window": 30.0, "algorithm": "sliding"},
            retry_config={"max_retries": 5, "initial_delay": 1.0},
        )

        assert config.mode == ExecutionMode.Threads
        assert isinstance(config.rate_limit, RateLimitConfig)
        assert config.rate_limit.max_calls == 50
        assert config.rate_limit.time_window == 30.0
        assert config.rate_limit.algorithm == RateLimitAlgorithm.SlidingWindow

        assert isinstance(config.retry_config, RetryConfig)
        assert config.retry_config.max_retries == 5
        assert config.retry_config.initial_delay == 1.0

    def test_from_dict_complete(self):
        """Test creating ExecutorConfig entirely from dictionary."""
        data = {
            "mode": "processes",
            "max_workers": 8,
            "timeout": 60.0,
            "rate_limit": {
                "max_calls": 100,
                "time_window": 60.0,
                "algorithm": "token",
                "burst_capacity": 150,
            },
            "retry_config": {"max_retries": 10, "initial_delay": 2.0, "exponential_base": 1.8},
        }

        config = ExecutorConfig.from_dict(data)

        assert config.mode == ExecutionMode.Processes
        assert config.max_workers == 8
        assert config.timeout == 60.0

        # Verify rate limiting works end-to-end
        assert config.rate_limit.max_calls == 100
        assert config.rate_limit.algorithm == RateLimitAlgorithm.TokenBucket
        assert config.rate_limit.burst_capacity == 150

        # Verify retry config works end-to-end
        assert config.retry_config.max_retries == 10
        assert config.retry_config.exponential_base == 1.8

    def test_mixed_objects_and_dicts(self):
        """Test mixing pre-created objects with dictionaries."""
        rate_limit = RateLimitConfig.per_hour(5000)

        config = ExecutorConfig(
            mode="asyncio",
            rate_limit=rate_limit,  # Pre-created object
            retry_config={  # Dictionary
                "max_retries": 3,
                "initial_delay": 0.5,
            },
        )

        assert config.rate_limit is rate_limit
        assert isinstance(config.retry_config, RetryConfig)
        assert config.retry_config.max_retries == 3

    def test_threads_mode_requires_max_workers(self):
        """Test validation: Threads mode requires explicit max_workers."""
        with pytest.raises(ValueError, match="max_workers must be explicitly provided for Threads"):
            ExecutorConfig(mode=ExecutionMode.Threads)

    def test_processes_mode_requires_max_workers(self):
        """Test validation: Processes mode requires explicit max_workers."""
        with pytest.raises(ValueError, match="max_workers must be explicitly provided for Processes"):
            ExecutorConfig(mode=ExecutionMode.Processes)

    def test_validation_errors(self):
        """Test key validation scenarios."""
        # Invalid max_workers
        with pytest.raises(ValueError, match="max_workers must be positive"):
            ExecutorConfig(max_workers=0)

        # Invalid timeout
        with pytest.raises(ValueError, match="timeout must be positive"):
            ExecutorConfig(timeout=0.0)

    def test_invalid_dict_input(self):
        """Test error handling for invalid dictionary input."""
        with pytest.raises(TypeError, match="Expected dict"):
            ExecutorConfig.from_dict("not a dict")


class TestRateLimitConfig:
    """Test RateLimitConfig key functionality."""

    def test_convenience_constructors(self):
        """Test convenience constructors for different time windows."""
        # per_second
        config = RateLimitConfig.per_second(10)
        assert config.max_calls == 10
        assert config.time_window == 1.0

        # per_minute
        config = RateLimitConfig.per_minute(100)
        assert config.max_calls == 100
        assert config.time_window == 60.0

        # per_hour
        config = RateLimitConfig.per_hour(1000)
        assert config.max_calls == 1000
        assert config.time_window == 3600.0

    def test_token_bucket_algorithm_defaults(self):
        """Test TokenBucket algorithm sets appropriate defaults."""
        config = RateLimitConfig(max_calls=10, time_window=5.0, algorithm=RateLimitAlgorithm.TokenBucket)

        assert config.burst_capacity == 10  # Same as max_calls
        assert abs(config.refill_rate - 2.0) < 1e-10  # 10/5

    def test_from_dict_with_algorithm_conversion(self):
        """Test dictionary conversion with string-to-enum algorithm conversion."""
        data = {
            "max_calls": 100,
            "time_window": 60.0,
            "algorithm": "token",  # String gets converted to enum
            "burst_capacity": 150,
        }

        config = RateLimitConfig.from_dict(data)
        assert config.algorithm == RateLimitAlgorithm.TokenBucket
        assert config.burst_capacity == 150

    def test_validation_errors(self):
        """Test key validation scenarios."""
        with pytest.raises(ValueError, match="max_calls must be positive"):
            RateLimitConfig(max_calls=0, time_window=1.0)

        with pytest.raises(ValueError, match="time_window must be positive"):
            RateLimitConfig(max_calls=10, time_window=0.0)


class TestRetryConfig:
    """Test RetryConfig key functionality."""

    def test_basic_creation_and_validation(self):
        """Test basic creation and key validation."""
        config = RetryConfig(max_retries=3)
        assert config.max_retries == 3
        assert config.initial_delay == 0.0
        assert config.exponential_base == 2.0

        # Test validation
        with pytest.raises(ValueError, match="max_retries must be non-negative"):
            RetryConfig(max_retries=-1)

    def test_from_dict(self):
        """Test creating RetryConfig from dictionary."""
        data = {"max_retries": 5, "initial_delay": 1.0, "exponential_base": 1.5}

        config = RetryConfig.from_dict(data)
        assert config.max_retries == 5
        assert config.initial_delay == 1.0
        assert config.exponential_base == 1.5


class TestIntegration:
    """Integration tests for real-world usage patterns."""

    def test_api_rate_limiting_scenario(self):
        """Test typical API rate limiting configuration."""
        config = ExecutorConfig.from_dict(
            {
                "mode": "threads",
                "max_workers": 4,
                "rate_limit": {
                    "max_calls": 100,
                    "time_window": 60.0,  # 100 calls per minute
                    "algorithm": "sliding",
                },
                "retry_config": {"max_retries": 3, "initial_delay": 1.0, "exponential_base": 2.0},
            }
        )

        # Verify the configuration makes sense for API usage
        assert config.mode == ExecutionMode.Threads
        assert config.max_workers == 4
        assert config.rate_limit.calls_per_second == 100 / 60  # About 1.67 calls per second
        assert config.retry_config.max_retries == 3

    def test_burst_traffic_scenario(self):
        """Test configuration for handling burst traffic."""
        config = ExecutorConfig(
            mode=ExecutionMode.Threads,
            max_workers=8,
            rate_limit=RateLimitConfig(
                max_calls=50,
                time_window=60.0,
                algorithm=RateLimitAlgorithm.TokenBucket,
                burst_capacity=100,  # Allow bursts up to 100
            ),
        )

        assert config.rate_limit.algorithm == RateLimitAlgorithm.TokenBucket
        assert config.rate_limit.burst_capacity == 100
        assert config.rate_limit.max_calls == 50

    def test_high_throughput_scenario(self):
        """Test configuration for high throughput processing."""
        config = ExecutorConfig.from_dict(
            {
                "mode": "processes",
                "max_workers": 16,
                "rate_limit": {
                    "max_calls": 1000,
                    "time_window": 3600.0,  # 1000 per hour
                    "algorithm": "leaky",
                },
            }
        )

        assert config.mode == ExecutionMode.Processes
        assert config.max_workers == 16
        assert config.rate_limit.algorithm == RateLimitAlgorithm.LeakyBucket
        assert config.rate_limit.calls_per_second == 1000 / 3600
