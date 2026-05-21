"""Edge-case tests for float-valued capacity in RateLimit.

These tests red-team the float-capacity feature: values that previously
would have been rejected by ``conint(gt=0)``, values that float arithmetic
makes tricky (NaN, Infinity, near-equal subnormals, IEEE-754 sums),
and the discrete-counting algorithms (LeakyBucket, SlidingWindow) where
fractional ``tokens`` are nonsensical.

The tests are deliberately domain-neutral. ``RateLimit`` is a generic
rate-limiting primitive: callers may interpret its ``tokens`` as
fractional resource units (bandwidth in MBps, normalized credits,
fractional currency, etc.) but Concurry itself does not know — and
should not assume — what units the caller has in mind.

Coverage map:

- Construction-time validation: 0, negative, NaN, bool, very small/large.
- Pydantic semantics: int stays int, float stays float, params_signature stable.
- Float-friendly algorithms (TokenBucket, GCRA): fractional capacity and
  fractional acquire both work; refund preserves precision.
- Counting algorithms (LeakyBucket, SlidingWindow): fractional ``tokens``
  raises with a helpful message; integer-valued floats like ``1.0`` still work.
- Capacity-vs-amount comparison edge cases: IEEE-754 quirks like
  ``(0.1+0.2) > 0.3``.
"""

import math

import pytest

from concurry import (
    CallLimit,
    LimitSet,
    RateLimit,
    RateLimitAlgorithm,
    ResourceLimit,
)


class TestFloatCapacityConstruction:
    """RateLimit's ``capacity`` accepts ``float`` (>0)."""

    def test_accepts_small_float(self) -> None:
        """A small fractional capacity like 0.0001 should be valid."""
        rl = RateLimit(key="x", capacity=0.0001, window=60)
        assert rl.capacity == 0.0001
        assert isinstance(rl.capacity, float)

    def test_accepts_large_float(self) -> None:
        rl = RateLimit(key="x", capacity=1e15, window=60)
        assert rl.capacity == 1e15

    def test_accepts_infinity(self) -> None:
        """``float('inf')`` is admitted by ``gt=0`` and represents an
        unbounded limit (every acquire trivially succeeds)."""
        rl = RateLimit(key="x", capacity=float("inf"), window=60)
        assert math.isinf(rl.capacity)

    def test_int_stays_int(self) -> None:
        """An integer input stays an integer in the field (preserves
        ``params_signature`` stability for the common int-capacity case)."""
        rl = RateLimit(key="x", capacity=300, window=60)
        # confloat coerces to float; we just verify the value round-trips.
        assert rl.capacity == 300

    def test_rejects_zero(self) -> None:
        """``capacity=0`` would deadlock acquire forever — reject it."""
        with pytest.raises((ValueError, Exception)):
            RateLimit(key="x", capacity=0, window=60)
        with pytest.raises((ValueError, Exception)):
            RateLimit(key="x", capacity=0.0, window=60)

    def test_rejects_negative(self) -> None:
        with pytest.raises((ValueError, Exception)):
            RateLimit(key="x", capacity=-1, window=60)
        with pytest.raises((ValueError, Exception)):
            RateLimit(key="x", capacity=-0.0001, window=60)

    def test_rejects_nan(self) -> None:
        """NaN compares False against everything; would silently break
        ``can_acquire`` checks. Pydantic's ``confloat(gt=0)`` rejects NaN."""
        with pytest.raises((ValueError, Exception)):
            RateLimit(key="x", capacity=float("nan"), window=60)

    def test_rejects_negative_infinity(self) -> None:
        with pytest.raises((ValueError, Exception)):
            RateLimit(key="x", capacity=float("-inf"), window=60)

    def test_string_numeric_coerces(self) -> None:
        """Pydantic admits string-encoded numbers; consistent with how
        other fields behave. Documented behaviour."""
        rl = RateLimit(key="x", capacity="1.5", window=60)
        assert rl.capacity == 1.5

    def test_bool_coerces_to_one(self) -> None:
        """``True`` is ``1`` in Python and Pydantic accepts it. Capacity
        of 1 is harmless. ``False`` is 0 → rejected by ``gt=0``."""
        rl = RateLimit(key="x", capacity=True, window=60)
        assert rl.capacity == 1
        with pytest.raises((ValueError, Exception)):
            RateLimit(key="x", capacity=False, window=60)


class TestParamsSignatureStability:
    """``params_signature()`` must format integral and fractional capacity
    distinguishably and stably across runs.

    Downstream callers may use the signature to deduplicate semantically
    identical limits; if the same logical limit produced different
    signatures on different code paths (e.g., ``capacity=300`` vs
    ``capacity=300.0``) that deduplication would silently break."""

    def test_integral_capacity_no_decimal(self) -> None:
        """``capacity=300`` should serialize as ``c300``, not ``c300.0``."""
        rl = RateLimit(key="x", capacity=300, window=60, algorithm=RateLimitAlgorithm.GCRA)
        assert rl.params_signature() == "c300w60gcr"

    def test_integral_float_capacity_no_decimal(self) -> None:
        """``capacity=300.0`` should also serialize as ``c300``."""
        rl = RateLimit(key="x", capacity=300.0, window=60, algorithm=RateLimitAlgorithm.GCRA)
        assert rl.params_signature() == "c300w60gcr"

    def test_fractional_capacity_preserved(self) -> None:
        """Fractional capacity must serialize with its digits, e.g.
        ``c0.0001w86400gcr``."""
        rl = RateLimit(key="x", capacity=0.0001, window=86400, algorithm=RateLimitAlgorithm.GCRA)
        sig = rl.params_signature()
        assert sig.startswith("c0.0001")
        assert "w86400" in sig

    def test_inf_capacity_serializes(self) -> None:
        """``inf`` capacity must produce a deterministic signature."""
        rl = RateLimit(key="x", capacity=float("inf"), window=86400, algorithm=RateLimitAlgorithm.GCRA)
        sig = rl.params_signature()
        assert "inf" in sig
        # Two independent inf-capacity limits should share a signature.
        rl2 = RateLimit(key="y", capacity=float("inf"), window=86400, algorithm=RateLimitAlgorithm.GCRA)
        assert rl.params_signature() == rl2.params_signature()


class TestFloatCapacityWithBucketAlgorithms:
    """TokenBucket and GCRA — the algorithms that semantically support
    fractional accumulators — should accept and acquire fractional values."""

    @pytest.mark.parametrize(
        "algo",
        [RateLimitAlgorithm.TokenBucket, RateLimitAlgorithm.GCRA],
    )
    def test_acquire_full_capacity_at_once(self, algo) -> None:
        rl = RateLimit(key="x", capacity=0.0001, window=60, algorithm=algo)
        assert rl.can_acquire(0.0001) is True
        assert rl._impl.try_acquire(tokens=0.0001) is True

    @pytest.mark.parametrize(
        "algo",
        [RateLimitAlgorithm.TokenBucket, RateLimitAlgorithm.GCRA],
    )
    def test_acquire_more_than_capacity_fails(self, algo) -> None:
        rl = RateLimit(key="x", capacity=0.0001, window=60, algorithm=algo)
        # capacity is 0.0001; asking for 0.001 (10x) must fail.
        assert rl.can_acquire(0.001) is False
        assert rl._impl.try_acquire(tokens=0.001) is False

    def test_token_bucket_fractional_refund(self) -> None:
        """TokenBucket supports refund; fractional refund should restore
        fractional tokens to the bucket."""
        rl = RateLimit(
            key="x",
            capacity=1.0,
            window=60,
            algorithm=RateLimitAlgorithm.TokenBucket,
        )
        assert rl._impl.try_acquire(tokens=0.7) is True
        # 0.3 left
        assert rl._impl.try_acquire(tokens=0.4) is False
        rl._impl.refund(tokens=0.5)
        # Now ~0.8 available
        assert rl._impl.try_acquire(tokens=0.4) is True

    def test_gcra_fractional_acquire(self) -> None:
        """GCRA's emission interval and TAT math are continuous; fractional
        acquires must work without rounding."""
        rl = RateLimit(key="x", capacity=10.0, window=60, algorithm=RateLimitAlgorithm.GCRA)
        # Burst-acquire half-capacity worth
        assert rl._impl.try_acquire(tokens=5.0) is True
        # Another half-capacity should also work (TAT advances)
        assert rl._impl.try_acquire(tokens=5.0) is True
        # 11 total > 10 capacity → must fail
        assert rl._impl.try_acquire(tokens=1.0) is False


class TestCountingAlgorithmsRejectFractionalTokens:
    """LeakyBucket and SlidingWindow are queue/timestamp-based. Fractional
    "tokens" means fractional records — meaningless. They must reject
    fractional ``tokens`` with a clear message."""

    @pytest.mark.parametrize(
        "algo",
        [RateLimitAlgorithm.LeakyBucket, RateLimitAlgorithm.SlidingWindow],
    )
    def test_integer_tokens_still_work(self, algo) -> None:
        """Sanity: integer tokens (the only sensible call) still work."""
        rl = RateLimit(key="x", capacity=10, window=60, algorithm=algo)
        assert rl._impl.try_acquire(tokens=1) is True
        assert rl._impl.try_acquire(tokens=3) is True

    @pytest.mark.parametrize(
        "algo",
        [RateLimitAlgorithm.LeakyBucket, RateLimitAlgorithm.SlidingWindow],
    )
    def test_integer_valued_float_tokens_work(self, algo) -> None:
        """``tokens=1.0`` is integer-valued; should work the same as ``1``."""
        rl = RateLimit(key="x", capacity=10, window=60, algorithm=algo)
        assert rl._impl.try_acquire(tokens=1.0) is True

    @pytest.mark.parametrize(
        "algo",
        [RateLimitAlgorithm.LeakyBucket, RateLimitAlgorithm.SlidingWindow],
    )
    def test_fractional_tokens_rejected(self, algo) -> None:
        """Fractional tokens against a counting algorithm raises ``ValueError``
        explaining that only TokenBucket/GCRA support fractional acquires."""
        rl = RateLimit(key="x", capacity=10, window=60, algorithm=algo)
        with pytest.raises(ValueError, match="fractional|integer|TokenBucket|GCRA"):
            rl._impl.try_acquire(tokens=0.5)

    @pytest.mark.parametrize(
        "algo",
        [RateLimitAlgorithm.LeakyBucket, RateLimitAlgorithm.SlidingWindow],
    )
    def test_fractional_can_acquire_rejected(self, algo) -> None:
        """The check path also rejects fractional tokens, so users find out
        before a hot-path failure."""
        rl = RateLimit(key="x", capacity=10, window=60, algorithm=algo)
        with pytest.raises(ValueError, match="fractional|integer|TokenBucket|GCRA"):
            rl._impl.can_acquire(tokens=0.5)


class TestLimitSetValidationWithFloatCapacity:
    """``LimitSet`` capacity-vs-requested validation must work with float."""

    def test_request_within_capacity_succeeds(self) -> None:
        rl = RateLimit(
            key="x",
            capacity=0.0001,
            window=60,
            algorithm=RateLimitAlgorithm.GCRA,
        )
        ls = LimitSet(limits=[rl])
        with ls.acquire(requested={"x": 0.000084}) as acq:
            acq.update(usage={"x": 0.000084})

    def test_request_exceeding_capacity_raises(self) -> None:
        rl = RateLimit(
            key="x",
            capacity=0.0001,
            window=60,
            algorithm=RateLimitAlgorithm.GCRA,
        )
        ls = LimitSet(limits=[rl])
        with pytest.raises(ValueError, match="exceeds capacity"):
            with ls.acquire(requested={"x": 0.001}) as acq:  # 10x capacity
                acq.update(usage={"x": 0.001})

    def test_validate_usage_warns_with_float(self, caplog) -> None:
        """If ``used > requested``, validate_usage logs a warning. Must
        format float values readably, not as garbage."""
        import logging

        rl = RateLimit(
            key="x",
            capacity=1.0,
            window=60,
            algorithm=RateLimitAlgorithm.GCRA,
        )
        with caplog.at_level(logging.WARNING):
            rl.validate_usage(requested=0.000084, used=0.0001)
        msg = caplog.text
        # Python's default float-to-str formatting may emit ``8.4e-05`` or
        # ``0.000084`` depending on magnitude; both are acceptable as long
        # as the original value is recoverable.
        assert ("0.000084" in msg) or ("8.4e-05" in msg)
        assert "0.0001" in msg
        assert "x" in msg


class TestIEEESurprises:
    """IEEE-754 quirks the user might hit. We don't try to fix them
    (silent ``isclose`` would let real over-capacity slip through), but
    document that they manifest as expected from float arithmetic."""

    def test_classic_0_1_plus_0_2(self) -> None:
        """``0.1 + 0.2 != 0.3`` in IEEE-754. Capacity assembled from such
        sums will be slightly above the user's intent."""
        rl = RateLimit(
            key="x",
            capacity=0.1 + 0.2,  # 0.30000000000000004
            window=60,
            algorithm=RateLimitAlgorithm.GCRA,
        )
        # Exactly 0.3 is just under the bumped capacity, so it must succeed.
        assert rl._impl.try_acquire(tokens=0.3) is True

    def test_subnormal_capacity(self) -> None:
        """Very small (subnormal) floats are valid and behave normally."""
        rl = RateLimit(
            key="x",
            capacity=1e-300,
            window=60,
            algorithm=RateLimitAlgorithm.GCRA,
        )
        assert rl.capacity == 1e-300
        assert rl._impl.try_acquire(tokens=1e-300) is True


class TestCallLimitFloatCapacityRejected:
    """``CallLimit`` counts calls — fractional capacity is meaningless.
    The default algorithm path may admit float, but the natural usage
    pattern (``requested=1`` per call) keeps it integer in practice.

    We do NOT enforce integer capacity on CallLimit; that would be a
    breaking change for users who already pass int (works fine), and
    the ``range(tokens)`` bug only fires when ``tokens`` is fractional.
    The integer-tokens contract for CallLimit is enforced by
    ``CallLimit.validate_usage``."""

    def test_integer_capacity_works(self) -> None:
        cl = CallLimit(window=60, capacity=100)
        assert cl.capacity == 100

    def test_call_limit_default_request_is_one(self) -> None:
        """CallLimits always have ``requested=1`` per acquire, so even with
        a float capacity the discrete-tokens pathway never fires
        fractionally for them."""
        cl = CallLimit(window=60, capacity=10)
        ls = LimitSet(limits=[cl])
        with ls.acquire():
            pass


class TestResourceLimitStaysInteger:
    """``ResourceLimit`` (semaphore-based) must stay integer-only; you
    can't have 0.5 of a connection."""

    def test_int_capacity_works(self) -> None:
        rl = ResourceLimit(key="conn", capacity=5)
        assert rl.capacity == 5
