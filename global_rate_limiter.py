"""Global rate limiter and concurrency control for provider APIs.

Provides a thread-safe and process-safe sliding window rate limiter
to ensure total requests to a provider never exceed a configured RPM.
Supports global semaphores to bound concurrent outbound requests.

Persistent state is stored in ~/.hermes/rate_limits/<provider>.json
to coordinate limits across multiple CLI sessions and background tasks.
"""

from __future__ import annotations

import json
import logging
import os
import time
import threading
import tempfile
import random
import hashlib
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

# Try to import from project, fallback to standard path
try:
    from hermes_constants import get_hermes_home
except ImportError:
    def get_hermes_home():
        return os.path.join(os.path.expanduser("~"), ".hermes")

# Simple atomic replacement helper if not available
def atomic_replace(src, dst):
    try:
        os.replace(src, dst)
    except OSError:
        os.remove(dst)
        os.rename(src, dst)

logger = logging.getLogger(__name__)

_STATE_SUBDIR = "rate_limits"

@dataclass
class ProviderLimitConfig:
    max_rpm: int = 40
    max_concurrent: int = 5
    cooldown_on_429: float = 60.0  # seconds

_DEFAULT_CONFIGS = {
    "nvidia": ProviderLimitConfig(max_rpm=40, max_concurrent=2, cooldown_on_429=60.0),
    "mistral": ProviderLimitConfig(max_rpm=30, max_concurrent=2, cooldown_on_429=30.0),
    "openai": ProviderLimitConfig(max_rpm=100, max_concurrent=10, cooldown_on_429=10.0),
    "anthropic": ProviderLimitConfig(max_rpm=50, max_concurrent=5, cooldown_on_429=20.0),
}

class RequestCache:
    """Simple TTL-based cache for identical request payloads."""
    def __init__(self, ttl: float = 30.0):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self.ttl = ttl

    def _hash_payload(self, api_kwargs: Dict) -> str:
        # Sort keys for consistent hashing, exclude non-serializable or irrelevant keys
        try:
            # Create a clean copy without non-json keys
            clean = {k: v for k, v in api_kwargs.items() if not k.startswith('_') and k != 'stream_callback'}
            payload_str = json.dumps(clean, sort_keys=True, default=str)
            return hashlib.sha256(payload_str.encode()).hexdigest()
        except Exception:
            return ""

    def get(self, api_kwargs: Dict) -> Optional[Any]:
        key = self._hash_payload(api_kwargs)
        if not key: return None
        with self._lock:
            entry = self._cache.get(key)
            if entry and time.time() < entry["expires_at"]:
                logger.debug("Request cache hit for key %s", key[:8])
                return entry["response"]
            return None

    def set(self, api_kwargs: Dict, response: Any):
        key = self._hash_payload(api_kwargs)
        if not key: return
        with self._lock:
            self._cache[key] = {
                "response": response,
                "expires_at": time.time() + self.ttl
            }

class GlobalRateLimiter:
    """Orchestrates RPM and concurrency limits across all agent components."""

    _instances: Dict[str, GlobalRateLimiter] = {}
    _global_lock = threading.Lock()

    def __init__(self, provider: str):
        self.provider = provider.lower()
        self.config = _DEFAULT_CONFIGS.get(self.provider, ProviderLimitConfig())
        
        self.state_file = os.path.join(get_hermes_home(), _STATE_SUBDIR, f"{self.provider}.json")
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        
        # In-process concurrency control
        self.semaphore = threading.Semaphore(self.config.max_concurrent)
        self.process_lock = threading.Lock()
        self.cache = RequestCache()
        
        # Instrumentation
        self.total_requests = 0
        self.total_waits = 0
        self.total_wait_time = 0.0

    @classmethod
    def for_provider(cls, provider: str) -> GlobalRateLimiter:
        provider = (provider or "default").lower()
        # Strip provider from model if passed as provider (e.g. nvidia/llama-...)
        if "/" in provider:
            provider = provider.split("/")[0]
        with cls._global_lock:
            if provider not in cls._instances:
                cls._instances[provider] = cls(provider)
            return cls._instances[provider]

    def _load_state(self) -> Dict:
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return data
        except Exception as e:
            logger.debug("Failed to load rate limit state for %s: %s", self.provider, e)
        return {"requests": [], "locked_until": 0}

    def _save_state(self, state: Dict):
        try:
            fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(self.state_file))
            with os.fdopen(fd, 'w') as f:
                json.dump(state, f)
            atomic_replace(tmp_path, self.state_file)
        except Exception as e:
            logger.debug("Failed to save rate limit state for %s: %s", self.provider, e)

    def acquire(self):
        """Block until a request slot is available under the RPM cap."""
        start_wait = time.time()
        self.total_requests += 1
        
        # 1. Concurrency check
        self.semaphore.acquire()
        
        try:
            while True:
                with self.process_lock:
                    state = self._load_state()
                    now = time.time()
                    
                    # Check for 429 cooldown
                    locked_until = state.get("locked_until", 0)
                    if locked_until > now:
                        wait_time = locked_until - now
                        logger.warning("Provider %s is in cooldown. Waiting %.1fs...", self.provider, wait_time)
                        time.sleep(min(wait_time, 2.0)) 
                        continue
                    
                    # Sliding window check (60s)
                    window_start = now - 60.0
                    timestamps = [t for t in state.get("requests", []) if t > window_start]
                    
                    if len(timestamps) < self.config.max_rpm:
                        # Success! Update and save
                        timestamps.append(now)
                        state["requests"] = timestamps
                        self._save_state(state)
                        
                        wait_duration = time.time() - start_wait
                        if wait_duration > 0.1:
                            self.total_waits += 1
                            self.total_wait_time += wait_duration
                            logger.info("Acquired slot for %s after waiting %.2fs. RPM currently %d/%d.", 
                                        self.provider, wait_duration, len(timestamps), self.config.max_rpm)
                        return
                    
                    # Window full, wait for the oldest one to expire
                    wait_time = timestamps[0] - window_start + 0.1 # Buffer
                    
                logger.info("RPM limit (%d) reached for %s. Waiting %.2fs...", self.config.max_rpm, self.provider, wait_time)
                time.sleep(max(0.1, wait_time))

        except Exception:
            self.semaphore.release()
            raise

    def release(self):
        """Release the concurrency slot."""
        self.semaphore.release()

    def record_429(self, retry_after: Optional[float] = None):
        """Record a rate limit error and trigger a global cooldown."""
        with self.process_lock:
            state = self._load_state()
            cooldown = retry_after if retry_after else self.config.cooldown_on_429
            state["locked_until"] = time.time() + cooldown
            self._save_state(state)
            logger.warning("Recorded 429 for %s. Global cooldown for %.1fs triggered.", self.provider, cooldown)

class ProviderSlot:
    """Context manager for acquiring a provider request slot."""
    def __init__(self, provider: str):
        self.provider = (provider or "default").lower()
        if "/" in self.provider:
            self.provider = self.provider.split("/")[0]
        self.limiter = GlobalRateLimiter.for_provider(self.provider)

    def __enter__(self):
        self.limiter.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            from agent.error_classifier import is_rate_limit_error
            if is_rate_limit_error(exc_val):
                # Extract retry-after if available
                _ra = None
                if hasattr(exc_val, "response") and exc_val.response:
                    _ra = exc_val.response.headers.get("retry-after")
                self.limiter.record_429(retry_after=float(_ra) if _ra else None)
        self.limiter.release()

# --- Singleton helpers ---

def acquire_provider_slot(provider: str):
    GlobalRateLimiter.for_provider(provider).acquire()

def release_provider_slot(provider: str):
    GlobalRateLimiter.for_provider(provider).release()

def report_provider_429(provider: str, retry_after: Optional[float] = None):
    GlobalRateLimiter.for_provider(provider).record_429(retry_after)

def get_cached_response(provider: str, api_kwargs: Dict) -> Optional[Any]:
    return GlobalRateLimiter.for_provider(provider).cache.get(api_kwargs)

def set_cached_response(provider: str, api_kwargs: Dict, response: Any):
    GlobalRateLimiter.for_provider(provider).cache.set(api_kwargs, response)
