# Hermes Agent - Global Rate Limiter & Concurrency Control

This repository contains a global rate-limiting and concurrency control system for the Hermes agent, specifically optimized for **NVIDIA NIM** and other rate-limit sensitive providers.

## Key Features
- **Global RPM Enforcer:** Hard cap of 40 RPM for NVIDIA (and configurable for others).
- **Concurrency Guard:** Limits concurrent outbound requests to prevent API burst triggers.
- **Cross-Session Persistence:** Uses `~/.hermes/rate_limits/*.json` to coordinate limits across multiple CLI sessions and background workers.
- **Adaptive Cooldown:** Detects HTTP 429 errors and triggers a global cooldown to prevent "Retry Storms."
- **Request Deduplication:** Caches identical request payloads to avoid redundant API calls and save tokens.

## Files
- `global_rate_limiter.py`: The core logic for tracking timestamps and enforcing bottlenecks.
- `chat_completion_helpers.py`: Patched to inject the limiter into the main chat path.
- `auxiliary_client.py`: Patched to protect side-tasks (titling, memory sync, etc.).
- `error_classifier.py`: Added helpers for unified rate-limit error detection.

## Usage
These files are intended to replace or augment the corresponding files in the `hermes-agent` project.
