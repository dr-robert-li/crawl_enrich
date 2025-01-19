from dataclasses import dataclass

@dataclass
class RateLimitConfig:
    requests_per_minute: int
    time_window: int
    base_delay: int
    max_retries: int