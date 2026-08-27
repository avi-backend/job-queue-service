"""Shared Redis client.

Redis is the ready-work index only. PostgreSQL remains the source of truth, so
nothing here may be treated as authoritative job state.
"""

from redis.asyncio import Redis

from app.core.config import settings

redis_client: Redis = Redis.from_url(
    settings.redis_url,
    encoding="utf-8",
    decode_responses=True,
)


async def get_redis() -> Redis:
    return redis_client


async def close_redis() -> None:
    await redis_client.aclose()
