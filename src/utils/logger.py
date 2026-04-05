"""
Structured logger factory for FreshSip Beverages CPG Data Platform.

Provides a LoggerAdapter that injects pipeline context (layer, domain, batch_id)
into every log record automatically.

Usage:
    from src.utils.logger import get_logger, log_pipeline_start, log_pipeline_end

    logger = get_logger(__name__, layer="bronze", domain="sales", batch_id="abc-123")
    logger.info("Starting ingestion")
"""

import logging
from typing import Optional


class PipelineLoggerAdapter(logging.LoggerAdapter):
    """
    LoggerAdapter that injects layer, domain, and batch_id into every log record.

    The adapter merges context fields into the 'extra' dict so they are available
    to the formatter as %(layer)s, %(domain)s, %(batch_id)s.
    """

    def process(self, msg: str, kwargs: dict) -> tuple:
        """Merge context fields into log record extras."""
        return msg, kwargs

    def info(self, msg, *args, **kwargs):
        kwargs.setdefault("extra", {})
        kwargs["extra"].update(self.extra)
        super().info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        kwargs.setdefault("extra", {})
        kwargs["extra"].update(self.extra)
        super().warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        kwargs.setdefault("extra", {})
        kwargs["extra"].update(self.extra)
        super().error(msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        kwargs.setdefault("extra", {})
        kwargs["extra"].update(self.extra)
        super().debug(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        kwargs.setdefault("extra", {})
        kwargs["extra"].update(self.extra)
        super().exception(msg, *args, **kwargs)


class _ContextFilter(logging.Filter):
    """
    Filter that injects default context fields when they are not already present.
    Prevents KeyError in formatter when context fields are missing.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.layer = getattr(record, "layer", "unknown")
        record.domain = getattr(record, "domain", "unknown")
        record.batch_id = getattr(record, "batch_id", "none")
        return True


_FORMAT = "%(asctime)s [%(levelname)s] [%(layer)s/%(domain)s] [batch=%(batch_id)s] %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_configured = False


def _configure_root_logger() -> None:
    """Configure the root logger with the pipeline format (idempotent)."""
    global _configured
    if _configured:
        return

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))
    handler.addFilter(_ContextFilter())

    root = logging.getLogger()
    if not root.handlers:
        root.addHandler(handler)
        root.setLevel(logging.INFO)

    _configured = True


def get_logger(
    name: str,
    layer: Optional[str] = None,
    domain: Optional[str] = None,
    batch_id: Optional[str] = None,
) -> PipelineLoggerAdapter:
    """
    Return a structured PipelineLoggerAdapter with injected context.

    Args:
        name: Logger name (use __name__ from the calling module).
        layer: Pipeline layer label (e.g., 'bronze', 'silver', 'gold').
        domain: Data domain label (e.g., 'sales', 'inventory', 'production').
        batch_id: UUID string identifying the current pipeline batch run.

    Returns:
        PipelineLoggerAdapter that injects layer/domain/batch_id into every record.
    """
    _configure_root_logger()
    base_logger = logging.getLogger(name)
    context = {
        "layer": layer or "unknown",
        "domain": domain or "unknown",
        "batch_id": batch_id or "none",
    }
    return PipelineLoggerAdapter(base_logger, context)


def log_pipeline_start(
    logger: PipelineLoggerAdapter,
    pipeline_name: str,
    source: str,
    target: str,
) -> None:
    """
    Log a standardised pipeline-start message.

    Args:
        logger: PipelineLoggerAdapter from get_logger().
        pipeline_name: Human-readable pipeline name.
        source: Source table or file path.
        target: Target table name.
    """
    logger.info(
        "PIPELINE START | name=%s | source=%s | target=%s",
        pipeline_name,
        source,
        target,
    )


def log_pipeline_end(
    logger: PipelineLoggerAdapter,
    pipeline_name: str,
    record_count: int,
) -> None:
    """
    Log a standardised pipeline-end message with record count.

    Args:
        logger: PipelineLoggerAdapter from get_logger().
        pipeline_name: Human-readable pipeline name.
        record_count: Number of records written to the target.
    """
    logger.info(
        "PIPELINE END   | name=%s | records_written=%d",
        pipeline_name,
        record_count,
    )
