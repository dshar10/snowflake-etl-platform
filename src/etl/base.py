from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .logger import get_logger


@dataclass(frozen=True)
class RunResult:
    pipeline_name: str
    started_at: datetime
    finished_at: datetime
    status: str
    rows_processed: Optional[int] = None
    message: Optional[str] = None


class BaseETL(ABC):
    """
    Base class for ETL pipelines.

    Design goals:
    - Clear lifecycle: extract -> transform -> load
    - Centralized logging
    - Consistent run result (useful later for Snowflake audit tables)
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pipeline_name = str(config.get("pipeline_name", self.__class__.__name__))
        self.log = get_logger(self.pipeline_name)

    @abstractmethod
    def extract(self) -> Any:
        """Fetch data from the source."""
        raise NotImplementedError

    def transform(self, data: Any) -> Any:
        """Optional transformation step."""
        return data

    @abstractmethod
    def load(self, data: Any) -> int:
        """Load data into the target. Return rows processed."""
        raise NotImplementedError

    def validate(self, data: Any) -> None:
        """Basic validation hook. Override for stronger checks."""
        if data is None:
            raise ValueError("Validation failed: extracted data is None")

    def run(self) -> RunResult:
        started = datetime.now(timezone.utc)
        self.log.info("Run started")

        try:
            raw = self.extract()
            self.validate(raw)

            transformed = self.transform(raw)
            self.validate(transformed)

            rows = self.load(transformed)

            finished = datetime.now(timezone.utc)
            self.log.info("Run finished successfully | rows_processed=%s", rows)

            return RunResult(
                pipeline_name=self.pipeline_name,
                started_at=started,
                finished_at=finished,
                status="SUCCESS",
                rows_processed=rows,
            )

        except Exception as e:
            finished = datetime.now(timezone.utc)
            self.log.exception("Run failed: %s", e)

            return RunResult(
                pipeline_name=self.pipeline_name,
                started_at=started,
                finished_at=finished,
                status="FAILED",
                message=str(e),
            )
