from typing import Any, Dict, List, Optional

from cohere.responses.base import CohereObject
from cohere.responses.dataset import Dataset
from cohere.utils import JobWithStatus


class EmbedJob(CohereObject, JobWithStatus):
    job_id: str
    status: str
    created_at: str
    input_url: Optional[str]
    output_urls: Optional[List[str]]
    output: Dataset
    model: str
    truncate: str
    percent_complete: float

    def __init__(
        self,
        job_id: str,
        status: str,
        created_at: str,
        input_url: Optional[str],
        output_urls: Optional[List[str]],
        model: str,
        truncate: str,
        percent_complete: float,
    ) -> None:
        self.job_id = job_id
        self.status = status
        self.created_at = created_at
        self.input_url = input_url
        self.output_urls = output_urls
        self.model = model
        self.truncate = truncate
        self.percent_complete = percent_complete

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EmbedJob":
        return cls(
            job_id=data["job_id"],
            status=data["status"],
            created_at=data["created_at"],
            input_url=data.get("input_url"),
            output_urls=data.get("output_urls"),
            model=data["model"],
            truncate=data["truncate"],
            percent_complete=data["percent_complete"],
        )

    def has_terminal_status(self) -> bool:
        return self.status in ["complete", "failed", "cancelled"]


class CreateEmbedJobResponse(CohereObject):
    job_id: str

    def __init__(self, job_id: str, wait_fn):
        self.job_id = job_id
        self._wait_fn = wait_fn

    @classmethod
    def from_dict(cls, data: Dict[str, Any], wait_fn) -> "CreateEmbedJobResponse":
        return cls(
            job_id=data["job_id"],
            wait_fn=wait_fn,
        )

    def wait(
        self,
        timeout: Optional[float] = None,
        interval: float = 10,
    ) -> EmbedJob:
        """Wait for embed job completion.

        Args:
            timeout (Optional[float], optional): Wait timeout in seconds, if None - there is no limit to the wait time.
                Defaults to None.
            interval (float, optional): Wait poll interval in seconds. Defaults to 10.

        Raises:
            TimeoutError: wait timed out

        Returns:
            EmbedJob: Embed job.
        """

        return self._wait_fn(job_id=self.job_id, timeout=timeout, interval=interval)


class AsyncCreateEmbedJobResponse(CreateEmbedJobResponse):
    async def wait(
        self,
        timeout: Optional[float] = None,
        interval: float = 10,
    ) -> EmbedJob:
        """Wait for embed job completion.

        Args:
            timeout (Optional[float], optional): Wait timeout in seconds, if None - there is no limit to the wait time.
                Defaults to None.
            interval (float, optional): Wait poll interval in seconds. Defaults to 10.

        Raises:
            TimeoutError: wait timed out

        Returns:
            EmbedJob: Embed job.
        """

        return await self._wait_fn(job_id=self.job_id, timeout=timeout, interval=interval)
