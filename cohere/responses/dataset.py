from typing import Any, Dict, List, Optional

import requests
from fastavro import reader

from cohere.responses.base import CohereObject


class Dataset(CohereObject):
    id: str
    name: str
    dataset_type: str
    size_bytes: int
    validation_status: str
    dataset_parts: List["DatasetPart"]

    def __init__(
        self, id: str, name: str, dataset_type: str, validation_status: str, dataset_parts: List["DatasetPart"]
    ) -> None:
        self.id = id
        self.name = name
        self.dataset_type = dataset_type
        self.validation_status = validation_status
        self.dataset_parts = dataset_parts

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Dataset":
        return cls(
            id=data["id"],
            name=data["name"],
            dataset_type=data["dataset_type"],
            validation_status=data["validation_status"],
            dataset_parts=[DatasetPart.from_dict(part) for part in data["dataset_parts"]],
        )

    def open(self):
        for part in self.dataset_parts:
            resp = requests.get(part.url, stream=True)
            for record in reader(resp.raw):
                yield record


class DatasetPart(CohereObject):
    id: str
    name: str
    url: Optional[str] = None
    index: Optional[int] = None

    def __init__(self, id: str, name: str, url: Optional[str] = None, index: Optional[int] = None) -> None:
        self.id = id
        self.name = name
        self.url = url  # optional
        self.index = index  # optional

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetPart":
        return cls(id=data["id"], name=data["name"], url=data.get("url", None), index=data.get("index", None))
