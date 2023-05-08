import csv
import json
from typing import Any, Dict, List

import requests
from fastavro import reader

from cohere.error import CohereError
from cohere.responses.base import CohereObject

supported_formats = ["jsonl", "csv"]


class Dataset(CohereObject):
    id: str
    name: str
    dataset_type: str
    validation_status: str
    urls: List[str]
    size_bytes: int

    def __init__(self, id: str, name: str, dataset_type: str, validation_status: str, urls: List[str]) -> None:
        self.id = id
        self.name = name
        self.dataset_type = dataset_type
        self.validation_status = validation_status
        self.urls = urls

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Dataset":
        return cls(
            id=data["id"],
            name=data["name"],
            dataset_type=data["dataset_type"],
            validation_status=data["validation_status"],
            urls=[part.get("url") for part in data["dataset_parts"]],
        )

    def open(self):
        for url in self.urls:
            # todo stream lines back instead
            if "result" not in self.dataset_type:
                raise CohereError(message="cannot open input dataset")
            resp = requests.get(url, stream=True)
            for record in reader(resp.raw):
                yield record

    def save(self, filepath: str, format: str = "jsonl"):
        if format == "jsonl":
            return self.save_jsonl(filepath)
        if format == "csv":
            return self.save_csv(filepath)
        raise CohereError(message=f"unsupported format must be one of : {supported_formats}")

    def save_jsonl(self, filepath: str):
        with open(filepath, "w") as outfile:
            for data in self.open():
                json.dump(data, outfile)
                outfile.write("\n")

    def save_csv(self, filepath: str):
        with open(filepath, "w") as outfile:
            for i, data in enumerate(self.open()):
                if i == 0:
                    writer = csv.DictWriter(outfile, fieldnames=list(data.keys()))
                    writer.writeheader()
                writer.writerow(data)
