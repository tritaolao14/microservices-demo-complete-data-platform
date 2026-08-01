"""Load module for {dataset_name} data."""

from typing import Iterator, Dict, Any
from abc import ABC, abstractmethod


class DataSink(ABC):
    """Abstract base class for data sinks."""
    
    @abstractmethod
    def load(self, records: Iterator[Dict[str, Any]]) -> None:
        """Load data to sink."""
        pass


class {dataset_name}DataSink(DataSink):
    """Concrete implementation for {dataset_name} data loading."""
    
    def __init__(self, config: dict):
        self.config = config
    
    def load(self, records: Iterator[Dict[str, Any]]) -> None:
        """Load {dataset_name} data to destination."""
        # Example loading logic:
        # for record in records:
        #     self._write_to_database(record)
        
        raise NotImplementedError("Implement data loading logic")