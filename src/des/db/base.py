"""
Abstract database connector.
"""
from abc import ABC, abstractmethod

class DatabaseConnector(ABC):
    """
    Abstract base dla database connectors.
    """
    
    @abstractmethod
    def connect(self, config: dict):
        ...
    
    @abstractmethod
    def get_files_to_migrate(self, limit: int):
        ...
    
    @abstractmethod
    def update_status(self, file_id, status, **kwargs):
        ...