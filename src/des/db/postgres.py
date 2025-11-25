"""
PostgreSQL implementation.
"""
from des.db.base import DatabaseConnector

class PostgresConnector(DatabaseConnector):
    """
    PostgreSQL specific implementation.
    """
    def connect(self, config):
        ...
    
    def claim_files_atomic(self, shard_ids, pod_name, limit):
        """
        Atomic claim z FOR UPDATE SKIP LOCKED.
        """
        ...