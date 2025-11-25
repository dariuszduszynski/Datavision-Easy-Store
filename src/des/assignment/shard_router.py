"""
Shard routing logic - mapowanie pod → shardy.
"""

class ShardAssignment:
    """
    Zarządza mapowaniem podów na shardy.
    """
    def __init__(self, n_bits: int, num_pods: int):
        ...
    
    def get_shards_for_pod(self, pod_index: int) -> List[int]:
        ...
    
    def compute_shard_id(self, snowflake_name: str) -> int:
        ...