"""
Daily rollover logic - finalizacja shardów o północy.
"""

class DailyRolloverManager:
    """
    Zarządza codzienną finalizacją i uploadem shardów.
    """
    def __init__(self, packer):
        ...
    
    def check_and_rollover(self):
        """Sprawdź czy nowy dzień i wykonaj rollover"""
        ...
    
    def finalize_shard(self, shard_id, writer):
        """Finalizuj i upload konkretny shard"""
        ...