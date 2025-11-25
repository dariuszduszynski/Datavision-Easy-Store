#!/usr/bin/env python3
"""
Entrypoint dla Multi-Shard Packer.
"""
import os
import sys
import logging
import signal
from des.config.config import Config
from des.db.postgres import PostgresConnector
from des.assignment.shard_router import ShardAssignment
from des.packer.multi_shard_packer import MultiShardPacker
from des.utils.signals import setup_signal_handlers

def extract_pod_index() -> int:
    """Extract pod index from hostname"""
    hostname = os.environ['HOSTNAME']
    # des-packer-7 → 7
    return int(hostname.split('-')[-1])

def main():
    logging.basicConfig(level=logging.INFO)
    
    # Load config
    config = Config.from_env()
    
    # Get pod index
    pod_index = extract_pod_index()
    logging.info(f"Starting packer for pod index: {pod_index}")
    
    # Connect to DB
    db = PostgresConnector()
    db.connect(config.database)
    
    # Setup shard assignment
    assignment = ShardAssignment(
        n_bits=config.migration.n_bits,
        num_pods=config.migration.num_pods
    )
    
    # Create packer
    packer = MultiShardPacker(pod_index, assignment, config, db)
    
    # Setup graceful shutdown
    setup_signal_handlers(packer)
    
    # Run
    packer.run_forever()

if __name__ == '__main__':
    main()