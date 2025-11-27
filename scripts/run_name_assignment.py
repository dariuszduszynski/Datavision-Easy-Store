#!/usr/bin/env python3
"""
Entrypoint dla Name Assignment Service.
"""

import logging

from des.assignment.service import NameAssignmentService
from des.config.config import Config
from des.db.postgres import PostgresConnector


def main():
    logging.basicConfig(level=logging.INFO)

    # Load config
    config = Config.from_env()

    # Connect to DB
    db = PostgresConnector()
    db.connect(config.database)

    # Start service
    service = NameAssignmentService(config, db)
    service.run_forever()


if __name__ == "__main__":
    main()
