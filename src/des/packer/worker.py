"""
Worker thread logic - przetwarzanie batchy w wątkach.
"""


class PackerWorker:
    """
    Worker który przetwarza batch plików w osobnym wątku.
    """

    def __init__(self, packer, worker_id): ...

    def process_files(self, files): ...
