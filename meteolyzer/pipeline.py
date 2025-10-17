from meteolyzer import Processor, Loader

class Pipeline:
    # TODO: logging

    def __init__(self, loader: Loader, processor: Processor, nproc: int):
        self.loader = loader
        self.processor = processor
        self.nproc = nproc

    def run(self):
        while (batch := self.loader._load_batch(self.nproc)):
            self.processor._process_batch(batch)
