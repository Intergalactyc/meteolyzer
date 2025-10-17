from meteolyzer import Processor, Loader
from tqdm import tqdm

class Pipeline:
    # TODO: logging

    def __init__(self, loader: Loader, processor: Processor, nproc: int):
        self.loader = loader
        self.processor = processor
        self.nproc = nproc

    def run(self):
        pbar = tqdm(total=self.loader.count)
        while (batch := self.loader._load_batch(self.nproc)):
            self.processor._process_batch(batch, self.nproc, pbar)
