from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime, timedelta
import pathlib
import os
from tqdm import tqdm
from multiprocessing import Pool

def _picklable_worker(worker, *args, **kwargs):
    return worker(*args, **kwargs)

class Processor(ABC):
    def __init__(self, directory: pathlib.Path, *args, **kwargs):
        self._directory = directory
        if not os.path.isdir(directory):
            os.makedirs(directory)

    def _process_batch(self, batch: list[tuple[datetime, pd.DataFrame, timedelta]], nproc: int, pbar: tqdm):
        results = []
        pool = Pool(processes=nproc)
        batch_rules = [(self._worker, df, ts, dt) for ts, df, dt in batch]
        for res in pool.imap(_picklable_worker, batch_rules):
        #for ts, df, dt in batch:
            # TODO: multiprocessing
            #for res in pool.imap(_picklable_worker, self._worker, df, timestamp=ts, interval=dt)
            res |= {"timestamp" : ts}
            results.append(res)
        out = pd.DataFrame(results)
        out.set_index("timestamp", inplace=True)
        out.sort_index(inplace=True)
        start = out.index[0]
        out.to_csv(self._directory.joinpath(start.strftime("%Y-%m-%d_%H-%M-%S") + ".csv"))

    def _worker(self, df, *args, **kwargs):
        return self.process_frame(df)

    @abstractmethod
    def process_frame(self, df: pd.DataFrame, *args, **kwargs) -> dict:
        """
        Result should be a dictionary representing the resulting summary row.

        kwargs will include timestamp (datetime) and interval (timedelta) for optional use.

        However, do not make the timestamp part of the result, it will automatically be added.
        """
        pass
    