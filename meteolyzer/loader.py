from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import os
import pandas as pd
from multiprocessing import Queue

def _picklable_worker(worker, *args, **kwargs):
    return worker(*args, **kwargs)

class Loader(ABC):
    def __init__(self, *args, **kwargs):
        self._loaded_and_waiting = Queue()
        all_files = self.get_files()
        self._to_process = self.get_files()
        self._to_process.reverse()
        self.count = len(all_files)

        try:
            self._timestamps = [self.file_to_timestamp(f) for f in all_files]
            self._files = all_files
        except NotImplementedError:
            pass

    def _load_batch(self, batch_size: int) -> list[pd.DataFrame]:
        results = []
        while not self._loaded_and_waiting.empty():
            results.append(self._loaded_and_waiting.get())
        for _ in range(len(results), batch_size):
            # TODO: multithreading
            if not self._to_process:
                break
            for frame in _picklable_worker(self._worker, self._to_process.pop()):
                results.append(frame)
        for r in results[batch_size:]:
            self._loaded_and_waiting.put(r)
        return results[:batch_size]

    def _worker(self, filepath):
        return self.handle_file(filepath)

    @property
    def frames_per_file(*args, **kwargs) -> int:
        """
        Must set this to an integer representing how many dataframes each file is split into.
        Future: would like some way to handle a variable number.
        """
        raise NotImplementedError("Attribute frames_per_file must be specified in Loader subclass definition")

    @abstractmethod
    def get_files(self, **kwargs) -> list[os.PathLike]:
        """
        Get list of files to process, possibly based on some configuration passed at init.
        """
        pass

    @abstractmethod
    def handle_file(self, filepath, **kwargs) -> tuple[datetime, pd.DataFrame, timedelta|pd.Timedelta]:
        """
        The bread and butter - define how to handle a file, such that in the end it is split
        into <frames_per_file> different pandas DataFrames, returned as a tuple
        start_timestamp, dataframe, sample_interval

        Determining start_timestamp may make use of file_to_timestamp (though for files being
        split into several frames, offsets should be added to the result of this for each).
        
        No need for there to be a timestamp column/index in the resulting dataframes.
        """
        pass

    def file_to_timestamp(self, filepath: os.PathLike, **kwargs) -> tuple[datetime, datetime]:
        """
        Define how to determine a time interval from a file path.

        Should return the start and end timestamp of the file's data.
        
        Recommended that this is implemented but it is not strictly necessary.
        """
        return NotImplementedError(f"file_to_timestamp has not been implemented for {self.__class__}; either it or a custom timestamp_to_file method must be implemented")

    def timestamp_to_file(self, timestamp: datetime, **kwargs) -> os.PathLike:
        """
        Define how to get a file path from a timestamp.

        Should return the path of the file which contains the data.

        file_to_timestamp should be specified for this implementation to work;
        alternatively, this method may be custom-written.

        If no file contains that timestamp, should return None.
        """
        try:
            ts = self._timestamps
            fs = self._files
        except AttributeError:
            raise NotImplementedError("file_to_timestamp must be implemented for default timestamp_to_file implementation to succeed")

        for (start, end), f in zip(ts, fs): # Future: binary search
            if start <= timestamp <= end:
                return f
        
        return None
