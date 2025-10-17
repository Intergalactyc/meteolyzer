from datetime import datetime, timedelta
import os
import pathlib
from meteolyzer import Loader, Processor, Pipeline
import pandas as pd

class BareLoader(Loader):
    frames_per_file = 3

    def __init__(self, config):
        self.directory = config["directory"]
        super().__init__()

    def get_files(self, **kwargs) -> list:
        return [os.path.join(self.directory, f) for f in os.listdir(self.directory)]

    def handle_file(self, filepath, **kwargs) -> list[pd.DataFrame]:
        df = pd.read_csv(filepath)
        chunk_size = len(df) // self.frames_per_file # does assume even divisibility
        timestamp = self.file_to_timestamp(filepath)[0]
        return [
            (
                timestamp + i*chunk_size*timedelta(hours=4),
                df.iloc[chunk_size*i:chunk_size*(i+1)],
                timedelta(hours=4)
            )
            for i in range(self.frames_per_file)
        ]

    def file_to_timestamp(self, filepath: os.PathLike, **kwargs) -> tuple[datetime, datetime]:
        basename = os.path.basename(filepath).split(".")[0]
        d, m, y = [int(x) for x in basename.split("-")]
        return datetime(y, m, d, 0, 0), datetime(y, m, d, 23, 59)

class BareProcessor(Processor):
    def __init__(self, config):
        super().__init__(config["output"])

    def process_frame(self, df, *args, **kwargs):
        return {"rows":len(df), "a":df["a"].mean(), "b":df["b"].mean()}

def get_config():
    p = pathlib.Path(__file__).parent
    return {"directory": p.joinpath("data"), "output": p.joinpath("processed")}

def main():
    args = get_config()
    loader = BareLoader(args)
    processor = BareProcessor(args)
    pipeline = Pipeline(loader, processor, 2)
    pipeline.run()

if __name__ == "__main__":
    main()
