from __future__ import annotations
import logging
import pandas as pd
from queue import Queue

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)-8s %(message)s',)

class ETLComponent:
    def run(self, staging_area: pd.DataFrame):
        raise NotImplementedError('Subclasses must implement run() method')

class ETLPipeline:
    components: Queue = Queue()
    staging_area: dict[str, pd.DataFrame] = {}

    def enqueue(self, component: ETLComponent) -> ETLPipeline:
        self.components.put(component)
        return self

    def start(self):
        while not self.components.empty():
            component: ETLComponent = self.components.get()
            logging.info(f'Running ETL Component { component.__class__.__name__ }')
            component.run(self.staging_area)
            logging.info(f'ETL Component { component.__class__.__name__ } done running')
            self.components.task_done()
    
