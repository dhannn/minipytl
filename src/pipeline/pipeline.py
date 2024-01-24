from __future__ import annotations
import logging
import pandas as pd
from queue import Queue

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)-8s %(message)s',)

class ETLComponent:
    """Base class for components in the ETL pipeline."""
    def run(self, staging_area: dict[str, pd.DataFrame]):
        """Abstract method to be implemented by subclasses.
        
        Args:
            staging_area (dict): The staging area containing DataFrames.
        """
        raise NotImplementedError('Subclasses must implement run() method')

class ETLPipeline:
    """Class representing the ETL pipeline."""
    components: Queue = Queue()
    staging_area: dict[str, pd.DataFrame] = {}

    def enqueue(self, component: ETLComponent) -> ETLPipeline:
        """Enqueue an ETL component for processing.
        
        Args:
            component (ETLComponent): The ETL component to be enqueued.
        
        Returns:
            ETLPipeline: The pipeline object for method chaining.
        """
        self.components.put(component)
        return self

    def start(self):
        """Start the ETL pipeline."""
        while not self.components.empty():
            component: ETLComponent = self.components.get()
            logging.info(f'Running ETL Component { component.__class__.__name__ }')
            component.run(self.staging_area)
            logging.info(f'ETL Component { component.__class__.__name__ } done running')
            self.components.task_done()
    
