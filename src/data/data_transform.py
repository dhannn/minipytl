from pipeline.pipeline import ETLComponent

class DataTransform(ETLComponent):
    """Class for data transformation."""
    func = None

    def __init__(self, func):
        """Initialize DataTransform with a transformation function.
        
        Args:
            func (function): The transformation function.
        """
        self.func = func
    
    def run(self, staging_area):
        """Run the data transformation.
        
        Args:
            staging_area (dict): The staging area containing DataFrames.
        """
        self.func(staging_area)
