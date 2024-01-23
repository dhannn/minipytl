from pipeline.pipeline import ETLComponent

class DataTransform(ETLComponent):
    
    func = None

    def __init__(self, func):
        self.func = func
    
    def run(self, staging_area):
        self.func(staging_area)
