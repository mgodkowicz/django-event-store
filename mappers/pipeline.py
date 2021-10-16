

class Pipeline:
    def __init__(self, *transformations):
        self.transformations = transformations

    def dump(self, domain_event):
        for transformation in self.transformations:
            domain_event = transformation.dump(domain_event)
        return domain_event

    def load(self, record):
        for transformation in self.transformations[::-1]:
            record = transformation.load(record)

        return record
