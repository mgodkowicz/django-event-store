from mappers.transformations.domain_event import DomainEvent


class Pipeline:
    def __init__(self, *transformations, to_domain_event=DomainEvent()):
        self.transformations = [to_domain_event, *transformations]

    def dump(self, domain_event):
        for transformation in self.transformations:
            domain_event = transformation.dump(domain_event)
        return domain_event

    def load(self, record):
        for transformation in self.transformations[::-1]:
            record = transformation.load(record)

        return record
