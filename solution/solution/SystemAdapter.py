import json

from sdk.AbstractSystemAdapter import AbstractSystemAdapter


class SystemAdapter(AbstractSystemAdapter):

    def __init__(self, predictor):
        self.predictor = predictor
        self.count = 0

    # this exists b.c. of how the AbstractSystemAdapter is built
    def init(self):
        super().init()



