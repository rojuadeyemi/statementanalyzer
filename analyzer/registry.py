class ProcessorRegistry:
    _processors = []

    @classmethod
    def register(cls, processor_cls):
        cls._processors.append(processor_cls)

    @classmethod
    def get_processors(cls):
        return cls._processors
