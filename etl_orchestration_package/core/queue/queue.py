class Queue:
    def __init__(self, **queue_config):
        self.consumers = dict()
        self.producers = dict()

        for consumer_config in queue_config.get('consumers_configs', list()):
            self.consumers.update({
                consumer_config.pop('consumer_id'): self.create_consumer(**consumer_config)
            })

        for producer_config in queue_config.get('producers_configs', list()):
            self.producers.update({
                producer_config.pop('producer_id'): self.create_producer(**producer_config)
            })

    def create_consumer(self, **consumer_config):
        pass

    def create_producer(self, **producer_config):
        pass

    def get_consumers(self):
        return [c for c in self.consumers.values()]

    def get_producers(self):
        return [p for p in self.producers.values()]

    def get_consumer(self, consumer_id):
        return self.consumers.get(consumer_id)

    def get_producer(self, producer_id):
        return self.producers.get(producer_id)

    def consume_data(self, consumer_id):
        pass

    def send_message(self, producer_id, message):
        pass
