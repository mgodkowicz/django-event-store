import yaml


class YAML:
    """
    Port of https://github.com/RailsEventStore/rails_event_store/blob/v2.11.1/ruby_event_store/lib/ruby_event_store/serializers/yaml.rb
    """

    @staticmethod
    def dump(value):
        return yaml.dump(value, explicit_start=True).replace("...\n", "")

    @staticmethod
    def load(serialized):
        return yaml.unsafe_load(serialized)
