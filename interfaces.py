from abc import ABC, abstractmethod


class ManagerInterface(ABC):
    @abstractmethod
    def populate_all(self):
        pass

    @abstractmethod
    def fetch_resource(self, id_):
        pass

    @abstractmethod
    def db_initialize(self):
        pass
