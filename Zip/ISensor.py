from abc import ABC, abstractmethod

class ISensor:
    # Method: runs the sensor's iterations
    @abstractmethod
    def run(self, iterations,description):
        pass

    # Method: read the information about the sensor
    @abstractmethod
    def read_data(self):
        pass