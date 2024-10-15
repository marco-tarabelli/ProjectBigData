from abc import ABC, abstractmethod
# ISensorManager interface: defines the interface for a sensor manager
class ISensorManager:
    # Method: adds a sensor to the manager
    @abstractmethod
    def add_sensor(self, sensor):
        
        pass

    # Method: runs the iterations for all sensors in the manager
    @abstractmethod
    def run_sensors(self, iterations):
        
        pass