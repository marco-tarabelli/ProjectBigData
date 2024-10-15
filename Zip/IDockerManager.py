from abc import ABC, abstractmethod

# IDockerManager interface: defines the interface for a Docker manager
class IDockerManager:
    # Method: starts the Docker container
    @abstractmethod
    def start_container(self):
        
        pass

    # Method: stops the Docker container
    @abstractmethod
    def stop_container(self):
        
        pass

   # Method: run Docker container for specified time
    @abstractmethod
    def run_container(self, keep_alive_ms):
       
        pass

    # Method: gets the existing Docker container
    @abstractmethod
    def get_existing_container(self):
       
        pass
    # Method: remove the existing Docker container
    @abstractmethod
    def remove_container(self):
        pass  