"""
client base class
"""
import abc


class Observer(abc.ABC):
    """
    Observer mode Base class
    """
    @abc.abstractmethod
    def update(self, message: dict):
        """
        please override
        """
        pass
