"""
get message base class
"""
import abc
from base.observer import Observer


class Subject(abc.ABC):
    """
    subject
    """
    action_objects = []

    @abc.abstractmethod
    def get_message(self):
        """
        get message by kafka or canal_server
        """
        pass

    def attach(self, action_object: Observer):
        """
        add action_object
        """
        self.action_objects.append(action_object)

    def detach(self, action_object: Observer):
        """
        remove  action_object
        """
        self.action_objects.remove(action_object)

    def notify(self, message: dict):
        """
        具体的执行
        """
        try:
            for action in self.action_objects:
                action.update(message)
        except:
            import traceback
            traceback.print_exc()
