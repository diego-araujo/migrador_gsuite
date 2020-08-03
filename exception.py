# define Python user-defined exceptions
class ProcessException(Exception):

    def __init__(self, message, e):
        super().__init__("{0}: {1}".format(str(message), str(e)))