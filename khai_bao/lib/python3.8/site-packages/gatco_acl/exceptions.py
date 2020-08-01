from gatco.exceptions import GatcoException

class AccessDenied(GatcoException):
    """ This error is raised when a user is not allowed to access a resource
    """
    pass