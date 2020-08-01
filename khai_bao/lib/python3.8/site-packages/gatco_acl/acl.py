from .models import Ability
from .exceptions import AccessDenied

class ACL(object):

    """This class is used to control the Abilities Integration to one or more Gatco applications"""

    def __init__(self, app=None, **kwargs):
        self._authorization_method = None
        self.get_current_user = self.default_user_loader
        self.app = None
        if app is not None:
            self.init_app(app, **kwargs)

    def get_app(self, reference_app=None):
        """Helper method that implements the logic to look up an application."""

        if reference_app is not None:
            return reference_app

        if self.app is not None:
            return self.app

        raise RuntimeError('Application not registered on ACL'
                           ' instance and no application bound'
                           ' to current context')

    def init_app(self, app, **kwargs):
        
        self.app = app

        if not hasattr(self.app, 'extensions'):
            self.app.extensions = {}

        self.app.extensions['acl'] = self

    def default_user_loader(self, request):
        raise AccessDenied("Expected user_loader method to be set")
        #if hasattr(g, 'current_user'):
        #    return g.current_user
        #elif hasattr(g, 'user'):
        #    return g.user
        #else:
        #    raise Exception("Excepting current_user on flask's g")

    def user_loader(self, value):
        """
        Use this method decorator to overwrite the default user loader
        """
        self.get_current_user = value
        return value

    def authorization_method(self, value):
        """
        the callback for defining user abilities
        """
        self._authorization_method = value
        return self._authorization_method

    def get_authorization_method(self):
        if self._authorization_method is not None:
            return self._authorization_method
        else:
            raise AccessDenied('Expected authorication method to be set')
        
    def ensure(self, request, action, subject):
        current_user = self.get_current_user(request)
        if self.cannot(request, action, subject):
            msg = "{0} does not have {1} access to {2}".format(current_user, action, subject)
            raise AccessDenied(msg)
    
    def can(self, request, action, subject):
        current_user = self.get_current_user(request)
        ability = Ability(current_user, self.get_authorization_method())
        return ability.can(action, subject)
            
    
    def cannot(self, request, action, subject):
        return not self.can(request, action, subject)