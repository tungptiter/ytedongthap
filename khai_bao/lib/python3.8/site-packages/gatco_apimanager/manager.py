from collections import defaultdict
from collections import namedtuple
from gatco import Blueprint

from .views import ModelView

class IllegalArgumentError(Exception):
    """This exception is raised when a calling function has provided illegal
    arguments to a function or method.

    """
    pass

#: The set of methods which are allowed by default when creating an API
READONLY_METHODS = frozenset(('GET', ))

RestInfo = namedtuple('RestInfo', ['db', 'universal_preprocess', 'universal_postprocess'])

class APIManager(object):
    name = "restapi"
    APINAME_FORMAT = '{0}api'
    BLUEPRINTNAME_FORMAT = '{0}{1}'
    view_cls = None
    
    @staticmethod
    def _next_blueprint_name(blueprints, basename):
        
        # blueprints is a dict whose keys are the names of the blueprints
        existing = [name for name in blueprints if name.startswith(basename)]
        # if this is the first one...
        if not existing:
            next_number = 0
        else:
            # for brevity
            b = basename
            existing_numbers = [int(n.partition(b)[-1]) for n in existing]
            next_number = max(existing_numbers) + 1
        return APIManager.BLUEPRINTNAME_FORMAT.format(basename, next_number)
    
    @staticmethod
    def api_name(collection_name):
        """Returns the name of the :class:`API` instance exposing models of the
        specified type of collection.

        `collection_name` must be a string.

        """
        return APIManager.APINAME_FORMAT.format(collection_name)
    
    def __init__(self, name="restapi", app=None, **kw):
        self.name = name
        self.app = app
        self.apis_to_create = defaultdict(list)

        #: A mapping whose keys are models for which this object has created an
        #: API via the :meth:`create_api_blueprint` method and whose values are
        #: the corresponding collection names for those models.
        self.created_apis_for = {}

        # Stash this instance so that it can be examined later by other
        # functions in this module.
        #url_for.created_managers.append(self)

        if self.app is not None:
            self.init_app(self.app, **kw)
            
            
    def init_app(self, app, view_cls=ModelView, preprocess=None, postprocess=None, db=None, *args, **kw):
        
        if not hasattr(app, 'extensions'):
            app.extensions = {}
            
        if self.name in app.extensions:
            raise ValueError(self.name + ' has already been initialized on'
                             ' this application: {0}'.format(app))
        app.extensions[self.name] = RestInfo(db, preprocess or {}, postprocess or {})
        
        if app is not None:
            self.app = app
            
        if view_cls is not None:
            self.view_cls = view_cls
            
        apis = self.apis_to_create
        to_create = apis.pop(app, []) + apis.pop(None, [])
        
        for args, kw in to_create:
            blueprint = self.create_api_blueprint(app=app, *args, **kw)
            app.register_blueprint(blueprint)
            
    def create_api_blueprint(self, model=None, collection_name=None, app=None, methods=READONLY_METHODS,
                             url_prefix='/api', exclude_columns=None,
                             include_columns=None, include_methods=None,
                             results_per_page=10, max_results_per_page=100,
                             preprocess=None, postprocess=None, primary_key=None, *args, **kw):
        if collection_name is None:
            msg = ('collection_name is not valid.')
            raise IllegalArgumentError(msg)
            
        if exclude_columns is not None and include_columns is not None:
            msg = ('Cannot simultaneously specify both include columns and'
                   ' exclude columns.')
            raise IllegalArgumentError(msg)
        
        if app is None:
            app = self.app
            
        restapi_ext = app.extensions[self.name]
        
        methods = frozenset((m.upper() for m in methods))
        no_instance_methods = methods & frozenset(('POST', ))
        instance_methods = methods & frozenset(('GET', 'PATCH', 'DELETE', 'PUT'))
        possibly_empty_instance_methods = methods & frozenset(('GET', ))
        
        # the base URL of the endpoints on which requests will be made
        collection_endpoint = '/{0}'.format(collection_name)
        
        apiname = APIManager.api_name(collection_name)
        
        preprocessors_ = defaultdict(list)
        postprocessors_ = defaultdict(list)
        preprocessors_.update(preprocess or {})
        postprocessors_.update(postprocess or {})
        
        api_view = self.view_cls.as_view(model=model, collection_name=collection_name,exclude_columns=exclude_columns,\
                            include_columns=include_columns, include_methods=include_methods,\
                            results_per_page=results_per_page, max_results_per_page=max_results_per_page, \
                            preprocess=preprocessors_, postprocess=postprocessors_, primary_key=primary_key,\
                            db=restapi_ext.db)
                               
        blueprintname = APIManager._next_blueprint_name(app.blueprints, apiname)
        blueprint = Blueprint(blueprintname, url_prefix=url_prefix)
        blueprint.add_route(api_view,collection_endpoint,methods=no_instance_methods)
        
        #DELETE, GET, PUT
        instance_endpoint = '{0}/<instid>'.format(collection_endpoint)
        blueprint.add_route(api_view,instance_endpoint,methods=instance_methods)
        
        return blueprint
    
    def create_api(self, *args, **kw):
        # Check if the user is providing a specific Flask application with
        # which the model's API will be associated.
        if 'app' in kw:
            # If an application object was already provided in the constructor,
            # raise an error indicating that the user is being confusing.
            if self.app is not None:
                msg = ('Cannot provide a Gatco application in the APIManager'
                       ' constructor and in create_api(); must choose exactly'
                       ' one')
                raise IllegalArgumentError(msg)
            app = kw.pop('app')
            # If the Flask application has already been initialized, then
            # immediately create the API blueprint.
            #
            # TODO This is something of a fragile check for whether or not
            # init_app() has been called on kw['app'], since some other
            # (malicious) code could simply add the key 'restless' to the
            # extensions dictionary.
            if self.name in app.extensions:
                blueprint = self.create_api_blueprint(app=app, *args, **kw)
                app.register_blueprint(blueprint)
            # If the Flask application has not yet been initialized, then stash
            # the positional and keyword arguments for later initialization.
            else:
                self.apis_to_create[app].append((args, kw))
        # The user did not provide a Flask application here.
        else:
            # If a Flask application object was already provided in the
            # constructor, immediately create the API blueprint.
            if self.app is not None:
                app = self.app
                blueprint = self.create_api_blueprint(app=app, *args, **kw)
                app.register_blueprint(blueprint)
            # If no Flask application was provided in the constructor either,
            # then stash the positional and keyword arguments for later
            # initalization.
            else:
                self.apis_to_create[None].append((args, kw))