import asyncio
from collections import defaultdict
from functools import wraps
import math
import warnings

from gatco.exceptions import GatcoException, ServerError
from gatco.response import json, text, HTTPResponse
from gatco.request import json_loads
from gatco.views import HTTPMethodView

from sqlalchemy import Column
from sqlalchemy.exc import DataError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.query import Query

from sqlalchemy import inspect



from .helpers import count
from .helpers import evaluate_functions
from .helpers import get_by
from .helpers import get_columns
from .helpers import get_or_create
from .helpers import get_related_model
from .helpers import get_relations
from .helpers import has_field
from .helpers import is_like_list
from .helpers import partition
from .helpers import primary_key_name
from .helpers import query_by_primary_key
from .helpers import session_query
from .helpers import strings_to_dates
from .helpers import to_dict
from .helpers import upper_keys
from .helpers import get_related_association_proxy_model
from .search import create_query
from .search import search

from .. import ModelView

#from .mimerender import SanicMimeRender

#: Format string for creating Link headers in paginated responses.
LINKTEMPLATE = '<{0}?page={1}&results_per_page={2}>; rel="{3}"'

#: String used internally as a dictionary key for passing header information
#: from view functions to the :func:`jsonpify` function.
_HEADERS = '__restapi_headers'

#: String used internally as a dictionary key for passing status code
#: information from view functions to the :func:`jsonpify` function.
_STATUS = '__restapi_status_code'


class ProcessingException(GatcoException):
    """Raised when a preprocess or postprocess encounters a problem.

    This exception should be raised by functions supplied in the
    ``preprocess`` and ``postprocess`` keyword arguments to
    :class:`APIManager.create_api`. When this exception is raised, all
    preprocessing or postprocessing halts, so any processors appearing later in
    the list will not be invoked.

    `code` is the HTTP status code of the response supplied to the client in
    the case that this exception is raised. `description` is an error message
    describing the cause of this exception. This message will appear in the
    JSON object in the body of the response to the client.

    """
    def __init__(self, message='', status_code=520):
        super(ProcessingException, self).__init__(message, status_code)
        self.status_code = status_code
        self.message = message

def response_exception(exception):
    if type(exception.message) is dict:
        return json(exception.message, status=exception.status_code)
    else:
        return text(exception.message, status=exception.status_code)


class ValidationError(GatcoException):
    """Raised when there is a problem deserializing a dictionary into an
    instance of a SQLAlchemy model.

    """
    pass

def _is_msie8or9(request):
    #TODO: user request
    """Returns ``True`` if and only if the user agent of the client making the
    request indicates that it is Microsoft Internet Explorer 8 or 9.

    .. note::

       We have no way of knowing if the user agent is lying, so we just make
       our best guess based on the information provided.

    """
    # request.user_agent.version comes as a string, so we have to parse it
    version = lambda ua: tuple(int(d) for d in ua.version.split('.'))
    return (request.user_agent is not None
            and request.user_agent.version is not None
            and request.user_agent.browser == 'msie'
            and (8, 0) <= version(request.user_agent) < (10, 0))


def create_link_string(request, page, last_page, per_page):
    """Returns a string representing the value of the ``Link`` header.

    `page` is the number of the current page, `last_page` is the last page in
    the pagination, and `per_page` is the number of results per page.

    """
    linkstring = ''
    if page < last_page:
        next_page = page + 1
        linkstring = LINKTEMPLATE.format(request.url, next_page,
                                         per_page, 'next') + ', '
    linkstring += LINKTEMPLATE.format(request.url, last_page,
                                      per_page, 'last')
    return linkstring

def catch_integrity_errors(session):
    """Returns a decorator that catches database integrity errors.

    `session` is the SQLAlchemy session in which all database transactions will
    be performed.

    View methods can be wrapped like this::

        @catch_integrity_errors(session)
        def get(self, *args, **kw):
            return '...'

    Specifically, functions wrapped with the returned decorator catch
    :exc:`IntegrityError`s, :exc:`DataError`s, and
    :exc:`ProgrammingError`s. After the exceptions are caught, the session is
    rolled back, the exception is logged on the current Flask application, and
    an error response is returned to the client.

    """
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kw):
            try:
                return func(*args, **kw)
            # TODO should `sqlalchemy.exc.InvalidRequestError`s also be caught?
            except (DataError, IntegrityError, ProgrammingError) as exception:
                session.rollback()
                #current_app.logger.exception(str(exception))
                #return dict(message=type(exception).__name__), 400
                return json({"message":type(exception).__name__}, status=520)
        return wrapped
    return decorator

def set_headers(response, headers):
    """Sets the specified headers on the specified response.

    `response` is a Flask response object, and `headers` is a dictionary of
    headers to set on the specified response. Any existing headers that
    conflict with `headers` will be overwritten.

    """
    for key, value in headers.items():
        response.headers[key] = value


# This code is (lightly) adapted from the ``requests`` library, in the
# ``requests.utils`` module. See <http://python-requests.org> for more
# information.
def _link_to_json(value):
    """Returns a list representation of the specified HTTP Link header
    information.

    `value` is a string containing the link header information. If the link
    header information (the part of after ``Link:``) looked like this::

        <url1>; rel="next", <url2>; rel="foo"; bar="baz"

    then this function returns a list that looks like this::

        [{"url": "url1", "rel": "next"},
         {"url": "url2", "rel": "foo", "bar": "baz"}]

    This example is adapted from the documentation of GitHub's API.

    """
    links = []
    replace_chars = " '\""
    for val in value.split(","):
        try:
            url, params = val.split(";", 1)
        except ValueError:
            url, params = val, ''
        link = {}
        link["url"] = url.strip("<> '\"")
        for param in params.split(";"):
            try:
                key, value = param.split("=")
            except ValueError:
                break
            link[key.strip(replace_chars)] = value.strip(replace_chars)
        links.append(link)
    return links


def _headers_to_json(headers):
    """Returns a dictionary representation of the specified dictionary of HTTP
    headers ready for use as a JSON object.

    Pre-condition: headers is not ``None``.

    """
    link = headers.pop('Link', None)
    # Shallow copy is fine here because the `headers` dictionary maps strings
    # to strings to strings.
    result = headers.copy()
    if link:
        result['Link'] = _link_to_json(link)
    return result


def _parse_includes(column_names):
    """Returns a pair, consisting of a list of column names to include on the
    left and a dictionary mapping relation name to a list containing the names
    of fields on the related model which should be included.

    `column_names` must be a list of strings.

    If the name of a relation appears as a key in the dictionary, then it will
    not appear in the list.

    """
    dotted_names, columns = partition(column_names, lambda name: '.' in name)
    # Create a dictionary mapping relation names to fields on the related
    # model.
    relations = defaultdict(list)
    for name in dotted_names:
        relation, field = name.split('.', 1)
        # Only add the relation if it's column has been specified.
        if relation in columns:
            relations[relation].append(field)
    # Included relations need only be in the relations dictionary, not the
    # columns list.
    for relation in relations:
        if relation in columns:
            columns.remove(relation)
    return columns, relations


def _parse_excludes(column_names):
    """Returns a pair, consisting of a list of column names to exclude on the
    left and a dictionary mapping relation name to a list containing the names
    of fields on the related model which should be excluded.

    `column_names` must be a list of strings.

    If the name of a relation appears in the list then it will not appear in
    the dictionary.

    """
    dotted_names, columns = partition(column_names, lambda name: '.' in name)
    # Create a dictionary mapping relation names to fields on the related
    # model.
    relations = defaultdict(list)
    for name in dotted_names:
        relation, field = name.split('.', 1)
        # Only add the relation if it's column has not been specified.
        if relation not in columns:
            relations[relation].append(field)
    # Relations which are to be excluded entirely need only be in the columns
    # list, not the relations dictionary.
    for column in columns:
        if column in relations:
            del relations[column]
    return columns, relations


def extract_error_messages(exception):
    """Tries to extract a dictionary mapping field name to validation error
    messages from `exception`, which is a validation exception as provided in
    the ``validation_exceptions`` keyword argument in the constructor of this
    class.

    Since the type of the exception is provided by the user in the constructor
    of this class, we don't know for sure where the validation error messages
    live inside `exception`. Therefore this method simply attempts to access a
    few likely attributes and returns the first one it finds (or ``None`` if no
    error messages dictionary can be extracted).

    """
    # 'errors' comes from sqlalchemy_elixir_validations
    if hasattr(exception, 'errors'):
        return exception.errors
    # 'message' comes from savalidation
    if hasattr(exception, 'message'):
        # TODO this works only if there is one validation error
        try:
            left, right = str(exception).rsplit(':', 1)
            left_bracket = left.rindex('[')
            right_bracket = right.rindex(']')
        except ValueError as exc:
            #current_app.logger.exception(str(exc))
            # could not parse the string; we're not trying too hard here...
            return None
        msg = right[:right_bracket].strip(' "')
        fieldname = left[left_bracket + 1:].strip()
        return {fieldname: msg}
    return None



# class ModelView(HTTPMethodView):
#     """Base class for :class:`sanic.views.HTTPMethodView` classes which represent a view
#     of a SQLAlchemy model.
# 
#     The model class for this view can be accessed from the :attr:`model`
#     attribute, and the session in which all database transactions will be
#     performed when dealing with this model can be accessed from the
#     :attr:`session` attribute.
# 
#     When subclasses wish to make queries to the database model specified in the
#     constructor, they should access the ``self.query`` function, which
#     delegates to the appropriate SQLAlchemy query object or Flask-SQLAlchemy
#     query object, depending on how the model has been defined.
# 
#     """
# 
#     #: List of decorators applied to every method of this class.
#     #decorators = [mimerender]
#     decorators = []
# 
#     def __init__(self, session, model, *args, **kw):
#         """Calls the constructor of the superclass and specifies the model for
#         which this class provides a ReSTful API.
# 
#         `session` is the SQLAlchemy session in which all database transactions
#         will be performed.
# 
#         `model` is the SQLALchemy declarative model class of the database model
#         for which this instance of the class is an API.
# 
#         """
#         super(ModelView, self).__init__(*args, **kw)
#         self.session = session
#         self.model = model
# 
#     def query(self, model=None):
#         """Returns either a SQLAlchemy query or Flask-SQLAlchemy query object
#         (depending on the type of the model) on the specified `model`, or if
#         `model` is ``None``, the model specified in the constructor of this
#         class.
# 
#         """
#         return session_query(self.session, model or self.model)
# 
# class FunctionAPI(ModelView):
#     """Provides method-based dispatching for :http:method:`get` requests which
#     wish to apply SQL functions to all instances of a model.
# 
#     .. versionadded:: 0.4
# 
#     """
# 
#     async def get(self, request):
#         """Returns the result of evaluating the SQL functions specified in the
#         body of the request.
# 
#         For a description of the request and response formats, see
#         :ref:`functionevaluation`.
# 
#         """
#         if 'q' not in request.args or not request.args.get('q'):
#             return json(dict(message='Empty query parameter'), status=520)
#         # if parsing JSON fails, return a 400 error in JSON format
#         try:
#             data = json.loads(str(request.args.get('q'))) or {}
#         except (TypeError, ValueError, OverflowError) as exception:
#             #current_app.logger.exception(str(exception))
#             return json(dict(message='Unable to decode data'), status=520)
#         try:
# 
#             result = evaluate_functions(self.session, self.model,
#                                         data.get('functions', []))
#             if not result:
#                 return json({}, status=204)
#             return json(result, status=200)
#         except AttributeError as exception:
#             #current_app.logger.exception(str(exception))
#             message = 'No such field "{0}"'.format(exception.field)
#             return json(dict(message=message), status=520)
#         except OperationalError as exception:
#             #current_app.logger.exception(str(exception))
#             message = 'No such function "{0}"'.format(exception.function)
#             return json(dict(message=message), status=520)


class APIView(ModelView):
    db = None
    session = None
    #: List of decorators applied to every method of this class.
    #decorators = ModelView.decorators + [catch_processing_exceptions]
    #decorators = [catch_processing_exceptions]
    
    def __init__(self, model=None, collection_name=None, exclude_columns=None,
                 include_columns=None, include_methods=None, results_per_page=10,
                 max_results_per_page=1000, preprocess=None, postprocess=None,
                 primary_key=None, db=None, *args, **kw):
    
#     def __init__(self, model=None, collection_name=None, exclude_columns=None,
#                  include_columns=None, include_methods=None, results_per_page=10,
#                  max_results_per_page=1000, preprocess=None, postprocess=None,
#                  primary_key=None, db=None, serializer=None, deserializer=None, *args, **kw):

#     def __init__(self, session, model, exclude_columns=None,
#                  include_columns=None, include_methods=None,
#                  validation_exceptions=None, results_per_page=10,
#                  max_results_per_page=100, post_form_preprocessor=None,
#                  preprocess=None, postprocess=None, primary_key=None,
#                  serializer=None, deserializer=None, *args, **kw):


        super(APIView, self).__init__(model,collection_name, exclude_columns, include_columns, \
                                  include_methods, results_per_page, max_results_per_page, \
                                  preprocess, postprocess, primary_key, db, *args, **kw)
        
        self.session = kw.pop('session', None)
        validation_exceptions = kw.get('validation_exceptions', None)
        
        serializer = kw.get('serializer', None)
        deserializer = kw.get('deserializer', None)
        
        
        if db is not None:
            if self.session is None:
                self.session = getattr(self.db, 'session', None)
        
        if exclude_columns is None:
            self.exclude_columns, self.exclude_relations = (None, None)
        else:
            self.exclude_columns, self.exclude_relations = _parse_excludes(
                [self._get_column_name(column) for column in exclude_columns])
        if include_columns is None:
            self.include_columns, self.include_relations = (None, None)
        else:
            self.include_columns, self.include_relations = _parse_includes(
                [self._get_column_name(column) for column in include_columns])
        self.include_methods = include_methods
        
        self.validation_exceptions = tuple(validation_exceptions or ())
        
        
#         self.results_per_page = results_per_page
#         self.max_results_per_page = max_results_per_page
#         self.primary_key = primary_key
        # Use our default serializer and deserializer if none are specified.
        if serializer is None:
            self.serialize = self._inst_to_dict
        else:
            self.serialize = serializer
        if deserializer is None:
            self.deserialize = self._dict_to_inst
            # And check for our own default ValidationErrors here
            self.validation_exceptions = tuple(list(self.validation_exceptions)
                                               + [ValidationError])
        else:
            self.deserialize = deserializer
        

        # HACK: We would like to use the :attr:`API.decorators` class attribute
        # in order to decorate each view method with a decorator that catches
        # database integrity errors. However, in order to rollback the session,
        # we need to have a session object available to roll back. Therefore we
        # need to manually decorate each of the view functions here.
        decorate = lambda name, f: setattr(self, name, f(getattr(self, name)))
        for method in ['get', 'post', 'patch', 'put', 'delete']:
            decorate(method, catch_integrity_errors(self.session))

    def _get_column_name(self, column):
        """Retrieve a column name from a column attribute of SQLAlchemy
        model class, or a string.

        Raises `TypeError` when argument does not fall into either of those
        options.

        Raises `ValueError` if argument is a column attribute that belongs
        to an incorrect model class.

        """
        if hasattr(column, '__clause_element__'):
            clause_element = column.__clause_element__()
            if not isinstance(clause_element, Column):
                msg = ('Column must be a string or a column attribute'
                       ' of SQLAlchemy ORM class')
                raise TypeError(msg)
            model = column.class_
            if model is not self.model:
                msg = ('Cannot specify column of model {0} while creating API'
                       ' for model {1}').format(model.__name__,
                                                self.model.__name__)
                raise ValueError(msg)
            return clause_element.key

        return column

    def _add_to_relation(self, query, relationname, toadd=None):
        """Adds a new or existing related model to each model specified by
        `query`.

        This function does not commit the changes made to the database. The
        calling function has that responsibility.

        `query` is a SQLAlchemy query instance that evaluates to all instances
        of the model specified in the constructor of this class that should be
        updated.

        `relationname` is the name of a one-to-many relationship which exists
        on each model specified in `query`.

        `toadd` is a list of dictionaries, each representing the attributes of
        an existing or new related model to add. If a dictionary contains the
        key ``'id'``, that instance of the related model will be
        added. Otherwise, the :func:`helpers.get_or_create` class method will
        be used to get or create a model to add.

        """
        submodel = get_related_model(self.model, relationname)
        if isinstance(toadd, dict):
            toadd = [toadd]
        for dictionary in toadd or []:
            subinst = get_or_create(self.session, submodel, dictionary)
            try:
                for instance in query:
                    getattr(instance, relationname).append(subinst)
            except AttributeError as exception:
                #current_app.logger.exception(str(exception))
                setattr(instance, relationname, subinst)

    def _remove_from_relation(self, query, relationname, toremove=None):
        """Removes a related model from each model specified by `query`.

        This function does not commit the changes made to the database. The
        calling function has that responsibility.

        `query` is a SQLAlchemy query instance that evaluates to all instances
        of the model specified in the constructor of this class that should be
        updated.

        `relationname` is the name of a one-to-many relationship which exists
        on each model specified in `query`.

        `toremove` is a list of dictionaries, each representing the attributes
        of an existing model to remove. If a dictionary contains the key
        ``'id'``, that instance of the related model will be
        removed. Otherwise, the instance to remove will be retrieved using the
        other attributes specified in the dictionary. If multiple instances
        match the specified attributes, only the first instance will be
        removed.

        If one of the dictionaries contains a mapping from ``'__delete__'`` to
        ``True``, then the removed object will be deleted after being removed
        from each instance of the model in the specified query.

        """
        submodel = get_related_model(self.model, relationname)
        for dictionary in toremove or []:
            remove = dictionary.pop('__delete__', False)
            if 'id' in dictionary:
                subinst = get_by(self.session, submodel, dictionary['id'])
            else:
                subinst = self.query(submodel).filter_by(**dictionary).first()
            for instance in query:
                getattr(instance, relationname).remove(subinst)
            if remove:
                self.session.delete(subinst)

    def _set_on_relation(self, query, relationname, toset=None):
        """Sets the value of the relation specified by `relationname` on each
        instance specified by `query` to have the new or existing related
        models specified by `toset`.

        This function does not commit the changes made to the database. The
        calling function has that responsibility.

        `query` is a SQLAlchemy query instance that evaluates to all instances
        of the model specified in the constructor of this class that should be
        updated.

        `relationname` is the name of a one-to-many relationship which exists
        on each model specified in `query`.

        `toset` is either a dictionary or a list of dictionaries, each
        representing the attributes of an existing or new related model to
        set. If a dictionary contains the key ``'id'``, that instance of the
        related model will be added. Otherwise, the
        :func:`helpers.get_or_create` method will be used to get or create a
        model to set.

        """
        submodel = get_related_model(self.model, relationname)
        if isinstance(toset, list):
            value = [get_or_create(self.session, submodel, d) for d in toset]
        else:
            value = get_or_create(self.session, submodel, toset)
        for instance in query:
            setattr(instance, relationname, value)

    # TODO change this to have more sensible arguments
    def _update_relations(self, query, params):
        """Adds, removes, or sets models which are related to the model
        specified in the constructor of this class.

        This function does not commit the changes made to the database. The
        calling function has that responsibility.

        This method returns a :class:`frozenset` of strings representing the
        names of relations which were modified.

        `query` is a SQLAlchemy query instance that evaluates to all instances
        of the model specified in the constructor of this class that should be
        updated.

        `params` is a dictionary containing a mapping from name of the relation
        to modify (as a string) to either a list or another dictionary. In the
        former case, the relation will be assigned the instances specified by
        the elements of the list, which are dictionaries as described below.
        In the latter case, the inner dictionary contains at most two mappings,
        one with the key ``'add'`` and one with the key ``'remove'``. Each of
        these is a mapping to a list of dictionaries which represent the
        attributes of the object to add to or remove from the relation.

        If one of the dictionaries specified in ``add`` or ``remove`` (or the
        list to be assigned) includes an ``id`` key, the object with that
        ``id`` will be attempt to be added or removed. Otherwise, an existing
        object with the specified attribute values will be attempted to be
        added or removed. If adding, a new object will be created if a matching
        object could not be found in the database.

        If a dictionary in one of the ``'remove'`` lists contains a mapping
        from ``'__delete__'`` to ``True``, then the removed object will be
        deleted after being removed from each instance of the model in the
        specified query.

        """
        relations = get_relations(self.model)
        tochange = frozenset(relations) & frozenset(params)

        for columnname in tochange:
            # Check if 'add' or 'remove' is being used
            if (isinstance(params[columnname], dict)
                and any(k in params[columnname] for k in ['add', 'remove'])):

                toadd = params[columnname].get('add', [])
                toremove = params[columnname].get('remove', [])
                self._add_to_relation(query, columnname, toadd=toadd)
                self._remove_from_relation(query, columnname,
                                           toremove=toremove)
            else:
                toset = params[columnname]
                self._set_on_relation(query, columnname, toset=toset)
        return tochange

    def _handle_validation_exception(self, exception):
        """Rolls back the session, extracts validation error messages, and
        returns a :func:`flask.jsonify` response with :http:statuscode:`400`
        containing the extracted validation error messages.

        Again, *this method calls
        :meth:`sqlalchemy.orm.session.Session.rollback`*.

        """
        self.session.rollback()
        errors = extract_error_messages(exception) or \
            'Could not determine specific validation errors'
        return json(dict(validation_errors=errors), status=520)

    def _compute_results_per_page(self, request):
        """Helper function which returns the number of results per page based
        on the request argument ``results_per_page`` and the server
        configuration parameters :attr:`results_per_page` and
        :attr:`max_results_per_page`.

        """
        try:
            results_per_page = int(request.args.get('results_per_page'))
        except:
            results_per_page = self.results_per_page
        if results_per_page <= 0:
            results_per_page = self.results_per_page
        return min(results_per_page, self.max_results_per_page)

    def _paginated(self, request, instances, deep):
        """Returns a paginated JSONified response from the specified list of
        model instances.

        `instances` is either a Python list of model instances or a
        :class:`~sqlalchemy.orm.Query`.

        `deep` is the dictionary which defines the depth of submodels to output
        in the JSON format of the model instances in `instances`; it is passed
        directly to :func:`helpers.to_dict`.

        The response data is JSON of the form:

        .. sourcecode:: javascript

           {
             "page": 2,
             "total_pages": 3,
             "num_results": 8,
             "objects": [{"id": 1, "name": "Jeffrey", "age": 24}, ...]
           }

        """
        if isinstance(instances, list):
            num_results = len(instances)
        else:
            num_results = count(self.session, instances)
        results_per_page = self._compute_results_per_page(request)
        if results_per_page > 0:
            # get the page number (first page is page 1)
            page_num = int(request.args.get('page', 1))
            start = (page_num - 1) * results_per_page
            end = min(num_results, start + results_per_page)
            total_pages = int(math.ceil(num_results / results_per_page))
        else:
            page_num = 1
            start = 0
            end = num_results
            total_pages = 1
        objects = [to_dict(x, deep, exclude=self.exclude_columns,
                           exclude_relations=self.exclude_relations,
                           include=self.include_columns,
                           include_relations=self.include_relations,
                           include_methods=self.include_methods)
                   for x in instances[start:end]]
        return dict(page=page_num, objects=objects, total_pages=total_pages,
                    num_results=num_results)


    def _inst_to_dict(self, inst):
        """Returns the dictionary representation of the specified instance.

        This method respects the include and exclude columns specified in the
        constructor of this class.

        """
        # create a placeholder for the relations of the returned models
        relations = frozenset(get_relations(self.model))
        # do not follow relations that will not be included in the response
        if self.include_columns is not None:
            cols = frozenset(self.include_columns)
            rels = frozenset(self.include_relations)
            relations &= (cols | rels)
        elif self.exclude_columns is not None:
            relations -= frozenset(self.exclude_columns)
        deep = dict((r, {}) for r in relations)
        return to_dict(inst, deep, exclude=self.exclude_columns,
                       exclude_relations=self.exclude_relations,
                       include=self.include_columns,
                       include_relations=self.include_relations,
                       include_methods=self.include_methods)

    def _dict_to_inst(self, data):
        """Returns an instance of the model with the specified attributes."""
        # Check for any request parameter naming a column which does not exist
        # on the current model.
        for field in data:
            if not has_field(self.model, field):
                msg = "Model does not have field '{0}'".format(field)
                raise ValidationError(msg)

        # Getting the list of relations that will be added later
        cols = get_columns(self.model)
        relations = get_relations(self.model)

        # Looking for what we're going to set on the model right now
        colkeys = cols.keys()
        paramkeys = data.keys()
        props = set(colkeys).intersection(paramkeys).difference(relations)

        # Special case: if there are any dates, convert the string form of the
        # date into an instance of the Python ``datetime`` object.
        data = strings_to_dates(self.model, data)

        # Instantiate the model with the parameters.
        modelargs = dict([(i, data[i]) for i in props])
        instance = self.model(**modelargs)

        # Handling relations, a single level is allowed
        for col in set(relations).intersection(paramkeys):
            submodel = get_related_model(self.model, col)

            if type(data[col]) == list:
                # model has several related objects
                for subparams in data[col]:
                    subinst = get_or_create(self.session, submodel,
                                            subparams)
                    try:
                        getattr(instance, col).append(subinst)
                    except AttributeError:
                        attribute = getattr(instance, col)
                        attribute[subinst.key] = subinst.value
            else:
                # model has single related object
                if data[col] is not None:
                    subinst = get_or_create(self.session, submodel, data[col])
                    setattr(instance, col, subinst)

        return instance

    def _instid_to_dict(self, instid):
        """Returns the dictionary representation of the instance specified by
        `instid`.

        If no such instance of the model exists, this method aborts with a
        :http:statuscode:`404`.

        """
        inst = get_by(self.session, self.model, instid, self.primary_key)
        if inst is None:
            return json(dict(message='No result found'), status=520)
        return self._inst_to_dict(inst)



    async def _search(self, request):
        try:
            search_params = json_loads(request.args.get('q', '{}'))
        except (TypeError, ValueError, OverflowError) as exception:
            #current_app.logger.exception(str(exception))
            return json(dict(message='Unable to decode data'), status=520)

        try:
            for preprocess in self.preprocess['GET_MANY']:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request,search_params=search_params, Model=self.model)
                else:
                    resp = preprocess(request=request,search_params=search_params, Model=self.model)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)


        print("TODO: datatime in paramstring - gatco_restapi views line 1146")
        # resolve date-strings as required by the model
        '''for param in search_params.get('filters', list()):
            if 'name' in param and 'val' in param:
                query_model = self.model
                query_field = param['name']
                if '__' in param['name']:
                    fieldname, relation = param['name'].split('__')
                    submodel = getattr(self.model, fieldname)
                    if isinstance(submodel, InstrumentedAttribute):
                        query_model = submodel.property.mapper.class_
                        query_field = relation
                    elif isinstance(submodel, AssociationProxy):
                        # For the sake of brevity, rename this function.
                        get_assoc = get_related_association_proxy_model
                        query_model = get_assoc(submodel)
                        query_field = relation
                to_convert = {query_field: param['val']}
                try:
                    result = strings_to_dates(query_model, to_convert)
                except ValueError as exception:
                    #current_app.logger.exception(str(exception))
                    return json(dict(message='Unable to construct query'), status=520)
                param['val'] = result.get(query_field)'''

        # perform a filtered search
        try:
            result = search(self.session, self.model, search_params)
        except NoResultFound:
            return json(dict(message='No result found'), status=520)
        except MultipleResultsFound:
            return json(dict(message='Multiple results found'), status=520)
        except Exception as exception:
            #current_app.logger.exception(str(exception))
            return json(dict(message='Unable to construct query'), status=520)

        # create a placeholder for the relations of the returned models
        relations = frozenset(get_relations(self.model))
        # do not follow relations that will not be included in the response
        if self.include_columns is not None:
            cols = frozenset(self.include_columns)
            rels = frozenset(self.include_relations)
            relations &= (cols | rels)
        elif self.exclude_columns is not None:
            relations -= frozenset(self.exclude_columns)
        deep = dict((r, {}) for r in relations)

        # for security purposes, don't transmit list as top-level JSON
        if isinstance(result, Query):
            result = self._paginated(request, result, deep)
            # Create the Link header.
            #
            # TODO We are already calling self._compute_results_per_page() once
            # in _paginated(); don't compute it again here.
            #page, last_page = result['page'], result['total_pages']
            #linkstring = create_link_string(request, page, last_page,
            #                                self._compute_results_per_page(request))
            #headers = dict(Link=linkstring)
        else:
            primary_key = self.primary_key or primary_key_name(result)
            result = to_dict(result, deep, exclude=self.exclude_columns,
                             exclude_relations=self.exclude_relations,
                             include=self.include_columns,
                             include_relations=self.include_relations,
                             include_methods=self.include_methods)
            # The URL at which a client can access the instance matching this
            # search query.
            #url = '{0}/{1}'.format(request.url, result[primary_key])
            #headers = dict(Location=url)

        try:
            headers = {}
            for postprocess in self.postprocess['GET_MANY']:
                if asyncio.iscoroutinefunction(postprocess):
                    resp = await postprocess(request=request, result=result, search_params=search_params, Model=self.model, headers=headers)
                else:
                    resp = postprocess(request=request, result=result, search_params=search_params, Model=self.model, headers=headers)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        # HACK Provide the headers directly in the result dictionary, so that
        # the :func:`jsonpify` function has access to them. See the note there
        # for more information.
        #result[_HEADERS] = headers
        #return result, 200, headers
        return json(result, headers=headers, status=200)

    async def get(self, request, instid=None, relationname=None, relationinstid=None):
        """Returns a JSON representation of an instance of model with the
        specified name.

        If ``instid`` is ``None``, this method returns the result of a search
        with parameters specified in the query string of the request. If no
        search parameters are specified, this method returns all instances of
        the specified model.

        If ``instid`` is an integer, this method returns the instance of the
        model with that identifying integer. If no such instance exists, this
        method responds with :http:status:`404`.

        """
        

        if instid is None:
            return await self._search(request)

        try:
            for preprocess in self.preprocess['GET_SINGLE']:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request, instance_id=instid, Model=self.model)
                else:
                    resp = preprocess(request=request, instance_id=instid, Model=self.model)
                # Let the return value of the preprocess be the new value of
                # instid, thereby allowing the preprocess to effectively specify
                # which instance of the model to process on.
                #
                # We assume that if the preprocess returns None, it really just
                # didn't return anything, which means we shouldn't overwrite the
                # instid.
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
                
                if resp is not None:
                    instid = resp
                    
        except ProcessingException as exception:
            return response_exception(exception)

        # get the instance of the "main" model whose ID is instid

        instance = get_by(self.session, self.model, instid, self.primary_key)

        if instance is None:
            return json(dict(message='No result found'),status=520)
        # If no relation is requested, just return the instance. Otherwise,
        # get the value of the relation specified by `relationname`.
        if relationname is None:
            result = self.serialize(instance)
        else:
            related_value = getattr(instance, relationname)
            # create a placeholder for the relations of the returned models
            related_model = get_related_model(self.model, relationname)
            relations = frozenset(get_relations(related_model))
            deep = dict((r, {}) for r in relations)
            if relationinstid is not None:
                related_value_instance = get_by(self.session, related_model,
                                                relationinstid)
                if related_value_instance is None:
                    return json(dict(message='No result found'),status=520)
                result = to_dict(related_value_instance, deep)
            else:
                # for security purposes, don't transmit list as top-level JSON
                if is_like_list(instance, relationname):
                    result = self._paginated(list(related_value), deep)
                else:
                    result = to_dict(related_value, deep)
        if result is None:
            return json(dict(message='No result found'),status=520)

        try:
            headers = {}
            for postprocess in self.postprocess['GET_SINGLE']:
                if asyncio.iscoroutinefunction(postprocess):
                    resp = await postprocess(request=request, result=result, Model=self.model, headers=headers)
                else:
                    resp = postprocess(request=request, result=result, Model=self.model, headers=headers)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        return json(result, headers=headers, status=200)
        #return result

    async def _delete_many(self, request):
        """Deletes multiple instances of the model.

        If search parameters are provided via the ``q`` query parameter, only
        those instances matching the search parameters will be deleted.

        If no instances were deleted, this returns a
        :http:status:`404`. Otherwise, it returns a :http:status:`200` with the
        number of deleted instances in the body of the response.

        """
        # try to get search query from the request query parameters
        try:
            search_params = json_loads(request.args.get('q', '{}'))
        except (TypeError, ValueError, OverflowError) as exception:
            #current_app.logger.exception(str(exception))
            return json(dict(message='Unable to decode search query'), status=520)

        try:
            for preprocess in self.preprocess['DELETE_MANY']:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request,search_params=search_params, Model=self.model)
                else:
                    resp = preprocess(request=request, search_params=search_params, Model=self.model)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        # perform a filtered search
        try:
            # HACK We need to ignore any ``order_by`` request from the client,
            # because for some reason, SQLAlchemy does not allow calling
            # delete() on a query that has an ``order_by()`` on it. If you
            # attempt to call delete(), you get this error:
            #
            #     sqlalchemy.exc.InvalidRequestError: Can't call Query.delete()
            #     when order_by() has been called
            #
            result = search(self.session, self.model, search_params,
                            _ignore_order_by=True)
        except NoResultFound:
            return json(dict(message='No result found'), status=520)
        except MultipleResultsFound:
            return json(dict(message='Multiple results found'), status=520)
        except Exception as exception:
            #current_app.logger.exception(str(exception))
            return json(dict(message='Unable to construct query'), status=520)

        # for security purposes, don't transmit list as top-level JSON
        if isinstance(result, Query):
            # Implementation note: `synchronize_session=False`, described in
            # the SQLAlchemy documentation for
            # :meth:`sqlalchemy.orm.query.Query.delete`, states that this is
            # the most efficient option for bulk deletion, and is reliable once
            # the session has expired, which occurs after the session commit
            # below.
            num_deleted = result.delete(synchronize_session=False)
        else:
            self.session.delete(result)
            num_deleted = 1
        self.session.commit()
        result = dict(num_deleted=num_deleted)

        try:
            headers = {}
            for postprocess in self.postprocess['DELETE_MANY']:
                if asyncio.iscoroutinefunction(postprocess):
                    resp = await postprocess(request=request, result=result, search_params=search_params, Model=self.model, headers=headers)
                else:
                    resp = postprocess(request=request, result=result, search_params=search_params, Model=self.model, headers=headers)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        return (json(result, headers=headers, status=200)) if num_deleted > 0 else json({}, headers=headers, status=520)

    async def delete(self, request, instid=None, relationname=None, relationinstid=None):
        """Removes the specified instance of the model with the specified name
        from the database.

        Although :http:method:`delete` is an idempotent method according to
        :rfc:`2616`, idempotency only means that subsequent identical requests
        cannot have additional side-effects. Since the response code is not a
        side effect, this method responds with :http:status:`204` only if an
        object is deleted, and with :http:status:`404` when nothing is deleted.

        If `relationname

        .. versionadded:: 0.12.0
           Added the `relationinstid` keyword argument.

        .. versionadded:: 0.10.0
           Added the `relationname` keyword argument.

        """

        if instid is None:
            # If no instance ID is provided, this request is an attempt to
            # delete many instances of the model via a search with possible
            # filters.
            return await self._delete_many(request)
        was_deleted = False

        try:
            for preprocess in self.preprocess['DELETE_SINGLE']:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request, instance_id=instid,
                                           relation_name=relationname,
                                           relation_instance_id=relationinstid, Model=self.model)
                else:
                    resp = preprocess(request=request, instance_id=instid,
                                           relation_name=relationname,
                                           relation_instance_id=relationinstid, Model=self.model)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
                # See the note under the preprocess in the get() method.
                if resp is not None:
                    instid = resp
        except ProcessingException as exception:
            return response_exception(exception)

        inst = get_by(self.session, self.model, instid, self.primary_key)
        if relationname:
            # If the request is ``DELETE /api/person/1/computers``, error 400.
            if not relationinstid:
                msg = ('Cannot DELETE entire "{0}"'
                       ' relation').format(relationname)
                return json(dict(message=msg), status=520)
            # Otherwise, get the related instance to delete.
            relation = getattr(inst, relationname)
            related_model = get_related_model(self.model, relationname)
            relation_instance = get_by(self.session, related_model,
                                       relationinstid)
            # Removes an object from the relation list.
            relation.remove(relation_instance)
            was_deleted = len(self.session.dirty) > 0
        elif inst is not None:
            self.session.delete(inst)
            was_deleted = len(self.session.deleted) > 0
        self.session.commit()

        try:
            headers = {}
            for postprocess in self.postprocess['DELETE_SINGLE']:
                if asyncio.iscoroutinefunction(postprocess):
                    resp = await postprocess(request=request, instance_id=instid, was_deleted=was_deleted, Model=self.model, headers=headers)
                else:
                    resp = postprocess(request=request, instance_id=instid, was_deleted=was_deleted, Model=self.model, headers=headers)
                    
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)

        return json({}, headers=headers, status=200) if was_deleted else json({}, headers=headers, status=520)

    async def post(self, request):
        """Creates a new instance of a given model based on request data.

        This function parses the string contained in
        :attr:`flask.request.data`` as a JSON object and then validates it with
        a validator specified in the constructor of this class.

        The :attr:`flask.request.data` attribute will be parsed as a JSON
        object containing the mapping from field name to value to which to
        initialize the created instance of the model.

        After that, it separates all columns that defines relationships with
        other entities, creates a model with the simple columns and then
        creates instances of these submodels and associates them with the
        related fields. This happens only at the first level of nesting.

        Currently, this method can only handle instantiating a model with a
        single level of relationship data.

        """
        content_type = request.headers.get('Content-Type', "")
        content_is_json = content_type.startswith('application/json')

        #print(content_type)
        #is_msie = _is_msie8or9(request)
        # Request must have the Content-Type: application/json header, unless
        # the User-Agent string indicates that the client is Microsoft Internet
        # Explorer 8 or 9 (which has a fixed Content-Type of 'text/html'; see
        # issue #267).

        if not content_is_json:
            msg = 'Request must have "Content-Type: application/json" header'
            return json(dict(message=msg),status=520)

        # try to read the parameters for the model from the body of the request
        try:
            # HACK Requests made from Internet Explorer 8 or 9 don't have the
            # correct content type, so request.get_json() doesn't work.

            #if is_msie:
            #    data = json.loads(request.get_data()) or {}
            #else:
            #    data = request.get_json() or {}
            data = request.json or {}
        except (ServerError, TypeError, ValueError, OverflowError) as exception:
            #current_app.logger.exception(str(exception))
            return json(dict(message='Unable to decode data'),status=520)

        # apply any preprocess to the POST arguments
        try:
            for preprocess in self.preprocess['POST']:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request, data=data, Model=self.model)
                else:
                    resp = preprocess(request=request, data=data, Model=self.model)
                    
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)



        try:
            # Convert the dictionary representation into an instance of the
            # model.

            instance = self.deserialize(data)
            # Add the created model to the session.
            self.session.add(instance)
            self.session.commit()
            # Get the dictionary representation of the new instance as it
            # appears in the database.
            result = self.serialize(instance)
        except self.validation_exceptions as exception:
            return self._handle_validation_exception(exception)
        # Determine the value of the primary key for this instance and
        # encode URL-encode it (in case it is a Unicode string).
        pk_name = self.primary_key or primary_key_name(instance)
        primary_key = result[pk_name]
        try:
            primary_key = str(primary_key)
        except UnicodeEncodeError:
            #primary_key = url_quote_plus(primary_key.encode('utf-8'))
            print("TODO: url_quote_plus() implement in gatco_restapi views")
            primary_key = primary_key.encode('utf-8')
        # The URL at which a client can access the newly created instance
        # of the model.
        url = '{0}/{1}'.format(request.url, primary_key)
        # Provide that URL in the Location header in the response.
        headers = dict(Location=url)

        try:
            headers = {}
            for postprocess in self.postprocess['POST']:
                if asyncio.iscoroutinefunction(postprocess):
                    resp = await postprocess(request=request, result=result, Model=self.model, headers=headers)
                else:
                    resp = postprocess(request=request, result=result, Model=self.model, headers=headers)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except ProcessingException as exception:
            return response_exception(exception)
        return json(result,headers=headers, status=201)

    async def put(self, request, instid=None, relationname=None, relationinstid=None):
        """Updates the instance specified by ``instid`` of the named model, or
        updates multiple instances if ``instid`` is ``None``.

        The :attr:`flask.request.data` attribute will be parsed as a JSON
        object containing the mapping from field name to value to which to
        update the specified instance or instances.

        If ``instid`` is ``None``, the query string will be used to search for
        instances (using the :func:`_search` method), and all matching
        instances will be updated according to the content of the request data.
        See the :func:`_search` documentation on more information about search
        parameters for restricting the set of instances on which updates will
        be made in this case.

        This function ignores the `relationname` and `relationinstid` keyword
        arguments.

        .. versionadded:: 0.12.0
           Added the `relationinstid` keyword argument.

        .. versionadded:: 0.10.0
           Added the `relationname` keyword argument.

        """
        content_type = request.headers.get('Content-Type', "")
        content_is_json = content_type.startswith('application/json')

        #is_msie = _is_msie8or9()
        # Request must have the Content-Type: application/json header, unless
        # the User-Agent string indicates that the client is Microsoft Internet
        # Explorer 8 or 9 (which has a fixed Content-Type of 'text/html'; see
        # issue #267).
        if not content_is_json:
            msg = 'Request must have "Content-Type: application/json" header'
            return json(dict(message=msg),status=520)

        # try to load the fields/values to update from the body of the request
        try:
            # HACK Requests made from Internet Explorer 8 or 9 don't have the
            # correct content type, so request.get_json() doesn't work.
            #if is_msie:
            #    data = json.loads(request.get_data()) or {}
            #else:
            #    data = request.get_json() or {}
            data = request.json or {}
        except (ServerError, TypeError, ValueError, OverflowError) as exception:
            # this also happens when request.data is empty
            #current_app.logger.exception(str(exception))
            return json(dict(message='Unable to decode data'),status=520)

        # Check if the request is to patch many instances of the current model.

        patchmany = instid is None
        # Perform any necessary preprocessing.
        if patchmany:
            # Get the search parameters; all other keys in the `data`
            # dictionary indicate a change in the model's field.
            search_params = data.pop('q', {})
            try:
                for preprocess in self.preprocess['PATCH_MANY']:
                    if asyncio.iscoroutinefunction(preprocess):
                        resp = await preprocess(request=request, search_params=search_params, data=data, Model=self.model)
                    else:
                        resp = preprocess(request=request, search_params=search_params, data=data, Model=self.model)
                    
                    if (resp is not None) and isinstance(resp, HTTPResponse):
                        return resp
            except ProcessingException as exception:
                return response_exception(exception)

        else:
            for preprocess in self.preprocess['PATCH_SINGLE']:
                try:
                    if asyncio.iscoroutinefunction(preprocess):
                        resp = await preprocess(request=request, instance_id=instid, data=data, Model=self.model)
                    else:
                        resp = preprocess(request=request, instance_id=instid, data=data, Model=self.model)
                    
                    if (resp is not None) and isinstance(resp, HTTPResponse):
                        return resp
                    # See the note under the preprocess in the get() method.
                    if resp is not None:
                        instid = resp
                except ProcessingException as exception:
                    return response_exception(exception)


        # Check for any request parameter naming a column which does not exist
        # on the current model.
        for field in data:
            if not has_field(self.model, field):
                msg = "Model does not have field '{0}'".format(field)
                return json(dict(message=msg),status=520)

        if patchmany:
            try:
                # create a SQLALchemy Query from the query parameter `q`
                query = create_query(self.session, self.model, search_params)
            except Exception as exception:
                #current_app.logger.exception(str(exception))
                return json(dict(message='Unable to construct query'),status=520)
        else:
            # create a SQLAlchemy Query which has exactly the specified row
            query = query_by_primary_key(self.session, self.model, instid,
                                         self.primary_key)
            if query.count() == 0:
                return json(dict(message='No result found'), status=520)
            assert query.count() == 1, 'Multiple rows with same ID'
        try:
            relations = self._update_relations(query, data)
        except self.validation_exceptions as exception:
            #current_app.logger.exception(str(exception))
            return self._handle_validation_exception(exception)
        field_list = frozenset(data) ^ relations

        data = dict((field, data[field]) for field in field_list)
        # Special case: if there are any dates, convert the string form of the
        # date into an instance of the Python ``datetime`` object.
        data = strings_to_dates(self.model, data)
        try:
            # Let's update all instances present in the query
            num_modified = 0
            if data:
                for item in query.all():
                    for field, value in data.items():
                        setattr(item, field, value)
                    num_modified += 1
            self.session.commit()
        except self.validation_exceptions as exception:
            #current_app.logger.exception(str(exception))
            return self._handle_validation_exception(exception)

        # Perform any necessary postprocessing.
        headers = {}
        if patchmany:
            result = dict(num_modified=num_modified)
            try:
                for postprocess in self.postprocess['PATCH_MANY']:
                    if asyncio.iscoroutinefunction(postprocess):
                        resp = await postprocess(request=request, query=query, result=result,
                              search_params=search_params, Model=self.model, headers=headers)
                    else:
                        resp = postprocess(request=request, query=query, result=result,
                              search_params=search_params, Model=self.model, headers=headers)
                        
                    if (resp is not None) and isinstance(resp, HTTPResponse):
                        return resp
            except ProcessingException as exception:
                return response_exception(exception)

        else:
            result = self._instid_to_dict(instid)

            try:
                for postprocess in self.postprocess['PATCH_SINGLE']:
                    if asyncio.iscoroutinefunction(postprocess):
                        resp = await postprocess(request=request, result=result, Model=self.model, headers=headers)
                    else:
                        resp = postprocess(request=request, result=result, Model=self.model, headers=headers)
                        
                    if (resp is not None) and isinstance(resp, HTTPResponse):
                        return resp
            except ProcessingException as exception:
                return response_exception(exception)

        return json(result, headers=headers, status=200)

    async def patch(self, *args, **kw):
        """Alias for :meth:`patch`."""
        return self.put(*args, **kw)
