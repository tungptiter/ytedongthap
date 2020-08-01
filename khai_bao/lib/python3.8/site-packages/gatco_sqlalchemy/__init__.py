# -*- coding: utf-8 -*-
import functools
import os
import sys
import time
from operator import itemgetter
import warnings
from threading import Lock
import asyncio
import sqlalchemy
from sqlalchemy import event, inspect, orm
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm.exc import UnmappedClassError
from sqlalchemy.orm.session import Session as SessionBase

from .model import Model, DefaultMeta

__version__ = '0.1.0'

def itervalues(d):
    return iter(d.values())

def _make_table(db):
    def _make_table(*args, **kwargs):
        if len(args) > 1 and isinstance(args[1], db.Column):
            args = (args[0], db.metadata) + args[1:]
        info = kwargs.pop('info', None) or {}
        info.setdefault('bind_key', None)
        kwargs['info'] = info
        return sqlalchemy.Table(*args, **kwargs)
    return _make_table


def _set_default_query_class(d, cls):
    if 'query_class' not in d:
        d['query_class'] = cls


def _wrap_with_default_query_class(fn, cls):
    @functools.wraps(fn)
    def newfn(*args, **kwargs):
        _set_default_query_class(kwargs, cls)
        if "backref" in kwargs:
            backref = kwargs['backref']
            if isinstance(backref, str):
                backref = (backref, {})
            _set_default_query_class(backref[1], cls)
        return fn(*args, **kwargs)
    return newfn


def _include_sqlalchemy(obj, cls):
    for module in sqlalchemy, sqlalchemy.orm:
        for key in module.__all__:
            if not hasattr(obj, key):
                setattr(obj, key, getattr(module, key))
    # Note: obj.Table does not attempt to be a SQLAlchemy Table class.
    obj.Table = _make_table(obj)
    obj.relationship = _wrap_with_default_query_class(obj.relationship, cls)
    obj.relation = _wrap_with_default_query_class(obj.relation, cls)
    obj.dynamic_loader = _wrap_with_default_query_class(obj.dynamic_loader, cls)
    obj.event = event



class SignallingSession(SessionBase):
    def __init__(self, db, autocommit=False, autoflush=True, **options):
        #: The application that this session belongs to.
        self.app = app = db.get_app()
        #track_modifications = app.config['SQLALCHEMY_TRACK_MODIFICATIONS']
        bind = options.pop('bind', None) or db.engine
        binds = options.pop('binds', db.get_binds(app))

        #if track_modifications is None or track_modifications:
        #    _SessionSignalEvents.register(self)
        SessionBase.__init__(
            self, autocommit=autocommit, autoflush=autoflush,
            bind=bind, binds=binds, **options
        )
    
    def get_bind(self, mapper=None, clause=None):
        # mapper is None if someone tries to just get a connection
        if mapper is not None:
            info = getattr(mapper.mapped_table, 'info', {})
            bind_key = info.get('bind_key')
            if bind_key is not None:
                state = get_state(self.app)
                return state.db.get_engine(self.app, bind=bind_key)
        return SessionBase.get_bind(self, mapper, clause)
    
    
class BaseQuery(orm.Query):
    pass

class _QueryProperty(object):
    def __init__(self, sa):
        self.sa = sa

    def __get__(self, obj, type):
        try:
            mapper = orm.class_mapper(type)
            if mapper:
                return type.query_class(mapper, session=self.sa.session())
        except UnmappedClassError:
            return None

class _EngineConnector(object):

    def __init__(self, sa, app, bind=None):
        self._sa = sa
        self._app = app
        self._engine = None
        self._connected_for = None
        self._bind = bind
        self._lock = Lock()

    def get_uri(self):
        if self._bind is None:
            return self._app.config.get('SQLALCHEMY_DATABASE_URI')
        binds = self._app.config.get('SQLALCHEMY_BINDS') or ()
        assert self._bind in binds, \
            'Bind %r is not specified.  Set it in the SQLALCHEMY_BINDS ' \
            'configuration variable' % self._bind
        return binds[self._bind]

    def get_engine(self):
        with self._lock:
            uri = self.get_uri()
            echo = self._app.config.get('SQLALCHEMY_ECHO')
            if (uri, echo) == self._connected_for:
                return self._engine
            info = make_url(uri)
            options = {'convert_unicode': True}
            self._sa.apply_pool_defaults(self._app, options)
            self._sa.apply_driver_hacks(self._app, info, options)
            if echo:
                options['echo'] = echo
            self._engine = rv = sqlalchemy.create_engine(info, **options)
            #if _record_queries(self._app):
            #    _EngineDebuggingSignalEvents(self._engine,
            #                                 self._app.import_name).register()
            self._connected_for = (uri, echo)
            return rv


def get_state(app):
    """Gets the state for the application"""
    assert 'sqlalchemy' in app.extensions, \
        'The sqlalchemy extension was not registered to the current ' \
        'application.  Please make sure to call init_app() first.'
    return app.extensions['sqlalchemy']

class _SQLAlchemyState(object):
    """Remembers configuration for the (db, app) tuple."""

    def __init__(self, db):
        self.db = db
        self.connectors = {}
        
class SQLAlchemy(object):
    #: Default query class used by :attr:`Model.query` and other queries.
    #: Customize this by passing ``query_class`` to :func:`SQLAlchemy`.
    #: Defaults to :class:`BaseQuery`.
    Query = None

    def __init__(self, app=None, use_native_unicode=True, session_options=None,
                 metadata=None, query_class=BaseQuery, model_class=Model):

        self.use_native_unicode = use_native_unicode
        self.Query = query_class
        self.session = self.create_scoped_session(session_options)
        self.Model = self.make_declarative_base(model_class, metadata)
        self._engine_lock = Lock()
        self.app = app
        _include_sqlalchemy(self, query_class)

        if app is not None:
            self.init_app(app)
            
    @property
    def metadata(self):
        """The metadata associated with ``db.Model``."""

        return self.Model.metadata
    
    def create_scoped_session(self, options=None):
        """Create a :class:`~sqlalchemy.orm.scoping.scoped_session`
        on the factory from :meth:`create_session`.
        An extra key ``'scopefunc'`` can be set on the ``options`` dict to
        specify a custom scope function.
        :param options: dict of keyword arguments passed to session class  in
            ``create_session``
        """

        if options is None:
            options = {}

        scopefunc = options.pop('scopefunc', asyncio.Task.current_task)
        options['query_cls'] = options['query_cls'] if 'query_cls' in options else self.Query
        
        return orm.scoped_session(
            self.create_session(options), scopefunc=scopefunc
        )
    
    
    def create_session(self, options):
        """Create the session factory used by :meth:`create_scoped_session`.
        The factory **must** return an object that SQLAlchemy recognizes as a session,
        or registering session events may raise an exception.
        Valid factories include a :class:`~sqlalchemy.orm.session.Session`
        class or a :class:`~sqlalchemy.orm.session.sessionmaker`.
        The default implementation creates a ``sessionmaker`` for :class:`SignallingSession`.
        :param options: dict of keyword arguments passed to session class
        """

        #return orm.sessionmaker(class_=SessionBase, db=self, **options)
        return orm.sessionmaker(class_=SignallingSession, db=self, **options)
    
    def make_declarative_base(self, model, metadata=None):
        """Creates the declarative base that all models will inherit from.
        :param model: base model class (or a tuple of base classes) to pass
            to :func:`~sqlalchemy.ext.declarative.declarative_base`. Or a class
            returned from ``declarative_base``, in which case a new base class
            is not created.
        :param: metadata: :class:`~sqlalchemy.MetaData` instance to use, or
            none to use SQLAlchemy's default.
            ``model`` can be an existing declarative base in order to support
            complex customization such as changing the metaclass.
        """
        if not isinstance(model, DeclarativeMeta):
            model = declarative_base(
                cls=model,
                name='Model',
                metadata=metadata,
                metaclass=DefaultMeta
            )

        # if user passed in a declarative base and a metaclass for some reason,
        # make sure the base uses the metaclass
        if metadata is not None and model.metadata is not metadata:
            model.metadata = metadata

        if not getattr(model, 'query_class', None):
            model.query_class = self.Query

        model.query = _QueryProperty(self)
        return model
    
    def init_app(self, app):
        """This callback can be used to initialize an application for the
        use with this database setup.  Never use a database in the context
        of an application not initialized that way or connections will
        leak.
        """
        if (
            'SQLALCHEMY_DATABASE_URI' not in app.config and
            'SQLALCHEMY_BINDS' not in app.config
        ):
            warnings.warn(
                'Neither SQLALCHEMY_DATABASE_URI nor SQLALCHEMY_BINDS is set. '
                'Defaulting SQLALCHEMY_DATABASE_URI to "sqlite:///:memory:".'
            )

        app.config['SQLALCHEMY_DATABASE_URI'] = app.config.get('SQLALCHEMY_DATABASE_URI') or 'sqlite:///:memory:'
        app.config['SQLALCHEMY_BINDS'] = app.config.get('SQLALCHEMY_BINDS') or None
        app.config['SQLALCHEMY_NATIVE_UNICODE'] = app.config.get('SQLALCHEMY_NATIVE_UNICODE') or None
        
        app.config['SQLALCHEMY_ECHO'] =  app.config.get('SQLALCHEMY_ECHO') or False
        app.config['SQLALCHEMY_RECORD_QUERIES'] =  app.config.get('SQLALCHEMY_RECORD_QUERIES') or None
        
        app.config['SQLALCHEMY_POOL_SIZE'] =  app.config.get('SQLALCHEMY_POOL_SIZE') or None
        app.config['SQLALCHEMY_POOL_TIMEOUT'] =  app.config.get('SQLALCHEMY_POOL_TIMEOUT') or None
        app.config['SQLALCHEMY_POOL_RECYCLE'] =  app.config.get('SQLALCHEMY_POOL_RECYCLE') or None
        app.config['SQLALCHEMY_MAX_OVERFLOW'] =  app.config.get('SQLALCHEMY_MAX_OVERFLOW') or None
        app.config['SQLALCHEMY_COMMIT_ON_RESPONSE'] =  app.config.get('SQLALCHEMY_COMMIT_ON_RESPONSE') or False
        
        self.app = app
        
        if (not hasattr(app, 'extensions')) or (app.extensions is None):
            app.extensions = {}
        app.extensions['sqlalchemy'] = _SQLAlchemyState(self)
        
        
        
        @app.middleware('response')
        async def shutdown_session(request, response):
            try:
                if app.config['SQLALCHEMY_COMMIT_ON_RESPONSE']:
                    self.session.commit()
            except:
                self.session.rollback()
                raise
            finally:
                #self.session.close()
                self.session.remove()
    
    def apply_pool_defaults(self, app, options):
        def _setdefault(optionkey, configkey):
            value = app.config.get(configkey)
            if value is not None:
                options[optionkey] = value
        _setdefault('pool_size', 'SQLALCHEMY_POOL_SIZE')
        _setdefault('pool_timeout', 'SQLALCHEMY_POOL_TIMEOUT')
        _setdefault('pool_recycle', 'SQLALCHEMY_POOL_RECYCLE')
        _setdefault('max_overflow', 'SQLALCHEMY_MAX_OVERFLOW')

    def apply_driver_hacks(self, app, info, options):
        """This method is called before engine creation and used to inject
        driver specific hacks into the options.  The `options` parameter is
        a dictionary of keyword arguments that will then be used to call
        the :func:`sqlalchemy.create_engine` function.
        The default implementation provides some saner defaults for things
        like pool sizes for MySQL and sqlite.  Also it injects the setting of
        `SQLALCHEMY_NATIVE_UNICODE`.
        """
        if info.drivername.startswith('mysql'):
            info.query.setdefault('charset', 'utf8')
            if info.drivername != 'mysql+gaerdbms':
                options.setdefault('pool_size', 10)
                options.setdefault('pool_recycle', 7200)
        elif info.drivername == 'sqlite':
            pool_size = options.get('pool_size')
            detected_in_memory = False
            if info.database in (None, '', ':memory:'):
                detected_in_memory = True
                from sqlalchemy.pool import StaticPool
                options['poolclass'] = StaticPool
                if 'connect_args' not in options:
                    options['connect_args'] = {}
                options['connect_args']['check_same_thread'] = False

                # we go to memory and the pool size was explicitly set
                # to 0 which is fail.  Let the user know that
                if pool_size == 0:
                    raise RuntimeError('SQLite in memory database with an '
                                       'empty queue not possible due to data '
                                       'loss.')
            # if pool size is None or explicitly set to 0 we assume the
            # user did not want a queue for this sqlite connection and
            # hook in the null pool.
            elif not pool_size:
                from sqlalchemy.pool import NullPool
                options['poolclass'] = NullPool

            # if it's not an in memory database we make the path absolute.
            #if not detected_in_memory:
            #    info.database = os.path.join(app.root_path, info.database)

        unu = app.config['SQLALCHEMY_NATIVE_UNICODE']
        if unu is None:
            unu = self.use_native_unicode
        if not unu:
            options['use_native_unicode'] = False

    @property
    def engine(self):
        """Gives access to the engine.  If the database configuration is bound
        to a specific application (initialized with an application) this will
        always return a database connection.  If however the current application
        is used this might raise a :exc:`RuntimeError` if no application is
        active at the moment.
        """
        return self.get_engine()

    def make_connector(self, app=None, bind=None):
        """Creates the connector for a given state and bind."""
        return _EngineConnector(self, self.get_app(app), bind)

    def get_engine(self, app=None, bind=None):
        """Returns a specific engine."""

        app = self.get_app(app)
        state = get_state(app)

        with self._engine_lock:
            connector = state.connectors.get(bind)

            if connector is None:
                connector = self.make_connector(app, bind)
                state.connectors[bind] = connector

            return connector.get_engine()

    def get_app(self, reference_app=None):
        """Helper method that implements the logic to look up an
        application."""

        if reference_app is not None:
            return reference_app

        if self.app is not None:
            return self.app

        raise RuntimeError(
            'No application found. Either work inside a view function or push'
            ' an application context.'
        )

    def get_tables_for_bind(self, bind=None):
        """Returns a list of all tables relevant for a bind."""
        result = []
        for table in itervalues(self.Model.metadata.tables):
            if table.info.get('bind_key') == bind:
                result.append(table)
        return result

    def get_binds(self, app=None):
        """Returns a dictionary with a table->engine mapping.
        This is suitable for use of sessionmaker(binds=db.get_binds(app)).
        """
        app = self.get_app(app)
        binds = [None] + list(app.config.get('SQLALCHEMY_BINDS') or ())
        retval = {}
        for bind in binds:
            engine = self.get_engine(app, bind)
            tables = self.get_tables_for_bind(bind)
            retval.update(dict((table, engine) for table in tables))
        return retval

    def _execute_for_all_tables(self, app, bind, operation, skip_tables=False):
        app = self.get_app(app)

        if bind == '__all__':
            binds = [None] + list(app.config.get('SQLALCHEMY_BINDS') or ())
        elif isinstance(bind, str) or bind is None:
            binds = [bind]
        else:
            binds = bind

        for bind in binds:
            extra = {}
            if not skip_tables:
                tables = self.get_tables_for_bind(bind)
                extra['tables'] = tables
            op = getattr(self.Model.metadata, operation)
            op(bind=self.get_engine(app, bind), **extra)

    def create_all(self, bind='__all__', app=None):
        """Creates all tables.
        """
        self._execute_for_all_tables(app, bind, 'create_all')

    def drop_all(self, bind='__all__', app=None):
        """Drops all tables.
        """
        self._execute_for_all_tables(app, bind, 'drop_all')

    def reflect(self, bind='__all__', app=None):
        """Reflects tables from the database.
        """
        self._execute_for_all_tables(app, bind, 'reflect', skip_tables=True)

    def __repr__(self):
        return '<%s engine=%r>' % (
            self.__class__.__name__,
            self.engine.url if self.app else None
        )
