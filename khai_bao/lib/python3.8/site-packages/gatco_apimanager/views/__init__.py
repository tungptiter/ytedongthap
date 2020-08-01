from collections import defaultdict
from gatco.views import HTTPMethodView

from ..helpers import upper_keys

class ModelView(HTTPMethodView):
    
    primary_key = "id"
    
    def __init__(self, model=None, collection_name=None, exclude_columns=None,
                 include_columns=None, include_methods=None, results_per_page=10,
                 max_results_per_page=1000, preprocess=None, postprocess=None,
                 primary_key=None, db=None, *args, **kw):
        
        super(ModelView, self).__init__(*args, **kw)
        
        if primary_key is not None:
            self.primary_key = primary_key
        
        if db is not None:
            self.db = db
        
        self.model = model
        
        self.collection_name = collection_name
        self.include_methods = include_methods
        
        self.results_per_page = results_per_page
        self.max_results_per_page = max_results_per_page
        self.include_columns = include_columns
        self.exclude_columns = exclude_columns
        
        self.postprocess = defaultdict(list)
        self.preprocess = defaultdict(list)
        self.postprocess.update(upper_keys(postprocess or {}))
        self.preprocess.update(upper_keys(preprocess or {}))
        
        
        for postprocess in self.postprocess['PUT_SINGLE']:
            self.postprocess['PATCH_SINGLE'].append(postprocess)
        for preprocess in self.preprocess['PUT_SINGLE']:
            self.preprocess['PATCH_SINGLE'].append(preprocess)
        for postprocess in self.postprocess['PUT_MANY']:
            self.postprocess['PATCH_MANY'].append(postprocess)
        for preprocess in self.preprocess['PUT_MANY']:
            self.preprocess['PATCH_MANY'].append(preprocess)
            
        #decorate = lambda name, f: setattr(self, name, f(getattr(self, name)))
        
        #for method in ['get', 'post', 'patch', 'put', 'delete']:
        #    decorate(method, catch_integrity_errors(self.motor_db))