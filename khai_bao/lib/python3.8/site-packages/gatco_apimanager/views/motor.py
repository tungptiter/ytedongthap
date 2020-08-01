import asyncio
import pymongo
import ujson
import math
from bson.objectid import ObjectId

from gatco.exceptions import GatcoException, ServerError
from gatco.response import json, text, HTTPResponse
from gatco.request import json_loads

def instids(instid):
    list_ids = [instid]
    try:
        list_ids.append(ObjectId(instid))
    except:
        pass
    return list_ids

from . import ModelView

def response_exception(exception):    
    if type(exception.message) is dict:
        return json(exception.message, status=exception.status_code)
    else:
        return json({"error_code": "UNKNOWN_ERROR", "error_message": exception.message}, status=520)

class APIView(ModelView):
    primary_key = "_id"
    db = None
    
    def _compute_results_per_page(self, request):
        try:
            results_per_page = int(request.args.get('results_per_page'))
        except:
            results_per_page = self.results_per_page
        if results_per_page <= 0:
            results_per_page = self.results_per_page
            
        return min(results_per_page, self.max_results_per_page)
    
    async def _search(self, request, search_params):
        is_single = search_params.get('single')
        order_by_list = search_params.get('order_by', [])
        
        #paginate
        num_results = 20
        results_per_page = self._compute_results_per_page(request)
        
        page_num = 1
        if results_per_page > 0:
            page_num = int(request.args.get('page', 1))
            start = (page_num - 1) * results_per_page
            end = start + results_per_page
            #total_pages = int(math.ceil(num_results / results_per_page))
        else:
            start = 0
            total_pages = 1
        
        if is_single:
            resp = None
            if 'filters' in search_params:
                resp = await self.db.db[self.collection_name].find_one(search_params['filters'])
            else:
                resp = await self.db.db[self.collection_name].find_one()
                
            if (resp is not None)  and ("_id" in resp):
                resp["_id"] = str(resp["_id"])
                
            result =  resp
        else:
            if 'filters' in search_params:
                cursor = self.db.db[self.collection_name].find(search_params['filters'])
                num_results = await self.db.db[self.collection_name].count_documents(search_params['filters'])
            else:
                cursor = self.db.db[self.collection_name].find()
                num_results = await self.db.db[self.collection_name].count_documents({})
            
            order_by_list_apply = []
            
            if len(order_by_list) > 0:
                for i in range(0, len(order_by_list)):
                    if isinstance(order_by_list[i] ,dict):
                        for k, v in order_by_list[i].items():
                            if (v == "desc") or (v == -1):
                                order_by_list_apply.append((k,pymongo.DESCENDING))
                            elif (v == "asc") or (v == 1):
                                order_by_list_apply.append((k,pymongo.ASCENDING))
            
            if len(order_by_list_apply) > 0:    
                cursor.sort(order_by_list_apply)
            
            limit = results_per_page
            offset = start
            
            if (results_per_page is not None) and (results_per_page > 0):
                cursor.limit(results_per_page)
                 
            if (start is not None) and (start > 0):
                cursor.skip(start)
            
            objects = []
            async for data in cursor:
                if "_id" in data:
                    data["_id"] = str(data["_id"])
                    objects.append(data)
            
            total_pages = int(math.ceil(num_results / results_per_page))
            new_page = page_num + 1 if page_num < total_pages else None
            result = {
                         "num_results": num_results,
                         "page": page_num,
                         "next_page": new_page,
                         "total_pages": total_pages,
                         "objects": objects
                         }
        return result
    
    
    async def search(self, request):
        try:
            search_params = json_loads(request.args.get('q', '{}'))
        except (TypeError, ValueError, OverflowError) as exception:
            #current_app.logger.exception(str(exception))
            return json(dict(error_code="PARAM_ERROR", error_message='Unable to decode data'), status=520)
        
        try:
            for preprocess in self.preprocess['GET_MANY']:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request,search_params=search_params, collection_name=self.collection_name)
                else:
                    resp = preprocess(request=request,search_params=search_params, collection_name=self.collection_name)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except Exception as exception:
            return response_exception(exception)
        
        result = await self._search(request, search_params)
        
        if result is None:
            return json(dict(error_code="NOT_FOUND", error_message='No result found'), status=520)
        
        try:
            headers = {}
            for postprocess in self.postprocess['GET_MANY']:
                if asyncio.iscoroutinefunction(postprocess):
                    resp = await postprocess(request=request, result=result, search_params=search_params, collection_name=self.collection_name, headers=headers)
                else:
                    resp = postprocess(request=request, result=result, search_params=search_params, collection_name=self.collection_name, headers=headers)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except Exception as exception:
            return response_exception(exception)
        
        return json(result, headers=headers, status=200)
    
    async def _get(self, request, instid):
        result = await self.db.db[self.collection_name].find_one({'_id': {'$in': instids(instid)}})
        if "_id" in result:
            result["_id"] = str(result["_id"])
        
        return result
    
    async def get(self, request, instid=None):
        if instid is None:
            return await self.search(request)
        try:
            for preprocess in self.preprocess['GET_SINGLE']:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request, instance_id=instid, collection_name=self.collection_name)
                else:
                    resp = preprocess(request=request, instance_id=instid, collection_name=self.collection_name)
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
                
                if resp is not None:
                    instid = resp
        except Exception as exception:
            return response_exception(exception)
        
        result = await self._get(request, instid)
        
        if result is None:
            return json(dict(error_code="NOT_FOUND", error_message='No result found'), status=520)
    
        try:
            headers = {}
            for postprocess in self.postprocess['GET_SINGLE']:
                if asyncio.iscoroutinefunction(postprocess):
                    resp = await postprocess(request=request, result=result, collection_name=self.collection_name, headers=headers)
                else:
                    resp = postprocess(request=request, result=result, collection_name=self.collection_name, headers=headers)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except Exception as exception:
            return response_exception(exception)

        return json(result, headers=headers, status=200)
    
    async def delete(self, request, instid=None):
        try:
            for preprocess in self.preprocess['DELETE_SINGLE']:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request, instance_id=instid, collection_name=self.collection_name)
                else:
                    resp = preprocess(request=request, instance_id=instid, collection_name=self.collection_name)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
                # See the note under the preprocess in the get() method.
                if resp is not None:
                    instid = resp
        except Exception as exception:
            return response_exception(exception)
        
        was_deleted = False
        if instid is not None:
            #result = await self.db.db[self.collection_name].delete_one({'_id': {'$eq': ObjectId(instid)}})
            result = await self.db.db[self.collection_name].delete_one({'_id': {'$in': instids(instid)}})
            was_deleted = result.deleted_count > 0
            
        try:
            headers = {}
            for postprocess in self.postprocess['DELETE_SINGLE']:
                if asyncio.iscoroutinefunction(postprocess):
                    resp = await postprocess(request=request, was_deleted=was_deleted, collection_name=self.collection_name, headers=headers)
                else:
                    resp = postprocess(request=request, was_deleted=was_deleted, collection_name=self.collection_name, headers=headers)
                    
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except Exception as exception:
            return response_exception(exception)

        return json({}, headers=headers, status=200) if was_deleted else json({}, headers=headers, status=520)
    
    async def _post(self, request, data):
        if "_id" in data:
            del data["_id"]
        
        if self.primary_key is not None:
            result = await self.db.db[self.collection_name].insert_one(data)
            data["_id"] = str(result.inserted_id)
            return data
    
    async def post(self, request):
        content_type = request.headers.get('Content-Type', "")
        content_is_json = content_type.startswith('application/json')
        
        if not content_is_json:
            msg = 'Request must have "Content-Type: application/json" header'
            return json(dict(message=msg),status=520)
        try:
            data = request.json or {}
        except (ServerError, TypeError, ValueError, OverflowError) as exception:
            #current_app.logger.exception(str(exception))
            return json(dict(message='Unable to decode data'),status=520)
        
        
        try:
            for preprocess in self.preprocess['POST']:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request, data=data, collection_name=self.collection_name)
                else:
                    resp = preprocess(request=request, data=data, collection_name=self.collection_name)
                    
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except Exception as exception:
            return response_exception(exception)
        
        result = await self._post(request, data)
        if result is None:
            json(dict(error_code='UNKNOWN_ERROR', error_message=''),status=520)
        
        
        try:
            headers = {}
            for postprocess in self.postprocess['POST']:
                if asyncio.iscoroutinefunction(postprocess):
                    resp = await postprocess(request=request, result=result, collection_name=self.collection_name, headers=headers)
                else:
                    resp = postprocess(request=request, result=result, collection_name=self.collection_name, headers=headers)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except Exception as exception:
            return response_exception(exception)
        
        return json(result,headers=headers, status=201)
    
    async def _put(self, request, data, instid):
        if "_id" in data:
            del data["_id"]
        
        #result = await self.db.db[self.collection_name].update_one({'_id': ObjectId(instid)}, {'$set': data})
        result = await self.db.db[self.collection_name].replace_one({'_id': {'$in': instids(instid)}},  data)

        if result.matched_count > 0:
            data["_id"] = instid
            return data
        
        return None
    
    async def put(self, request, instid=None):
        content_type = request.headers.get('Content-Type', "")
        content_is_json = content_type.startswith('application/json')
        
        if not content_is_json:
            msg = 'Request must have "Content-Type: application/json" header'
            return json(dict(message=msg),status=520)
        try:
            data = request.json or {}
        except (ServerError, TypeError, ValueError, OverflowError) as exception:
            #current_app.logger.exception(str(exception))
            return json(dict(error_code='PARAM_ERROR', error_message='Unable to decode data'),status=520)
        
        
        for preprocess in self.preprocess['PATCH_SINGLE']:
            try:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request, instance_id=instid, data=data, collection_name=self.collection_name)
                else:
                    resp = preprocess(request=request, instance_id=instid, data=data,collection_name=self.collection_name)
                
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
                # See the note under the preprocess in the get() method.
                if resp is not None:
                    instid = resp
            except Exception as exception:
                return response_exception(exception)
        
        result = await self._put(request, data, instid)
        
        if result is None:
            return json(dict(error_code='UNKNOWN_ERROR', error_message=''),status=520)
        
        headers = {}
        try:
            for postprocess in self.postprocess['PATCH_SINGLE']:
                if asyncio.iscoroutinefunction(postprocess):
                    resp = await postprocess(request=request, result=result, collection_name=self.collection_name, headers=headers)
                else:
                    resp = postprocess(request=request, result=result, collection_name=self.collection_name, headers=headers)
                    
                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
        except Exception as exception:
            return response_exception(exception)

        return json(result, headers=headers, status=200)
    
    async def patch(self, *args, **kw):
        """Alias for :meth:`patch`."""
        return await self.put(*args, **kw)