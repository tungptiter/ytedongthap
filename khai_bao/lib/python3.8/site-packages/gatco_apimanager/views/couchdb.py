# -*- coding: utf-8 -
import asyncio
# import pymongo
import ujson
import math

# from bson.objectid import ObjectId
from cloudant.query import Query
from gatco.exceptions import GatcoException, ServerError
from gatco.response import json, text, HTTPResponse
from gatco.request import json_loads

from . import ModelView


# https://python-cloudant.readthedocs.io/en/latest/getting_started.html
# https://github.com/cloudant/python-cloudant


def to_dict(document, exclude=None, include=None):
    obj = dict(document)
    columns = list(obj.keys())
    if exclude is not None:
        for c in columns:
            if c in exclude:
                del (obj[c])
    elif include is not None:
        for c in columns:
            if c not in include:
                del (obj[c])

    return obj


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
        order_by_list = search_params.get('order_by', None)

        # paginate
        # num_results = 20
        results_per_page = self._compute_results_per_page(request)

        page_num = 1
        if results_per_page > 0:
            page_num = int(request.args.get('page', 1))
            start = (page_num - 1) * results_per_page
            # end = start + results_per_page
            # total_pages = int(math.ceil(num_results / results_per_page))
        else:
            start = 0
            # total_pages = 1

        query = None
        filters = {"doc_type": self.collection_name}

        if 'filters' in search_params:
            filters = search_params['filters']
            filters["doc_type"] = self.collection_name
        
        if order_by_list is not None:
            query = Query(self.db.db, selector=filters, sort=order_by_list)
        else:
            query = Query(self.db.db, selector=filters)
        
        if is_single:
            for document in query(limit=1)['docs']:
                result = to_dict(document, exclude=self.exclude_columns, include=self.include_columns)
                break
        else:
            query_result = None
            if (results_per_page is not None) and (results_per_page > 0):
                if (start is not None) and (start > 0):
                    query_result = query.result[start:start + results_per_page]
                else:
                    query_result = query.result[:results_per_page]
            else:
                if (start is not None) and (start > 0):
                    query_result = query.result[start:]
                else:
                    query_result = query.result

            objects = []
            for document in query_result:
                objects.append(to_dict(document, exclude=self.exclude_columns, include=self.include_columns))

            new_page = page_num + 1
            result = {
                "page": page_num,
                "next_page": new_page,
                "objects": objects
            }
        return result

    async def search(self, request):
        try:
            search_params = json_loads(request.args.get('q', '{}'))
        except (TypeError, ValueError, OverflowError) as exception:
            # current_app.logger.exception(str(exception))
            return json(dict(error_code="PARAM_ERROR", error_message='Unable to decode data'), status=520)

        try:
            for preprocess in self.preprocess['GET_MANY']:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request, search_params=search_params, collection_name=self.collection_name)
                else:
                    resp = preprocess(request=request, search_params=search_params, collection_name=self.collection_name)

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
        document = self.db.db.get(instid, remote=True)
        if document is not None:
            obj = to_dict(document, exclude=self.exclude_columns, include=self.include_columns)
            return obj
        
        return None

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
            # result = await self.db.db[self.collection_name].delete_one({'_id': {'$eq': ObjectId(instid)}})
            # result = self.db.db[self.collection_name].delete_one({'_id': {'$in': [ObjectId(instid), instid]}})
            # was_deleted = result.deleted_count > 0

            document = self.db.db.get(instid,remote=True)
            if document is not None:
                document.delete()
                was_deleted = True
            else:
                was_deleted = False

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
        if "_rev" in data:
            del data["_rev"]

        data["doc_type"] = self.collection_name

        if self.primary_key is not None:
            # if data["_id"] not in self.db.db:
            document = self.db.db.create_document(data)
            obj = to_dict(document, exclude=self.exclude_columns, include=self.include_columns)
            del (obj["_rev"])
            return obj

    async def post(self, request):
        content_type = request.headers.get('Content-Type', "")
        content_is_json = content_type.startswith('application/json')

        if not content_is_json:
            msg = 'Request must have "Content-Type: application/json" header'
            return json(dict(message=msg), status=520)
        try:
            data = request.json or {}
        except (ServerError, TypeError, ValueError, OverflowError) as exception:
            # current_app.logger.exception(str(exception))
            return json(dict(message='Unable to decode data'), status=520)

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
            json(dict(error_code='UNKNOWN_ERROR', error_message=''), status=520)

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

        return json(result, headers=headers, status=201)

    async def _put(self, request, data, instid):
        if "doc_type" in data:
            del data["doc_type"]
        if "_rev" in data:
            del data["_rev"]
        
        
        
        if instid is not None:
            document = self.db.db.get(instid, remote=True)
            if document is not None:
                update = False
                for key, value in data.items():
                    try:
                        if document.get(key) != value:
                            update = True
                            document[key] = value
                    except:
                        pass
                if update:
                    document.save()
    
                if document is not None:
                    return to_dict(document, exclude=self.exclude_columns, include=self.include_columns)
            else:
            	if self.primary_key is not None:
                    data["doc_type"] = self.collection_name
                    document = self.db.db.create_document(data)
                    obj = to_dict(document, exclude=self.exclude_columns, include=self.include_columns)
                    del (obj["_rev"])
                    return obj
        else:
            if self.primary_key is not None:
                data["doc_type"] = self.collection_name
                document = self.db.db.create_document(data)
                obj = to_dict(document, exclude=self.exclude_columns, include=self.include_columns)
                del (obj["_rev"])
                return obj
        return None

    async def put(self, request, instid=None):
        content_type = request.headers.get('Content-Type', "")
        content_is_json = content_type.startswith('application/json')

        if not content_is_json:
            msg = 'Request must have "Content-Type: application/json" header'
            return json(dict(message=msg), status=520)
        try:
            data = request.json or {}
        except (ServerError, TypeError, ValueError, OverflowError) as exception:
            # current_app.logger.exception(str(exception))
            return json(dict(error_code='PARAM_ERROR', error_message='Unable to decode data'), status=520)

        for preprocess in self.preprocess['PATCH_SINGLE']:
            try:
                if asyncio.iscoroutinefunction(preprocess):
                    resp = await preprocess(request=request, instance_id=instid, data=data, collection_name=self.collection_name)
                else:
                    resp = preprocess(request=request, instance_id=instid, data=data, collection_name=self.collection_name)

                if (resp is not None) and isinstance(resp, HTTPResponse):
                    return resp
                # See the note under the preprocess in the get() method.
                if resp is not None:
                    instid = resp
            except Exception as exception:
                return response_exception(exception)

        result = await self._put(request, data, instid)

        if result is None:
            return json(dict(error_code='UNKNOWN_ERROR', error_message=''), status=520)

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
