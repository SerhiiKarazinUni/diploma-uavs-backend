from typing import List

from bson.objectid import ObjectId
from bson.typings import _DocumentType
from pymongo.collection import Collection


class PrefixTree:
    _prefix_tree: Collection

    def __init__(self, tree: Collection, dummy_root_id: ObjectId):
        self._prefix_tree = tree
        self._dummy_root = tree.find_one({'_id': dummy_root_id})

    def get_collection(self):
        return self._prefix_tree

    def get_dymmyroot(self):
        return self._dummy_root

    # NOTE: this routine is very similar to "unwind" in phones demo
    def search(self, root: _DocumentType, query: List[bytes], max_depth_overhead: int = 1, depth: int = 0):
        result = []

        # max_depth_overhead adjusts maximum possible search depth (after the query ends)
        if (depth - len(query)) > max_depth_overhead:
            return result

        # add current branch document IDs to result set
        if depth >= len(query) and 'documents' in root and len(root['documents']) > 0:
            for doc in root['documents']:
                result.append(doc)

        # check all the children
        for child in root['children']:
            child_obj = self._prefix_tree.find_one({'_id': child})

            if child_obj is None:
                continue  # something went wrong, cannot find this child object, skip
            if depth < len(query) and query[depth] != child_obj['hash']:
                continue  # this branch is invalid, do not look into it
            else:
                # look into this branch
                result.extend(self.search(child_obj, query, max_depth_overhead, depth + 1))

        return result

    def get_all_children(self, vertex: _DocumentType):
        return self._prefix_tree.find({'_id':{'$in': vertex['children']}})

    def insert(self, path: List[bytes], document_id: ObjectId):
        inserted = []
        updated = []

        current_root = self._dummy_root
        current_depth = 0
        # 1. loop through each prefix tree item, look for existing ones
        for depth in range(0, len(path)):
            found = False

            for child in self.get_all_children(current_root):
                if child['hash'] == path[depth]:
                    # found next corresponding child. Mark it as current root and continue search
                    current_root = child
                    found = True
                    current_depth += 1
                    continue

            # if there is no corresponding child on the current depth - interrupt the search
            if not found:
                break

        try:
            # 2. create new prefix tree vertices if needed
            # 2.a need to append prefix tree with new vertices
            if current_depth < len(path):
                for k in range(current_depth, len(path)):
                    # prepare and insert new child element data, including target document ID on the last iteration
                    child_data = {'hash': path[current_depth], 'children':[]}
                    if k+1 == len(path):
                        child_data['documents'] = [document_id]
                    child = self._prefix_tree.insert_one(child_data)
                    inserted.append(child.inserted_id)

                    # add this child element to index
                    self._prefix_tree.update_one(
                        {'_id': current_root['_id']},
                        {'$push': {'children': child.inserted_id}},
                        upsert=False
                    )
                    updated.append({'_id': current_root['_id'], 'child': child.inserted_id})
                    current_root = self._prefix_tree.find_one({'_id': child.inserted_id})
                    current_depth += 1
            else:
                # 2.b no need to append the prefix tree with new vertices, just insert new data
                self._prefix_tree.update_one(
                    {'_id': current_root['_id']},
                    {'$push': {'documents': document_id}},
                    upsert=False
                )
                updated.append(current_root['_id'])

        except Exception as e:
            # in case of error remove all created records and rollback updates
            self._prefix_tree.update_many(
                { '_id': {'$in': [x['_id'] for x in updated]}},
                {'$pull': {'children': {'$in': [x['child'] for x in updated]}}}
            )
            self._prefix_tree.delete_many({'_id': {'$in': inserted}})
            raise e

        return [inserted, updated]


