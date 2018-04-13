import os

from muscope.util import take

from irods.keywords import FORCE_FLAG_KW
from irods.session import iRODSSession
from irods.exception import CAT_NO_ROWS_FOUND, CollectionDoesNotExist, DataObjectDoesNotExist


def irods_session_manager():
    return iRODSSession(irods_env_file=os.path.expanduser('~/.irods/irods_environment.json'))


def irods_collection_exists(irods_session, collection_path):
    try:
        irods_session.collections.get(collection_path)
        return True
    except CollectionDoesNotExist:
        return False


def irods_create_collection(irods_session, target_collection_path):
    """Create the specified collection and all parent collections
    that do not exist.

    :param irods_session:
    :param target_collection_path:
    :return:
    """
    collection_list = []
    parent = target_collection_path
    child = 'just to get started'
    while len(child) > 0:
        try:
            irods_session.collections.get(parent)
            break
        except CollectionDoesNotExist:
            collection_list.insert(0, parent)
            parent, child = os.path.split(parent)
            print('parent: {}'.format(parent))
            print('child : {}'.format(child))

    while len(collection_list) > 0:
        collection_path = collection_list.pop(0)
        print('creating collection "{}"'.format(collection_path))
        irods_session.collections.create(collection_path)


def irods_data_object_checksums_match(irods_session, path_1, path_2):
    data_object_1 = irods_session.data_objects.get(path_1)
    data_object_2 = irods_session.data_objects.get(path_2)
    print('data object "{}" has checksum {}'.format(path_1, data_object_1.checksum))
    print('data object "{}" has checksum {}'.format(path_2, data_object_2.checksum))
    return data_object_1.checksum == data_object_2.checksum


def irods_write_data_object(irods_session, dest_path, content):
    if irods_data_object_exists(irods_session, dest_path):
        irods_delete(irods_session, dest_path)
    else:
        pass

    target_obj = irods_session.data_objects.create(dest_path)
    with target_obj.open('r+') as target:
        target.write(content.encode('utf-8'))


def irods_copy(irods_session, src_path, dest_path):
    irods_session.data_objects.copy(src_path=src_path, dest_path=dest_path, **{FORCE_FLAG_KW: True})


def irods_put(irods_session, src_path, dest_path):
    irods_session.data_objects.put(src_path, dest_path)


def irods_data_object_exists(irods_session, target_path):
    try:
        irods_session.data_objects.get(target_path)
        return True
    except CollectionDoesNotExist:
        return False
    except DataObjectDoesNotExist:
        return False


def irods_delete(irods_session, target_path):
    try:
        irods_session.data_objects.unlink(path=target_path, force=True)
    except CAT_NO_ROWS_FOUND:
        print('unable to delete data_object "{}" because it does not exist'.format(target_path))


def irods_delete_collection(irods_session, target_collection_path):
    try:
        irods_session.collections.remove(target_collection_path)
    except CAT_NO_ROWS_FOUND:
        print('unable to delete collection "{}" because it does not exist'.format(target_collection_path))


def walk(walk_root, verbose=False):
    if verbose:
        print('walk root is "{}"'.format(walk_root))

    with irods_session_manager() as irods_session:
        collection_stack = list()
        p = irods_session.collections.get(walk_root)
        collection_stack.append(p)
        while len(collection_stack) > 0:
            parent_collection = collection_stack.pop(0)
            yield parent_collection, parent_collection.subcollections, parent_collection.data_objects
            for s in parent_collection.subcollections:
                collection_stack.insert(0, s)
            # keep the stack in sorted order for reproducible behavior
            collection_stack.sort(key=lambda c: c.path)


def get_project_sample_collection_paths(
        collection_root='/iplant/home/shared/imicrobe/projects',
        sample_limit=None):
    """Return a dictionary of project paths to lists of sample paths.

    This function is intended to be used to get a complete listing of sample collection paths.
    Walking the collections looking for specific files takes a lot of time so this is an attempt
    at a faster solution.

    The original application for this function was finding UProC results files.

    :param collection_root: the top of the collection tree to be searched
    :param sample_limit: maximum number of samples to return
    :return: dictionary such as
        {
            '/project/alice/': ['/project/alice/samples/abe', '/project/alice/samples/aoife', ...],
            '/project/bob/': ['/projects/bob/samples/betsy', '/projects/bob/samples/ben', ...],
            ...
        }
    """
    sample_total = 0
    project_to_sample_collections = dict()

    with irods_session_manager() as irods_session:
        # from the top
        project_collections = irods_session.collections.get(collection_root).subcollections
        for project_collection in project_collections:
            samples_collection_path = os.path.join(project_collection.path, 'samples')
            try:
                sample_collections_for_project = irods_session.collections.get(samples_collection_path).subcollections
                print('{} sample collection(s) for project {}'.format(
                    len(sample_collections_for_project),
                    project_collection.name))
                # '\n\t'.join([s.name for s in sample_collections_for_project])))
            except CollectionDoesNotExist:
                print('collection "{}" does not exist'.format(samples_collection_path))
                sample_collections_for_project = []

            if sample_limit is None:
                sample_path_list = [
                    s.path for s
                    in sample_collections_for_project]
            else:
                sample_path_list = [
                    s.path for s
                    in take((sample_limit - sample_total), sample_collections_for_project)]

            sample_total += len(sample_path_list)
            project_to_sample_collections[project_collection.path] = sample_path_list

            if sample_limit is not None and sample_limit <= sample_total:
                print('sample limit {} has been reached'.format(sample_limit))
                break
            else:
                pass

    return project_to_sample_collections
