"""
# one time
# systems-roles-addupdate -v -u scope -r USER tacc-stampede2-jklynch

# for each app
# apps-pems-update -v -u scope -p READ_EXECUTE muscope-last-0.0.4\

usage:

python grant_app_permission.py --app muscope-last-0.0.4

"""
import argparse
import subprocess
import sys

from muscope.models import User
from orminator import session_manager_from_db_uri


def get_args(argv):
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('--app', required=True,
                            help='')
    arg_parser.add_argument('--db-uri', required=True,
                            help='')

    args = arg_parser.parse_args(argv[1:])
    print(args)
    return args


def main(argv):
    args = get_args(argv)

    print('add permission for app "{}"'.format(args.app))
    with session_manager_from_db_uri(db_uri=args.db_uri) as db_session:
        for user in db_session.query(User).all():
            print('  adding permissions for user "{}"'.format(user))

            #subprocess.run(['system-roles-addupdate', '-v', '-u', args.user, '-r', 'USER', 'tacc-stampede2-jklynch'])
            #subprocess.run(['apps-pems-update', '-v', '-u', args.user, '-p', 'READ_EXECUTE', args.app])


def cli():
    main(sys.argv)


if __name__ == '__main__':
    cli()