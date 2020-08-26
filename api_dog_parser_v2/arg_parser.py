import argparse
import logging
import os
from typing import Dict, Any

from api_dog_parser_v2.constants_and_enum import GrabbingFilter


def arg_parser():
    def validate_limit(limit_value: str):
        arg_error = 'Incorrect value for limit. Possible value in [1, 500)'
        if not limit_value.isnumeric():
            raise argparse.ArgumentTypeError(arg_error)
        limit = int(limit_value)
        if limit not in range(1, 500):
            raise argparse.ArgumentTypeError(arg_error)
        return limit

    parser = argparse.ArgumentParser('API dog dialog v2 parser')
    parser.add_argument('paths',
                        help='Path(s) for json scanning. '
                        'Allowed . / <folder paths>\n'
                        'Default "." - current dir',
                        nargs='*',
                        default=['.'])
    parser.add_argument('-r',
                        '--recursive',
                        help='Recursive walking flag. '
                        'W/o the flag function is off',
                        action='store_true',
                        default=False)
    parser.add_argument('-l',
                        '--limit',
                        type=validate_limit,
                        help='Download limit. '
                        'Default value - 50',
                        default=50)

    parser.add_argument(
        '-c',
        '--collect',
        choices=[filter_.value for filter_ in GrabbingFilter],
        default='ALL',
        help='Grabbing filter. By default - ALL.'
        '\nowner - grab only owner photos (info from meta).'
        '\nopponent - grab only opponent photos (info from meta).'
        '\npair - grab owner and opponent photos (info from meta).'
        '\n all_except_pair - grab all except photos of owner and opponent '
        '(it is grabbing forwarding photos in fact). '
        'Can be useful if some one forward "leaked" content.'
        '\nall - grab all photos from dialog (groups photo albums excluded).')

    parser.add_argument('-n',
                        '--dont-get-names',
                        help='Default: try to get real name from vk '
                        'and write it into the folder name. '
                        'With the flag folder will be contain only id'
                        " (don't send get request on the VK servers ->"
                        " it's a little bit faster)",
                        action='store_false',
                        default=True)
    path_group = parser.add_mutually_exclusive_group()
    path_group.description = (
        'json-name - folder with json '
        'file name will be created in the folders near the parsed file.\n'
        'wo-sub-folder - sub folder will be not created. '
        'All dialog photos (with id sub folder)'
        ' will be put into the common folder - '
        "it's a directory with json files.\n"
        "custom-name - custom name for a dialog - name of the future folder"
        "\n\nDefault - json-name")

    path_group.add_argument('--json-name', action='store_true')
    path_group.add_argument('--wo-sub-folder', action='store_true')
    path_group.add_argument('--custom-name', help='Name of the future folder')
    return parser.parse_args()


def collect_path_list(args_dict: dict):

    paths = set(
        os.path.abspath(path) for path in args_dict['paths']
        if os.path.exists(path))
    if args_dict.pop('recursive'):
        for path in paths.copy():
            for root_dir, dirnames, _ in os.walk(path):
                paths |= {
                    os.path.join(root_dir, dirname)
                    for dirname in dirnames
                }
        _msg = (f'Script was collected {len(paths)} '
                f'folders with --recursive flag')
        logging.info(_msg)
    else:
        if not paths:
            logging.warning('There are no existing paths!')
            logging.warning('Check an argument with path (first arg).')
            return []
        _paths = args_dict['paths']
        if len(_paths) == 1:
            abs_path = os.path.abspath(_paths[0])
            if _paths[0] == '.':
                _msg = (f'JSON files will be searched in current '
                        f'directory - {abs_path}.')
            else:
                _msg = f'JSON files will be searched in {abs_path}.'
        else:
            _msg = f'JSON files will be searched in {len(_paths)} paths.'
        logging.info(_msg)
    return list(paths)


def parse_arguments():
    args_dict: Dict[str, Any] = vars(arg_parser())
    args_dict['paths'] = collect_path_list(args_dict)
    if not any((args_dict['json_name'], args_dict['wo_sub_folder'],
                args_dict['custom_name'])):
        args_dict['json_name'] = True
    return args_dict
