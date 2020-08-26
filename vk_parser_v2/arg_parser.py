import argparse
from typing import Dict, Any

from vk_parser_v2.constants_and_enum import GrabbingFilter


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
                        'Allowed */./<folder paths>\n'
                        'Default "." - current dir',
                        nargs='?',
                        default='.')
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
                        '--get-names',
                        help='Try to get real name from vk '
                        'and write it into the folder name. '
                        'W/o this flag folder will be contain only id',
                        action='store_true',
                        default=False)

    dialog_folder = parser.add_subparsers(
        dest='folder-name',
        title='Dialog out folder',
        description='json-name - folder with json '
        'file name will be created in the folders near the parsed file.\n'
        'wo-sub-folder - sub folder will be not created. '
        'All dialog photos (with id sub folder)'
        ' will be put into the common folder - '
        "it's a directory with json file.\n"
        "custom-name - custom name for a dialog. Required --name")
    dialog_folder.add_parser('json-name')
    dialog_folder.add_parser('wo-sub-folder')
    custom_folder = dialog_folder.add_parser('custom-name')
    custom_folder.add_argument('-n',
                               '--name',
                               required=True,
                               help='Name of the future folder')
    return parser.parse_args()


def parse_arguments():
    args_dict: Dict[str, Any] = vars(arg_parser())
    print(args_dict)
