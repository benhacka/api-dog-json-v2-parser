#!/usr/bin/env python3

import logging

from api_dog_parser_v2.arg_parser import parse_arguments
from api_dog_parser_v2.constants_and_enum import (GrabbingFilter)
from api_dog_parser_v2.parser_classes.download_manager import DownloadManager
from api_dog_parser_v2.parser_classes.parser_manager import ParserManager


def _main_parse(*, parse_folders, download_limiter, get_names,
                is_folder_name_as_json, folder_name, grabbing_filter):
    if not parse_folders:
        logging.warning('Empty list with folder gotted!')
        logging.info('Exiting...')
        return
    download_manager = DownloadManager(download_limiter, get_names,
                                       is_folder_name_as_json, folder_name)
    parsers = [ParserManager(folder) for folder in parse_folders]
    for parser in parsers:
        parser.parse_files(grabbing_filter)
        if not parser.has_content:
            continue
        download_manager.add_dict(parser.data_dict)
        download_manager.add_id_to_collection(parser.id_collection)
    download_manager.download_photos()


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_arguments()
    kwargs = {
        'parse_folders': args['paths'],
        'download_limiter': args['limit'],
        'get_names': args['dont_get_names'],
        'grabbing_filter': GrabbingFilter(args['collect']),
        'is_folder_name_as_json': args['json_name'],
        'folder_name': args['custom_name']
    }
    try:
        _main_parse(**kwargs)
    except KeyboardInterrupt:
        print('\n')
        logging.info('Work cancelled...')


if __name__ == '__main__':
    main()
