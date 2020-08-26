import logging
import os
from typing import Dict, List

from api_dog_parser_v2.constants_and_enum import VERIFY_FILE_HEAD, GrabbingFilter
from api_dog_parser_v2.parser_classes.single_parser import SingleDialogParser


class ParserManager:
    def __init__(self, folder_with_json):
        self._folder_with_json = folder_with_json
        self._files = self._filter_folder_files()
        abspath = os.path.abspath(folder_with_json)
        file_count = len(self._files)
        file_substring = (('is', '', 'was') if file_count == 1 else
                          ('are', 's', 'were'))
        if file_count:
            _msg = (
                f'There {file_substring[0]} {file_count} json '
                f'file{file_substring[1]} {file_substring[2]} taken from the '
                f'{abspath}')
        else:
            _msg = f'No allowed JSON files in the {abspath}'
        logging.info(_msg)
        self._parsed_data: Dict[str, List[SingleDialogParser]] = {
            'path': abspath,
            'url_data': [],
        }

    @property
    def id_collection(self):
        id_collection = set()
        for parser in self._parsed_data['url_data']:
            id_collection |= parser.id_collection
        return id_collection

    @property
    def is_contain_files(self):
        return bool(self._files)

    @property
    def data_dict(self):
        url_data = {}
        for parser in self._parsed_data['url_data']:
            url_data.update(parser.data_dict)
        return {**self._parsed_data, 'url_data': url_data}

    @property
    def has_content(self):
        return bool(sum(self.data_dict.get('url_data', {}).values(), []))

    @staticmethod
    def _verify_file(file):
        if not os.path.isfile(file):
            return None
        try:
            with open(file, mode='r', encoding='utf-8') as fp:
                file_head = fp.read(len(VERIFY_FILE_HEAD) + 1)
                if VERIFY_FILE_HEAD in file_head:
                    return file
            return None
        except OSError:
            return None

    def _add_parser(self, parser: SingleDialogParser):
        self._parsed_data['url_data'].append(parser)

    def _filter_folder_files(self):
        folder = self._folder_with_json
        if not os.path.isdir(folder):
            return []
        files = [
            self._verify_file(os.path.join(folder, file))
            for file in os.listdir(folder) if file.endswith('.json')
        ]
        return [file for file in files if file]

    @staticmethod
    def sizeof_fmt(size, suffix='B'):
        for unit in ['', 'Ki', 'Mi']:
            if abs(size) < 1024.0:
                return "%3.1f %s%s" % (size, unit, suffix)
            size /= 1024.0
        return "%.1f%s%s" % (size, 'Gi', suffix)

    def parse_files(self, grabbing_filter: GrabbingFilter):
        if not self.is_contain_files:
            return

        for file in self._files:
            parser = SingleDialogParser(file, grabbing_filter=grabbing_filter)
            self._add_parser(parser)
            size = self.sizeof_fmt(os.path.getsize(file))
            msg_ = f'Parsing {file} [{size}]'
            logging.info(msg_)
            parser.parse_messages()
