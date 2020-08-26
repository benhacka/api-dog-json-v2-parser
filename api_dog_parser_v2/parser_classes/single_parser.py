import json
from pathlib import Path
from typing import List

from tqdm.auto import tqdm

from api_dog_parser_v2.constants_and_enum import GrabbingFilter


class SingleDialogParser:
    def __init__(self, file, grabbing_filter: GrabbingFilter):
        self._file_key = self._convert_file_name_to_key(file)
        self._parsed_data = {self._file_key: []}
        self._json_data: dict = self._parse_json(file)
        self._owner = None
        self._peer = None
        self._id_collection = set()
        self._message_data = []
        self._grabbing_filter = grabbing_filter
        self._parse_json_data()

    @property
    def data_dict(self):
        return self._parsed_data

    @property
    def id_collection(self):
        return self._id_collection

    @staticmethod
    def _convert_file_name_to_key(file_name):
        return Path(file_name).stem

    @staticmethod
    def _parse_json(file):
        with open(file, 'r', encoding='utf-8-sig') as fp:
            return json.load(fp)

    def _parse_json_data(self):
        assert self._json_data
        meta_info: dict = self._json_data.get('meta')
        self._owner = meta_info.get('ownerId')
        self._peer = meta_info.get('peer')
        self._message_data = self._json_data.get('data', [])

    def _need_to_add(self, owner_id):
        pair = (self._owner, self._peer)
        if self._grabbing_filter == GrabbingFilter.ALL:
            return True
        elif (self._grabbing_filter is GrabbingFilter.ALL_EXCEPT_PAIR
              and owner_id in pair):
            return False
        elif (self._grabbing_filter is GrabbingFilter.PAIR
              and owner_id not in pair):
            return False
        elif (self._grabbing_filter is GrabbingFilter.OPPONENT
              and owner_id != self._peer):
            return False
        elif (self._grabbing_filter is GrabbingFilter.OWNER
              and owner_id != self._owner):
            return False
        return True

    def _add_photo_to_parsed_data(self, owner_id, date, photo_url):
        if not self._need_to_add(owner_id):
            return
        self._id_collection.add(owner_id)
        self._parsed_data[self._file_key].append({
            'owner_id': owner_id,
            'date': date,
            'photo_url': photo_url
        })

    def _parse_attachments(self, attachment_list: List[dict]):
        if not attachment_list:
            return
        for attachment_dict in attachment_list:
            if attachment_dict.get('type') != 'photo':
                continue
            photo = attachment_dict.get('photo', {})
            if not photo:
                continue
            sizes = photo.get('sizes', [])
            if not sizes:
                continue
            owner_id = photo.get('owner_id', 0)
            date = photo.get('date', 0)
            photo_dict = max(sizes, key=lambda x: x.get('width', 0))
            if not photo_dict or 'url' not in photo_dict:
                continue
            photo_url = photo_dict['url']
            self._add_photo_to_parsed_data(owner_id, date, photo_url)

    def _parse_message(self, data_dict: dict):
        attachments = data_dict.get('attachments', [])
        if attachments:
            self._parse_attachments(attachments)
        fwd_messages = data_dict.get('fwd_messages', [])
        if not fwd_messages:
            return
        for fwd_message in fwd_messages:
            self._parse_message(fwd_message)

    def parse_messages(self):
        for data in tqdm(self._message_data, position=1):
            self._parse_message(data)
