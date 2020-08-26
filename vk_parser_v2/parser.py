#!/usr/bin/env python3

import datetime
import json
import os
from pprint import pprint
from re import Match
from typing import List, Dict, Union, Tuple, Optional
from pathlib import Path
import asyncio
import logging

import aiofiles
import aiohttp

from vk_parser_v2.arg_parser import parse_arguments
from vk_parser_v2.constants_and_enum import (VERIFY_FILE_HEAD, TITLE_RE,
                                             GrabbingFilter, HEADER)


class NameGrabber:
    def __init__(self, name_grabber_limiter):
        self._name_grabber_semaphore = asyncio.Semaphore(name_grabber_limiter)

    @staticmethod
    async def _parse(vk_id: int,
                     session: aiohttp.ClientSession) -> Tuple[int, str]:
        url = f'https://m.vk.com/id{vk_id}'
        str_vk_id = str(vk_id)
        async with session.get(url) as request:
            try:
                html = await request.text()
                title: Match = TITLE_RE.search(html)
                if not title:
                    return vk_id, str_vk_id
                str_title = title.group(1).split('|')[0].strip()
                if str_title.lower() in ('ВКонтакте'.lower(), 'VK'.lower()):
                    return vk_id, str_vk_id
                return vk_id, f'{vk_id} ({str_title})'

            except aiohttp.ClientError:
                return vk_id, str_vk_id
            except Exception as e:
                print(e)
                return vk_id, str_vk_id

    async def _bulk_crawl(self, id_list: list) -> None:
        async with self._name_grabber_semaphore, aiohttp.ClientSession(
        ) as session:
            tasks = [
                self._parse(vk_id=int(vk_id), session=session)
                for vk_id in id_list
            ]
            results = await asyncio.gather(*tasks)
        return results

    def bulk_crawl(self, id_list: List[Union[str, int]]):
        result = asyncio.get_event_loop().run_until_complete(
            self._bulk_crawl(id_list))
        return dict(result)


class DownloadManager:
    def __init__(self,
                 download_limiter,
                 get_name: bool,
                 is_folder_name_as_json: bool,
                 folder_name: Optional[str] = None):
        self._get_name = get_name
        self._download_semaphore = asyncio.Semaphore(download_limiter)
        self._download_raw_tuple = []
        self._download_tuple_list = []
        self._is_folder_name_as_json = is_folder_name_as_json
        self._folder_name = folder_name
        self._id_collection = set()
        self._id_name_collection = {}
        self._name_grabber = NameGrabber(10)

    @staticmethod
    def _convert_timestamp_to_str(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime(
            '%Y%m%d_%H%M%S')

    def _add_download_tuple(self, root_path, json_name,
                            photo_dict: Dict[str, Union[int, str]]):
        if not self._is_folder_name_as_json:
            json_name = self._folder_name or ''
        owner_id = photo_dict['owner_id']
        owner_folder = self._id_name_collection.get(owner_id, str(owner_id))
        str_date = self._convert_timestamp_to_str(photo_dict['date'])
        url = photo_dict['photo_url']
        file_name = f'{str_date}_{url.split("/")[-1]}'
        photo_path = os.path.join(root_path, json_name, owner_folder,
                                  file_name)
        self._download_tuple_list.append((photo_path, url))

    def _grab_names(self):
        if self._get_name:
            self._id_name_collection = self._name_grabber.bulk_crawl(
                list(self._id_collection))

    def add_dict(self, file_dict):
        root_path = file_dict['path']
        url_data = file_dict['url_data']
        for json_name, data in url_data.items():
            for photo_dict in data:
                self._download_raw_tuple.append(
                    (root_path, json_name, photo_dict))

    def _convert_raw_to_normal(self):
        for raw_tuple in self._download_raw_tuple:
            self._add_download_tuple(*raw_tuple)

    def add_id_to_collection(self, id_collection: set):
        self._id_collection |= id_collection

    @property
    def _headers(self):
        return HEADER

    async def _download_photo(self, file_name, url, *, try_count=3):
        async with self._download_semaphore, aiohttp.ClientSession(
                headers=self._headers) as session:
            session: aiohttp.ClientSession
            try:
                async with session.get(url) as request:
                    os.makedirs(os.path.dirname(file_name), exist_ok=True)
                    async with aiofiles.open(file_name, 'wb') as file:
                        await file.write(await request.read())
                    return 0
            except (aiohttp.ClientError, OSError) as e:
                if not try_count:
                    _msg = f'Problem with downloading image from {url} - {e}'
                    logging.error(_msg)
                    return 1
                return await self._download_photo(file_name,
                                                  url,
                                                  try_count=try_count - 1)

    @staticmethod
    def _is_file_exist(file_name):
        if os.path.isfile(file_name) and os.path.getsize(file_name):
            return True

    def _filter_existing_photos(self):
        delete_list = []
        for tuple_object in self._download_tuple_list:
            file_name, _ = tuple_object
            if self._is_file_exist(file_name):
                delete_list.append(tuple_object)
        if not delete_list:
            return
        msg_ = f'{len(delete_list)} pictures are exist, they will be ignored'
        logging.info(msg_)
        for data in delete_list:
            self._download_tuple_list.remove(data)

    def download_photos(self):
        self._grab_names()
        self._convert_raw_to_normal()
        self._filter_existing_photos()
        if not self._download_tuple_list:
            logging.info('Download list is empty...')
            return
        download_coroutines = [
            self._download_photo(*data) for data in self._download_tuple_list
        ]
        result = asyncio.get_event_loop().run_until_complete(
            asyncio.gather(*download_coroutines))
        error_count = sum(result)
        if not error_count:
            _msg = f'All {len(download_coroutines)} photos downloaded'
            logging.info(_msg)
        else:
            _msg = (f'{error_count}/{len(download_coroutines)} '
                    f'was not downloaded')
            logging.warning(_msg)


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
        for data in self._message_data:
            self._parse_message(data)


class ParserManager:
    def __init__(self, folder_with_json):
        self._folder_with_json = folder_with_json
        self._files = self._filter_folder_files()
        abspath = os.path.abspath(folder_with_json)
        file_count = len(self._files)
        file_substring = (('is', '', 'was') if file_count == 1 else
                          ('are', 's', 'were'))
        _msg = (f'There are {file_substring[0]} {file_count} json '
                f'file{file_substring[1]} {file_substring[2]} taken from the '
                f'{abspath}')
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

    def parse_files(self, grabbing_filter: GrabbingFilter):
        if not self.is_contain_files:
            return {}

        for file in self._files:
            parser = SingleDialogParser(file, grabbing_filter=grabbing_filter)
            self._add_parser(parser)
            parser.parse_messages()


def main(parse_folders, download_limiter):
    get_name = False
    is_folder_name_as_json = False
    folder_name = None
    grabbing_filter = GrabbingFilter.OWNER
    download_manager = DownloadManager(download_limiter, get_name,
                                       is_folder_name_as_json, folder_name)
    parsers = [ParserManager(folder) for folder in parse_folders]
    for parser in parsers:
        parser.parse_files(grabbing_filter)
        if not parser.has_content:
            continue
        download_manager.add_dict(parser.data_dict)
        download_manager.add_id_to_collection(parser.id_collection)
    download_manager.download_photos()


if __name__ == '__main__':
    parse_arguments()
    logging.basicConfig(level=logging.INFO)
    folders = ['.']
    limiter = 50
    main(folders, limiter)
