import asyncio
import datetime
import logging
import os
import time
from typing import Optional, Dict, Union

import aiofiles
import aiohttp
from tqdm import tqdm

from api_dog_parser_v2.constants_and_enum import HEADER
from api_dog_parser_v2.parser_classes.name_grabber import NameGrabber


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

    async def _download_photo(self, file_name, url, *, tqdm_, try_count=3):
        async with self._download_semaphore, aiohttp.ClientSession(
                headers=self._headers) as session:
            session: aiohttp.ClientSession
            try:
                async with session.get(url) as request:
                    os.makedirs(os.path.dirname(file_name), exist_ok=True)
                    async with aiofiles.open(file_name, 'wb') as file:
                        await file.write(await request.read())
                        tqdm_.update()
                    return 0
            except (aiohttp.ClientError, OSError) as exp:
                if not try_count:
                    _msg = f'Problem with downloading image from {url} - {exp}'
                    logging.error(_msg)
                    return 1
                return await self._download_photo(file_name,
                                                  url,
                                                  try_count=try_count - 1,
                                                  tqdm_=tqdm_)

    @staticmethod
    def _is_file_exist(file_name):
        if os.path.isfile(file_name) and os.path.getsize(file_name):
            return True
        return False

    def _filter_existing_photos(self):
        delete_list = []
        for tuple_object in self._download_tuple_list:
            file_name, _ = tuple_object
            if self._is_file_exist(file_name):
                delete_list.append(tuple_object)
        if not delete_list:
            return
        msg_ = f'{len(delete_list)} pictures are exists, they will be ignored'
        logging.info(msg_)
        for data in delete_list:
            self._download_tuple_list.remove(data)

    def download_photos(self):
        if not self._download_raw_tuple:
            logging.info('Download list is empty...')
            logging.info('Finished')
            return

        print('\n')
        time.sleep(0.1)
        logging.info('Download step started')
        self._grab_names()
        self._convert_raw_to_normal()
        logging.info('Filter existing photos')
        self._filter_existing_photos()
        if not self._download_tuple_list:
            logging.info('All photos were filtered!')
            return
        logging.info('Downloading pictures')
        tqdm_ = tqdm(total=len(self._download_tuple_list))
        download_coroutines = [
            self._download_photo(*data, tqdm_=tqdm_)
            for data in self._download_tuple_list
        ]
        result = asyncio.get_event_loop().run_until_complete(
            asyncio.gather(*download_coroutines))
        error_count = sum(result)
        if not error_count:
            tqdm_.close()
            _msg = f'All {len(download_coroutines)} photos downloaded'
            logging.info(_msg)
        else:
            _msg = (f'{error_count}/{len(download_coroutines)} '
                    f'was not downloaded')
            logging.warning(_msg)
