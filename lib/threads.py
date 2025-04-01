import asyncio
import json
import os
import random
import shutil
import time
import traceback
from json import JSONDecodeError
from pathlib import Path
from urllib.parse import unquote

import aiohttp
import requests
from PyQt5.QtCore import QThread, pyqtSignal
from bs4 import BeautifulSoup


class GetSoftwareListThread(QThread):
    signal = pyqtSignal('PyQt_PyObject')

    def __init__(self, url, json_file, rebuild=False):
        super().__init__()
        self.url = url
        self.json_file = json_file
        self.running = True
        self.rebuild = rebuild

    def run(self):
        try:
            iso_list = []
            if not self.rebuild:
                if os.path.exists(self.json_file):
                    with open(self.json_file, 'r') as file:
                        try:
                            iso_list = json.load(file)
                            print(f'Using existing ISO list cache: {self.json_file}')
                        except JSONDecodeError:
                            print(f'error loading {self.json_file} - rebuilding cache.')
                            pass

            if not iso_list and self.running:
                print(f'Downloading and building ISO list cache for: {self.json_file}')
                response = requests.get(self.url)
                soup = BeautifulSoup(response.text, 'html.parser')
                iso_list = [(unquote(link.get('href')), link.find_next('td', class_='size').text.strip())
                            for link in soup.find_all('a', href=lambda href: href and href.endswith('.zip'))]
                with open(self.json_file, 'w') as file:
                    json.dump(iso_list, file)
                    print(f'ISO list cache updated. {self.json_file}')

            if self.running:
                self.signal.emit(iso_list)
                return
        except Exception as e:
            print(f"Error updating ISO list cache {self.json_file}: {e}")
            traceback.print_exc()
            return
        print(f'Failure loading ISO list cache file: {self.json_file}')

    def stop(self):
        self.running = False


class SplitPkgThread(QThread):
    progress = pyqtSignal(str)
    status = pyqtSignal(bool)

    def __init__(self, file_path):
        QThread.__init__(self)
        self.file_path = file_path

    def run(self):
        file_size = os.path.getsize(self.file_path)
        if file_size < 4294967295:
            self.status.emit(False)
            return
        else:
            chunk_size = 4294967295
            num_parts = -(-file_size // chunk_size)
            with open(self.file_path, 'rb') as f:
                i = 0
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    with open(f"{Path(self.file_path).stem}.pkg.666{str(i).zfill(2)}", 'wb') as chunk_file:
                        chunk_file.write(chunk)
                    print(f"Splitting {self.file_path}: part {i+1}/{num_parts} complete")
                    self.progress.emit(f"Splitting {self.file_path}: part {i+1}/{num_parts} complete")
                    i += 1
            os.remove(self.file_path)
            self.status.emit(True)


class SplitIsoThread(QThread):
    progress = pyqtSignal(str)
    status = pyqtSignal(bool)
    finished = pyqtSignal(list)  # Add this line

    def __init__(self, file_path):
        QThread.__init__(self)
        self.file_path = file_path

    def run(self):
        file_size = os.path.getsize(self.file_path)
        if file_size < 4294967295:
            self.status.emit(False)
            self.finished.emit([])  # Add this line
            return
        else:
            chunk_size = 4294967295
            num_parts = -(-file_size // chunk_size)
            split_files = []  # Add this line
            with open(self.file_path, 'rb') as f:
                i = 0
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    split_file = f"{os.path.splitext(self.file_path)[0]}.iso.{str(i)}"
                    with open(split_file, 'wb') as chunk_file:
                        chunk_file.write(chunk)
                    split_files.append(split_file)  # Add this line
                    msg = f"Splitting {self.file_path}: part {i+1}/{num_parts} complete"
                    print(msg)
                    self.progress.emit(msg)
                    i += 1
            self.status.emit(True)
            self.finished.emit(split_files)  # Add this line


class FileOperationsThread(QThread):
    progress_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()

    def __init__(self, operations):
        super().__init__()
        self.operations = operations

    def run(self):
        for operation in self.operations:
            try:
                if operation['type'] == 'rename':
                    self.progress_signal.emit(f"Renaming {operation['src']} to {operation['dst']}")
                    os.rename(operation['src'], operation['dst'])
                elif operation['type'] == 'move':
                    self.progress_signal.emit(f"Moving {operation['src']} to {operation['dst']}")
                    shutil.move(operation['src'], operation['dst'])
                elif operation['type'] == 'remove':
                    self.progress_signal.emit(f"Removing {operation['src']}")
                    if os.path.isdir(operation['src']):
                        shutil.rmtree(operation['src'])
                    else:
                        os.remove(operation['src'])
                self.progress_signal.emit(f"Performed {operation['type']} operation")
            except Exception as e:
                self.progress_signal.emit(f"Error during {operation['type']}: {e}")
                print(f"Error during {operation['type']}: {e}")
        self.finished_signal.emit()


class DownloadThread(QThread):
    progress_signal = pyqtSignal(int)
    speed_signal = pyqtSignal(str)
    eta_signal = pyqtSignal(str)
    download_complete_signal = pyqtSignal()

    def __init__(self, url, filename, retries=50):
        QThread.__init__(self)
        self.url = url
        self.filename = filename
        self.retries = retries
        self.existing_file_size = 0
        self.start_time = None
        self.total_downloaded = 0
        self.running = True

    async def download(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        for i in range(self.retries):
            try:
                if os.path.exists(self.filename):
                    self.existing_file_size = os.path.getsize(self.filename)
                    headers['Range'] = f'bytes={self.existing_file_size}-'

                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url, headers=headers) as response:
                        if response.status not in (200, 206):
                            raise aiohttp.ClientPayloadError()

                        if 'content-range' in response.headers:
                            total_size = int(response.headers['content-range'].split('/')[-1])
                        else:
                            total_size = int(response.headers.get('content-length'))

                        with open(self.filename, 'ab') as file:
                            self.start_time = time.time()
                            self.total_downloaded = self.existing_file_size
                            while True:
                                chunk = await response.content.read(8192)
                                if not chunk:
                                    break
                                file.write(chunk)
                                self.total_downloaded += len(chunk)
                                self.progress_signal.emit(int((self.total_downloaded / total_size) * 100))

                                # Calculate speed and ETA
                                elapsed_time = time.time() - self.start_time
                                if elapsed_time > 0:
                                    speed = (self.total_downloaded - self.existing_file_size) / elapsed_time
                                else:
                                    speed = 0
                                remaining_bytes = total_size - self.total_downloaded
                                eta = remaining_bytes / speed if speed > 0 else 0

                                # Convert speed to appropriate units
                                if speed > 1024**2:
                                    speed_str = f"{speed / (1024**2):.2f} MB/s"
                                else:
                                    speed_str = f"{speed / 1024:.2f} KB/s"

                                # Convert ETA to appropriate units
                                if eta >= 60:
                                    minutes, seconds = divmod(int(eta), 60)
                                    eta_str = f"{minutes} minutes {seconds} seconds remaining"
                                else:
                                    eta_str = f"{eta:.2f} seconds remaining"

                                # Emit the speed and ETA signals
                                self.speed_signal.emit(speed_str)
                                self.eta_signal.emit(eta_str)

                # If the download was successful, break the loop
                break
            except aiohttp.ClientPayloadError:
                print(f"Download interrupted. Retrying ({i+1}/{self.retries})...")
                await asyncio.sleep(2 ** i + random.random())  # Exponential backoff
                if i == self.retries - 1:  # If this was the last retry
                    raise  # Re-raise the exception
            except asyncio.TimeoutError:
                print(f"Download interrupted. Retrying ({i+1}/{self.retries})...")
                await asyncio.sleep(2 ** i + random.random())  # Exponential backoff
                if i == self.retries - 1:  # If this was the last retry
                    raise  # Re-raise the exception

    def run(self):
        asyncio.run(self.download())
        self.download_complete_signal.emit()

    def stop(self):
        self.running = False
