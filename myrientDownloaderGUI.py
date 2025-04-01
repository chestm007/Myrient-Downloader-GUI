import os
import zipfile
import platform
import shutil
import signal
import glob
import multiprocessing
import urllib
import urllib.request
import urllib.parse
import pickle
from typing import Callable

import requests
import yaml
from PyQt5.QtWidgets import QApplication, QGridLayout, QGroupBox, QWidget, QVBoxLayout, \
    QPushButton, QComboBox, QLineEdit, QListWidget, QLabel, QCheckBox, QTextEdit, \
    QFileDialog, QDialog, QHBoxLayout, QAbstractItemView, QProgressBar, \
    QTabWidget, QListWidgetItem, QTableWidget, QTableWidgetItem
from PyQt5.QtCore import QSettings, Qt, QTimer
from PyQt5.QtGui import QTextCursor
import sys
import traceback

from lib.runners import CommandRunner, UnzipRunner
from lib.threads import GetSoftwareListThread, SplitPkgThread, SplitIsoThread, FileOperationsThread, DownloadThread


class OutputWindow(QTextEdit):
    def __init__(self, *args, **kwargs):
        super(OutputWindow, self).__init__(*args, **kwargs)
        # sys.stdout = self
        self.setReadOnly(True)

    def write(self, text):
        cursor = self.textCursor()
        cursor.movePosition(QTextCursor.End)
        cursor.insertText(text)
        self.setTextCursor(cursor)
        QApplication.processEvents()

    def flush(self):
        pass


class GUIDownloader(QWidget):
    def __init__(self):
        super().__init__()
        #signal.signal(signal.SIGINT, self.closeEvent)
        self.threads = []  # Store thread references
        # Load the user's settings
        self.settings = QSettings('./myrientDownloaderGUI.ini', QSettings.IniFormat)
        self.ps3dec_binary = self.settings.value('ps3dec_binary', '')
        self.processing_dir = 'processing'
        os.makedirs(self.processing_dir, exist_ok=True)
        print("Initializing GUIDownloader...")
        
        try:
            # Load the user's settings
            self.settings = QSettings('./myrientDownloaderGUI.ini', QSettings.IniFormat)
            print("Settings loaded.")

            # Load system configurations
            with open('systems.yaml', 'r') as f:
                self.systems_config = yaml.safe_load(f)['systems']

            # Initialize system data
            self.systems_data = {}
            for system in self.systems_config:
                index = system['index']
                self.systems_data[index] = system  # Store the entire system config

                # Load output directories from settings or use default
                output_dir = self.settings.value(system['output_dir_key'], f"MyrientDownloads/{system['name']}")
                self.systems_data[index]['output_dir'] = output_dir

                # Create directories if they do not exist
                os.makedirs(output_dir, exist_ok=True)

            # Initialize software lists
            for index in self.systems_data:
                self.systems_data[index]['list'] = ['Loading... this will take a moment']

            # Check if the saved binary paths exist
            if not os.path.isfile(self.ps3dec_binary):
                self.ps3dec_binary = ''
                self.settings.setValue('ps3dec_binary', '')

            # Check if ps3dec is in the user's PATH
            ps3dec_in_path = shutil.which("ps3dec") or shutil.which("PS3Dec") or shutil.which("ps3dec.exe") or shutil.which("PS3Dec.exe")

            if ps3dec_in_path:
                self.ps3dec_binary = ps3dec_in_path
                self.settings.setValue('ps3dec_binary', self.ps3dec_binary)

            # Check if the saved settings are valid
            if not self.is_valid_binary(self.ps3dec_binary, 'ps3dec'):
                # If not, open the first startup prompt
                self.first_startup()

            # For displaying queue position in OutputWindow
            self.processed_items = 0 
            self.total_items = 0 

            # Load the queue from 'queue.txt'
            if os.path.exists('queue.txt'):
                with open('queue.txt', 'rb') as file:
                    self.queue = pickle.load(file)
            else:
                self.queue = []

            self.initUI()
            print("UI initialized.")

            # Set default manufacturer to Sony
            self.set_default_manufacturer()

            # Add the entries from 'queue.txt' to the queue
            for item_text, system_index in self.queue:
                new_item = QListWidgetItem(item_text)
                new_item.setData(Qt.UserRole, system_index)
                self.queue_list.addItem(new_item)

            # Add a signal handler for SIGINT to stop the download and save the queue
            signal.signal(signal.SIGINT, self.closeEvent)

            # Load software lists after the main window is shown
            QTimer.singleShot(0, self.load_software_lists)
            print("Software list loading scheduled.")

        except Exception as e:
            print(f"Error during initialization: {e}")
            traceback.print_exc()
            raise

    def closeEvent(self, event):
        # TODO: remove after daemon implemented
        print("Closing application...")
        self.stop_threads()
        # Save the queue to 'queue.txt'
        with open('queue.txt', 'wb') as file:
            pickle.dump([(self.queue_list.item(i).text(), self.queue_list.item(i).data(Qt.UserRole)) for i in range(self.queue_list.count())], file)
        print("Application closed.")
        event.accept()

    def stop_threads(self):
        print("Stopping threads...")
        if hasattr(self, 'threads'):
            for thread in self.threads:
                thread.stop()
        print("Threads stopped.")

    def initUI(self):
        def new_button(parent: GUIDownloader, label: str, click_action: Callable, enabled: bool=True) -> QPushButton:
            button = QPushButton(label, self)
            button.clicked.connect(click_action)
            button.setEnabled(enabled)
            parent.addWidget(button)
            return button

        def new_checkbox(parent: GUIDownloader, label: str, pos: tuple[int, int], checked: bool=False) -> QCheckBox:
            checkbox = QCheckBox(label, self)
            checkbox.setChecked(checked)
            parent.addWidget(checkbox, *pos)
            return checkbox

        print("Setting up UI...")
        vbox = QVBoxLayout()

        # Add a header for the Manufacturer list
        vbox.addWidget(QLabel('Manufacturer'))

        # Combobox for Manufacturer
        self.manufacturer = QComboBox(self)
        self.manufacturer.addItems(
                sorted(list(set(system.get('manufacturer', 'Unknown') for system in self.systems_data.values())))
        )
        vbox.addWidget(self.manufacturer)
        
        # Add a header for the software list
        vbox.addWidget(QLabel('Software'))

        # Create a search box
        self.search_box = QLineEdit(self)
        self.search_box.setPlaceholderText('Search...')
        self.search_box.textChanged.connect(self.update_results)
        vbox.addWidget(self.search_box)

        # Create a tab widget for the systems
        self.result_list = QTabWidget(self)
        for index in sorted(self.systems_data.keys()):
            system = self.systems_data[index]
            table_widget = QTableWidget()
            table_widget.setColumnCount(2)
            table_widget.setHorizontalHeaderLabels(['File Name', 'Size'])
            table_widget.setSelectionBehavior(QAbstractItemView.SelectRows)
            table_widget.setEditTriggers(QAbstractItemView.NoEditTriggers)
            table_widget.itemSelectionChanged.connect(self.update_add_to_queue_button)
            table_widget.setSelectionMode(QAbstractItemView.ExtendedSelection)
            self.result_list.addTab(table_widget, system['name'])

        self.result_list.currentChanged.connect(self.update_add_to_queue_button)
        self.result_list.currentChanged.connect(self.update_results)
        vbox.addWidget(self.result_list)

        # Manufacturer selection logic
        self.manufacturer.currentIndexChanged.connect(self.manufacturer_selection)

        # Create a horizontal box layout
        hbox = QHBoxLayout()

        # Create a button to add to queue
        self.add_to_queue_button = new_button(hbox, 'Add to Queue', self.add_to_queue, False)

        # Create a button to remove from queue
        self.remove_from_queue_button = new_button(hbox, 'Remove from Queue', self.remove_from_queue, False)

        # Add the horizontal box layout to the vertical box layout
        vbox.addLayout(hbox)

        # Add a header for the Queue
        vbox.addWidget(QLabel('Queue'))

        # Create queue list
        self.queue_list = QListWidget(self)
        self.queue_list.setSelectionMode(QAbstractItemView.MultiSelection)  
        self.queue_list.itemSelectionChanged.connect(self.update_remove_from_queue_button) 
        vbox.addWidget(self.queue_list)

        # Create a grid layout for the options
        grid = QGridLayout()

        # Add a header for the options
        grid.addWidget(QLabel('ISO Settings'), 0, 0)
        # Create a checkbox for decrypting the file
        self.decrypt_checkbox = new_checkbox(grid, 'Decrypt (if necessary)', (1, 0), True)
        # Create a checkbox for keeping or deleting the encrypted ISO file
        self.keep_enc_checkbox = new_checkbox(grid, 'Keep encrypted ISO', (2, 0), False)
        # Create a checkbox for splitting the file for FAT31 filesystems
        self.split_checkbox = new_checkbox(grid, 'Split for FAT31 (if > 4GB)', (3, 0), True)
        # Create a checkbox for keeping or deleting the unsplit decrypted ISO file
        self.keep_unsplit_dec_checkbox = new_checkbox(grid, 'Keep unsplit ISO', (4, 0), False)
        # Create a checkbox for keeping or deleting the dkey file
        self.keep_dkey_checkbox = new_checkbox(grid, 'Keep dkey file', (5, 0), False)
        # create a checkbox for debugging mode
        self.debug_checkbox = new_checkbox(grid, 'Debug Mode (Print URLs only)', (6, 0), False)

        grid.addWidget(QLabel('PKG Settings'), 0, 1)
        # Create a checkbox for splitting the PKG file
        self.split_pkg_checkbox = new_checkbox(grid, 'Split PKG', (1, 1), True)
        # Create a checkbox for keeping or deleting the unsplit PKG file
        self.keep_unsplit_pkg_checkbox = new_checkbox(grid, 'Keep unsplit PKG', (2, 1), False)

        # Connect the stateChanged signal of the split_pkg_checkbox to a slot that shows or hides the keep_unsplit_pkg_checkbox
        self.split_pkg_checkbox.stateChanged.connect(self.keep_unsplit_pkg_checkbox.setVisible)
        # Initially hide the keep_unsplit_pkg_checkbox if split_pkg_checkbox is unchecked
        self.keep_unsplit_pkg_checkbox.setVisible(self.split_pkg_checkbox.isChecked())
        # Connect the stateChanged signal of the decrypt_checkbox to a slot that shows or hides the keep_enc_checkbox
        self.decrypt_checkbox.stateChanged.connect(self.keep_enc_checkbox.setVisible)
        # Connect the stateChanged signal of the split_checkbox to a slot that shows or hides the keep_unsplit_dec_checkbox
        self.split_checkbox.stateChanged.connect(self.keep_unsplit_dec_checkbox.setVisible)

        # Create a group box to contain the grid layout
        group_box = QGroupBox()
        group_box.setLayout(grid)
        vbox.addWidget(group_box)

        # Create a settings button
        self.settings_button = new_button(vbox, 'Settings', self.open_settings)

        # Create a button to start the process
        self.start_button = new_button(vbox, 'Start', self.start_download)

        # Add a header for the Output Window
        vbox.addWidget(QLabel('Logs'))

        # Create an output window
        self.output_window = OutputWindow(self)
        vbox.addWidget(self.output_window)

        # Add a header for the progress bar
        vbox.addWidget(QLabel('Progress'))

        # Create a progress bar and add it to the layout
        self.progress_bar = QProgressBar(self)
        vbox.addWidget(self.progress_bar)

        # Add a header for the speed, eta
        vbox.addWidget(QLabel('Download Speed & ETA'))

        # Create labels for download speed and ETA
        self.download_speed_label = QLabel(self)
        vbox.addWidget(self.download_speed_label)
        self.download_eta_label = QLabel(self)
        vbox.addWidget(self.download_eta_label)

        self.setLayout(vbox)

        self.setWindowTitle('Myrient Downloader')
        self.resize(1920, 1080)
        self.show()
        print("UI setup complete.")
    
    def set_default_manufacturer(self):
        # Set the default manufacturer to Sony
        sony_index = self.manufacturer.findText("Sony")
        if sony_index >= 0:
            self.manufacturer.setCurrentIndex(sony_index)
        
        # Manually trigger the manufacturer selection logic
        self.manufacturer_selection()

    def manufacturer_selection(self):
        selected_manufacturer = self.manufacturer.currentText()
        for index in range(self.result_list.count()):
            system = self.systems_data.get(index)
            if system:
                is_visible = (system.get('manufacturer', 'Unknown') == selected_manufacturer)
                self.result_list.setTabVisible(index, is_visible)

    def load_software_lists(self, rebuild=False):
        # TODO: delete after daemon
        try:
            for index, system in self.systems_data.items():
                json_filename = f"{system['name'].replace(' ', '_').lower()}_list.json"
                thread = GetSoftwareListThread(system['url'], json_filename, rebuild)
                thread.signal.connect(lambda iso_list, idx=index: self.set_system_list(idx, iso_list))
                self.threads.append(thread)  # Keep a reference to the thread
                thread.finished.connect(thread.deleteLater)  # Ensure thread is deleted when finished
                thread.start()
            print("loading ISO list cache...")

        except Exception as e:
            print(f"FATAL:: Error loading ISO lists: {e}")
            traceback.print_exc()

    def start_download(self):
        self._set_enabled_clickables(False)
        self.process_next_item()

    def _set_enabled_clickables(self, enabled: bool):
        """
        this function enables or disables GUI buttons while downloading
        """
        for clickable in (
                self.settings_button, self.add_to_queue_button, self.remove_from_queue_button,
                self.decrypt_checkbox, self.split_checkbox, self.keep_dkey_checkbox,
                self.keep_enc_checkbox, self.keep_unsplit_dec_checkbox, self.split_pkg_checkbox,
                self.keep_dkey_checkbox, self.keep_unsplit_pkg_checkbox, self.start_button):
            clickable.setEnabled(enabled)

    def process_next_item(self):
        if self.queue_list.count() > 0:
            item = self.queue_list.item(0)
            item_text = item.text()
            system_index = item.data(Qt.UserRole)
            system = self.systems_data.get(system_index)

            # Get the total number of items in the queue
            if self.processed_items == 0:  # Only update total_items at the start of the download process
                self.total_items = self.queue_list.count()

            # Increment the processed_items counter
            self.processed_items += 1

            if system:
                queue_position = f"{self.processed_items}/{self.total_items}"
                # Check for special handling
                if system.get('requires_decryption'):
                    self.download_and_process_ps3_iso(item_text, system, queue_position)
                elif system.get('requires_pkg_handling'):
                    self.download_and_process_psn_pkg(item_text, system, queue_position)
                elif system.get('has_multiple_extensions'):
                    self.download_and_process_multi_file(item_text, system, queue_position)
                else:
                    self.download_file(item_text, system, queue_position)
            else:
                print(f"No system found for index {system_index}")
                # Proceed to next item
                self.queue_list.takeItem(0)
                self.process_next_item()
        else:
            self.processed_items = 0
            self.total_items = 0

            # Save the queue to 'queue.txt'
            with open('queue.txt', 'wb') as file:
                pickle.dump([(self.queue_list.item(i).text(), self.queue_list.item(i).data(Qt.UserRole)) for i in range(self.queue_list.count())], file)

            # Re-enable the buttons
            self._set_enabled_clickables(True)

    def downloadhelper(self, selected_iso, queue_position, url, callback):
        # URL-encode the selected_iso
        selected_iso_encoded = urllib.parse.quote(selected_iso)
        
        download_url = f"{url}/{selected_iso_encoded}"
    
        # Print the download link if debugging mode is enabled
        print(f"Downloading from: {download_url}")
        self.output_window.append(f"({queue_position}) Downloading from: {download_url}")
        
        # If debugging mode is enabled, skip the actual download
        if self.debug_checkbox.isChecked():
            print("Debug mode is enabled. Skipping actual download.")
            callback("DEBUG_MODE")
            return  # Skip the download process

        # Compute base_name from selected_iso
        base_name = os.path.splitext(selected_iso)[0]

        # Define the path for the .zip file
        zip_file_path = os.path.join(self.processing_dir, base_name + '.zip')

        # If the .zip file exists, compare its size to that of the remote URL
        if os.path.exists(zip_file_path):
            local_file_size = os.path.getsize(zip_file_path)

            # Get the size of the remote file
            response = requests.head(f"{url}/{selected_iso_encoded}")
            if 'content-length' in response.headers:
                remote_file_size = int(response.headers['content-length'])
            else:
                print("Could not get the size of the remote file.")
                callback(zip_file_path)
                return

            # If the local file is smaller, attempt to resume the download
            if local_file_size < remote_file_size:
                print(f"Local file is smaller than the remote file. Attempting to resume download...")
            # If the local file is the same size as the remote file, skip the download
            elif local_file_size == remote_file_size:
                print(f"Local file is the same size as the remote file. Skipping download...")
                callback(zip_file_path)
                return

        # If the file does not exist, proceed with the download
        self.output_window.append(f"({queue_position}) Download started for {base_name}...")
        self.progress_bar.reset()  # Reset the progress bar to 0
        self.download_thread = DownloadThread(f"{url}/{selected_iso_encoded}", zip_file_path)
        self.download_thread.progress_signal.connect(self.progress_bar.setValue)
        self.download_thread.speed_signal.connect(self.download_speed_label.setText)
        self.download_thread.eta_signal.connect(self.download_eta_label.setText)
        self.download_thread.finished.connect(lambda: callback(zip_file_path))
        self.download_thread.start()

    def download_file(self, selected_iso, system, queue_position):
        url = system['url']
        base_name = os.path.splitext(selected_iso)[0]
        file_extension = system['file_extension']
        output_dir = system['output_dir']

        def after_download(file_path):
            if file_path == "DEBUG_MODE":
                self.queue_list.takeItem(0)
                self.process_next_item()
                return

            if not file_path.lower().endswith('.zip'):
                print(f"File {file_path} is not a .zip file. Skipping unzip.")
                self.queue_list.takeItem(0)
                self.process_next_item()
                return

            self.output_window.append(f"({queue_position}) Unzipping {base_name}.zip...")

            self.unzip_runner = UnzipRunner(file_path, self.processing_dir)
            self.unzip_runner.progress_signal.connect(self.progress_bar.setValue)
            self.unzip_runner.finished_signal.connect(lambda files: self.handle_extracted_files(files, base_name, file_extension, output_dir, queue_position))
            self.unzip_runner.start()

        self.downloadhelper(selected_iso, queue_position, url, after_download)

    def handle_extracted_files(self, extracted_files, base_name, file_extension, output_dir, queue_position):
        operations = []
        for file in extracted_files:
            if file.endswith(file_extension):
                new_file_path = os.path.join(self.processing_dir, f"{base_name}{file_extension}")
                operations.append({'type': 'rename', 'src': file, 'dst': new_file_path})
                dst = os.path.join(output_dir, os.path.basename(new_file_path))
                operations.append({'type': 'move', 'src': new_file_path, 'dst': dst})
        
        # Add operation to remove the zip file
        zip_file = os.path.join(self.processing_dir, f"{base_name}.zip")
        if os.path.exists(zip_file):
            operations.append({'type': 'remove', 'src': zip_file})

        self.file_operations_thread = FileOperationsThread(operations)
        self.file_operations_thread.progress_signal.connect(lambda msg: self.output_window.append(f"({queue_position}) {msg}"))
        self.file_operations_thread.finished_signal.connect(lambda: self.file_operations_finished(queue_position, base_name))
        self.file_operations_thread.start()

    def file_operations_finished(self, queue_position, base_name):
        self.output_window.append(f"({queue_position}) {base_name} ready!")
        self.queue_list.takeItem(0)
        self.process_next_item()

    def download_and_process_ps3_iso(self, selected_iso, system, queue_position):
        url = system['url']
        base_name = os.path.splitext(selected_iso)[0]
        output_dir = system['output_dir']

        def after_download(file_path):
            if file_path == "DEBUG_MODE":
                self.queue_list.takeItem(0)
                self.process_next_item()
                return

            self.output_window.append(f"({queue_position}) Unzipping {base_name}.zip...")

            # Unzip the ISO into the processing directory
            self.unzip_runner = UnzipRunner(file_path, self.processing_dir)
            self.unzip_runner.progress_signal.connect(self.progress_bar.setValue)
            self.unzip_runner.finished_signal.connect(lambda files: self.handle_ps3_files(files, base_name, output_dir, queue_position))
            self.unzip_runner.start()

        self.downloadhelper(selected_iso, queue_position, url, after_download)

    def handle_ps3_files(self, extracted_files, base_name, output_dir, queue_position):
        iso_file = None
        dkey_file = None

        # Identify the ISO file and dkey file
        for file in extracted_files:
            if file.endswith('.iso'):
                iso_file = file

        # Check if the corresponding .dkey file already exists
        dkey_file_path = os.path.join(self.processing_dir, f"{base_name}.dkey")
        if not os.path.isfile(dkey_file_path):
            if self.decrypt_checkbox.isChecked() or self.keep_dkey_checkbox.isChecked():
                # Download the corresponding .dkey file
                self.output_window.append(f"({queue_position}) Getting dkey for {base_name}...")
                dkey_url = f"https://myrient.erista.me/files/Redump/Sony%20-%20PlayStation%203%20-%20Disc%20Keys%20TXT/{base_name}.zip"
                dkey_zip_path = os.path.join(self.processing_dir, f"{base_name}_dkey.zip")
                self.download_thread = DownloadThread(dkey_url, dkey_zip_path)
                self.download_thread.progress_signal.connect(self.progress_bar.setValue)
                self.download_thread.finished.connect(lambda: self.after_dkey_download(dkey_zip_path, iso_file, base_name, output_dir, queue_position))
                self.download_thread.start()
        else:
            self.after_dkey_download(None, iso_file, base_name, output_dir, queue_position)

    def after_dkey_download(self, dkey_zip_path, iso_file, base_name, output_dir, queue_position):
        if dkey_zip_path:
            # Unzip the dkey file and delete the ZIP file
            with zipfile.ZipFile(dkey_zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.processing_dir)
            os.remove(dkey_zip_path)

        dkey_file_path = os.path.join(self.processing_dir, f"{base_name}.dkey")
        if os.path.isfile(dkey_file_path):
            dkey_file = dkey_file_path
        else:
            dkey_file = None

        if self.decrypt_checkbox.isChecked() and iso_file and dkey_file:
            self.decrypt_ps3_iso(iso_file, dkey_file, base_name, queue_position)
        else:
            self.after_decryption(iso_file, base_name, output_dir, queue_position)

    def decrypt_ps3_iso(self, iso_file, dkey_file, base_name, queue_position):
        with open(dkey_file, 'r') as file:
            key = file.read(32)

        self.output_window.append(f"({queue_position}) Decrypting ISO for {base_name}...")
        if platform.system() == 'Windows':
            thread_count = multiprocessing.cpu_count() // 2
            command = [f"{self.ps3dec_binary}", "--iso", iso_file, "--dk", key, "--tc", str(thread_count)]
        else:
            command = [self.ps3dec_binary, 'd', 'key', key, iso_file]

        self.runner = CommandRunner(command)
        self.runner.finished.connect(lambda: self.decryption_finished(iso_file, base_name, queue_position))
        self.runner.start()

    def decryption_finished(self, iso_file, base_name, queue_position):
        # Handle the output file name
        if platform.system() == 'Windows':
            decrypted_iso = os.path.join(self.processing_dir, f"{base_name}.iso_decrypted.iso")
        else:
            decrypted_iso = os.path.join(self.processing_dir, f"{base_name}.iso.dec")

        if os.path.exists(decrypted_iso):
            # Check if the user wants to keep the encrypted ISO
            if not self.keep_enc_checkbox.isChecked():
                os.remove(iso_file)  # Remove the encrypted ISO

            # Determine the final name for the decrypted ISO
            if self.keep_enc_checkbox.isChecked():
                final_iso = os.path.join(self.processing_dir, f"{base_name}.iso.dec")
            else:
                final_iso = os.path.join(self.processing_dir, f"{base_name}.iso")

            # Rename the decrypted ISO
            if os.path.exists(final_iso):
                os.remove(final_iso)  # Remove any existing file with the same name
            os.rename(decrypted_iso, final_iso)

            self.after_decryption(final_iso, base_name, self.systems_data[self.result_list.currentIndex()]['output_dir'], queue_position)
        else:
            print(f"Decrypted ISO not found for {base_name}.")
            # Handle error accordingly
            self.queue_list.takeItem(0)
            self.process_next_item()

    def after_decryption(self, iso_file, base_name, output_dir, queue_position):
        if self.split_checkbox.isChecked() and os.path.getsize(iso_file) >= 4294967295:
            self.split_ps3_iso(iso_file, base_name, queue_position)
        else:
            self.finalize_ps3_files(iso_file, base_name, queue_position)

    def split_ps3_iso(self, iso_file, base_name, queue_position):
        self.output_window.append(f"({queue_position}) Splitting ISO for {base_name}...")
        self.split_iso_thread = SplitIsoThread(iso_file)
        self.split_iso_thread.progress.connect(lambda msg: self.output_window.append(msg))
        self.split_iso_thread.finished.connect(lambda split_files: self.splitting_finished(split_files, base_name, queue_position, iso_file))
        self.split_iso_thread.start()

    def splitting_finished(self, split_files, base_name, queue_position, iso_file):
        # Proceed to move files after splitting
        self.finalize_ps3_files(iso_file, base_name, queue_position, split_files)

    def finalize_ps3_files(self, iso_file, base_name, queue_position, split_files=None):
        operations = []
        output_dir = self.systems_data[self.result_list.currentIndex()]['output_dir']

        if split_files:
            for split_file in split_files:
                dest = os.path.join(output_dir, os.path.basename(split_file))
                operations.append({'type': 'move', 'src': split_file, 'dst': dest})
            if not self.keep_unsplit_dec_checkbox.isChecked() and iso_file:
                operations.append({'type': 'remove', 'src': iso_file})
        else:
            dest = os.path.join(output_dir, os.path.basename(iso_file))
            operations.append({'type': 'move', 'src': iso_file, 'dst': dest})

        # Handle dkey file
        if self.keep_dkey_checkbox.isChecked() and os.path.isfile(os.path.join(self.processing_dir, f"{base_name}.dkey")):
            dkey_file = os.path.join(self.processing_dir, f"{base_name}.dkey")
            dest = os.path.join(output_dir, os.path.basename(dkey_file))
            operations.append({'type': 'move', 'src': dkey_file, 'dst': dest})
        else:
            dkey_file = os.path.join(self.processing_dir, f"{base_name}.dkey")
            if os.path.isfile(dkey_file):
                operations.append({'type': 'remove', 'src': dkey_file})

        # Remove the zip file
        zip_file = os.path.join(self.processing_dir, f"{base_name}.zip")
        if os.path.exists(zip_file):
            operations.append({'type': 'remove', 'src': zip_file})

        self.file_operations_thread = FileOperationsThread(operations)
        self.file_operations_thread.progress_signal.connect(lambda msg: self.output_window.append(f"({queue_position}) {msg}"))
        self.file_operations_thread.finished_signal.connect(lambda: self.ps3_file_operations_finished(queue_position, base_name))
        self.file_operations_thread.start()

    def ps3_file_operations_finished(self, queue_position, base_name):
        self.output_window.append(f"({queue_position}) {base_name} complete!")
        self.queue_list.takeItem(0)
        self.process_next_item()

    def download_and_process_psn_pkg(self, selected_iso, system, queue_position):
        url = system['url']
        base_name = os.path.splitext(selected_iso)[0]
        output_dir = system['output_dir']  # This is psn_pkg_dir
        psn_rap_dir = self.settings.value('psn_rap_dir', 'MyrientDownloads/exdata')
        os.makedirs(psn_rap_dir, exist_ok=True)

        def after_download(file_path):
            if file_path == "DEBUG_MODE":
                self.queue_list.takeItem(0)
                self.process_next_item()
                return

            if not file_path.lower().endswith('.zip'):
                print(f"File {file_path} is not a .zip file. Skipping unzip.")
                self.queue_list.takeItem(0)
                self.process_next_item()
                return

            self.output_window.append(f"({queue_position}) Unzipping {base_name}.zip...")

            self.unzip_runner = UnzipRunner(file_path, self.processing_dir)
            self.unzip_runner.progress_signal.connect(self.progress_bar.setValue)
            self.unzip_runner.finished_signal.connect(lambda files: self.handle_psn_files(files, base_name, output_dir, psn_rap_dir, queue_position))
            self.unzip_runner.start()

        self.downloadhelper(selected_iso, queue_position, url, after_download)

    def handle_psn_files(self, extracted_files, base_name, output_dir, psn_rap_dir, queue_position):
        operations = []
        self.output_window.append(f"({queue_position}) Processing files for {base_name}...")
        
        for file in extracted_files:
            self.output_window.append(f"({queue_position}) Processing file: {file}")
            if file.endswith('.pkg'):
                new_file_path = os.path.join(self.processing_dir, f"{base_name}.pkg")
                self.output_window.append(f"({queue_position}) Renaming {file} to {new_file_path}")
                os.rename(file, new_file_path)
                
                if self.split_pkg_checkbox.isChecked():
                    self.output_window.append(f"({queue_position}) Splitting PKG file: {new_file_path}")
                    self.split_pkg(new_file_path, queue_position)
                    
                    split_parts = glob.glob(f"{new_file_path}.666*")
                    self.output_window.append(f"({queue_position}) Found {len(split_parts)} split parts")
                    
                    if split_parts:
                        for split_file in split_parts:
                            dest = os.path.join(output_dir, os.path.basename(split_file))
                            self.output_window.append(f"({queue_position}) Adding move operation: {split_file} -> {dest}")
                            operations.append({'type': 'move', 'src': split_file, 'dst': dest})
                        
                        if not self.keep_unsplit_pkg_checkbox.isChecked():
                            self.output_window.append(f"({queue_position}) Adding remove operation for original PKG: {new_file_path}")
                            operations.append({'type': 'remove', 'src': new_file_path})
                        else:
                            dest = os.path.join(output_dir, os.path.basename(new_file_path))
                            self.output_window.append(f"({queue_position}) Keeping unsplit PKG. Moving: {new_file_path} -> {dest}")
                            operations.append({'type': 'move', 'src': new_file_path, 'dst': dest})
                    else:
                        # If no split parts are found, move the original PKG file
                        dest = os.path.join(output_dir, os.path.basename(new_file_path))
                        self.output_window.append(f"({queue_position}) No split parts found. Moving original PKG: {new_file_path} -> {dest}")
                        operations.append({'type': 'move', 'src': new_file_path, 'dst': dest})
                else:
                    dest = os.path.join(output_dir, os.path.basename(new_file_path))
                    self.output_window.append(f"({queue_position}) Adding move operation for unsplit PKG: {new_file_path} -> {dest}")
                    operations.append({'type': 'move', 'src': new_file_path, 'dst': dest})
            
            elif file.endswith('.rap'):
                dest = os.path.join(psn_rap_dir, os.path.basename(file))
                self.output_window.append(f"({queue_position}) Adding move operation for RAP file: {file} -> {dest}")
                operations.append({'type': 'move', 'src': file, 'dst': dest})

        zip_file = os.path.join(self.processing_dir, f"{base_name}.zip")
        if os.path.exists(zip_file):
            self.output_window.append(f"({queue_position}) Adding remove operation for ZIP file: {zip_file}")
            operations.append({'type': 'remove', 'src': zip_file})

        self.output_window.append(f"({queue_position}) Starting file operations...")
        self.file_operations_thread = FileOperationsThread(operations)
        self.file_operations_thread.progress_signal.connect(
            lambda msg: self.output_window.append(f"({queue_position}) {msg}")
        )
        self.file_operations_thread.finished_signal.connect(
            lambda: self.psn_file_operations_finished(queue_position, base_name)
        )
        self.file_operations_thread.start()

    def psn_file_operations_finished(self, queue_position, base_name):
        self.output_window.append(f"({queue_position}) All operations completed for {base_name}")
        self.queue_list.takeItem(0)
        self.process_next_item()

    def split_pkg(self, pkg_file, queue_position):
        self.output_window.append(f"({queue_position}) Splitting PKG file...")
        split_pkg_thread = SplitPkgThread(pkg_file)
        split_pkg_thread.progress.connect(lambda msg: self.output_window.append(msg))
        split_pkg_thread.start()
        split_pkg_thread.wait()  # Wait for the splitting to complete

    def download_and_process_multi_file(self, selected_iso, system, queue_position):
        url = system['url']
        base_name = os.path.splitext(selected_iso)[0]
        output_dir = system['output_dir']

        def after_download(file_path):
            if file_path == "DEBUG_MODE":
                self.queue_list.takeItem(0)
                self.process_next_item()
                return

            if not file_path.lower().endswith('.zip'):
                print(f"File {file_path} is not a .zip file. Skipping unzip.")
                self.queue_list.takeItem(0)
                self.process_next_item()
                return

            self.output_window.append(f"({queue_position}) Unzipping {base_name}.zip...")

            # Create a subdirectory in the processing directory
            extract_dir = os.path.join(self.processing_dir, base_name)

            self.unzip_runner = UnzipRunner(file_path, extract_dir)
            self.unzip_runner.progress_signal.connect(self.progress_bar.setValue)
            self.unzip_runner.finished_signal.connect(lambda files: self.handle_extracted_folder(extract_dir, base_name, output_dir, queue_position))
            self.unzip_runner.start()

        self.downloadhelper(selected_iso, queue_position, url, after_download)

    def handle_extracted_folder(self, extract_dir, base_name, output_dir, queue_position):
        operations = []

        # Destination folder
        dest_dir = os.path.join(output_dir, base_name)

        # Handle potential naming conflicts
        if os.path.exists(dest_dir):
            count = 1
            new_dest_dir = f"{dest_dir}_{count}"
            while os.path.exists(new_dest_dir):
                count += 1
                new_dest_dir = f"{dest_dir}_{count}"
            dest_dir = new_dest_dir

        operations.append({'type': 'move', 'src': extract_dir, 'dst': dest_dir})

        # Remove the zip file
        zip_file = os.path.join(self.processing_dir, f"{base_name}.zip")
        if os.path.exists(zip_file):
            operations.append({'type': 'remove', 'src': zip_file})

        self.file_operations_thread = FileOperationsThread(operations)
        self.file_operations_thread.progress_signal.connect(lambda msg: self.output_window.append(f"({queue_position}) {msg}"))
        self.file_operations_thread.finished_signal.connect(lambda: self.multi_file_operations_finished(queue_position, base_name))
        self.file_operations_thread.start()

    def multi_file_operations_finished(self, queue_position, base_name):
        self.output_window.append(f"({queue_position}) {base_name} ready!")
        self.queue_list.takeItem(0)
        self.process_next_item()

    def set_system_list(self, system_index, iso_list):
        self.systems_data[system_index]['list'] = iso_list
        table_widget = self.result_list.widget(system_index)
        table_widget.setRowCount(len(iso_list))
        for row, (file_name, file_size) in enumerate(iso_list):
            # Hide the file extension in the table
            display_name = os.path.splitext(file_name)[0]
            table_widget.setItem(row, 0, QTableWidgetItem(display_name))
            table_widget.setItem(row, 1, QTableWidgetItem(file_size))
            # Store the full filename as item data
            table_widget.item(row, 0).setData(Qt.UserRole, file_name)
        table_widget.resizeColumnsToContents()

    def update_results(self):
        search_term = self.search_box.text().lower().split()
        current_system_index = self.result_list.currentIndex()
        system = self.systems_data.get(current_system_index)

        if system:
            list_to_search = system['list']
        else:
            list_to_search = []

        filtered_list = [item for item in list_to_search if all(word in item[0].lower() for word in search_term)]

        # Clear the current table widget and add the filtered items
        current_table_widget = self.result_list.currentWidget()
        current_table_widget.setRowCount(len(filtered_list))
        for row, (file_name, file_size) in enumerate(filtered_list):
            # Hide the file extension in the table
            display_name = os.path.splitext(file_name)[0]
            current_table_widget.setItem(row, 0, QTableWidgetItem(display_name))
            current_table_widget.setItem(row, 1, QTableWidgetItem(file_size))
            # Store the full filename as item data
            current_table_widget.item(row, 0).setData(Qt.UserRole, file_name)
        current_table_widget.resizeColumnsToContents()

    def add_to_queue(self):
        selected_items = self.result_list.currentWidget().selectedItems()
        current_system_index = self.result_list.currentIndex()
        for item in selected_items:
            if item.column() == 0:  # Only add items from the first column (file names)
                # Get the full filename (including extension) from item data
                full_filename = item.data(Qt.UserRole)
                # Check for duplicates by comparing both full_filename and system_index
                duplicate = False
                for i in range(self.queue_list.count()):
                    queue_item = self.queue_list.item(i)
                    if queue_item.text() == full_filename and queue_item.data(Qt.UserRole) == current_system_index:
                        duplicate = True
                        break
                if not duplicate:
                    new_item = QListWidgetItem(full_filename)
                    new_item.setData(Qt.UserRole, current_system_index)
                    self.queue_list.addItem(new_item)

    def remove_from_queue(self):
        selected_items = self.queue_list.selectedItems()
        for item in selected_items:
            # Remove the item from the queue list
            self.queue_list.takeItem(self.queue_list.row(item))

        # Save the queue to 'queue.txt'
        with open('queue.txt', 'wb') as file:
            pickle.dump([(self.queue_list.item(i).text(), self.queue_list.item(i).data(Qt.UserRole)) for i in range(self.queue_list.count())], file)

    def update_add_to_queue_button(self):
        self.add_to_queue_button.setEnabled(bool(self.result_list.currentWidget().selectedItems()))

    def update_remove_from_queue_button(self):
        self.remove_from_queue_button.setEnabled(bool(self.queue_list.selectedItems()))

    def settings_welcome_dialog(self, title, close_button_text, add_iso_list_section=False, welcome_text=None):
        dialog = QDialog()
        dialog.setWindowTitle(title)
        vbox = QVBoxLayout(dialog)

        # Adds welcome text when provided
        if welcome_text is not None:
            welcome_label = QLabel(welcome_text)
            vbox.addWidget(welcome_label)

        def select_location(name, select_button, path_textbox, download_button=None):
            hbox = QHBoxLayout()
            hbox.addWidget(select_button)
            hbox.addWidget(path_textbox)
            if download_button is not None:
                hbox.addWidget(download_button)
            vbox.addLayout(hbox)

        # PS3Dec section
        ps3decSelectButton = QPushButton('Choose PS3Dec Binary')
        ps3decPathTextbox = QLineEdit(self.settings.value('ps3dec_binary', ''))
        ps3decSelectButton.clicked.connect(lambda: self.open_file_dialog(ps3decPathTextbox, 'ps3dec_binary'))
        ps3decDownloadButton = QPushButton('Download PS3Dec')
        if sys.platform == "win32":
            ps3decDownloadButton.clicked.connect(lambda: self.download_ps3dec(ps3decDownloadButton, ps3decPathTextbox))
        else:
            ps3decDownloadButton.setEnabled(False)
            ps3decDownloadButton.setToolTip('PS3Dec can only be retrieved on Windows')
        select_location("PS3Dec:", ps3decSelectButton, ps3decPathTextbox, ps3decDownloadButton)

        # Output directories for systems
        for system in self.systems_config:
            select_button = QPushButton(f"Choose {system['name']} Directory")
            path_textbox = QLineEdit(self.settings.value(system['output_dir_key'], f"MyrientDownloads/{system['name']}"))
            select_button.clicked.connect(lambda checked, textbox=path_textbox, key=system['output_dir_key']: self.open_directory_dialog(textbox, key))
            select_location(f"{system['name']} Directory:", select_button, path_textbox)

        # PSN RAP Directory (special case)
        psn_rap_SelectButton = QPushButton('Choose PSN RAP Directory')
        psn_rap_PathTextbox = QLineEdit(self.settings.value('psn_rap_dir', 'MyrientDownloads/exdata'))
        psn_rap_SelectButton.clicked.connect(lambda: self.open_directory_dialog(psn_rap_PathTextbox, 'psn_rap_dir'))
        select_location("PSN RAP Directory:", psn_rap_SelectButton, psn_rap_PathTextbox)

        # ISO List section
        if add_iso_list_section:
            iso_list_button = QPushButton('Update software lists')
            iso_list_button.clicked.connect(self.update_iso_list)
            vbox.addWidget(iso_list_button)

        # Close button
        closeButton = QPushButton(close_button_text)
        closeButton.clicked.connect(dialog.close)
        vbox.addWidget(closeButton)

        dialog.exec_()

    def open_file_dialog(self, textbox, setting_key):
        options = QFileDialog.Options()
        options |= QFileDialog.ReadOnly
        fileName, _ = QFileDialog.getOpenFileName(self, "Select File", "", "All Files (*);;Executable Files (*.exe)", options=options)
        if fileName:
            self.settings.setValue(setting_key, fileName)
            textbox.setText(fileName)  # Update the textbox with the new path

    def open_directory_dialog(self, textbox, setting_key):
        options = QFileDialog.Options()
        options |= QFileDialog.ReadOnly
        dirName = QFileDialog.getExistingDirectory(self, "Select Directory", options=options)
        if dirName:
            self.settings.setValue(setting_key, dirName)
            textbox.setText(dirName)  # Update the textbox with the new path

            # Update the directory path in the application
            for system in self.systems_config:
                if setting_key == system['output_dir_key']:
                    self.systems_data[system['index']]['output_dir'] = dirName

    def open_settings(self):
        self.settings_welcome_dialog("Tools", "Close", add_iso_list_section=True)

    def first_startup(self):
        welcome_text = "Welcome! The script can attempt to grab PS3Dec automatically or you can set it manually"
        self.settings_welcome_dialog("Welcome!", "Continue", welcome_text=welcome_text)

    def update_iso_list(self):
        self.load_software_lists(rebuild=True)

    def is_valid_binary(self, path, binary_name):
        # Check if the path is not empty, the file exists and the filename ends with the correct binary name
        if path and os.path.isfile(path):
            filename = os.path.basename(path)
            if sys.platform == "win32":
                # On Windows, check if the filename ends with .exe (case insensitive)
                return filename.lower() == f"{binary_name}.exe"
            else:
                # On other platforms, just check the filename (case insensitive)
                return filename.lower() == binary_name.lower()
        return False

    def download_ps3dec(self, ps3decButton, textbox):
        urllib.request.urlretrieve("https://github.com/Redrrx/ps3dec/releases/download/0.1.0/ps3dec.exe", "ps3dec.exe")
        self.ps3dec_binary = os.path.join(os.getcwd(), "ps3dec.exe")
        self.settings.setValue('ps3dec_binary', self.ps3dec_binary)

        # Update the button
        ps3decButton.setText('PS3Dec downloaded! ✅')
        ps3decButton.setEnabled(False)

        self.ps3dec_binary = './ps3dec' if sys.platform != "Windows" else './ps3dec.exe'
        self.settings.setValue('ps3dec_binary', self.ps3dec_binary)
        textbox.setText(self.ps3dec_binary)

if __name__ == '__main__':
    print("Starting application...")
    app = QApplication(sys.argv)
    try:
        ex = GUIDownloader()
        print("GUIDownloader instance created.")
        ex.show()
        print("Main window shown.")
        sys.exit(app.exec_())
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        sys.exit(1)
