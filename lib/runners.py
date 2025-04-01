import os
import platform
import subprocess
import threading
import zipfile

from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtWidgets import QApplication


class CommandRunner(QThread):
    finished = pyqtSignal()

    def __init__(self, command):
        super().__init__()
        self.command = command

    def run(self):
        process = subprocess.Popen(self.command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.PIPE, bufsize=1, universal_newlines=True)

        # If on Windows, send a newline character to ps3dec's standard input
        if platform.system() == 'Windows':
            process.stdin.write('\n')
            process.stdin.flush()

        def reader_thread(process):
            for line in iter(process.stdout.readline, ''):
                print(line.rstrip('\n'))
                QApplication.processEvents()

        thread = threading.Thread(target=reader_thread, args=(process,))
        thread.start()
        process.wait()
        thread.join()

        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, self.command)
        self.finished.emit()


class UnzipRunner(QThread):
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(list)

    def __init__(self, zip_path, output_path):
        super().__init__()
        self.zip_path = zip_path
        self.output_path = output_path
        self.extracted_files = []
        self.running = True

    def run(self):
        if not self.zip_path.lower().endswith('.zip'):
            print(f"File {self.zip_path} is not a .zip file. Skipping unzip.")
            self.finished_signal.emit([])
            return

        with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
            total_size = sum([info.file_size for info in zip_ref.infolist()])
            extracted_size = 0

            for info in zip_ref.infolist():
                if not self.running:
                    break
                # Extract to self.output_path
                zip_ref.extract(info, self.output_path)
                extracted_file_path = os.path.join(self.output_path, info.filename)
                self.extracted_files.append(extracted_file_path)
                extracted_size += info.file_size
                self.progress_signal.emit(int((extracted_size / total_size) * 100))
                QApplication.processEvents()

        self.finished_signal.emit(self.extracted_files)

    def stop(self):
        self.running = False
