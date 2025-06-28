import sys
import os
import logging
import re
import time
import heapq
import ast
import operator
import multiprocessing
from datetime import datetime
from collections import deque, defaultdict
from queue import Queue, Empty, Full
from threading import Thread, Lock
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import xml.etree.ElementTree as ET

from typing import List, Dict, Tuple, Optional

from PyQt5.QtWidgets import (
    QApplication, QPushButton, QLabel, QFileDialog,
    QMessageBox, QVBoxLayout, QWidget, QHBoxLayout, QTableWidget,
    QTableWidgetItem, QDialog, QDialogButtonBox, QFormLayout,
    QComboBox, QHeaderView, QAbstractItemView, QListWidget, QListWidgetItem,QTextEdit,
    QLineEdit
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer

from PyQt5.QtGui import QTextCharFormat,QTextCursor

from pymodbus.server.sync import ModbusTcpServer
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext, ModbusSequentialDataBlock
from functools import lru_cache

# Mengatur batas rekursi
sys.setrecursionlimit(2000)

# Optimasi Logging
import logging.handlers
from queue import Queue as LogQueue

# Konfigurasi logging dengan asynchronous logging dan RotatingFileHandler
log_queue = LogQueue(maxsize=10000)
queue_handler = logging.handlers.QueueHandler(log_queue)
logger = logging.getLogger()
logger.addHandler(queue_handler)
logger.setLevel(logging.WARNING)  # Set ke WARNING untuk produksi

log_formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

file_handler = logging.handlers.RotatingFileHandler(
    'modbus_server.log',
    maxBytes=10*1024*1024,  # 10 MB
    backupCount=5
)
file_handler.setFormatter(log_formatter)

# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(log_formatter)

listener = logging.handlers.QueueListener(
    log_queue,
    file_handler,
    # stream_handler,
    respect_handler_level=True
)
listener.start()

class Filter:
    """
    Kelas untuk menyimpan informasi filter dari tag <FILTER_CONF>.
    """
    def __init__(self, fil_addr: str, fil_cyc: int, res_addr: int):
        self.fil_addr = fil_addr
        self.fil_cyc = fil_cyc
        self.res_addr = res_addr

    def __lt__(self, other):
        # Bandingkan berdasarkan RES_ADDR (atau atribut lain yang relevan)
        return self.res_addr < other.res_addr
    
    def __repr__(self):
        return f"Filter(FIL_ADDR={self.fil_addr}, FIL_CYC={self.fil_cyc}, RES_ADDR={self.res_addr})"
    
    def __hash__(self):
        return hash((self.fil_addr, self.fil_cyc, self.res_addr))
    
    def __eq__(self, other):
        if not isinstance(other, Filter):
            return False
        return (self.fil_addr, self.fil_cyc, self.res_addr) == (other.fil_addr, other.fil_cyc, other.res_addr)

class FilterEvaluatorThread(QThread):
    update_bit_signal = pyqtSignal(int, int)
    log_signal = pyqtSignal(str)

    def __init__(self, filter_queue: Queue, delay_map: dict, parent=None):
        super().__init__(parent)
        self.queue = filter_queue
        self.delay_map = delay_map  # key: address, value: delay_ms
        self.running = True
        self.heap = []  # (target_time, addr, value)

    def run(self):
        while self.running:
            try:
                # Ambil item baru
                addr, delay_ms, val = self.queue.get(timeout=0.01)
                target_time = time.time() + (delay_ms / 1000.0)
                heapq.heappush(self.heap, (target_time, addr, val))
                self.log_signal.emit(f"[FILTER] Delay {delay_ms}ms untuk addr {addr}")
            except Empty:
                pass

            # Cek apakah ada item yang waktunya sudah lewat
            now = time.time()
            while self.heap and self.heap[0][0] <= now:
                _, addr, val = heapq.heappop(self.heap)
                self.update_bit_signal.emit(addr, val)
                self.log_signal.emit(f"[FILTER] Update addr {addr} => {val}")

            time.sleep(0.005)

    def stop(self):
        self.running = False
        self.quit()
        self.wait()

class DependencyEvaluator(QThread):
    log_signal = pyqtSignal(str)
    update_bit_signal = pyqtSignal(int, int)

    def __init__(self, queue: Queue, evaluate_func, max_workers=8, batch_size=300, parent=None):
        super().__init__(parent)
        self.queue = queue
        self.evaluate_func = evaluate_func
        self.batch_size = batch_size
        self.running = True
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="LogicEval")
        self.max_workers = max_workers

    def run(self):
        while self.running:
            batch = {}
            start_time = time.time()
            while len(batch) < self.batch_size and (time.time() - start_time) < 0.05:
                try:
                    addr, val = self.queue.get(timeout=0.01)
                    batch[addr] = val
                except Empty:
                    break

            if batch:
                first_addr = next(iter(batch))
                first_val = batch[first_addr]

                # Kirim tugas ke thread pool
                future = self.executor.submit(self.evaluate_func, first_addr, first_val)

                # Tangani hasilnya (jika ada string log yang dikembalikan)
                def handle_result(fut):
                    try:
                        result = fut.result()
                        if isinstance(result, tuple) and len(result) == 2:
                            log_text, changes = result
                            if isinstance(log_text, str):
                                for line in log_text.splitlines():
                                    self.log_signal.emit(line)
                                if hasattr(self, 'expr_perf_box'):
                                    lines = log_text.strip().splitlines()
                                    if lines:
                                        last_line = lines[-1]
                                        self.expr_perf_box.append(
                                            f"{len(changes)} bit dipicu, {last_line}"
                                        )
                            if isinstance(changes, list):
                                for addr, val in changes:
                                    self.update_bit_signal.emit(addr, val)
                    except Exception as e:
                        self.log_signal.emit(f"[ERROR] Evaluator task failed: {e}")
                future.add_done_callback(handle_result)
            else:
                time.sleep(0.01)

    def stop(self):
        self.running = False
        self.quit()
        self.wait()
        self.executor.shutdown(wait=True)
        self.log_signal.emit("[STOP] LogicEvaluatorThread stopped and executor shutdown.")

class BitChangeDialog(QDialog):
    """
    Dialog untuk mengubah nilai bit.
    """
    def __init__(self, current_value: int, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Ubah Nilai Bit")
        self.setModal(True)

        self.new_value = current_value

        # Layout
        layout = QVBoxLayout()
        self.setLayout(layout)

        # Form Layout
        form_layout = QFormLayout()
        layout.addLayout(form_layout)

        # Dropdown untuk memilih nilai bit
        self.value_combo = QComboBox()
        self.value_combo.addItems(["0", "1"])
        self.value_combo.setCurrentText(str(int(current_value)))
        form_layout.addRow("Nilai Bit:", self.value_combo)

        # Dialog Buttons
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_value(self) -> int:
        """
        Mengambil nilai bit yang dipilih.
        """
        return int(self.value_combo.currentText())

class ObservableDataBlock(ModbusSequentialDataBlock):
    """
    Data block Modbus yang mengamati perubahan nilai dan memanggil callback.
    """
    def __init__(self, address: int, values: list, callback=None):
        super().__init__(address, values)
        self.callback = callback
        logging.debug("ObservableDataBlock initialized.")

    def setValues(self, address: int, values: list):
        super().setValues(address, values)
        if self.callback:
            for offset, value in enumerate(values):
                new_address = address + offset
                if isinstance(value, bool):
                    value = int(value)
                self.callback(new_address, value)

class ModbusServerThread(QThread):
    """
    Thread untuk menjalankan Modbus TCP Server.
    """
    coil_changed = pyqtSignal(int, int)

    def __init__(self, datastore: ModbusServerContext, identity: ModbusDeviceIdentification,
                 var_to_expressions: dict, addr_to_var: dict,
                 port: int = 502, coil_update_callback=None):
        super().__init__()
        self.datastore = datastore
        self.identity = identity
        self.var_to_expressions = var_to_expressions
        self.addr_to_var = addr_to_var
        self.port = port
        self.coil_update_callback = coil_update_callback
        self.server = None
        logging.debug("ModbusServerThread initialized.")

    def run(self):
        """
        Menjalankan Modbus TCP Server.
        """
        try:
            for slave_id in self.datastore.slaves():
                slave = self.datastore[slave_id]
                original_values = slave.getValues(1, 0, count=self.get_total_coils())
                observable_block = ObservableDataBlock(0, original_values, callback=self.coil_update_callback)
                slave.store['co'] = observable_block  # Perbarui data block 'co'

            self.server = ModbusTcpServer(
                context=self.datastore,
                identity=self.identity,
                address=("0.0.0.0", self.port),
            )
            logging.info(f"Modbus server dimulai pada port {self.port}.")
            print(f"Modbus server dimulai pada port {self.port}.")
            self.server.serve_forever()
        except OSError as e:
            if e.errno == 98:  # Address already in use
                logging.error(f"Port {self.port} sudah digunakan.")
                QMessageBox.critical(None, "Error", f"Port {self.port} sudah digunakan.")
            else:
                logging.error(f"Gagal memulai Modbus server: {e}")
                QMessageBox.critical(None, "Error", f"Gagal memulai Modbus server:\n{e}")
        except Exception as e:
            logging.error(f"Gagal memulai Modbus server: {e}")
            QMessageBox.critical(None, "Error", f"Gagal memulai Modbus server:\n{e}")

    def get_total_coils(self) -> int:
        """
        Menghitung jumlah total coil berdasarkan addr_to_var.
        """
        if not self.addr_to_var:
            logging.error("Data variabel kosong. Tidak dapat menghitung jumlah coil.")
            return 0
        return max(self.addr_to_var.keys()) + 1  # Pastikan addr_to_var memiliki integer keys

    def stop_server(self):
        """
        Menghentikan Modbus server.
        """
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            logging.info("Modbus server dihentikan.")
        self.quit()
        self.wait()
        logging.debug("ModbusServerThread stopped.")

class EquationViewDialog(QDialog):
    """
    Dialog untuk menampilkan logika variabel.
    """
    def __init__(self, equation: str, variable_values: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Equation Viewer")
        self.setModal(True)

        # Layout
        layout = QVBoxLayout(self)
        self.setLayout(layout)

        # Label untuk menampilkan logika
        equation_label = QLabel("Equation:")
        layout.addWidget(equation_label)

        # Text area untuk menampilkan logika dengan border warna
        self.equation_text = QTextEdit()
        self.equation_text.setReadOnly(True)
        self.equation_text.setText(equation)
        layout.addWidget(self.equation_text)

        # Highlighting berdasarkan nilai variabel
        self.highlight_variables(equation, variable_values)

    def highlight_variables(self, equation: str, variable_values: dict):
        """
        Highlight variabel dalam logika berdasarkan nilai mereka.
        Merah untuk nilai 0, hijau untuk nilai 1.
        Pewarnaan dijalankan di main thread dengan QTimer.singleShot.
        """
        pattern = r'\b[A-Za-z0-9_.-]+\b'
        matches = list(re.finditer(pattern, equation))  # Simpan semua match terlebih dahulu

        def apply_highlight():
            cursor = self.equation_text.textCursor()
            fmt_red = QTextCharFormat()
            fmt_red.setBackground(Qt.red)
            fmt_green = QTextCharFormat()
            fmt_green.setBackground(Qt.green)

            for match in matches:
                var = match.group(0)
                start = match.start()
                end = match.end()

                cursor.setPosition(start)
                cursor.setPosition(end, QTextCursor.KeepAnchor)

                if variable_values.get(var, 0) == 1:
                    cursor.setCharFormat(fmt_green)
                else:
                    cursor.setCharFormat(fmt_red)

        QTimer.singleShot(0, apply_highlight)

class ShowLogWindow(QDialog):
    """
    Jendela dialog non-modal yang menampilkan isi dari log_box secara real-time.
    """
    def __init__(self, source_log_box: QTextEdit, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Live Log Viewer")
        self.setMinimumSize(600, 300)
        self.setModal(False)  # <== penting: agar tidak memblokir MainWindow

        self.source_log_box = source_log_box

        layout = QVBoxLayout()
        self.setLayout(layout)

        self.viewer = QTextEdit()
        self.viewer.setReadOnly(True)
        layout.addWidget(self.viewer)

        self.timer = QTimer()
        self.timer.timeout.connect(self.update_log)
        self.timer.start(300)  # Update setiap 300 ms

        button_layout = QHBoxLayout()
        self.clear_button = QPushButton("Clear Log")
        self.clear_button.clicked.connect(self.source_log_box.clear)
        self.close_button = QPushButton("Close")
        self.close_button.clicked.connect(self.close)
        button_layout.addWidget(self.clear_button)
        button_layout.addWidget(self.close_button)
        layout.addLayout(button_layout)


    def update_log(self):
        new_text = self.source_log_box.toPlainText()
        if new_text != getattr(self, "_last_text", ""):
            self.viewer.setPlainText(new_text)
            self._last_text = new_text
            QTimer.singleShot(0, lambda: self.apply_log_update(new_text))
    
    def apply_log_update(self, text):
        self.viewer.setPlainText(text)
        self.viewer.moveCursor(QTextCursor.End)



class MainWindow (QWidget):
    """
    Aplikasi utama untuk browsing file, mengelola variabel, dan menjalankan Modbus TCP Server.
    """
    bit_changed = pyqtSignal(int, int)  # (alamat bit, nilai bit baru)
    start_filter_timer = pyqtSignal(object)  # Mengirim objek Filter
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Boolex")
        self.setFixedSize(720, 480)
        self.setStyleSheet("background-color: #eeeeee;")
        
        self.lock = Lock()

        # Variabel untuk Menyimpan Data
        self.selected_file_path: str = ""
        self.var_to_val: Dict[int, int] = {}      # {alamat bit: bit_value}
        self.var_to_val_name: Dict[str, int] = {}  # Tambahkan ini
        self.var_to_addr: Dict[str, List[int]] = defaultdict(list)  # {nama_variabel: [bit_address1, bit_address2, ...]}
        self.addr_to_var: Dict[int, str] = {}     # {bit_address: nama_variabel}
        self.displayed_variables: set = set()     # Set untuk menghindari duplikasi

        # Struktur untuk Menyimpan Ekspresi
        self.expressions: list = []

        # Mapping dari variabel ke ekspresi yang menggunakannya
        self.var_to_expressions: defaultdict = defaultdict(list)

        # Daftar Filter
        self.filters: List[Filter] = []

        # Priority queue untuk filter timers
        self.filter_heap: List[Tuple[float, Filter]] = []

        # Setup DataStore
        self.datastore: Optional[ModbusServerContext] = None

        #latch
        self.latched_outputs  = {}
        self.last_trigger_source = {}
        self.latch_enabled = True

        # Hubungkan sinyal
        self.previous_values = {}
        self.bit_changed.connect(self.update_table_coil)
        self.active_filters = {}  # Tambahkan ini di inisialisasi class
        logging.warning("Sinyal start_filter_timer dihubungkan ke handle_filter_timer")

        # Setup Identifikasi Server
        self.identity = ModbusDeviceIdentification()
        self.identity.VendorName = 'Python Modbus Server'
        self.identity.ProductCode = 'PM'
        self.identity.VendorUrl = 'http://github.com/riptideio/pymodbus/'
        self.identity.ProductName = 'PyModbus Server'
        self.identity.ModelName = 'PyModbus Server'
        self.identity.MajorMinorRevision = '1.0'

        # Server Thread
        self.server_thread: Optional[ModbusServerThread] = None
        self.server_active = False

        # Deteksi CPU dan sesuaikan konfigurasi evaluator secara otomatis
        cpu_cores = multiprocessing.cpu_count()

        if cpu_cores <= 2:
            max_workers = 4
            batch_size = 200
            self.max_iter = 10
            queue_max = 10000
        elif cpu_cores <= 4:
            max_workers = 6
            batch_size = 300
            self.max_iter = 12
            queue_max = 15000
        elif cpu_cores <= 8:
            max_workers = 12
            batch_size = 800
            self.max_iter = 15
            queue_max = 20000
        else:
            max_workers = 24
            batch_size = 1200
            self.max_iter = 20
            queue_max = 50000

        # Inisialisasi evaluator dengan parameter hasil deteksi CPU
        self.evaluation_queue = Queue(maxsize=queue_max)
        self.dependency_evaluator = DependencyEvaluator(
            queue=self.evaluation_queue,
            evaluate_func=self.evaluate_dependencies,
            max_workers=max_workers,
            batch_size=batch_size
        )
        self.dependency_evaluator.log_signal.connect(self.append_log)
        self.dependency_evaluator.update_bit_signal.connect(self.bit_changed.emit)
        self.dependency_evaluator.start()

        #filter
        self.filter_queue = Queue(maxsize=1000)
        self.filter_delays = {}  # key: address, value: delay in ms
        self.filter_evaluator = FilterEvaluatorThread(
            filter_queue=self.filter_queue,
            delay_map=self.filter_delays
        )
        self.filter_evaluator.update_bit_signal.connect(self.bit_changed.emit)
        self.filter_evaluator.log_signal.connect(self.append_log)
        self.filter_evaluator.start()

        # Initialize Expression Cache
        self.expression_cache: Dict[str, int] = {}

        #log window
        self.log_window = None  # Untuk menyimpan window log

        # Precompile regex pattern untuk mengganti variabel
        self.precompiled_regex = self.compile_replace_variables_regex()
        
        self.monitor_timer = QTimer()
        self.monitor_timer.timeout.connect(self.monitor_queue_load)
        self.monitor_timer.start(1000) 

        logging.debug("MainWindow initialized with expression_cache.")

        self.ini_labels = {}
        self.bit_data = {}
        self.addr_to_var = {}
        self.var_to_addr = {}
        self.equations = []
        self.previous_values_toggle = {}

        self.init_ui()

    def init_ui(self):
        blue_font = "color: blue; font-weight: bold;"
        left_panel = QVBoxLayout()
        version_label = QLabel("Version 0")
        version_label.setStyleSheet("color: green;")
        left_panel.addWidget(version_label)

        button_layout = QHBoxLayout()
        self.load_button = QPushButton("Load Dictionary")
        self.load_button.clicked.connect(self.browse_file)
        self.start_button = QPushButton("START")
        self.start_button.clicked.connect(self.start_server)
        self.start_button.setEnabled(False)
        self.reset_button = QPushButton("RESET")
        self.reset_button.clicked.connect(self.reset)
        button_layout.addWidget(self.load_button)
        button_layout.addWidget(self.start_button)
        button_layout.addWidget(self.reset_button)
        left_panel.addLayout(button_layout)

        self.info_fields = [
            "SYSTEM", "LOCATION", "NUMOFSYSTEM", "VERSION",
            "CHASIS_ID", "ELIX_IP1", "ELIX_IP2"
        ]
        for field in self.info_fields:
            label = QLabel(field.replace("_", " ") + ":")
            value = QLabel("-")
            value.setStyleSheet(blue_font)
            self.ini_labels[field] = value
            h_layout = QHBoxLayout()
            h_layout.addWidget(label)
            h_layout.addWidget(value)
            h_layout.addStretch()
            left_panel.addLayout(h_layout)

        left_panel.addWidget(QLabel("CONNECTION LIST"))
        self.conn_list = QTextEdit()
        self.conn_list.setReadOnly(True)
        self.conn_list.setFixedHeight(60)  # pendekkan koneksi
        left_panel.addWidget(self.conn_list)

        self.log_box = QTextEdit()
        self.log_box.setFixedHeight(160)
        self.log_box.setReadOnly(True)
        left_panel.addWidget(self.log_box)

        self.view_log_btn = QPushButton("VIEW LOG")
        self.view_log_btn.clicked.connect(self.show_log_window)
        left_panel.addWidget(self.view_log_btn)

        right_panel = QVBoxLayout()
        self.input_line = QLineEdit()
        self.input_line.setPlaceholderText("Cari variabel...")
        self.input_line.textChanged.connect(self.perform_search)
        self.input_line.textChanged.connect(self.update_search_list_position)
        self.input_line.returnPressed.connect(self.handle_search_enter_pressed)
        right_panel.addWidget(self.input_line)

        self.search_result_list = QListWidget(self)
        self.search_result_list.setMaximumHeight(150)
        self.search_result_list.setStyleSheet("background-color: white; border: 1px solid gray;")
        self.search_result_list.hide()
        self.search_result_list.setFocusPolicy(Qt.NoFocus)
        self.search_result_list.itemClicked.connect(self.handle_variable_selected)
        self.search_result_list.itemDoubleClicked.connect(self.handle_variable_double_clicked)
        self.search_result_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.selected_search_item = None
        
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Variable", "Addres", "Bit"])
        self.table.setColumnWidth(0, 175)  # Kolom 'Variable'
        self.table.setColumnWidth(1, 60)  # Kolom 'Addres'
        self.table.setColumnWidth(2, 30)   # Kolom 'Bit'       
        self.table.horizontalHeader().setSectionsMovable(False)      # Tidak bisa dipindah
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)  # Tidak bisa diubah ukurannya
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.horizontalHeader().setStyleSheet("background :grey; color :Black")
        self.table.setStyleSheet("background-color: black; color:White;")
        self.table.cellDoubleClicked.connect(self.handle_cell_double_click)
        self.table.horizontalHeader().hide()
        self.table.verticalHeader().setVisible(False)  # Hilangkan nomor baris di kiri
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)  # Klik sel = pilih seluruh baris
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)  # Hilangkan scrollbar horizontal
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.cellClicked.connect(self.ensure_row_selected)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)

        right_panel.addWidget(self.table)
        
        self.expr_perf_box = QTextEdit(self)
        self.expr_perf_box.setReadOnly(True)
        self.expr_perf_box.setGeometry(800, 10, 300, 100)
        self.expr_perf_box.setStyleSheet("background-color: #f0f0f0; font-size: 10pt;")
        
        button_row1 = QHBoxLayout()
        self.add_btn = QPushButton("ADD")
        self.add_btn.clicked.connect(self.handle_add_variable)
        self.rem_btn = QPushButton("REMOVE")
        button_row1.addWidget(self.add_btn)
        self.rem_btn.clicked.connect(self.remove_selected_variable)
        button_row1.addWidget(self.rem_btn)
        right_panel.addLayout(button_row1)
        
        button_row2 = QHBoxLayout()
        self.rem_all_btn = QPushButton("REM ALL")
        self.rem_all_btn.clicked.connect(self.remove_all_variables)
        self.view_equ_btn = QPushButton("VIEW EQU")
        self.view_equ_btn.clicked.connect(self.view_equation)
        button_row2.addWidget(self.rem_all_btn)
        button_row2.addWidget(self.view_equ_btn)
        right_panel.addLayout(button_row2)
        
        self.add_btn.setEnabled(False)
        self.rem_btn.setEnabled(False)
        self.rem_all_btn.setEnabled(False)
        self.view_equ_btn.setEnabled(False)

        layout = QHBoxLayout()
        layout.addLayout(left_panel, 3)
        layout.addLayout(right_panel, 2)
        self.setLayout(layout)
        
        self.add_btn.setEnabled(False)
        self.rem_btn.setEnabled(False)
        self.rem_all_btn.setEnabled(False)
        self.view_equ_btn.setEnabled(False)

        self.input_line.textChanged.connect(self.update_search_list_position)
        self.input_line.returnPressed.connect(self.handle_search_enter_pressed)
    
    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open .dat file", "", "DAT Files (*.dat)")
        if file_path:
            try:
                with open(file_path, 'r') as f:
                    lines = f.readlines()
                self.bit_data.clear()
                self.addr_to_var.clear()
                self.var_to_addr.clear()
                self.equations.clear()
                self.remove_all_variables()
                self.selected_file_path = file_path


                for line in lines:
                    line = line.strip()
                    if not line or ',' not in line:
                        continue
                    parts = line.split(',')
                    if len(parts) < 2:
                        continue
                    var = parts[0].strip().rstrip('.')
                    addr_str = parts[1].strip()
                    if not addr_str.isdigit():
                        continue
                    addr = int(addr_str)
                    self.bit_data[addr] = 0
                    self.addr_to_var[addr] = var
                    self.var_to_addr[var] = addr

                dat_filename = os.path.basename(file_path)
                dat_base = os.path.splitext(dat_filename)[0].replace('dictionary_bit-all', 'config-all')
                ini_filename = f"{dat_base}.ini"
                ini_path = os.path.join(os.path.dirname(file_path), ini_filename)
                for fname in os.listdir(os.path.dirname(file_path)):
                    if fname.endswith(".ini") and fname.split('-')[-1].split('.')[0] == dat_base:
                        ini_path = os.path.join(os.path.dirname(file_path), fname)
                        break

                if ini_path and os.path.exists(ini_path):
                    try:
                        tree = ET.parse(ini_path)
                        root = tree.getroot()
                        general = root.find("GENERAL")
                        if general is not None:
                            for field in self.info_fields:
                                element = general.find(field)
                                if element is not None:
                                    self.ini_labels[field].setText(element.text)

                        for exp in root.findall(".//EXP"):
                            eq = exp.findtext("EQUATION", default="").strip()
                            res_addr = exp.findtext("EXP_RES_ADDR", default="0").strip()
                            if eq and res_addr.isdigit():
                                self.equations.append({"equation": eq, "result_addr": int(res_addr)})

                        self.log_box.append(f"Loaded config from {ini_path}")
                    except Exception as e:
                        self.log_box.append(f"Failed to parse .ini file: {str(e)}")
                else:
                    self.log_box.append(f"INI file with matching code '{dat_base}' not found.")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to load file:\n{str(e)}")
        """
        Membaca dan memparsing file .dat serta file .ini yang terkait.
        """
        if not self.selected_file_path:
            QMessageBox.warning(self, "Peringatan", "Tidak ada file yang dipilih.")
            return

        try:
            # Membaca isi file .dat
            with open(self.selected_file_path, 'r') as file_dat:
                data_dat = file_dat.readlines()

            # Mengurai data .dat
            self.var_to_val = {}
            self.var_to_addr = defaultdict(list)
            self.addr_to_var = {}
            for line in data_dat:
                line = line.strip()
                if not line:
                    continue  # Lewati baris kosong
                if ',' not in line:
                    logging.warning(f"Format baris tidak valid: {line}")
                    continue
                parts = line.split(',')
                if len(parts) < 2:
                    logging.warning(f"Format baris tidak valid: {line}")
                    continue
                var = parts[0].strip().rstrip('.')  # Hapus trailing dots
                addr = parts[1].strip()
                if not addr.isdigit():
                    logging.warning(f"Alamat bit tidak valid pada baris: {line}")
                    continue

                # Pastikan nama variabel valid
                if not self.is_valid_variable(var):
                    logging.warning(f"Nama variabel tidak valid: {var}")
                    continue

                # Nilai bit default adalah False (0)
                val = 0
                addr_int = int(addr)
                self.var_to_val[addr_int] = val
                self.var_to_addr[var].append(addr_int)
                self.addr_to_var[addr_int] = var

            self.var_to_val_name = {}
            for var, addrs in self.var_to_addr.items():
                if addrs:
                    # Ambil nilai dari alamat pertama jika ada
                    addr = addrs[0]
                    self.var_to_val_name[var] = self.var_to_val.get(addr, 0)
                else:
                    self.var_to_val_name[var] = 0  # Default nilai jika tidak ada alamat
            
            logging.info(f"Peta variabel ke alamat bit: {dict(self.var_to_addr)}")
            logging.debug(f"Mapping variabel ke alamat bit: {dict(self.var_to_addr)}")
            for var, addrs in self.var_to_addr.items():
                for addr in addrs:
                    logging.debug(f"Variabel: {var}, Alamat Bit: {addr}")

            # Validasi bahwa ada variabel yang berhasil dibaca
            if not self.var_to_val:
                QMessageBox.warning(self, "Peringatan", "Tidak ada variabel yang berhasil dibaca dari file .dat.")
                return

            # Menentukan path file .ini yang terkait
            ini_file_path = self.get_corresponding_ini_file(self.selected_file_path)

            if not os.path.exists(ini_file_path):
                QMessageBox.warning(self, "Peringatan", f"File .ini terkait tidak ditemukan:\n{ini_file_path}")
                logging.warning(f"File .ini terkait tidak ditemukan: {ini_file_path}")
            else:
                # Membaca dan memparsing isi file .ini
                self.parse_ini_file(ini_file_path)
                self.precompile_expressions()  # Precompile expressions
                
                self.bit_to_expressions = {}
                for idx, exp in enumerate(self.expressions):
                    used_vars = self.extract_variables_from_equation(exp["equation"])
                    exp["used_vars"] = set(used_vars)
                    for var in used_vars:
                        self.bit_to_expressions.setdefault(var, set()).add(idx)
            
            # Setup DataStore setelah memuat variabel
            self.setup_datastore()

            # Precompile regex pattern setelah variabel dimuat
            self.precompiled_regex = self.compile_replace_variables_regex()

            # Precompile expressions
            self.precompile_expressions()

            # Validasi setelah setup datastore
            if not self.expressions:
                logging.warning("Tidak ada ekspresi yang berhasil diparsing dari file .ini.")
            if not self.var_to_expressions:
                logging.warning("Peta variabel ke ekspresi kosong.")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Gagal membaca file:\n{e}")
            logging.error(f"Gagal membaca file: {e}")

        
        self.add_btn.setEnabled(True)
        self.rem_btn.setEnabled(True)
        self.rem_all_btn.setEnabled(True)
        self.view_equ_btn.setEnabled(True)
        self.start_button.setEnabled(True)

        self.start_latch_system()
        
    def tokenize_equation(self, equation_str: str) -> list:
        """
        Memecah string ekspresi menjadi daftar token.
        Operator: ( ), +, *, ~
        Variabel: [A-Za-z0-9._-]+
        """
        token_pattern = re.compile(r'(\(|\)|\+|\*|~)|([A-Za-z0-9._-]+)')
        tokens = []
        for match in token_pattern.finditer(equation_str):
            op, var = match.groups()
            if op:
                # ini adalah token operator/parenthesis: +, *, ~, (, )
                tokens.append(op)
            else:
                # ini adalah token variabel, misal BDW1.W15-RWZ
                tokens.append(var)
        return tokens

    def tokens_to_python_expr(self, tokens: list) -> str:
        """
        Mengonversi token ke bentuk ekspresi Python yang valid:
          + -> or
          * -> and
          ~ -> not
          '(' dan ')' tetap sama
        Variabel -> variables["..."]
        """
        python_expr_parts = []
        for t in tokens:
            if t == '+':
                python_expr_parts.append(' or ')
            elif t == '*':
                python_expr_parts.append(' and ')
            elif t == '~':
                python_expr_parts.append(' not ')
            elif t in ('(', ')'):
                python_expr_parts.append(t)
            else:
                # Anggap t sebagai nama variabel
                # Periksa dulu apakah t ada di var_to_addr (opsional)
                if t not in self.var_to_addr:
                    # Beri warning, karena variabel ini tidak ada di .dat
                    logging.warning(f"Variabel '{t}' tidak ditemukan dalam file .dat.")
                # Bungkus jadi variables["..."]
                python_expr_parts.append(f'variables["{t}"]')

        return "".join(python_expr_parts)

    def parse_ini_file(self, ini_file_path: str):
        """
        Memparsing file .ini (XML) untuk ekstraksi ekspresi logika (EXP) dan filter (FILTER).
        Menggunakan pendekatan tokenizing agar nama variabel dengan '.' atau '-' tidak memicu syntax error.
        """
        try:
            logging.info(f"Memulai parsing file .ini: {ini_file_path}")
            tree = ET.parse(ini_file_path)
            root = tree.getroot()
            if root is None:
                logging.error("Tag <EXP_configuration> tidak ditemukan dalam file.")
                QMessageBox.warning(self, "Error", "Tag <EXP_configuration> tidak ditemukan dalam file.")
                return

            # Iterasi melalui GEN_EXP_CONF
            for gen_exp_conf in root.findall('.//GEN_EXP_CONF'):
                logging.debug(f"GEN_EXP_CONF ditemukan: {ET.tostring(gen_exp_conf, encoding='unicode')}")
                for exp in gen_exp_conf.findall('EXP'):
                    equation_elem = exp.find('EQUATION')
                    res_addr_elem = exp.find('EXP_RES_ADDR')

                    if equation_elem is None or res_addr_elem is None:
                        logging.warning(f"EXP tanpa EQUATION atau EXP_RES_ADDR: {ET.tostring(exp, encoding='unicode')}")
                        continue

                    equation_str = (equation_elem.text or "").strip()
                    res_addr_str = (res_addr_elem.text or "").strip()

                    if not equation_str:
                        logging.warning("EQUATION kosong dalam ekspresi.")
                        continue

                    if not res_addr_str.isdigit():
                        logging.warning(f"EXP_RES_ADDR tidak valid: {res_addr_str}")
                        continue

                    # Lakukan tokenizing
                    tokens = self.tokenize_equation(equation_str)

                    # Ubah token menjadi ekspresi Python
                    transformed_expr = self.tokens_to_python_expr(tokens)

                    # Validasi ekspresi (pastikan tidak syntax error)
                    if not self.validate_expression(transformed_expr):
                        logging.warning(f"Ekspresi tidak valid setelah transformasi: {transformed_expr}")
                        continue

                    # Buat dictionary untuk menyimpan informasi ekspresi
                    exp_dict = {
                        'equation': transformed_expr,
                        'result_addr': int(res_addr_str)
                    }
                    self.expressions.append(exp_dict)
                    logging.debug(f"Ekspresi ditambahkan: {exp_dict}")

                    # Deteksi variabel dalam tokens
                    # variabel -> token yang bukan operator/kurung
                    vars_in_equation = [
                        t for t in tokens
                        if t not in ('+', '*', '~', '(', ')')
                    ]
                    logging.debug(f"Variabel dalam ekspresi '{equation_str}': {vars_in_equation}")

                    # Tambahkan peta variabel -> ekspresi
                    for var in vars_in_equation:
                        # Jika var ada di var_to_addr, daftarkan
                        if var in self.var_to_addr:
                            self.var_to_expressions[var].append(exp_dict)
                            logging.debug(f"Ekspresi {exp_dict} ditambahkan ke variabel {var}.")
                        else:
                            # Sudah di-warning di tokens_to_python_expr, tapi boleh diulang di sini.
                            logging.warning(f"Variabel '{var}' tidak ditemukan di .dat (saat menautkan).")

            # Iterasi melalui FILTER_CONF (logika sama seperti sebelumnya)
            for filter_conf in root.findall('.//FILTER_CONF'):
                logging.debug(f"FILTER_CONF ditemukan: {ET.tostring(filter_conf, encoding='unicode')}")
                for filt in filter_conf.findall('FILTER'):
                    fil_addr_elem = filt.find('FIL_ADDR')
                    fil_cyc_elem = filt.find('FIL_CYC')
                    res_addr_elem = filt.find('RES_ADDR')

                    if fil_addr_elem is None or fil_cyc_elem is None or res_addr_elem is None:
                        logging.warning(f"FILTER tanpa FIL_ADDR, FIL_CYC, atau RES_ADDR: {ET.tostring(filt, encoding='unicode')}")
                        continue

                    fil_addr = (fil_addr_elem.text or "").strip()
                    fil_cyc = (fil_cyc_elem.text or "").strip()
                    res_addr = (res_addr_elem.text or "").strip()

                    if not fil_addr or not fil_cyc or not res_addr:
                        logging.warning(f"FILTER memiliki nilai kosong: {ET.tostring(filt, encoding='unicode')}")
                        continue

                    if not fil_cyc.isdigit() or not res_addr.isdigit():
                        logging.warning(f"FIL_CYC atau RES_ADDR tidak valid: {ET.tostring(filt, encoding='unicode')}")
                        continue

                    # Create Filter instance
                    filter_instance = Filter(
                        fil_addr=fil_addr,
                        fil_cyc=int(fil_cyc),
                        res_addr=int(res_addr)
                    )
                    self.filters.append(filter_instance)
                    logging.warning(f"Filter ditambahkan: {filter_instance}")

            logging.info(f"Total ekspresi diparsing: {len(self.expressions)}")
            logging.info(f"Total filter diparsing: {len(self.filters)}")
            logging.debug(f"Peta variabel ke ekspresi: {dict(self.var_to_expressions)}")
            logging.debug(f"Daftar filter: {self.filters}")

        except ET.ParseError as e:
            logging.error(f"Kesalahan parsing XML: {e}")
            QMessageBox.critical(self, "Error", f"Kesalahan parsing file .ini:\n{e}")
        except Exception as e:
            logging.error(f"Gagal memparsing file .ini: {e}")
            QMessageBox.critical(self, "Error", f"Gagal memparsing file .ini:\n{e}")
    
    def apply_global_toggle(self):
        """
        Sistem toggle global otomatis:
        - Jika suatu bit bernilai 1 lalu berubah menjadi 0, maka dianggap sebagai tombol.
        - Bit dengan nama {nama}_TOGGLED akan di-flip (toggle).
        - Tidak perlu konfigurasi di .ini atau .dat, hanya berdasarkan nama variabel yang sudah dibaca.
        """
        for var_name, current_val in self.var_to_val_name.items():
            prev_val = self.previous_values_toggle.get(var_name, current_val)

            if prev_val == 0 and current_val == 1:
                toggled_var = f"{var_name}_TOGGLED"
                output_addrs = self.var_to_addr.get(toggled_var, [])

                for out_addr in output_addrs:
                    prev_output = self.var_to_val.get(out_addr, 0)
                    new_output = 0 if prev_output == 1 else 1

                    self.var_to_val[out_addr] = new_output
                    self.var_to_val_name[toggled_var] = new_output
                    self.evaluation_queue.put((out_addr, new_output))
                    self.bit_changed.emit(out_addr, new_output)
                    self.log_bit_change(out_addr, new_output, f"Toggled by {var_name}")

            self.previous_values_toggle[var_name] = current_val

    def replace_operators(self, equation: str) -> str:
        equation = equation.replace('+', ' or ')
        equation = equation.replace('*', ' and ')
        equation = re.sub(r'~\s*', ' not ', equation)
        return equation
    
    def sanitize_variable_name(self, var: str) -> str:
        return re.sub(r'[^A-Za-z0-9_.-]', '', var)
    
    def validate_expression(self, expression: str) -> bool:
        """
        Mengecek apakah expression adalah sintaks Python yang valid.
        (Setelah diubah menjadi variables["..."] dsb.)
        """
        try:
            ast.parse(expression, mode='eval')
            return True
        except SyntaxError as e:
            logging.error(f"Syntax error dalam ekspresi '{expression}': {e}")
            return False

    def reset(self):
        """
        Menghentikan Modbus TCP Server, evaluator, dan reset semua bit ke nilai awal.
        """

        logging.info("Memulai proses RESET.")

        # --- 1. Hentikan DependencyEvaluator ---
        if hasattr(self, "dependency_evaluator") and self.dependency_evaluator.isRunning():
            logging.info("Menghentikan DependencyEvaluator...")
            self.dependency_evaluator.stop()
            self.dependency_evaluator.quit()  # ✅ minta thread berhenti
            self.dependency_evaluator.wait()
            logging.info("DependencyEvaluator berhasil dihentikan.")

        # --- 2. Hentikan Modbus Server Thread ---
        if self.server_thread:
            logging.info("Menghentikan Modbus Server...")
            self.server_thread.stop_server()
            self.server_thread = None
            self.conn_list.setText("Modbus server dihentikan.")
            self.start_button.setEnabled(True)
            self.reset_button.setEnabled(False)
            logging.info("Modbus server thread dihentikan.")

        # --- 3. Hentikan dan bersihkan filter timer ---
        if hasattr(self, 'central_timer') and self.central_timer.isActive():
            self.central_timer.stop()
            logging.info("Central timer untuk filter management dihentikan.")

        self.filter_heap.clear()
        self.active_filters.clear()

        # --- 4. Reset semua bit ke 0 ---
        for addr in self.var_to_val:
            self.var_to_val[addr] = 0
            var = self.addr_to_var.get(addr)
            if var:
                self.var_to_val_name[var] = 0
                self.previous_values_toggle[var] = 0
            self.previous_values[addr] = 0
            self.update_datastore_coil(addr, 0)
            self.bit_changed.emit(addr, 0)

        # --- 6. Inisialisasi ulang Queue & DependencyEvaluator ---
        self.evaluation_queue = Queue(maxsize=15000)
        self.dependency_evaluator = DependencyEvaluator(
            queue=self.evaluation_queue,
            evaluate_func=self.evaluate_dependencies,
            max_workers=8,
            batch_size=300
        )
        self.dependency_evaluator.start()
        logging.info("DependencyEvaluator baru dijalankan kembali.")

        logging.info("RESET selesai.")


    def precompile_expressions(self):
        logging.info("Memulai precompiling ekspresi logika.")
        self.compiled_expressions = {}
        for exp in self.expressions:
            final_expr = exp['equation']  # Sudah "variables["..."]"
            try:
                compiled_func = self.compile_expression(final_expr)
                self.compiled_expressions[final_expr] = compiled_func
                logging.debug(f"Ekspresi '{final_expr}' telah di-precompile.")
            except Exception as e:
                logging.error(f"Gagal precompile ekspresi '{final_expr}': {e}")
        logging.info("Precompiling ekspresi logika selesai.")

    @staticmethod
    def compile_expression(self, expr: str):
        """
        Mengubah string ekspresi menjadi fungsi yang dapat dieksekusi.
        """
        try:
            expr_ast = ast.parse(expr, mode='eval')
        except SyntaxError as e:
            logging.error(f"Syntax error dalam ekspresi '{expr}': {e}")
            raise e

        operators_map = {
            ast.And: operator.and_,
            ast.Or: operator.or_,
            ast.Not: operator.not_,
            ast.BitAnd: operator.and_,
            ast.BitOr: operator.or_,
            ast.BitXor: operator.xor,
            # Tambahkan operator lain sesuai kebutuhan
        }

        def eval_node(node, variable_values):
            logging.warning(f"eval_node memproses node: {node!r} (type={type(node)})")
            if isinstance(node, ast.BoolOp):
                # Dapatkan operator (contoh: and, or)
                op = operators_map.get(type(node.op))
                if not op:
                    raise ValueError(f"Unsupported boolean operator: {type(node.op)}")
        
                # Proses nilai-nilai BoolOp secara rekursif untuk setiap pasangan
                result = eval_node(node.values[0], variable_values)
                for value in node.values[1:]:
                    result = op(result, eval_node(value, variable_values))
                return result
            elif isinstance(node, ast.UnaryOp):
                # Contoh: not X
                op = operators_map.get(type(node.op))
                if not op:
                    raise ValueError(f"Unsupported unary operator: {type(node.op)}")
                return op(eval_node(node.operand, variable_values))

            elif isinstance(node, ast.Expr):
                # Terkadang tanda kurung ( ... ) terbungkus jadi node Expr
                # node.value biasanya berisi BoolOp, Subscript, dsb.
                return eval_node(node.value, variable_values)

            elif isinstance(node, ast.BinOp):
                # Kalau ada penggunaan &, |, ^, +, -, dsb. (bitwise/matematis)
                op = operators_map.get(type(node.op))
                if not op:
                    raise ValueError(f"Unsupported binary operator: {type(node.op)}")
                left_val = eval_node(node.left, variable_values)
                right_val = eval_node(node.right, variable_values)
                return op(left_val, right_val)

            elif isinstance(node, ast.Subscript):
                # Pastikan node.value adalah 'variables'
                if isinstance(node.value, ast.Name) and node.value.id == 'variables':
                    # Pada Python 3.12, slice biasanya ast.Constant(value='...')
                    slice_node = node.slice
                    if isinstance(slice_node, ast.Constant):
                        # Langsung ambil .value
                        var_name = slice_node.value  # Ini tipe str, misal "BDW2.J42B-CFEK-VI"
                        return bool(variable_values.get(var_name, False))
                    else:
                        raise ValueError(f"Unsupported subscript slice type: {type(slice_node)}")
                else:
                    raise ValueError("Unsupported subscript in expression")

            elif isinstance(node, ast.Constant):
                # Nilai literal True/False/angka
                return bool(node.value)

            elif isinstance(node, ast.Name):
                # Jika ada node semisal 'True', 'False', atau 'None'? 
                # Atau nama variabel tanpa 'variables["..."]' (jarang jika Anda selalu bungkus).
                # Bila perlu, tangani di sini:
                val = variable_values.get(node.id, False)
                return bool(val)

            else:
                raise ValueError(f"Unsupported expression: {expr}")
        logging.warning(f"Mau memanggil compiled_func untuk {expr['equation']}")
        result = compiled_func(self.var_to_val_name)
        logging.warning(f"Hasil = {result}")
        def compiled_func(variable_values):
            return int(eval_node(expr_ast.body, variable_values))

        return compiled_func

    def is_valid_variable(self, var: str) -> bool:
        """
        Memeriksa apakah nama variabel valid.
        """
        return re.match(r'^[A-Za-z_][A-Za-z0-9_.-]*$', var) is not None

    def extract_variables_from_equation(self, equation: str) -> list:
        """
        Mengekstrak nama-nama variabel dari EQUATION.
        """
        logging.debug(f"Ekstraksi variabel dari ekspresi: {equation}")
        pattern = r'\b[A-Za-z0-9_-]+\b'
        vars_found = re.findall(pattern, equation)
        logging.debug(f"Variabel ditemukan (sebelum filter): {vars_found}")

        operators = {'and', 'or', 'not'}
        vars_cleaned = [var.rstrip('.') for var in vars_found if var.lower() not in operators]
        logging.debug(f"Variabel setelah filter dan strip: {vars_cleaned}")
        return vars_cleaned

    def get_corresponding_ini_file(self, dat_file_path: str) -> str:
        """
        Mengganti prefix 'dictionary_bit' dengan 'config' dan mengubah ekstensi ke .ini
        Contoh:
            dictionary_bit_Rev.5.0.0.dat -> config_Rev.5.0.0.ini
            dictionary_bitxxx.dat -> configxxx.ini
        """
        directory, filename = os.path.split(dat_file_path)
        basename, _ = os.path.splitext(filename)

        if basename.startswith("dictionary_bit"):
            ini_basename = basename.replace("dictionary_bit", "config", 1)
        else:
            ini_basename = f"config_{basename}"  # Contoh alternatif

        ini_filename = ini_basename + ".ini"
        ini_file_path = os.path.join(directory, ini_filename)
        return ini_file_path

    def perform_search(self):
        search_text = self.input_line.text().strip().lower()
        self.search_result_list.clear()
        self.update_search_list_position()


        if not search_text:
            self.search_result_list.hide()
            return

        matched = [
            (var, addr)
            for var, addr_list in self.var_to_addr.items()
            for addr in addr_list
            if search_text in var.lower()
        ]

        if matched:
            for var, addr in matched:
                item = QListWidgetItem(f"{var} (Alamat: {addr})")
                item.setData(Qt.UserRole, (var, addr))
                self.search_result_list.addItem(item)
            self.search_result_list.show()
            self.update_search_list_position()
        else:
            self.search_result_list.hide()

    def handle_variable_selected(self,item):
        self.selected_var_data = item.data(Qt.UserRole)
        
    def handle_variable_double_clicked(self, item):
        """
        Menambahkan variabel langsung ke tabel jika pengguna melakukan double-click pada hasil pencarian.
        """
        var, addr = item.data(Qt.UserRole)
        if (var, addr) not in self.displayed_variables:
            val = self.var_to_val.get(addr, 0)
            self.add_variable_to_table(var, addr, val)
            self.displayed_variables.add((var, addr))

        # Bersihkan pilihan dan tampilan pencarian
        self.search_result_list.clear()
        self.search_result_list.hide()
        self.input_line.clear()

        self.rem_all_btn.setEnabled(True)
        self.rem_btn.setEnabled(True)

    def handle_add_variable(self):
        selected_items = self.search_result_list.selectedItems()

        if selected_items:
            for item in selected_items:
                var, addr = item.data(Qt.UserRole)
                if (var, addr) not in self.displayed_variables:
                    val = self.var_to_val.get(addr, 0)
                    self.add_variable_to_table(var, addr, val)
                    self.displayed_variables.add((var, addr))
            # Bersihkan pilihan
            self.search_result_list.clear()
            self.search_result_list.hide()
            self.input_line.clear()
            self.selected_var_data = None
            self.rem_all_btn.setEnabled(True)
            self.rem_btn.setEnabled(True)
            return

        # Jika tidak ada yang dipilih → tambahkan semua hasil
        for i in range(self.search_result_list.count()):
            item = self.search_result_list.item(i)
            var, addr = item.data(Qt.UserRole)
            if (var, addr) not in self.displayed_variables:
                val = self.var_to_val.get(addr, 0)
                self.add_variable_to_table(var, addr, val)
                self.displayed_variables.add((var, addr))

        # Reset
        self.selected_var_data = None
        self.search_result_list.clear()
        self.search_result_list.hide()
        self.input_line.clear()
        self.rem_all_btn.setEnabled(True)
        self.rem_btn.setEnabled(True)
        
    def handle_search_enter_pressed(self):
        selected_items = self.search_result_list.selectedItems()
        if not selected_items and self.search_result_list.count() > 0:
            # Jika tidak ada yang dipilih, pilih item pertama
            item = self.search_result_list.item(0)
            self.search_result_list.setCurrentItem(item)
            selected_items = [item]

        for item in selected_items:
            self.handle_variable_double_clicked(item)

    def show_log_window(self):
        """
        Menampilkan window log real-time tanpa memblokir MainWindow.
        """
        if self.log_window is None or not self.log_window.isVisible():
            self.log_window = ShowLogWindow(self.log_box, self)
            self.log_window.show()
        else:
            self.log_window.raise_()
            self.log_window.activateWindow()


    def add_variable_to_table(self, var: str, addr: int, val: int):
        """
        Menambahkan variabel ke tabel GUI.
        """
        self.table.horizontalHeader().show()
        row_position = self.table.rowCount()
        self.table.insertRow(row_position)

        # Variabel
        var_item = QTableWidgetItem(var)
        var_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
        self.table.setItem(row_position, 0, var_item)

        # Alamat Bit
        addr_item = QTableWidgetItem(str(addr))
        addr_item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
        self.table.setItem(row_position, 1, addr_item)

        # Nilai Bit (Clickable Item)
        val_item = QTableWidgetItem(str(val))
        val_item.setTextAlignment(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
        self.table.setItem(row_position, 2, val_item)
        
        val_item.setBackground(Qt.green if val == 1 else Qt.red)


    def update_search_list_position(self):
        if self.search_result_list.isHidden():
            return
        # Dapatkan posisi global input_line
        global_pos = self.input_line.mapToGlobal(self.input_line.rect().bottomLeft())
        local_pos = self.mapFromGlobal(global_pos)

        self.search_result_list.setGeometry(
            local_pos.x(),
            local_pos.y(),
            self.input_line.width(),
            150  # tinggi maksimal
        )
        self.search_result_list.raise_()

    def view_equation(self):
        """
        Menampilkan logika variabel yang dipilih.
        """
        selected_ranges = self.table.selectedRanges()
        if not selected_ranges:
            QMessageBox.warning(self, "Warning", "Pilih variabel terlebih dahulu.")
            return
        # Ambil baris pertama dari seleksi
        selected_rows = sorted(set(index.row() for index in self.table.selectedIndexes()))
        if not selected_rows:
            QMessageBox.warning(self, "Warning", "Pilih variabel terlebih dahulu.")
            return

        for row in selected_rows:
            addr_item = self.table.item(row, 1)
            if not addr_item:
                continue
            try:
                addr = int(addr_item.text())
            except ValueError:
                continue

            expressions = [exp for exp in self.expressions if exp['result_addr'] == addr]
            if expressions:
                for exp in expressions:
                    equation = exp['equation']
                    self.log_box.append(f"[Equation] {equation}")
        QTimer.singleShot(0, lambda: self.log_box.moveCursor(QTextCursor.End))    
        if not addr_item:
            QMessageBox.warning(self, "Warning", "Alamat variabel tidak ditemukan.")
            return

        addr = int(addr_item.text())
        var = self.get_variable_by_address(addr)

        # Cari ekspresi yang terkait dengan alamat ini
        expressions = [
            exp for exp in self.expressions if exp['result_addr'] == addr
        ]
        if expressions:
            equation = expressions[0]['equation']
            self.log_box.append(f"[Equation] {equation}")
            self.log_box.moveCursor(QTextCursor.End)

        if not expressions:
            QMessageBox.information(self, "Info", "Tidak ada logika terkait untuk variabel ini.")
            return

    def update_view_equation_button(self):
        """
        Mengaktifkan atau menonaktifkan tombol 'View Equation' berdasarkan seleksi tabel.
        """
        selected_items = self.table.selectedItems()
        if selected_items:
            self.view_equ_btn.setEnabled(True)
        else:
            self.view_equ_btn.setEnabled(False)

    def remove_selected_variable(self):
        """
        Menghapus variabel yang dipilih (satu atau lebih) dari tabel.
        """
        selected_rows = sorted(set(index.row() for index in self.table.selectedIndexes()), reverse=True)
        if not selected_rows:
            QMessageBox.warning(self, "Warning", "Pilih variabel yang ingin dihapus.")
            return

        for row in selected_rows:
            var_item = self.table.item(row, 0)
            addr_item = self.table.item(row, 1)
            if var_item and addr_item:
                var = var_item.text()
                try:
                    addr = int(addr_item.text())
                    self.displayed_variables.discard((var, addr))
                except ValueError:
                    continue
            self.table.removeRow(row)

        self.rem_btn.setEnabled(self.table.rowCount() > 0)
        self.rem_all_btn.setEnabled(self.table.rowCount() > 0)

    def filter_table(self):
        keyword = self.input_line.text().lower()
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            self.table.setRowHidden(row, keyword not in item.text().lower())

    def remove_all_variables(self):
        """
        Menghapus semua variabel dari tabel.
        """
        self.table.setRowCount(0)
        self.displayed_variables.clear()

    def handle_cell_double_click(self, row: int, column: int):
        """
       Menangani event double click pada sel tabel untuk langsung toggle nilai bit.
        """
        if column == 2:  # Kolom "Nilai Bit"
            var_item = self.table.item(row, 0)
            addr_item = self.table.item(row, 1)
            val_item = self.table.item(row, 2)
            if var_item and addr_item and val_item:
                var = var_item.text()
                addr = int(addr_item.text())
                current_val = int(val_item.text())

                # Toggle nilai
                new_val = 0 if current_val == 1 else 1

                # Update di tabel
                val_item.setText(str(new_val))
                val_item.setBackground(Qt.green if new_val == 1 else Qt.red)

                # Update nilai di struktur data
                self.var_to_val[addr] = new_val
                self.var_to_val_name[var] = new_val
                self.previous_values[addr] = new_val
                self.previous_values_toggle[var] = new_val

                # Update ke Modbus datastore
                self.update_datastore_coil(addr, new_val)

                # Masukkan ke antrian evaluasi
                try:
                    self.evaluation_queue.put_nowait((addr, new_val))
                    logging.debug(f"[DoubleClick] {var} (addr {addr}) toggled to {new_val}")
                except Full:
                    logging.error("Evaluation queue penuh. Tidak dapat memasukkan task baru.")

                # Evaluasi langsung juga (jika diinginkan)
                self.evaluation_queue.put((addr, new_val))

                # Jalankan sistem toggle global (jika aktif)
                self.apply_global_toggle()

                # Emit sinyal agar update tampilan di tempat lain
                self.bit_changed.emit(addr, new_val)

    def is_latch_candidate(self, affected_var, equation):
        # Tidak boleh di-latch jika hasil langsung dari input seperti -VI, -RD, dsb
        if any(kw in affected_var for kw in ['-TP','-VI', '-RD', '-TN', '-OOC']):
            return False

        # Di-latch hanya jika ekspresi mengandung trigger sesaat
        trigger_keywords = ['-SET', '-REL', '-LS', '-TP']
        return any(kw in equation for kw in trigger_keywords)

    def evaluate_dependencies(self, changed_addr: int, new_value: int):
        from datetime import datetime
        start_time = time.perf_counter()
        log_output = []
        changed_bits = []

        var = self.get_variable_by_address(changed_addr)
        if not var:
            return "", []

        with self.lock:
            self.var_to_val[changed_addr] = new_value
            self.var_to_val_name[var] = new_value

        to_process_vars = {var}
        processed_expressions = set()
        max_iter = self.max_iter
        iteration = 0

        while to_process_vars and iteration < max_iter:
            next_vars = set()
            iteration += 1

            for idx, exp in enumerate(self.expressions):  # urut sesuai config
                if idx in processed_expressions:
                    continue

                if not (exp["used_vars"] & to_process_vars):
                    continue

                try:
                    compiled_func = self.compiled_expressions.get(exp["equation"])
                    if not compiled_func:
                        continue

                    result = compiled_func(self.var_to_val_name)
                    res_addr = exp["result_addr"]
                    res_var = self.get_variable_by_address(res_addr)
                    prev_val = self.var_to_val.get(res_addr, 0)

                    if result != prev_val:
                        with self.lock:
                            self.var_to_val[res_addr] = result
                            if res_var:
                                self.var_to_val_name[res_var] = result

                        self.update_datastore_coil(res_addr, result)
                        self.handle_filters(res_addr, result)

                        changed_bits.append((res_addr, result))
                        if res_var:
                            next_vars.add(res_var)

                        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                        log_output.append(f"[{timestamp}] Bit '{res_var}' (addr {res_addr}) => {result} | EQ: {exp['equation']}")
                        logging.info(log_output[-1])

                    processed_expressions.add(idx)

                except Exception as e:
                    logging.error(f"[ERROR] Eval idx={idx}, EQ='{exp.get('equation', '')}': {e}")

            to_process_vars = next_vars

        total_time = time.perf_counter() - start_time
        log_output.append(f"[INFO] Evaluasi selesai: {len(processed_expressions)} ekspresi dalam {total_time:.4f} detik")
        return "\n".join(log_output), changed_bits

    @staticmethod
    def extract_variables_from_equation(equation: str) -> List[str]:
        """Ekstrak nama variabel dari ekspresi logika."""
        import re
        return re.findall(r'[A-Za-z0-9_.-]+', equation)
    
    def start_latch_system(self) :
        self.latch_enabled = True

    def handle_filters(self, changed_addr: int, new_value: int):
        var = self.get_variable_by_address(changed_addr)
        if not var:
            logging.warning(f"Tidak ada variabel terkait dengan address: {changed_addr}")
            return

        matched_filters = [f for f in self.filters if f.fil_addr == var]
        if not matched_filters:
            logging.info(f"Tidak ada filter untuk variabel {var}")
            return

        for filt in matched_filters:
            res_addr = self.addr_by_variable.get(filt.res_addr)
            delay_ms = int(filt.fil_cyc / 50 * 1000) if new_value == 1 else 0

            if delay_ms > 0:
                self.filter_queue.put((res_addr, delay_ms, new_value))
                logging.info(f"[FILTER] Delay {delay_ms}ms untuk {filt.fil_addr} → {filt.res_addr}")
            else:
                self.bit_changed.emit(res_addr, new_value)
                logging.info(f"[FILTER] Tanpa delay → langsung emit {filt.res_addr}")

    def start_filter_timer_with_duration(self, filt: Filter, duration: float):
        """
        Memulai atau menambahkan timer untuk filter dengan durasi tertentu.
        """
        target_time = time.time() + duration
        heapq.heappush(self.filter_heap, (target_time, filt))
        logging.warning(f"Filter {filt} dijadwalkan dengan durasi {duration:.2f} detik.")

    def process_filters(self):
        """
        Dipanggil oleh central_timer untuk memproses filter yang waktunya telah tiba.
        """
        current_time = time.time()
        while self.filter_heap and self.filter_heap[0][0] <= current_time:
            _, filt = heapq.heappop(self.filter_heap)
            self.handle_filter_timeout(filt)

    def handle_filter_timeout(self, filt: Filter):
        """
        Mengupdate nilai bit pada RES_ADDR sesuai dengan nilai bit di FIL_ADDR.
        """
        start_time = time.perf_counter()
        fil_addr_var = filt.fil_addr
        res_addr = filt.res_addr

        # Ambil semua alamat yang terkait dengan FIL_ADDR
        fil_addrs = self.var_to_addr.get(fil_addr_var, [])
        if not fil_addrs:
            logging.warning(f"FIL_ADDR '{fil_addr_var}' tidak ditemukan dalam var_to_addr.")
            # Remove from active_filters
            with self.lock:
                self.active_filters.pop(filt, None)
            return

        # Ambil nilai dari alamat pertama
        fil_addr = fil_addrs[0]
        fil_value = self.var_to_val.get(fil_addr, 0)
        logging.debug(f"FIL_ADDR {fil_addr_var} ({fil_addr}) memiliki nilai {fil_value}")

        # Update RES_ADDR dengan nilai FIL_ADDR
        with self.lock:
            self.var_to_val[res_addr] = fil_value
        logging.info(f"Filter Update: RES_ADDR {res_addr} diupdate menjadi nilai dari FIL_ADDR {fil_addr_var} ({fil_value}).")

        # Update datastore coil
        self.update_datastore_coil(res_addr, fil_value)
        logging.info(f"Datastore coil untuk RES_ADDR {res_addr} diupdate dengan nilai {fil_value}.")

        # Evaluasi dependencies untuk RES_ADDR
        self.evaluation_queue.put((res_addr, fil_value))
        logging.info(f"Dependencies dievaluasi untuk RES_ADDR {res_addr} dengan nilai {fil_value}.")

        # Emit signal untuk memperbarui tabel secara real-time
        self.bit_changed.emit(res_addr, fil_value)

        # Remove filter dari active_filters
        with self.lock:
            self.active_filters.pop(filt, None)
        logging.info(f"Filter {filt} telah diselesaikan dan dihapus dari active_filters.")

        end_time = time.perf_counter()
        duration = end_time - start_time
        logging.info(f"handle_filter_timeout untuk Filter {filt} memakan waktu {duration:.6f} detik.")


    def precompile_expression(self, expr: str):
        """
        Precompile a single expression.
        """
        try:
            compiled_func = self.compile_expression(expr)
            self.compiled_expressions[expr] = compiled_func
            logging.debug(f"Ekspresi '{expr}' telah di-precompile.")
        except Exception as e:
            logging.error(f"Gagal precompile ekspresi '{expr}': {e}")

    def compile_replace_variables_regex(self):
        """
        Mengompilasi pola regex untuk mengganti semua variabel.
        Urutkan variabel berdasarkan panjang untuk menghindari konflik penggantian.
        Gunakan word boundaries untuk memastikan pencocokan utuh.
        """
        var_names = sorted(set(self.addr_to_var.values()), key=lambda x: -len(x))
        escaped_var_names = [re.escape(var) for var in var_names]
        pattern = r'\b(' + '|'.join(escaped_var_names) + r')\b'
        compiled_regex = re.compile(pattern)
        logging.debug(f"Compiled regex pattern: {pattern}")
        return compiled_regex

    def replace_operators(self, equation: str) -> str:
        """
        Mengganti operator khusus dalam ekspresi dengan operator Python yang sesuai.
        """
        # Gantikan '+' dengan ' or '
        equation = equation.replace('+', ' or ')

        # Gantikan '*' dengan ' and '
        equation = equation.replace('*', ' and ')

        # Gantikan '~' dengan ' not '
        equation = re.sub(r'~\s*', ' not ', equation)

        return equation

    def replace_variables(self, equation: str) -> str:
        """
        Mengganti nama variabel dalam ekspresi dengan akses dictionary.
        Skip jika sudah berbentuk variables["..."].
        """
        logging.debug(f"Original equation before variable replacement: {equation}")

        # Tambahkan proteksi: jika sebuah token sudah mengandung 'variables["'
        # maka jangan diwrap ulang.
        def repl(match):
            var = match.group(0)
            # Cek apakah 'var' sudah berbentuk variables["..."]?
            # Kalau TIDAK, baru bungkus
            if var.startswith('variables["') and var.endswith('"]'):
                return var  # Skip wrapping
            else:
                return f'variables["{var}"]'

        replaced_equation = self.precompiled_regex.sub(repl, equation)
        logging.debug(f"Equation after variable replacement: {replaced_equation}")
        return replaced_equation


    def evaluate_logic_expression(self, expr: str) -> int:
        """
        Mengevaluasi ekspresi logika yang telah diganti variabelnya.
        """
        if expr in self.expression_cache:
            logging.debug(f"Cache hit untuk ekspresi: {expr}")
            return self.expression_cache[expr]

        try:
            # Evaluasi ekspresi menggunakan precompiled function
            compiled_func = self.compiled_expressions.get(expr)
            if not compiled_func:
                logging.error(f"Ekspresi tidak terkompilasi: {expr}")
                return 0
            # Gunakan var_to_val_name sebagai variable_values
            result = compiled_func(self.var_to_val_name)
            # Cache hasil
            self.expression_cache[expr] = result
            return result
        except Exception as e:
            logging.error(f"Gagal mengevaluasi logic expression '{expr}': {e}")
            return 0

    def append_log(self, msg):
        self.log_box.append(msg)
        self.log_box.moveCursor(QTextCursor.End)   

    def get_variable_by_address(self, addr: int) -> Optional[str]:
        """
        Mengambil nama variabel berdasarkan alamat bit.
        """
        return self.addr_to_var.get(addr)

    def setup_datastore(self):
        """
        Inisialisasi datastore dengan jumlah coil yang dinamis
        berdasarkan data dari file .dat.
        """
        if not self.var_to_val:
            logging.error("Data variabel kosong. Pastikan file .dat telah dibaca.")
            QMessageBox.warning(self, "Error", "Data variabel kosong. Pastikan file .dat telah dibaca.")
            return

        max_address = max(self.addr_to_var.keys()) + 1
        size = max_address

        # Buat daftar nilai coil awal
        coils = [False] * size
        for addr, var in self.addr_to_var.items():
            val = self.var_to_val.get(addr, 0)
            coils[addr] = bool(val)
            # Inisialisasi var_to_val_name
            if var not in self.var_to_val_name:
                self.var_to_val_name[var] = val

        # Simpan nilai sebelumnya untuk pengecekan perubahan
        self.previous_values = {addr: int(bool(val)) for addr, val in self.var_to_val.items()}

        # Inisialisasi ModbusSequentialDataBlock
        custom_coils = ObservableDataBlock(0, coils, callback=self.coil_update_callback)

        # Buat ModbusSlaveContext dan ModbusServerContext
        slave_context = ModbusSlaveContext(
            di=None,
            co=custom_coils,
            hr=None,
            ir=None
        )
        self.datastore = ModbusServerContext(slaves={1: slave_context}, single=False)

        logging.debug(f"DataStore initialized with {len(coils)} coils.")
        # print(f"DataStore initialized dengan {len(coils)} coils.")

    
    @lru_cache(maxsize=10000)  # Cache hingga 1000 kombinasi variabel dan nilai
    def evaluate_expression_cached(expression, variable_values):
        try:
            # Evaluasi ekspresi menggunakan dictionary nilai variabel
            return eval(expression, {"variables": variable_values})
        except Exception as e:
            logging.error(f"Gagal mengevaluasi ekspresi '{expression}': {e}")
            return 0
    
    def start_server(self):
        """
        Memulai Modbus TCP Server.
        """
        if not self.server_thread:
            # Pastikan datastore telah diinisialisasi
            if not self.datastore:
                QMessageBox.warning(self, "Peringatan", "Datastore belum diinisialisasi.")
                logging.warning("Datastore belum diinisialisasi.")
                return

            # Periksa apakah logika dependencies sudah ada
            if not self.expressions:
                QMessageBox.warning(self, "Peringatan", "Ekspresi logika belum tersedia.")
                logging.warning("Ekspresi logika belum tersedia.")
                return
            if not self.var_to_expressions or not self.addr_to_var:
                QMessageBox.warning(self, "Peringatan", "Logika dari file .ini belum tersedia.")
                logging.warning("Logika dari file .ini belum tersedia.")
                return

            # Inisialisasi ModbusServerThread dengan logika dari MainWindow
            self.server_thread = ModbusServerThread(
                datastore=self.datastore,
                identity=self.identity,
                var_to_expressions=self.var_to_expressions,
                addr_to_var=self.addr_to_var,
                port=502,
                coil_update_callback=self.coil_update_callback
            )
            # Hubungkan sinyal coil_changed ke metode update_table_coil
            self.server_thread.coil_changed.connect(self.update_table_coil)
            self.server_thread.start()
            
            import socket
            try:
                hostname = socket.gethostname()
                local_ip = socket.gethostbyname(hostname)
            except:
                local_ip = "0.0.0.0"

            self.conn_list.setHtml(f"""
                <span style="color:red; font-weight:bold;">
                    Modbus server aktif<br>
                    IP: {local_ip}<br>
                </span>
            """)
            
            self.start_button.setEnabled(False)
            self.reset_button.setEnabled(True)
            logging.warning("Modbus server thread dimulai.")

            # Central timer untuk filter management
            self.central_timer = QTimer(self)
            self.central_timer.setInterval(10)  # 10 ms
            self.central_timer.timeout.connect(self.process_filters)
            self.central_timer.start()
        # Aktifkan BITONE setelah server siap
        if 'BITONE' in self.var_to_addr:
            for addr in self.var_to_addr['BITONE']:
                self.var_to_val[addr] = 1
                self.previous_values[addr] = 1
                self.bit_data[addr] = 1
                self.update_datastore_coil(addr, 1)
                self.evaluation_queue.put((addr, 1))
                self.bit_changed.emit(addr, 1)
            self.var_to_val_name['BITONE'] = 1

            logging.warning("Central timer untuk filter management dimulai.")
            if self.server_active:
                return  # sudah jalan
            self.server_thread.start()
            self.server_active = True

    def set_delivery_bit(self, state: int):
        delivery_name = "DELIVERY"
        if delivery_name in self.var_to_addr:
            for delivery_addr in self.var_to_addr[delivery_name]:
                self.update_datastore_coil(delivery_addr, state)

    def coil_update_callback(self, address: int, value: int):
        """
        Callback yang dipanggil ketika nilai coil diubah.
        """
        self.set_delivery_bit(1)
        start_time = time.perf_counter()

        # Konversi nilai ke integer jika berupa boolean
        if isinstance(value, bool):
            logging.debug(f"Nilai bit di alamat {address} adalah boolean {value}, mengonversi ke integer.")
            value = int(value)
        else:
            logging.debug(f"Nilai bit di alamat {address} adalah integer {value}.")

        logging.info(f"Callback dipanggil untuk alamat {address} dengan nilai {value}")
        if self.previous_values.get(address) == value:
            logging.debug(f"Tidak ada perubahan pada alamat {address}, nilai tetap {value}")
            return

        # Perbarui nilai sebelumnya
        self.previous_values[address] = value
        var = self.get_variable_by_address(address)
        if var:
            logging.info(f"Bit {var} di alamat {address} diubah menjadi {value}")
            try:
                self.evaluation_queue.put_nowait((address, value))
                logging.debug(f"Task (changed_addr: {address}, new_value: {value}) dimasukkan ke antrian.")
            except Full:
                logging.error("Evaluation queue penuh. Tidak dapat memasukkan task baru.")

            # Update var_to_val_name
            with self.lock:
                self.var_to_val_name[var] = value

            # Emit signal untuk memperbarui tabel secara real-time
            self.bit_changed.emit(address, value)

        # Tambahkan logging untuk memeriksa isi queue
        logging.debug(f"Current evaluation_queue size: {self.evaluation_queue.qsize()}")

        end_time = time.perf_counter()
        duration = end_time - start_time
        logging.info(f"coil_update_callback untuk address {address} memakan waktu {duration:.6f} detik.")
        self.apply_global_toggle()


    def update_datastore_coil(self, addr: int, value: int):
        """
        Perbarui nilai coil di datastore pada alamat tertentu.
        """
        try:
            if not self.datastore:
                logging.warning("Datastore belum diinisialisasi.")
                return

            slave = self.datastore[1]  # Slave ID 1

            # Sesuaikan alamat ke zero-based
            zero_based_addr = addr - 1  # Jika addr adalah one-based
            logging.debug(f"Memanggil setValues untuk alamat {zero_based_addr} dengan nilai {value}")
            slave.setValues(1, zero_based_addr, [int(value)])  # Pastikan nilai adalah integer
            logging.info(f"Datastore diperbarui: Coil pada {addr} diubah menjadi {value}")

            # Verifikasi nilai yang di-set
            current_values = slave.getValues(1, zero_based_addr, count=1)
            if current_values:
                current_value = current_values[0]
                logging.info(f"Verifikasi: Coil pada {addr} sekarang bernilai {current_value}")
            else:
                logging.warning(f"Verifikasi: Coil pada {addr} tidak dapat diambil nilainya.")

        except Exception as e:
            logging.error(f"Gagal memperbarui datastore: {e}")
            QMessageBox.critical(self, "Error", f"Gagal memperbarui datastore: {e}")

    def update_table_coil(self, addr: int, new_val: int):
        """
        Memperbarui nilai bit di tabel berdasarkan alamat yang diubah.
        """
        var = self.get_variable_by_address(addr)
        if not var:
            logging.warning(f"Tidak ada variabel yang terkait dengan alamat {addr}")
            return
        # Cari baris di tabel yang sesuai
        for row in range(self.table.rowCount()):
            addr_item = self.table.item(row, 1)  # Kolom "Alamat Bit"
            if addr_item and int(addr_item.text()) == addr:
                val_item = self.table.item(row, 2)  # Kolom "Nilai Bit"
                if val_item:
                    val_item.setText(str(new_val))
                    # Update nilai di var_to_val
                    with self.lock:
                        self.var_to_val[addr] = new_val
                    logging.info(f"Coil pada alamat {addr} diupdate menjadi {new_val} dari Modbus client.")
                break
            
    def monitor_queue_load(self):
        try:
            qsize = self.evaluation_queue.qsize()
            limit = self.evaluation_queue.maxsize

            if qsize > 0.8 * limit:
                self.dependency_evaluator.batch_size = min(1500, self.dependency_evaluator.batch_size + 100)
                logging.info(f"[AUTO-SCALE] Batch size dinaikkan menjadi {self.dependency_evaluator.batch_size}")
            elif qsize < 0.2 * limit:
                self.dependency_evaluator.batch_size = max(200, self.dependency_evaluator.batch_size - 100)
                logging.info(f"[AUTO-SCALE] Batch size diturunkan menjadi {self.dependency_evaluator.batch_size}")
        except Exception as e:
            logging.warning(f"[AUTO-SCALE] Gagal evaluasi antrean: {e}")

    def compile_expression(self, expr: str):
        """
        Mengubah string ekspresi menjadi fungsi yang dapat dieksekusi.
        """
        expr_ast = ast.parse(expr, mode='eval')
        
        operators_map = {
            ast.And: operator.and_,
            ast.Or: operator.or_,
            ast.Not: operator.not_,
            ast.BitAnd: operator.and_,
            ast.BitOr: operator.or_,
            ast.BitXor: operator.xor,
            # Tambahkan operator lain sesuai kebutuhan
        }
        
        def eval_node(node, variable_values):
            if isinstance(node, ast.BoolOp):
                # Dapatkan operator (contoh: and, or)
                op = operators_map.get(type(node.op))
                if not op:
                    raise ValueError(f"Unsupported boolean operator: {type(node.op)}")
        
                # Proses nilai-nilai BoolOp secara rekursif untuk setiap pasangan
                result = eval_node(node.values[0], variable_values)
                for value in node.values[1:]:
                    result = op(result, eval_node(value, variable_values))
                return result
            elif isinstance(node, ast.UnaryOp):
                op = operators_map[type(node.op)]
                return op(eval_node(node.operand, variable_values))
            elif isinstance(node, ast.Expr):
                logging.debug("Node adalah ast.Expr, memanggil eval_node(node.value, ...)")
                return eval_node(node.value, variable_values)
            elif isinstance(node, ast.BinOp):
                ...
            elif isinstance(node, ast.Subscript):
                # Pastikan node.value adalah 'variables'
                if isinstance(node.value, ast.Name) and node.value.id == 'variables':
                    # Pada Python 3.12, slice biasanya ast.Constant(value='...')
                    slice_node = node.slice
                    if isinstance(slice_node, ast.Constant):
                        # Langsung ambil .value
                        var_name = slice_node.value  # Ini tipe str, misal "BDW2.J42B-CFEK-VI"
                        return bool(variable_values.get(var_name, False))
                    else:
                        raise ValueError(f"Unsupported subscript slice type: {type(slice_node)}")
                else:
                    raise ValueError("Unsupported subscript in expression")

            elif isinstance(node, ast.Name):
                return bool(variable_values.get(node.id, False))
            elif isinstance(node, ast.Constant):
                return bool(node.value)
            else:
                raise ValueError(f"Unsupported expression: {expr}")
        
        def compiled_func(variable_values):
            return int(eval_node(expr_ast.body, variable_values))
        
        return compiled_func

    def compile_replace_variables_regex(self):
        """
        Mengompilasi pola regex untuk mengganti semua variabel.
        Urutkan variabel berdasarkan panjang untuk menghindari konflik penggantian.
        Gunakan word boundaries untuk memastikan pencocokan utuh.
        """
        var_names = sorted(set(self.addr_to_var.values()), key=lambda x: -len(x))
        escaped_var_names = [re.escape(var) for var in var_names]
        pattern = r'\b(' + '|'.join(escaped_var_names) + r')\b'
        compiled_regex = re.compile(pattern)
        logging.debug(f"Compiled regex pattern: {pattern}")
        return compiled_regex

    def evaluate_logic_expression(self, expr: str) -> int:
        """
        Mengevaluasi ekspresi logika yang telah diganti variabelnya.
        """
        try:
            # Evaluasi ekspresi menggunakan fungsi yang di-precompile
            result = eval(expr, {"variables": self.var_to_val})
            return int(result)
        except Exception as e:
            logging.error(f"Gagal mengevaluasi logic expression '{expr}': {e}")
            return 0

    def setup_datastore(self):
        """
        Inisialisasi datastore dengan jumlah coil yang dinamis
        berdasarkan data dari file .dat.
        """
        if not self.var_to_val:
            logging.error("Data variabel kosong. Pastikan file .dat telah dibaca.")
            QMessageBox.warning(self, "Error", "Data variabel kosong. Pastikan file .dat telah dibaca.")
            return

        max_address = max(self.addr_to_var.keys()) + 1
        size = max_address

        # Buat daftar nilai coil awal
        coils = [False] * size
        for addr, var in self.addr_to_var.items():
            val = self.var_to_val.get(addr, 0)
            coils[addr] = bool(val)

        # Simpan nilai sebelumnya untuk pengecekan perubahan
        self.previous_values = {addr: int(bool(val)) for addr, val in self.var_to_val.items()}

        # Inisialisasi ModbusSequentialDataBlock
        custom_coils = ObservableDataBlock(0, coils, callback=self.coil_update_callback)

        # Buat ModbusSlaveContext dan ModbusServerContext
        slave_context = ModbusSlaveContext(
            di=None,
            co=custom_coils,
            hr=None,
            ir=None
        )
        self.datastore = ModbusServerContext(slaves={1: slave_context}, single=False)

        logging.debug(f"DataStore initialized with {len(coils)} coils.")
        # print(f"DataStore initialized dengan {len(coils)} coils.")

    def stop_server(self):
        """
        Menghentikan Modbus TCP Server.
        """
        if self.server_thread:
            self.server_thread.stop_server()
            self.server_thread = None
            self.conn_list.setText("Modbus server dihentikan.")
            self.start_button.setEnabled(True)
            self.reset_button.setEnabled(False)
            logging.info("Modbus server thread dihentikan.")

        # Stop central timer
        if hasattr(self, 'central_timer') and self.central_timer.isActive():
            self.central_timer.stop()
            logging.debug("Central timer untuk filter management dihentikan.")

        if self.server_active and self.server_thread:
            self.server_thread.stop_server()
            self.server_thread = None
            self.server_active = False

    def process_filters(self):
        """
        Dipanggil oleh central_timer untuk memproses filter yang waktunya telah tiba.
        """
        current_time = time.time()
        while self.filter_heap and self.filter_heap[0][0] <= current_time:
            _, filt = heapq.heappop(self.filter_heap)
            self.handle_filter_timeout(filt)

    def handle_filter_timeout_optimized(self, filt: Filter):
        """
        Mengupdate nilai bit pada RES_ADDR sesuai dengan nilai bit di FIL_ADDR.
        """
        # Implemented in handle_filter_timeout
        pass

    def closeEvent(self, event):
        """
        Menangani event penutupan aplikasi untuk memastikan semua thread dihentikan dengan benar.
        """
        self.stop_server()
        self.dependency_evaluator.stop()
        # Stop semua filter timers
        self.filter_heap.clear()
        listener.stop()  # Hentikan listener logging
        event.accept()

    def update_datastore_coil(self, addr: int, value: int):
        """
        Perbarui nilai coil di datastore pada alamat tertentu.
        """
        try:
            if not self.datastore:
                logging.warning("Datastore belum diinisialisasi.")
                return

            slave = self.datastore[1]  # Slave ID 1

            # Sesuaikan alamat ke zero-based
            zero_based_addr = addr - 1  # Jika addr adalah one-based
            logging.debug(f"Memanggil setValues untuk alamat {zero_based_addr} dengan nilai {value}")
            slave.setValues(1, zero_based_addr, [int(value)])  # Pastikan nilai adalah integer
            logging.info(f"Datastore diperbarui: Coil pada {addr} diubah menjadi {value}")

            # Verifikasi nilai yang di-set
            current_values = slave.getValues(1, zero_based_addr, count=1)
            if current_values:
                current_value = current_values[0]
                logging.info(f"Verifikasi: Coil pada {addr} sekarang bernilai {current_value}")
            else:
                logging.warning(f"Verifikasi: Coil pada {addr} tidak dapat diambil nilainya.")

        except Exception as e:
            logging.error(f"Gagal memperbarui datastore: {e}")
            QMessageBox.critical(self, "Error", f"Gagal memperbarui datastore: {e}")
    
    def ensure_row_selected(self, row, column):
        self.table.selectRow(row)

    def update_table_coil(self, addr: int, new_val: int):
        """
        Memperbarui nilai bit di tabel berdasarkan alamat yang diubah.
        """
        variable = self.addr_to_var.get(addr, f"ADDR_{addr}")
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3] 
        self.log_box.append(f"[{timestamp}] {variable} = {new_val}")
        self.log_box.moveCursor(QTextCursor.End)
        var = self.get_variable_by_address(addr)
        if not var:
            logging.warning(f"Tidak ada variabel yang terkait dengan alamat {addr}")
            return
        # Cari baris di tabel yang sesuai
        for row in range(self.table.rowCount()):
            addr_item = self.table.item(row, 1)  # Kolom "Alamat Bit"
            if addr_item and int(addr_item.text()) == addr:
                val_item = self.table.item(row, 2)  # Kolom "Nilai Bit"
                if val_item:
                    val_item.setText(str(new_val))
                    val_item.setBackground(Qt.green if new_val == 1 else Qt.red)
                    # Update nilai di var_to_val
                    with self.lock:
                        self.var_to_val[addr] = new_val
                    logging.info(f"Coil pada alamat {addr} diupdate menjadi {new_val} dari Modbus client.")
                break

    def main(self):
        """
        Menjalankan event loop aplikasi.
        """
        # Initialize central timer for filter management
        self.central_timer = QTimer(self)
        self.central_timer.setInterval(10)  # 10 ms
        self.central_timer.timeout.connect(self.process_filters)
        self.central_timer.start()

    # Tambahkan metode lainnya jika diperlukan

def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()