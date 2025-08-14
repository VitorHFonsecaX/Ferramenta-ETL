import sys
import os
import json
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget, QTextEdit, 
    QLabel, QHBoxLayout, QProgressBar, QFrame, QSizePolicy, QGridLayout, 
    QMessageBox
)
from PySide6.QtCore import QProcess, Qt, QTimer, QSize
from PySide6.QtGui import QPalette, QColor

# Arquivo de configuração que armazena a lista de scripts ETL.
# Isso torna a aplicação mais flexível.
CONFIG_FILE = "etl_scripts.json"

class ETLApp(QMainWindow):
    """
    Interface gráfica aprimorada para executar scripts ETL de forma individual e sequencial.
    Esta versão carrega a configuração de scripts de um arquivo JSON e verifica
    a existência dos arquivos antes de tentar executá-los.
    """
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Ferramenta de Carga ETL")
        self.setMinimumSize(QSize(900, 700))
        
        self.scripts = self.load_scripts_config()
        if not self.scripts:
            QMessageBox.critical(self, "Erro de Configuração", 
                                 f"Não foi possível carregar a configuração de scripts do arquivo '{CONFIG_FILE}'. "
                                 "Certifique-se de que o arquivo existe e está no formato JSON correto.")
            sys.exit(-1)

        self.current_process = None
        self.script_queue = []
        
        self.progress_timer = QTimer(self)
        self.progress_timer.timeout.connect(self.update_progress)

        self.setup_ui()

    def load_scripts_config(self):
        """Carrega a lista de scripts do arquivo de configuração JSON."""
        # Se o arquivo de configuração não existir, cria um de exemplo.
        if not os.path.exists(CONFIG_FILE):
            example_config = {
                "alimentacao_view_manifestos.py": {
                    "label": "Manifestos", 
                    "tooltip": "Processa e carrega dados de manifestos."
                },
                "alimentacao_view_movimento.py": {
                    "label": "Movimentos", 
                    "tooltip": "Processa e carrega dados de movimentos."
                },
                "alimentacao_view_manifestomovimento.py": {
                    "label": "Manifesto-Movimento", 
                    "tooltip": "Associa dados de manifestos e movimentos."
                },
                "alimentacao_view_adicionais.py": {
                    "label": "Adicionais", 
                    "tooltip": "Carrega dados adicionais para complementar as tabelas."
                },
                "alimentacao_parcela_ciot.py": {
                    "label": "Parcela e CIOT", 
                    "tooltip": "Processa dados de parcelas e CIOT."
                }
            }
            try:
                with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(example_config, f, indent=4, ensure_ascii=False)
                print(f"Arquivo de configuração de exemplo '{CONFIG_FILE}' criado.")
            except Exception as e:
                print(f"Erro ao criar arquivo de configuração de exemplo: {e}")
                return None
        
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Erro ao carregar arquivo de configuração: {e}")
            return None

    def setup_ui(self):
        """Configura todos os widgets e o layout da interface com o novo design."""
        
        # --- Estilo e Paleta de Cores ---
        palette = self.palette()
        palette.setColor(QPalette.Window, QColor("#1e2a3a"))
        palette.setColor(QPalette.WindowText, QColor("#e0e0e0"))
        palette.setColor(QPalette.Base, QColor("#283747"))
        palette.setColor(QPalette.Text, QColor("#e0e0e0"))
        self.setPalette(palette)
        
        self.setStyleSheet("""
            QWidget {
                background-color: #1e2a3a;
                color: #e0e0e0;
                font-family: 'Segoe UI', 'Roboto', 'Helvetica', 'Arial', sans-serif;
            }
            QLabel#title_label {
                font-size: 32px;
                font-weight: bold;
                color: #e0e0e0;
                padding-bottom: 15px;
            }
            QLabel {
                font-size: 14px;
            }
            QFrame {
                background-color: #283747;
                border-radius: 15px;
                padding: 15px;
            }
            QPushButton { 
                font-size: 14px; 
                padding: 8px;
                border-radius: 10px; 
                background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #3498db, stop:1 #2980b9);
                color: white; 
                border: none;
                font-weight: bold;
                letter-spacing: 0.5px;
            }
            QPushButton:hover { 
                background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #2980b9, stop:1 #3498db);
            }
            QPushButton:disabled { 
                background-color: #4a5a6b; 
                color: #8c97a5;
            }
            QPushButton#run_all_button {
                background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #27ae60, stop:1 #2ecc71);
            }
            QPushButton#run_all_button:hover {
                background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #2ecc71, stop:1 #27ae60);
            }
            QPushButton#cancel_button {
                background-color: #e74c3c;
            }
            QPushButton#cancel_button:hover {
                background-color: #c0392b;
            }
            QProgressBar {
                border: 2px solid #2980b9;
                border-radius: 6px;
                background-color: #3e516a;
                text-align: center;
                color: white;
            }
            QProgressBar::chunk {
                background-color: #3498db;
            }
            QTextEdit {
                font-family: 'Courier New', 'monospace'; 
                font-size: 12px; 
                background-color: #2c3e50;
                border: 1px solid #3e516a;
                border-radius: 8px; 
                padding: 10px;
                color: #ffffff;
            }
        """)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(40, 30, 40, 30)
        main_layout.setSpacing(25)

        title_label = QLabel("Ferramenta de Automação ETL")
        title_label.setObjectName("title_label")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(title_label)

        # Frame para os botões de execução individual com layout de grade
        button_frame = QFrame()
        button_layout = QGridLayout(button_frame)
        button_layout.setSpacing(15)
        self.buttons = {}
        self.status_labels = {}
        row, col = 0, 0
        for script_file, script_info in self.scripts.items():
            h_layout = QHBoxLayout()
            h_layout.setSpacing(5)
            
            # Botão
            button = QPushButton(script_info["label"])
            button.setObjectName(script_file)
            button.clicked.connect(self.start_etl_process)
            button.setToolTip(script_info["tooltip"])
            h_layout.addWidget(button)
            self.buttons[script_file] = button
            
            # Label de status
            status_label = QLabel("")
            status_label.setFixedSize(QSize(16, 16))
            status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            h_layout.addWidget(status_label)
            self.status_labels[script_file] = status_label
            
            button_layout.addLayout(h_layout, row, col)
            col += 1
            if col > 2: # 3 colunas
                col = 0
                row += 1
        
        main_layout.addWidget(button_frame)
        
        # Layout para os botões "Executar Todos" e "Cancelar"
        action_buttons_layout = QHBoxLayout()
        action_buttons_layout.setSpacing(15)

        # Botão de "Executar Todos"
        self.run_all_button = QPushButton("Executar Todos")
        self.run_all_button.setObjectName("run_all_button")
        self.run_all_button.clicked.connect(self.start_all_processes)
        action_buttons_layout.addWidget(self.run_all_button)
        
        # Botão de "Cancelar"
        self.cancel_button = QPushButton("Cancelar")
        self.cancel_button.setObjectName("cancel_button")
        self.cancel_button.clicked.connect(self.cancel_current_process)
        self.cancel_button.setEnabled(False)
        action_buttons_layout.addWidget(self.cancel_button)
        
        main_layout.addLayout(action_buttons_layout)
        
        # Frame para a barra de status e logs
        log_status_frame = QFrame()
        log_status_layout = QVBoxLayout(log_status_frame)
        log_status_layout.setSpacing(10)
        log_status_layout.setContentsMargins(10, 10, 10, 10)

        # Layout para o status e barra de progresso
        status_layout = QHBoxLayout()
        self.status_label = QLabel("Status: Pronto para iniciar.")
        self.status_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.progress_bar.hide()
        
        status_layout.addWidget(self.status_label)
        status_layout.addWidget(self.progress_bar)
        
        log_status_layout.addLayout(status_layout)
        
        log_header_layout = QHBoxLayout()
        log_title_label = QLabel("Logs do Processo")
        log_title_label.setStyleSheet("font-size: 18px; font-weight: bold; margin-bottom: 5px;")
        
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        
        # Botão para limpar os logs
        self.clear_logs_button = QPushButton("Limpar Logs")
        self.clear_logs_button.clicked.connect(self.log_area.clear)
        self.clear_logs_button.setStyleSheet("""
            QPushButton {
                font-size: 12px;
                padding: 5px;
                background-color: #3e516a;
                border-radius: 8px;
            }
            QPushButton:hover {
                background-color: #2c3e50;
            }
        """)
        self.clear_logs_button.setFixedSize(QSize(100, 30))
        
        log_header_layout.addWidget(log_title_label)
        log_header_layout.addStretch()
        log_header_layout.addWidget(self.clear_logs_button)
        log_status_layout.addLayout(log_header_layout)

        log_status_layout.addWidget(self.log_area)
        
        main_layout.addWidget(log_status_frame)

    def set_buttons_enabled(self, enabled):
        """Habilita ou desabilita todos os botões."""
        for button in self.buttons.values():
            button.setEnabled(enabled)
        self.run_all_button.setEnabled(enabled)
        self.cancel_button.setEnabled(not enabled)

    def _update_script_status(self, script_file, status_char):
        """Atualiza o label de status de um script específico."""
        if script_file in self.status_labels:
            label = self.status_labels[script_file]
            label.setText(status_char)
            
    def get_script_path(self, script_file):
        """
        Retorna o caminho completo para o script. 
        Tenta encontrar o script no diretório de trabalho atual e, 
        se não for encontrado, tenta um caminho alternativo com base no log de erro.
        """
        # Caminho direto no diretório de trabalho atual
        current_path = os.path.join(os.getcwd(), script_file)
        if os.path.exists(current_path):
            return current_path
        
        # Caminho alternativo com base no log de erro
        # Este é um ajuste para lidar com o problema de caminho que você reportou.
        base_path = "W:\\JAT\\PROJETOS\\PROJETOS\\PROJETOS\\01. Projetistas\\09. VITOR\\Documentação Banco\\Banco_JAT"
        alternative_path = os.path.join(base_path, script_file)
        
        # Corrigir o problema de codificação do caminho, se necessário
        # A parte "Documentao" do erro de log pode ser um problema de codificação.
        # Vamos tentar o caminho corrigido.
        
        alternative_path_corrected = alternative_path.replace("Documentao Banco", "Documentação Banco")

        if os.path.exists(alternative_path_corrected):
            return alternative_path_corrected
            
        return None # Retorna None se o arquivo não for encontrado em nenhum dos caminhos

    def start_etl_process(self, script_file=None):
        """Inicia a execução do script selecionado ou do próximo na fila."""
        if self.current_process and self.current_process.state() == QProcess.Running:
            return

        if not script_file:
            sender_button = self.sender()
            script_file = sender_button.objectName()
        
        script_label = self.scripts[script_file]["label"]
        
        # Obter o caminho completo do script usando a nova função
        script_path = self.get_script_path(script_file)
        
        if not script_path or not os.path.exists(script_path):
            self.log_area.append(f"\n<font color='#e74c3c'><b>--- ❌ ERRO: O arquivo '{script_file}' não foi encontrado! ---</b></font>")
            self.status_label.setStyleSheet("font-size: 14px; color: #e74c3c;")
            self.status_label.setText(f"Status: Arquivo '{script_label}' não encontrado. Verifique o caminho.")
            self._update_script_status(script_file, "❌")
            return
            
        self.log_area.append(f"\n<font color='#F1C40F'><b>Iniciando '{script_label}'...</b></font>")
        
        self.set_buttons_enabled(False)
        self._update_script_status(script_file, "⌛")
        self.progress_bar.show()
        self.progress_bar.setValue(0)
        self.status_label.setStyleSheet("font-size: 14px; color: #f1c40f;")
        self.status_label.setText(f"Status: Executando '{script_label}'...")

        process = QProcess(self)
        process.setWorkingDirectory(os.getcwd())
        process.script_file = script_file 
        
        process.readyReadStandardOutput.connect(self._handle_output)
        process.readyReadStandardError.connect(self._handle_output)
        process.finished.connect(lambda exit_code, exit_status: self._handle_finished(process, exit_code, exit_status))
        
        self.current_process = process
        self.progress_timer.start(150)
        
        # Executa o script usando o caminho completo verificado
        process.start(sys.executable, [script_path])

    def start_all_processes(self):
        """Inicia a execução de todos os scripts em sequência."""
        self.log_area.clear()
        for label in self.status_labels.values():
            label.setText("")
        self.log_area.append("Iniciando a sequência de todos os scripts...")
        self.script_queue = list(self.scripts.keys())
        self._run_next_script_from_queue()
        
    def _run_next_script_from_queue(self):
        """Função interna para executar o próximo script na fila."""
        if self.script_queue:
            script_to_run = self.script_queue.pop(0)
            self.start_etl_process(script_to_run)
        else:
            self.set_buttons_enabled(True)
            self.log_area.append("\n<font color='#2ecc71'><b>--- ✅ Todos os scripts foram executados com sucesso! ---</b></font>")
            self.status_label.setStyleSheet("font-size: 14px; color: #2ecc71;")
            self.status_label.setText("Status: Todas as cargas foram concluídas!")

    def update_progress(self):
        """Atualiza a barra de progresso para dar feedback visual."""
        if self.current_process and self.current_process.state() == QProcess.Running:
            current_value = self.progress_bar.value()
            if current_value < 99:
                self.progress_bar.setValue(current_value + 1)

    def _handle_output(self):
        """Captura e exibe o output do processo."""
        if self.current_process:
            data_out = self.current_process.readAllStandardOutput().data().decode('utf-8', errors='replace').strip()
            data_err = self.current_process.readAllStandardError().data().decode('utf-8', errors='replace').strip()
            if data_out:
                self.log_area.append(data_out)
            if data_err:
                self.log_area.append(f"<font color='#e74c3c'>{data_err}</font>")

    def _handle_finished(self, process, exit_code, exit_status):
        """Função que lida com o término de um script, atualizando o status visual."""
        try:
            script_file = process.script_file
            script_label = self.scripts.get(script_file, {"label": "Script Desconhecido"})["label"]

            self.progress_timer.stop()
            self.progress_bar.setValue(100)
            
            if exit_status == QProcess.ExitStatus.NormalExit and exit_code == 0:
                self.log_area.append(f"\n--- ✅ '{script_label}' finalizado com sucesso! ---\n")
                self._update_script_status(script_file, "✅")
            else:
                self.log_area.append(f"\n<font color='#e74c3c'><b>--- ❌ ERRO: '{script_label}' falhou com código {exit_code}! ---</b></font>")
                self._update_script_status(script_file, "❌")
                self.status_label.setStyleSheet("font-size: 14px; color: #e74c3c;")
                self.status_label.setText("Status: Falha na execução. Verifique os logs.")
                self.script_queue = []
                self.set_buttons_enabled(True)
                QMessageBox.critical(self, "Erro de Execução", 
                                     f"O script '{script_label}' falhou. Verifique os logs para detalhes.")

            self.progress_bar.hide()
            
            if self.script_queue and exit_code == 0:
                self._run_next_script_from_queue()
            elif not self.script_queue:
                self.set_buttons_enabled(True)
                self.status_label.setText("Status: Pronto para iniciar.")
            
            if self.current_process == process:
                self.current_process = None
        except Exception as e:
            self.log_area.append(f"\n<font color='#e74c3c'><b>--- ❌ ERRO NO MANUSEIO DE FINALIZAÇÃO: {e} ---</b></font>")
            self.set_buttons_enabled(True)

    def cancel_current_process(self):
        """Cancela o processo atual em execução e limpa a fila de scripts."""
        if self.current_process and self.current_process.state() == QProcess.Running:
            self.log_area.append("\n<font color='#e74c3c'><b>--- 🛑 Cancelando processo...</b></font>")
            self.current_process.terminate()
            self.current_process.waitForFinished(1000)
            self.current_process = None
        
        self.script_queue = []
        self.set_buttons_enabled(True)
        self.progress_timer.stop()
        self.progress_bar.hide()
        self.status_label.setStyleSheet("font-size: 14px; color: #e0e0e0;")
        self.status_label.setText("Status: Processo cancelado. Pronto para iniciar.")

    def closeEvent(self, event):
        """Garante que o processo filho seja encerrado ao fechar a janela."""
        if self.current_process and self.current_process.state() == QProcess.Running:
            self.current_process.terminate()
            self.current_process.waitForFinished(1000)
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ETLApp()
    window.show()
    sys.exit(app.exec())