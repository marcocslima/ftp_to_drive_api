# files_to_drive.py
import os
from dotenv import load_dotenv
import time

try:
    import ecarta_processor
    import upload_gdrive as gdrive_uploader # Módulo do Drive
except ImportError as e: print(f"ERRO de importação: {e}"); exit()

load_dotenv()

# --- CONFIGURAÇÕES DE PASTAS DO DRIVE ---
TARGET_DRIVE_FOLDER_ID_PRINCIPAL = os.getenv('TARGET_FOLDER_ID')
TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE = os.getenv('TARGET_FOLDER_ID_DEVOLUCAOAR_ARCHIVE')

# --- CONFIGURAÇÕES DE FTP ---
HOST_FTP = os.getenv('HOST')
PORT_FTP = int(os.getenv('PORT', 21))
USUARIO_FTP = os.getenv('USER_ECARTA')
SENHA_FTP = os.getenv('PASSWORD')
DIRETORIO_FTP = os.getenv('DIRECTORY')

# --- CONFIGURAÇÕES DO APPS SCRIPT ---
APPS_SCRIPT_ID = os.getenv('APPS_SCRIPT_ID')
APPS_SCRIPT_FUNCTION_NAME = os.getenv('APPS_SCRIPT_FUNCTION_NAME')
APPS_SCRIPT_DEPLOYMENT_ID = os.getenv('APPS_SCRIPT_DEPLOYMENT_ID') # Pode ser None


if __name__ == "__main__":
    # Validação inicial das configurações essenciais
    if not TARGET_DRIVE_FOLDER_ID_PRINCIPAL:
        print("ERRO: TARGET_FOLDER_ID (pasta principal do Drive) não definido no .env.")
        exit()
    if not TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE:
        print("ERRO: TARGET_FOLDER_ID_DEVOLUCAOAR_ARCHIVE (pasta de arquivo DevolucaoAR) não definido no .env.")
        exit()
    # Não é estritamente necessário validar APPS_SCRIPT_ID aqui, pois o script pode rodar sem ele.

    print("--- Iniciando fluxo: Processamento eCarta, Uploads para Google Drive, Limpeza FTP e Apps Script ---")
    start_time_total = time.perf_counter()

    # Flags de controle de sucesso das etapas
    processamento_local_ok = False
    upload_pdfs_finais_ok = False
    upload_arquivos_devolucaoAR_ok = False
    drive_service_ok = False # Flag para o serviço do Drive e credenciais

    # Obter serviço do Drive e Credenciais UMA VEZ no início
    drive_service, drive_credentials = gdrive_uploader.get_drive_service()
    if drive_service and drive_credentials:
        drive_service_ok = True
    else:
        print("Falha crítica ao obter o serviço do Google Drive ou credenciais. O programa não pode continuar com uploads ou Apps Script.")
        # Decidir se o programa deve parar completamente ou tentar continuar sem as partes do Google.
        # Para este fluxo, se o Drive falhar, as etapas seguintes que dependem dele não devem ocorrer.
        # A exclusão do FTP também deve ser condicionada ao sucesso dos uploads.

    # 1. Processar arquivos eCarta (independente do Drive inicialmente)
    if drive_service_ok: # Prosseguir apenas se o serviço do Drive estiver OK para as etapas seguintes
        print("\n--- Fase 1: Processamento de arquivos eCarta ---")
        resultado_proc = ecarta_processor.processar_arquivos_ecarta_ftp()
        
        if resultado_proc is None or resultado_proc[0] is None:
            print("Processamento eCarta falhou ou não retornou pasta de arquivos. Encerrando.")
            # Não definir processamento_local_ok = True
        else:
            pasta_pdfs_finais, nomes_todos_arquivos_baixados_ftp, caminhos_locais_devolucaoAR_originais = resultado_proc
            if not os.path.isdir(pasta_pdfs_finais):
                print(f"ERRO: Pasta de PDFs finais '{pasta_pdfs_finais}' não é um diretório válido.")
            else:
                processamento_local_ok = True # Processamento local dos arquivos foi bem-sucedido

        # 2. Upload dos PDFs FINAIS para a pasta principal do Drive
        if processamento_local_ok:
            arquivos_para_upload_principal = [
                os.path.join(root, fn) for root, _, fns in os.walk(pasta_pdfs_finais) for fn in fns if os.path.isfile(os.path.join(root,fn))
            ]
            if arquivos_para_upload_principal:
                print(f"\n--- Fase 2.1: Upload de PDFs FINAIS para Drive (Pasta Principal: {TARGET_DRIVE_FOLDER_ID_PRINCIPAL}) ---")
                sucesso = 0; falha = 0
                for arq_path in arquivos_para_upload_principal:
                    if gdrive_uploader.upload_file_to_folder(drive_service, arq_path, TARGET_DRIVE_FOLDER_ID_PRINCIPAL):
                        sucesso += 1
                    else: falha += 1
                print(f"Uploads de PDFs finais: {sucesso} sucesso(s), {falha} falha(s).")
                if falha == 0: upload_pdfs_finais_ok = True # Sucesso se não houve falhas (mesmo que 0 arquivos para upload)
                else: print("AVISO: Falhas no upload dos PDFs finais.")
            else:
                print("\nNenhum PDF final para upload na pasta principal do Drive.")
                upload_pdfs_finais_ok = True # Considerar OK se não havia nada para subir

        # 3. Upload dos ARQUIVOS DEVOLUCAOAR ORIGINAIS para a pasta de arquivamento do Drive
        if processamento_local_ok: # Ainda depende do processamento local ter sido ok
            if caminhos_locais_devolucaoAR_originais:
                print(f"\n--- Fase 2.2: Upload de ARQUIVOS DEVOLUCAOAR ORIGINAIS para Drive (Pasta Arquivo: {TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE}) ---")
                sucesso_dev = 0; falha_dev = 0
                for arq_dev_path in caminhos_locais_devolucaoAR_originais:
                    if os.path.exists(arq_dev_path):
                        if gdrive_uploader.upload_file_to_folder(drive_service, arq_dev_path, TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE):
                            sucesso_dev += 1
                        else: falha_dev += 1
                    else:
                        print(f"AVISO: Arquivo DevolucaoAR original '{arq_dev_path}' não encontrado para arquivamento.")
                        falha_dev +=1
                print(f"Uploads de arquivos DevolucaoAR originais: {sucesso_dev} sucesso(s), {falha_dev} falha(s).")
                if falha_dev == 0: upload_arquivos_devolucaoAR_ok = True # Sucesso se não houve falhas
                else: print("AVISO: Falhas no upload dos arquivos DevolucaoAR originais.")
            else:
                print("\nNenhum arquivo DevolucaoAR original para upload na pasta de arquivamento do Drive.")
                upload_arquivos_devolucaoAR_ok = True # Considerar OK se não havia nada para subir

        # 4. Excluir TODOS os arquivos baixados do servidor FTP se TUDO deu certo ATÉ AGORA
        if processamento_local_ok and upload_pdfs_finais_ok and upload_arquivos_devolucaoAR_ok:
            if nomes_todos_arquivos_baixados_ftp:
                print("\n--- Fase 3: Exclusão de TODOS os arquivos baixados do servidor FTP ---")
                ecarta_processor.excluir_arquivos_do_ftp(
                    HOST_FTP, PORT_FTP, USUARIO_FTP, SENHA_FTP, DIRETORIO_FTP,
                    nomes_todos_arquivos_baixados_ftp
                )
            else: # Isso não deveria acontecer se o processamento local foi ok e baixou arquivos
                print("\nAVISO: Processamento local OK, mas lista de arquivos para excluir do FTP está vazia.")
        else:
            print("\nExclusão de arquivos do FTP pulada devido a falhas no processamento local ou nos uploads para o Drive.")

        # 5. Disparar o Apps Script se todas as etapas anteriores (incluindo uploads) foram bem-sucedidas
        if processamento_local_ok and upload_pdfs_finais_ok and upload_arquivos_devolucaoAR_ok:
            if APPS_SCRIPT_ID and APPS_SCRIPT_FUNCTION_NAME:
                time.sleep(20)  # Pequena pausa para garantir que o Drive esteja pronto
                print(f"\n--- Fase 4: Disparando Google Apps Script ---")
                apps_script_response = gdrive_uploader.executar_apps_script(
                    drive_credentials,
                    APPS_SCRIPT_ID, # <<< ID DO SCRIPT
                    APPS_SCRIPT_FUNCTION_NAME,
                    dev_mode=True # Para executar a API Executável implantada
                    # deployment_id=APPS_SCRIPT_DEPLOYMENT_ID # Não passamos aqui para este teste
                )
                if apps_script_response:
                    print("Solicitação para Apps Script enviada e processada (verifique o resultado acima).")
                else:
                    print("Falha ao executar o Apps Script ou a solicitação não foi bem-sucedida.")
            else:
                print("\nAPPS_SCRIPT_ID ou APPS_SCRIPT_FUNCTION_NAME não definidos no .env. Apps Script não será disparado.")
        else:
            print("\nApps Script não será disparado devido a falhas em etapas anteriores (processamento local ou uploads).")
    else: # drive_service_ok é False
        print("Etapas dependentes do Google Drive (uploads, Apps Script, exclusão FTP condicionada) foram puladas.")


    end_time_total = time.perf_counter()
    print(f"\n--- Processo completo ---\nTempo total: {end_time_total - start_time_total:.2f} segundos")