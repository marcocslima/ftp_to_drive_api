import os
from dotenv import load_dotenv
import time

try:
    import ecarta_processor
    import upload_gdrive as gdrive_uploader # Módulo do Drive
except ImportError as e: print(f"ERRO de importação: {e}"); exit()

load_dotenv()

TARGET_DRIVE_FOLDER_ID_PRINCIPAL = os.getenv('TARGET_FOLDER_ID')
TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE = os.getenv('TARGET_FOLDER_ID_DEVOLUCAOAR_ARCHIVE') # Nova pasta

HOST_FTP = os.getenv('HOST')
PORT_FTP = int(os.getenv('PORT', 21))
USUARIO_FTP = os.getenv('USER_ECARTA')
SENHA_FTP = os.getenv('PASSWORD')
DIRETORIO_FTP = os.getenv('DIRECTORY')


if __name__ == "__main__":
    if not TARGET_DRIVE_FOLDER_ID_PRINCIPAL or not TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE:
        print("ERRO: IDs de pasta do Drive (TARGET_FOLDER_ID e/ou TARGET_FOLDER_ID_DEVOLUCAOAR_ARCHIVE) não definidos no .env.")
        exit()

    print("--- Iniciando fluxo: Processamento eCarta, Uploads para Google Drive e Limpeza FTP ---")
    start_time_total = time.perf_counter()
    processamento_local_ok = False
    upload_pdfs_finais_ok = False
    upload_arquivos_devolucaoAR_ok = False

    # 1. Processar arquivos eCarta
    print("\n--- Fase 1: Processamento de arquivos eCarta ---")
    # Retorna: (pasta_pdfs, nomes_TODOS_baixados_do_ftp, caminhos_locais_DevolucaoAR_originais)
    resultado_proc = ecarta_processor.processar_arquivos_ecarta_ftp()
    
    if resultado_proc is None or resultado_proc[0] is None:
        print("Processamento eCarta falhou ou não retornou pasta de arquivos. Encerrando.")
        exit()
    
    pasta_pdfs_finais, nomes_todos_arquivos_baixados_ftp, caminhos_locais_devolucaoAR_originais = resultado_proc
    processamento_local_ok = True # Se chegou aqui, o processamento local básico funcionou

    # 2. Upload dos PDFs FINAIS para a pasta principal do Drive
    if not os.path.isdir(pasta_pdfs_finais):
        print(f"ERRO: Pasta de PDFs finais '{pasta_pdfs_finais}' não é um diretório válido.")
        processamento_local_ok = False # Marcar falha se a pasta não for válida
    
    arquivos_para_upload_principal = []
    if processamento_local_ok:
        arquivos_para_upload_principal = [
            os.path.join(root, fn) for root, _, fns in os.walk(pasta_pdfs_finais) for fn in fns if os.path.isfile(os.path.join(root,fn))
        ]

    drive_service = gdrive_uploader.get_drive_service() # Obter serviço do Drive uma vez

    if not drive_service:
        print("Falha ao obter o serviço do Google Drive. Todos os uploads e exclusão FTP serão pulados.")
        processamento_local_ok = False # Não podemos prosseguir sem o serviço do Drive
    
    if processamento_local_ok and arquivos_para_upload_principal:
        print(f"\n--- Fase 2.1: Upload de PDFs FINAIS para Drive (Pasta Principal: {TARGET_DRIVE_FOLDER_ID_PRINCIPAL}) ---")
        sucesso = 0; falha = 0
        for arq_path in arquivos_para_upload_principal:
            if gdrive_uploader.upload_file_to_folder(drive_service, arq_path, TARGET_DRIVE_FOLDER_ID_PRINCIPAL):
                sucesso += 1
            else: falha += 1
        print(f"Uploads de PDFs finais: {sucesso} sucesso(s), {falha} falha(s).")
        if falha == 0 and sucesso > 0: upload_pdfs_finais_ok = True
        elif falha == 0 and sucesso == 0 and len(arquivos_para_upload_principal) == 0: upload_pdfs_finais_ok = True # Ok se não havia nada para subir
        else: print("AVISO: Falhas no upload dos PDFs finais.")
    elif processamento_local_ok:
        print("\nNenhum PDF final para upload na pasta principal do Drive.")
        upload_pdfs_finais_ok = True # Considerar OK se não havia nada para subir

    # 3. Upload dos ARQUIVOS DEVOLUCAOAR ORIGINAIS para a pasta de arquivamento do Drive
    if processamento_local_ok and caminhos_locais_devolucaoAR_originais:
        print(f"\n--- Fase 2.2: Upload de ARQUIVOS DEVOLUCAOAR ORIGINAIS para Drive (Pasta Arquivo: {TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE}) ---")
        sucesso_dev = 0; falha_dev = 0
        for arq_dev_path in caminhos_locais_devolucaoAR_originais:
            if os.path.exists(arq_dev_path): # Garantir que o arquivo original ainda existe
                if gdrive_uploader.upload_file_to_folder(drive_service, arq_dev_path, TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE):
                    sucesso_dev += 1
                else: falha_dev += 1
            else:
                print(f"AVISO: Arquivo DevolucaoAR original '{arq_dev_path}' não encontrado localmente para upload de arquivamento.")
                falha_dev +=1 # Considerar como falha se o arquivo sumiu
        print(f"Uploads de arquivos DevolucaoAR originais: {sucesso_dev} sucesso(s), {falha_dev} falha(s).")
        if falha_dev == 0 and sucesso_dev > 0 : upload_arquivos_devolucaoAR_ok = True
        elif falha_dev == 0 and sucesso_dev == 0 and len(caminhos_locais_devolucaoAR_originais) == 0 : upload_arquivos_devolucaoAR_ok = True # Ok se não havia DevolucaoAR
        else: print("AVISO: Falhas no upload dos arquivos DevolucaoAR originais.")
    elif processamento_local_ok:
        print("\nNenhum arquivo DevolucaoAR original para upload na pasta de arquivamento do Drive.")
        upload_arquivos_devolucaoAR_ok = True # Considerar OK se não havia nada para subir


    # 4. Excluir TODOS os arquivos baixados do servidor FTP se TUDO deu certo
    if processamento_local_ok and upload_pdfs_finais_ok and upload_arquivos_devolucaoAR_ok and nomes_todos_arquivos_baixados_ftp:
        print("\n--- Fase 3: Exclusão de TODOS os arquivos baixados do servidor FTP ---")
        ecarta_processor.excluir_arquivos_do_ftp(
            HOST_FTP, PORT_FTP, USUARIO_FTP, SENHA_FTP, DIRETORIO_FTP,
            nomes_todos_arquivos_baixados_ftp # Lista de NOMES dos arquivos no FTP
        )
    elif not nomes_todos_arquivos_baixados_ftp:
        print("\nNenhum arquivo foi baixado do FTP, portanto nada a excluir.")
    else:
        print("\nExclusão de arquivos do FTP pulada devido a falhas no processamento local ou nos uploads para o Drive.")

    end_time_total = time.perf_counter()
    print(f"\n--- Processo completo ---\nTempo total: {end_time_total - start_time_total:.2f} segundos")