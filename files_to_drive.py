# files_to_drive.py
import os
from dotenv import load_dotenv
import time

try:
    import ecarta_processor
except ImportError as e:
    print(f"ERRO: Não foi possível importar 'ecarta_processor.py': {e}")
    exit()
try:
    import upload_gdrive as gdrive_uploader # Alias para o módulo do Drive
except ImportError as e:
    print(f"ERRO: Não foi possível importar 'upload_ftp.py' (módulo do Drive): {e}")
    exit()

load_dotenv()

TARGET_DRIVE_FOLDER_ID = os.getenv('TARGET_FOLDER_ID')

# Configurações de FTP para exclusão (são as mesmas do download)
HOST_FTP_EXCLUSAO = os.getenv('HOST')
PORT_FTP_EXCLUSAO = int(os.getenv('PORT', 21))
USUARIO_FTP_EXCLUSAO = os.getenv('USER_ECARTA')
SENHA_FTP_EXCLUSAO = os.getenv('PASSWORD')
DIRETORIO_FTP_EXCLUSAO = os.getenv('DIRECTORY')


if __name__ == "__main__":
    if not TARGET_DRIVE_FOLDER_ID:
        print("ERRO: TARGET_FOLDER_ID não definido no arquivo .env.")
        exit()

    print("--- Iniciando fluxo completo: Processamento eCarta e Upload para Google Drive ---")
    start_time_total = time.perf_counter()
    fluxo_principal_sucesso = False # Flag para controlar a exclusão no FTP

    # 1. Processar arquivos eCarta
    print("\n--- Fase 1: Processamento de arquivos eCarta ---")
    # Agora retorna: (caminho_da_pasta_com_arquivos_finais, lista_de_zips_processados_para_excluir)
    resultado_processamento = ecarta_processor.processar_arquivos_ecarta_ftp()
    
    if resultado_processamento is None or resultado_processamento[0] is None:
        print("Processamento eCarta falhou ou não retornou uma pasta de arquivos. Encerrando.")
        exit()
    
    pasta_com_arquivos_finais, nomes_zips_para_excluir_ftp = resultado_processamento
    
    if not os.path.isdir(pasta_com_arquivos_finais):
        print(f"ERRO: A pasta '{pasta_com_arquivos_finais}' não existe ou não é um diretório.")
        exit()
        
    arquivos_para_upload = [
        os.path.join(root, file_name)
        for root, _, files in os.walk(pasta_com_arquivos_finais)
        for file_name in files if os.path.isfile(os.path.join(root, file_name))
    ]

    if not arquivos_para_upload:
        print(f"Nenhum arquivo encontrado em '{pasta_com_arquivos_finais}' para fazer upload ao Google Drive.")
        # Mesmo sem arquivos para upload, o processamento pode ter sido "bem-sucedido"
        # em termos de identificar o que excluir do FTP.
        fluxo_principal_sucesso = True # Considerar sucesso se chegou aqui sem erro crítico
    else:
        print(f"\n--- Fase 2: Upload para o Google Drive ({len(arquivos_para_upload)} arquivos) ---")
        drive_service = gdrive_uploader.get_drive_service()

        if drive_service:
            print(f"Iniciando upload para a pasta Drive ID: {TARGET_DRIVE_FOLDER_ID}")
            sucessos_upload = 0
            falhas_upload = 0
            todos_uploads_ok = True # Assumir sucesso até que uma falha ocorra
            for arquivo_path in arquivos_para_upload:
                if gdrive_uploader.upload_file_to_folder(drive_service, arquivo_path, TARGET_DRIVE_FOLDER_ID, drive_filename=os.path.basename(arquivo_path)):
                    sucessos_upload += 1
                else:
                    falhas_upload += 1
                    todos_uploads_ok = False # Marcar que houve falha
            print(f"Uploads para o Drive concluídos. Sucessos: {sucessos_upload}, Falhas: {falhas_upload}")
            if todos_uploads_ok and sucessos_upload > 0 : # Só considerar sucesso se houve uploads e todos foram ok
                fluxo_principal_sucesso = True
            elif falhas_upload > 0:
                 print("AVISO: Nem todos os arquivos foram enviados para o Google Drive com sucesso. A exclusão no FTP será pulada.")
            elif sucessos_upload == 0 and len(arquivos_para_upload) > 0: # Havia arquivos, mas nenhum foi enviado
                 print("AVISO: Nenhum arquivo foi enviado para o Google Drive. A exclusão no FTP será pulada.")

        else:
            print("Falha ao obter o serviço do Google Drive. Upload não realizado. A exclusão no FTP será pulada.")

    # 3. Excluir arquivos do FTP se tudo deu certo nas fases anteriores
    # E se houver arquivos ZIP marcados para exclusão
    if fluxo_principal_sucesso and nomes_zips_para_excluir_ftp:
        print("\n--- Fase 3: Exclusão de arquivos processados do servidor FTP ---")
        ecarta_processor.excluir_arquivos_do_ftp(
            HOST_FTP_EXCLUSAO,
            PORT_FTP_EXCLUSAO,
            USUARIO_FTP_EXCLUSAO,
            SENHA_FTP_EXCLUSAO,
            DIRETORIO_FTP_EXCLUSAO,
            nomes_zips_para_excluir_ftp
        )
    elif fluxo_principal_sucesso and not nomes_zips_para_excluir_ftp:
        print("\nNenhum arquivo ZIP marcado para exclusão no FTP (todos podem ser DevolucaoAR ou nenhum ZIP processado).")
    else:
        print("\nExclusão de arquivos do FTP pulada devido a falhas no processamento ou upload.")


    end_time_total = time.perf_counter()
    elapsed_time_total = end_time_total - start_time_total
    print("\n--- Processo completo ---")
    print(f"Tempo total de execução: {elapsed_time_total:.2f} segundos")