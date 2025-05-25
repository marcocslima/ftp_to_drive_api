from get_files import *
from upload_ftp import *
import os
from dotenv import load_dotenv
import time

load_dotenv()

tg_folder_id = os.getenv('TARGET_FOLDER_ID')

if __name__ == "__main__":
    
    # --- Início do cronômetro ---
    start_time = time.perf_counter()
    # --------------------------

    limpar_pasta(downloads_folder)
    limpar_pasta(unzip_files_folder)

    download_files_from_ftp(host, port, usuario, senha, directory, downloads_folder)

    time.sleep(3)

    for root, dirs, files in os.walk(downloads_folder):
      for file in files:
        descompactar_zip(f'{downloads_folder}/{file}', unzip_files_folder)

    time.sleep(3)

    arquivos = [os.path.join(raiz, arquivo) for raiz, _, arquivos in os.walk(unzip_files_folder) for arquivo in arquivos]

    drive_service = get_drive_service()

    if drive_service:
      for f in arquivos:
          upload_file_to_folder(drive_service, f, tg_folder_id)
    else:
      print("Falha ao obter o serviço do Google Drive.")

    # --- Fim do cronômetro ---
    end_time = time.perf_counter()
    # ------------------------

    elapsed_time = end_time - start_time
    print("\nProcesso concluído!")
    print(f"Tempo total de execução: {elapsed_time:.2f} segundos") # 2 casas decimais