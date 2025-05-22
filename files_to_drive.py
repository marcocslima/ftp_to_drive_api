from get_files import *
from upload_ftp import *

TARGET_FOLDER_ID = '1qpEBWglQbUH2m6Gh1R8_AfwNnztxVYDi'

if __name__ == "__main__":
    limpar_pasta(downloads_folder)
    limpar_pasta(unzip_files_folder)

    download_files_from_ftp(host, port, usuario, senha, directory, downloads_folder)

    time.sleep(3)  # Espera 3 segundos para garantir que os arquivos sejam baixados

    for root, dirs, files in os.walk(downloads_folder):
      for file in files:
        descompactar_zip(f'{downloads_folder}/{file}', unzip_files_folder)

    time.sleep(3)  # Espera 3 segundos para garantir que os arquivos sejam baixados

    # List comprehension com os.walk para incluir subdiretórios
    arquivos = [os.path.join(raiz, arquivo) for raiz, _, arquivos in os.walk(unzip_files_folder) for arquivo in arquivos]

    drive_service = get_drive_service()

    if drive_service:
      for f in arquivos:
          upload_file_to_folder(drive_service, f, TARGET_FOLDER_ID)
    else:
      print("Falha ao obter o serviço do Google Drive.")