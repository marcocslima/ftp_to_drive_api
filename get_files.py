from ftplib import FTP
import zipfile
import os
from os.path import isdir, isfile, join
import time
import shutil
from dotenv import load_dotenv



load_dotenv()

# Configurações
host = os.getenv('HOST')
port = int(os.getenv('PORT', 21))
usuario = os.getenv('USER_ECARTA')
senha = os.getenv('PASSWORD')
directory = os.getenv('DIRECTORY')
downloads_folder = os.getenv('DOWNLOADS_FOLDER')
unzip_files_folder = os.getenv('UNZIP_FILES_FOLDER')

def limpar_pasta(folder):
  # Verificar se o diretório existe
  if os.path.exists(folder):
      # Remover e recriar o diretório
      shutil.rmtree(folder)
      os.makedirs(folder)

#DOWNLOAD ARQUIVOS

def download_files_from_ftp(host, port, usuario, senha, directory, downloads_folder):
    """
    Faz o download de arquivos de um servidor FTP.
    
    Args:
        host (str): Endereço do servidor FTP.
        port (int): Porta do servidor FTP.
        usuario (str): Nome de usuário para autenticação.
        senha (str): Senha para autenticação.
        directory (str): Diretório remoto a ser acessado.
        downloads_folder (str): Diretório local onde os arquivos serão salvos.
    """
    # Cria o diretório local se não existir
    if not isdir(downloads_folder):
        os.makedirs(downloads_folder)
    try:
        ftp = FTP()
        ftp.connect(host, port)
        ftp.login(usuario, senha)

        ftp.cwd(directory)

        files = ftp.nlst()  # Obtém a lista de arquivos no diretório
        files_to_download = [file for file in files if not ('Recibo' in file or 'Inconsistencia' in file or 'DevolucaoAR' in file)]

        # for f in files_to_download:
        #   print(f)

        #print(f"Arquivos encontrados no diretório {directory}: {files}")

        #Baixando cada arquivo
        for file in files:
            local_file_path = os.path.join(downloads_folder, file)
            with open(local_file_path, "wb") as local_file:
                print(f"Baixando {file}...")
                ftp.retrbinary(f"RETR {file}", local_file.write)
            print(f"{file} baixado com sucesso para {local_file_path}.")

        ftp.quit()

    except Exception as e:
        print(f"Erro ao conectar ou baixar arquivos do FTP: {e}")


def descompactar_zip(caminho_arquivo_zip, pasta_destino):
    """
    Descompacta um arquivo ZIP para uma pasta de destino especificada.

    Args:
        caminho_arquivo_zip (str): O caminho completo para o arquivo .zip.
        pasta_destino (str): O caminho para a pasta onde os arquivos serão extraídos.
                             Se a pasta não existir, ela será criada.

    Returns:
        bool: True se a descompactação for bem-sucedida, False caso contrário.
    """
    # 1. Verificar se o arquivo ZIP existe
    if not os.path.exists(caminho_arquivo_zip):
        print(f"Erro: Arquivo ZIP não encontrado em '{caminho_arquivo_zip}'")
        return False

    # 2. Verificar se o arquivo é realmente um ZIP (opcional, mas bom)
    if not zipfile.is_zipfile(caminho_arquivo_zip):
        print(f"Erro: O arquivo '{caminho_arquivo_zip}' não parece ser um arquivo ZIP válido.")
        return False

    # 3. Criar a pasta de destino se ela não existir
    try:
        # os.makedirs cria diretórios pais se necessário
        # exist_ok=True não levanta erro se o diretório já existir
        os.makedirs(pasta_destino, exist_ok=True)
        print(f"Pasta de destino '{pasta_destino}' assegurada/criada.")
    except OSError as e:
        print(f"Erro ao criar a pasta de destino '{pasta_destino}': {e}")
        return False

    # 4. Descompactar o arquivo
    try:
        with zipfile.ZipFile(caminho_arquivo_zip, 'r') as zip_ref:
            print(f"Descompactando '{os.path.basename(caminho_arquivo_zip)}' para '{pasta_destino}'...")
            zip_ref.extractall(pasta_destino)
            print("Descompactação concluída com sucesso.")
            # Opcional: listar os arquivos extraídos
            # print("Arquivos extraídos:")
            # for nome_arquivo in zip_ref.namelist():
            #     print(f"  - {nome_arquivo}")
            return True
    except zipfile.BadZipFile:
        print(f"Erro: Arquivo ZIP corrompido ou inválido: '{caminho_arquivo_zip}'")
        return False
    except Exception as e:
        print(f"Um erro inesperado ocorreu durante a descompactação: {e}")
        return False

if __name__ == "__main__":
    limpar_pasta(downloads_folder)
    limpar_pasta(unzip_files_folder)

    download_files_from_ftp(host, port, usuario, senha, directory, downloads_folder)

    time.sleep(3)  # Espera 5 segundos para garantir que os arquivos sejam baixados

    for root, dirs, files in os.walk(downloads_folder):
      for file in files:
        descompactar_zip(f'{downloads_folder}/{file}', unzip_files_folder)