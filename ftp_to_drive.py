import os.path
import mimetypes # Para adivinhar o tipo MIME do arquivo
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# Se modificar esses escopos, delete o arquivo token.json.
SCOPES = ['https://www.googleapis.com/auth/drive'] # Escopo completo para Drive
# SCOPES = ['https://www.googleapis.com/auth/drive.file'] # Escopo para arquivos criados pelo app

TOKEN_FILE = 'credentials/token.json'
CREDENTIALS_FILE = 'credentials/credentials.json' # Arquivo baixado do Google Cloud Console

def get_drive_service():
    """Mostra o fluxo de login e cria o serviço da API do Drive."""
    creds = None
    # O arquivo token.json armazena os tokens de acesso e atualização do usuário,
    # e é criado automaticamente na primeira vez que o fluxo de autorização é completado.
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    # Se não houver credenciais válidas disponíveis, deixe o usuário fazer login.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES)
            # Certifique-se de que o servidor de redirecionamento pode ser iniciado.
            # Tente uma porta diferente se a padrão (8080) estiver em uso.
            # Você pode especificar a porta assim: flow.run_local_server(port=8081)
            creds = flow.run_local_server(port=0) # port=0 escolhe uma porta livre
        # Salve as credenciais para a próxima execução
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('drive', 'v3', credentials=creds)
        return service
    except HttpError as error:
        print(f'Um erro ocorreu ao construir o serviço: {error}')
        return None

def upload_file_to_folder(service, local_file_path, folder_id, drive_filename=None):
    """Faz upload de um arquivo para uma pasta específica no Google Drive.
    Args:
        service: Objeto de serviço da API do Google Drive autorizado.
        local_file_path: Caminho para o arquivo local a ser enviado.
        folder_id: ID da pasta do Google Drive onde o arquivo será salvo.
        drive_filename: Nome que o arquivo terá no Google Drive (opcional, usa o nome local se None).
    Returns:
        ID do arquivo criado no Drive, ou None se falhar.
    """
    if not os.path.exists(local_file_path):
        print(f"Arquivo local não encontrado: {local_file_path}")
        return None

    if drive_filename is None:
        drive_filename = os.path.basename(local_file_path)

    # Adivinha o tipo MIME do arquivo
    mimetype, _ = mimetypes.guess_type(local_file_path)
    if mimetype is None:
        mimetype = 'application/octet-stream' # Tipo genérico se não puder adivinhar

    file_metadata = {
        'name': drive_filename,
        'parents': [folder_id]  # Especifica a pasta pai
    }
    media = MediaFileUpload(local_file_path,
                            mimetype=mimetype,
                            resumable=True)
    try:
        file = service.files().create(body=file_metadata,
                                      media_body=media,
                                      fields='id, name, webViewLink').execute() # Adicionado webViewLink
        print(f"Arquivo '{file.get('name')}' enviado com sucesso!")
        print(f"ID do Arquivo: {file.get('id')}")
        print(f"Link do Arquivo: {file.get('webViewLink')}")
        return file.get('id')
    except HttpError as error:
        print(f'Um erro ocorreu durante o upload: {error}')
        return None

if __name__ == '__main__':
    # --- CONFIGURAÇÕES ---
    # 1. Coloque o ID da sua pasta compartilhada aqui:
    #    Você pode pegar o ID da URL da pasta no Google Drive.
    #    Ex: se a URL é https://drive.google.com/drive/u/0/folders/SEU_ID_DA_PASTA
    #    então o folder_id é 'SEU_ID_DA_PASTA'
    TARGET_FOLDER_ID = '1qpEBWglQbUH2m6Gh1R8_AfwNnztxVYDi' # <--- SUBSTITUA ISSO

    # 2. Crie um arquivo de exemplo para testar (ou use um existente)
    example_file_path = 'meu_arquivo_de_teste.txt'
    with open(example_file_path, 'w') as f:
        f.write('Este é um arquivo de teste para o Google Drive via Python!')
    # --- FIM DAS CONFIGURAÇÕES ---

    drive_service = get_drive_service()
    if drive_service:
        print(f"Tentando fazer upload do arquivo '{example_file_path}' para a pasta ID '{TARGET_FOLDER_ID}'...")
        # Você pode especificar um nome diferente para o arquivo no Drive:
        # upload_file_to_folder(drive_service, example_file_path, TARGET_FOLDER_ID, drive_filename="arquivo_no_drive.txt")
        upload_file_to_folder(drive_service, example_file_path, TARGET_FOLDER_ID)

        # Limpeza do arquivo de exemplo (opcional)
        # if os.path.exists(example_file_path):
        #     os.remove(example_file_path)