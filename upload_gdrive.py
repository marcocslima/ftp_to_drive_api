import os
import mimetypes
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from dotenv import load_dotenv

# Importa a função resource_path do seu módulo aux
try:
    from aux import resource_path
except ImportError:
    print("ERRO: Não foi possível importar 'resource_path' de 'aux.py'.")
    print("Certifique-se de que 'aux.py' existe e está acessível.")
    # Uma implementação fallback simples se aux não for encontrado,
    # mas isso não lida com PyInstaller.
    def resource_path(relative_path):
        return os.path.abspath(relative_path)


load_dotenv()

# SCOPES e caminhos para credenciais
SCOPES = ['https://www.googleapis.com/auth/drive']
# Assume que a pasta 'credentials' está na raiz do projeto
TOKEN_FILE = resource_path('credentials/token.json')
CREDENTIALS_FILE = resource_path('credentials/credentials.json')

def get_drive_service():
    """Mostra o fluxo de login e cria o serviço da API do Drive."""
    creds = None
    
    # Criar diretório de credenciais se não existir (para token.json)
    credentials_dir = os.path.dirname(TOKEN_FILE)
    if not os.path.exists(credentials_dir):
        try:
            os.makedirs(credentials_dir)
            print(f"Diretório de credenciais '{credentials_dir}' criado.")
        except OSError as e:
            print(f"Erro ao criar diretório de credenciais '{credentials_dir}': {e}")
            return None # Não pode salvar o token.json

    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as e:
            print(f"Erro ao carregar token de '{TOKEN_FILE}': {e}. Tentando reautenticar.")
            creds = None # Força reautenticação
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print("Token expirado. Tentando atualizar...")
                creds.refresh(Request())
                print("Token atualizado com sucesso.")
            except Exception as e:
                print(f"Erro ao atualizar token: {e}. Requer nova autenticação.")
                creds = None # Força reautenticação
        
        if not creds: # Se refresh falhou ou não havia token ou token inválido
            print("Nenhuma credencial válida encontrada ou token não pôde ser atualizado. Iniciando fluxo de autenticação...")
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"ERRO CRÍTICO: Arquivo de credenciais '{CREDENTIALS_FILE}' não encontrado.")
                print("Por favor, configure o OAuth 2.0 no Google Cloud Console,")
                print("baixe o arquivo JSON de credenciais e coloque-o em 'credentials/credentials.json'.")
                return None
            
            try:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
                print("Autenticação bem-sucedida.")
            except FileNotFoundError:
                 print(f"ERRO CRÍTICO: Arquivo de credenciais '{CREDENTIALS_FILE}' especificado não foi encontrado durante o InstalledAppFlow.")
                 return None
            except Exception as e:
                print(f"Falha ao iniciar servidor local para autenticação ou outro erro no fluxo: {e}")
                return None

        # Salve as credenciais para a próxima execução
        try:
            with open(TOKEN_FILE, 'w') as token_file_handle:
                token_file_handle.write(creds.to_json())
            print(f"Token salvo em '{TOKEN_FILE}'.")
        except IOError as e:
            print(f"Erro ao salvar o token em '{TOKEN_FILE}': {e}")
            # Continuar mesmo assim, mas a próxima execução exigirá nova autenticação

    try:
        service = build('drive', 'v3', credentials=creds)
        print("Serviço do Google Drive autenticado e construído com sucesso.")
        return service
    except HttpError as error:
        print(f'Um erro HTTP ocorreu ao construir o serviço do Drive: {error}')
    except Exception as e:
        print(f'Um erro inesperado ocorreu ao construir o serviço do Drive: {e}')
    return None

def upload_file_to_folder(service, local_file_path, folder_id, drive_filename=None):
    """Faz upload de um arquivo para uma pasta específica no Google Drive."""
    if not service:
        print("ERRO: Serviço do Drive não fornecido para upload_file_to_folder.")
        return None
    if not os.path.exists(local_file_path):
        print(f"Arquivo local não encontrado para upload: {local_file_path}")
        return None

    if drive_filename is None:
        drive_filename = os.path.basename(local_file_path)

    mimetype, _ = mimetypes.guess_type(local_file_path)
    if mimetype is None:
        mimetype = 'application/octet-stream'

    file_metadata = {
        'name': drive_filename,
        'parents': [folder_id]
    }
    media = MediaFileUpload(local_file_path, mimetype=mimetype, resumable=True)
    try:
        print(f"Fazendo upload de '{local_file_path}' como '{drive_filename}' para a pasta Drive ID '{folder_id}'...")
        file = service.files().create(body=file_metadata,
                                      media_body=media,
                                      fields='id, name, webViewLink').execute()
        print(f"Arquivo '{file.get('name')}' enviado com sucesso!")
        # print(f"  ID do Arquivo no Drive: {file.get('id')}")
        # print(f"  Link do Arquivo no Drive: {file.get('webViewLink')}")
        return file.get('id')
    except HttpError as error:
        print(f'Um erro HTTP ocorreu durante o upload de "{drive_filename}": {error}')
    except Exception as e:
        print(f'Um erro inesperado ocorreu durante o upload de "{drive_filename}": {e}')
    return None

# O bloco if __name__ == '__main__': abaixo é para teste direto deste arquivo.
# Ele não será executado quando este arquivo for importado como um módulo.
if __name__ == '__main__':
    print("--- Testando módulo de upload para Google Drive (upload_gdrive.py) ---")
    
    # Carrega o ID da pasta do .env para o teste
    # Se não for definido no .env, tg_folder_id será None
    tg_folder_id_teste = os.getenv('TARGET_FOLDER_ID_TESTE', os.getenv('TARGET_FOLDER_ID'))


    if not tg_folder_id_teste:
        print("ERRO: 'TARGET_FOLDER_ID' ou 'TARGET_FOLDER_ID_TESTE' não definido no .env para teste.")
        print("Por favor, defina um ID de pasta para teste.")
    else:
        # Crie um arquivo de exemplo para testar
        example_file_path = resource_path('meu_arquivo_de_teste_drive.txt')
        try:
            with open(example_file_path, 'w') as f:
                f.write('Este é um arquivo de teste para o Google Drive via Python!')
            print(f"Arquivo de teste '{example_file_path}' criado.")

            drive_service = get_drive_service()
            if drive_service:
                print(f"Tentando fazer upload do arquivo '{example_file_path}' para a pasta Drive ID '{tg_folder_id_teste}'...")
                upload_file_to_folder(drive_service, example_file_path, tg_folder_id_teste)
            else:
                print("Não foi possível obter o serviço do Google Drive para o teste.")
        finally:
            # Limpeza do arquivo de exemplo
            if os.path.exists(example_file_path):
                os.remove(example_file_path)
                print(f"Arquivo de teste '{example_file_path}' removido.")
    print("--- Fim do teste do módulo de upload ---")