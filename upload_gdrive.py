# upload_gdrive.py

import os
import json
import tempfile
import mimetypes
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from dotenv import load_dotenv
import logging

load_dotenv()

# Configurar logging
logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/drive']

# LEIA A VARIÁVEL DE AMBIENTE AQUI E ATRIBUA A UMA CONSTANTE/VARIÁVEL PYTHON
NEW_OWNER_EMAIL = os.getenv('NEW_OWNER_EMAIL_ENV_VAR_NAME') # Use o nome exato da sua variável de ambiente

# Verifique se foi lida corretamente
if not NEW_OWNER_EMAIL:
    logger.warning("Variável de ambiente para NEW_OWNER_EMAIL não está definida! A transferência de propriedade será pulada.")

# ✅ CORREÇÃO: Usar diretórios temporários para Vercel
def get_temp_credentials_dir():
    """Retorna diretório temporário para credenciais"""
    temp_dir = tempfile.gettempdir()  # /tmp no Vercel
    creds_dir = os.path.join(temp_dir, "google_credentials")
    return creds_dir

def get_token_file_path():
    """Retorna caminho do arquivo de token"""
    creds_dir = get_temp_credentials_dir()
    return os.path.join(creds_dir, "token.json")

def get_credentials_file_path():
    """Retorna caminho do arquivo de credenciais"""
    creds_dir = get_temp_credentials_dir()
    return os.path.join(creds_dir, "credentials.json")

def get_drive_service():
    """
    Obtém o serviço do Google Drive usando OAuth ou Service Account
    Prioriza Service Account (para produção) e fallback para OAuth (desenvolvimento)
    """
    # ✅ Tentar primeiro com Service Account (para produção/Vercel)
    google_credentials_env = os.getenv('GOOGLE_CREDENTIALS')
    if google_credentials_env:
        try:
            logger.info("Tentando autenticação com Service Account (variável de ambiente)")
            credentials_info = json.loads(google_credentials_env)
            creds = ServiceAccountCredentials.from_service_account_info(
                credentials_info, scopes=SCOPES
            )
            drive_service_obj = build('drive', 'v3', credentials=creds)
            logger.info("✓ Serviço do Google Drive criado com Service Account")
            return drive_service_obj, creds
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar GOOGLE_CREDENTIALS JSON: {e}")
        except Exception as e:
            logger.error(f"Erro ao criar Service Account: {e}")

    # ✅ Fallback para OAuth (desenvolvimento local)
    logger.info("Tentando autenticação OAuth (desenvolvimento local)")
    return get_drive_service_oauth()

def get_drive_service_oauth():
    """Obtém o serviço do Google Drive usando OAuth (para desenvolvimento local)"""
    creds = None
    
    # ✅ CORREÇÃO: Usar diretórios temporários
    token_file = get_token_file_path()
    credentials_file = get_credentials_file_path()
    credentials_dir = get_temp_credentials_dir()

    # ✅ Criar diretório de credenciais se não existir
    try:
        Path(credentials_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Diretório de credenciais configurado: {credentials_dir}")
    except Exception as e:
        logger.error(f"Erro ao criar diretório de credenciais: {e}")
        return None, None

    # ✅ Carregar token existente
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
            logger.info("Token OAuth carregado")
        except ValueError as e:
            logger.error(f"Erro ao carregar token (malformado?): {e}")
            creds = None
        except Exception as e:
            logger.error(f"Erro desconhecido ao carregar token: {e}")
            creds = None

    # ✅ Verificar se precisa renovar ou criar novo token
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                logger.info("Token expirado. Atualizando...")
                creds.refresh(Request())
                logger.info("✓ Token atualizado")
            except Exception as e:
                logger.error(f"Erro ao atualizar token: {e}")
                creds = None

        # ✅ Criar novo token se necessário (apenas em desenvolvimento)
        if not creds:
            logger.info("Iniciando novo fluxo de autenticação OAuth...")
            
            # ✅ Verificar se arquivo de credenciais existe
            if not os.path.exists(credentials_file):
                logger.error(f"ERRO CRÍTICO: '{credentials_file}' não encontrado")
                logger.error("Para desenvolvimento local, coloque o arquivo credentials.json no diretório temporário")
                return None, None
                
            try:
                flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
                creds = flow.run_local_server(port=0)
                logger.info("✓ Autenticação OAuth bem-sucedida")
            except FileNotFoundError:
                logger.error(f"ERRO CRÍTICO: '{credentials_file}' não encontrado durante auth")
                return None, None
            except Exception as e:
                logger.error(f"Falha na autenticação OAuth: {e}")
                return None, None

        # ✅ Salvar token
        if creds:
            try:
                with open(token_file, 'w') as token_f:
                    token_f.write(creds.to_json())
                logger.info(f"Token salvo em '{token_file}'")
            except IOError as e:
                logger.error(f"Erro ao salvar token: {e}")

    # ✅ Verificar se as credenciais são válidas
    if not creds or not creds.valid:
        logger.error("ERRO CRÍTICO: Não foi possível obter credenciais OAuth válidas")
        return None, None

    # ✅ Criar serviço do Drive
    try:
        drive_service_obj = build('drive', 'v3', credentials=creds)
        logger.info("✓ Serviço do Google Drive construído com OAuth")
        return drive_service_obj, creds
    except HttpError as e:
        logger.error(f'Erro HTTP ao construir serviço Drive: {e.resp.status} - {e.content.decode()}')
        return None, None
    except Exception as e:
        logger.error(f'Erro inesperado ao construir serviço Drive: {e}')
        return None, None

def upload_file_to_folder(service, local_file_path, folder_id, drive_filename=None):
    """
    Faz upload de um arquivo para uma pasta específica no Google Drive
    e tenta transferir a propriedade ou compartilhar como editor.

    Args:
        service: Serviço do Google Drive
        local_file_path: Caminho do arquivo local
        folder_id: ID da pasta no Drive
        drive_filename: Nome do arquivo no Drive (opcional)

    Returns:
        str: ID do arquivo no Drive se sucesso, None se falha
    """
    if not service:
        logger.error("Serviço Drive não fornecido para upload")
        return None

    if not os.path.exists(local_file_path):
        logger.error(f"Arquivo local não encontrado: {local_file_path}")
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

    file_id = None # Inicializa file_id aqui para o bloco finally

    try:
        logger.info(f"Iniciando upload: '{os.path.basename(local_file_path)}' -> '{drive_filename}'")
        media = MediaFileUpload(local_file_path, mimetype=mimetype, resumable=True)
        
        file_obj = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name' # Pedir id e name de volta
        ).execute()

        file_id = file_obj.get('id')
        file_name_uploaded = file_obj.get('name') # Nome como foi salvo no Drive

        if not file_id:
            logger.error(f"Falha ao obter ID do arquivo '{file_name_uploaded}' após upload.")
            return None
            
        logger.info(f"✓ Upload concluído: '{file_name_uploaded}' (ID: {file_id})")

        # --- INÍCIO DA LÓGICA DE TRANSFERÊNCIA/COMPARTILHAMENTO ---
        if NEW_OWNER_EMAIL:
            logger.info(f"Tentando transferir propriedade do arquivo '{file_name_uploaded}' (ID: {file_id}) para {NEW_OWNER_EMAIL}")
            try:
                permission_body = {
                    'role': 'owner',
                    'type': 'user',
                    'emailAddress': NEW_OWNER_EMAIL
                }
                service.permissions().create(
                    fileId=file_id,
                    body=permission_body,
                    transferOwnership=True,
                    # sendNotificationEmail=False, # Opcional
                    supportsAllDrives=True # Boa prática
                ).execute()
                logger.info(f"✓ Propriedade do arquivo '{file_name_uploaded}' transferida para {NEW_OWNER_EMAIL}")
            
            except HttpError as e_owner:
                logger.warning(f"Falha ao transferir propriedade para {NEW_OWNER_EMAIL}. Erro: {e_owner.resp.status} - {e_owner.content.decode()}")
                logger.info(f"Tentando compartilhar '{file_name_uploaded}' (ID: {file_id}) com {NEW_OWNER_EMAIL} como editor (writer)...")
                try:
                    editor_permission_body = {
                        'role': 'writer', # Papel de editor
                        'type': 'user',
                        'emailAddress': NEW_OWNER_EMAIL
                    }
                    service.permissions().create(
                        fileId=file_id,
                        body=editor_permission_body,
                        # sendNotificationEmail=False, # Opcional
                        supportsAllDrives=True
                    ).execute()
                    logger.info(f"✓ Arquivo '{file_name_uploaded}' compartilhado com {NEW_OWNER_EMAIL} como editor.")
                except HttpError as e_writer:
                    logger.error(f"Falha ao compartilhar como editor com {NEW_OWNER_EMAIL}. Erro: {e_writer.resp.status} - {e_writer.content.decode()}")
                    # Mesmo se o compartilhamento falhar, o upload foi um sucesso, então retorne o file_id
                except Exception as e_writer_generic:
                    logger.error(f"Erro inesperado ao compartilhar como editor com {NEW_OWNER_EMAIL}: {e_writer_generic}")
            except Exception as e_owner_generic:
                logger.error(f"Erro inesperado ao tentar transferir propriedade para {NEW_OWNER_EMAIL}: {e_owner_generic}")
        else:
            logger.warning("NEW_OWNER_EMAIL não definido. Propriedade não será transferida.")
        # --- FIM DA LÓGICA DE TRANSFERÊNCIA/COMPARTILHAMENTO ---

        return file_id

    except HttpError as e:
        logger.error(f'Erro HTTP no upload "{drive_filename}": {e.resp.status} - {e.content.decode()}')
        return None
    except Exception as e:
        logger.error(f'Erro inesperado no upload "{drive_filename}": {e}')
        return None

def test_drive_connection():
    """Testa a conexão com o Google Drive"""
    try:
        service, creds = get_drive_service()
        if not service:
            return False, "Falha ao obter serviço do Drive"

        # ✅ Testar listando arquivos (apenas 1 para teste)
        results = service.files().list(pageSize=1, fields="files(id, name)").execute()
        files = results.get('files', [])

        logger.info("✓ Conexão com Google Drive testada com sucesso")
        return True, f"Conexão OK. Teste retornou {len(files)} arquivo(s)"

    except Exception as e:
        logger.error(f"Erro no teste de conexão: {e}")
        return False, str(e)


# NOVA função (da resposta anterior, certifique-se que ela está no seu código)
def limpar_pasta_drive(service, folder_id):
    """
    Move todos os arquivos e subpastas de uma pasta específica no Google Drive para a lixeira.

    Args:
        service: Objeto de serviço do Google Drive autenticado.
        folder_id: ID da pasta do Google Drive a ser limpa.

    Returns:
        tuple: (int total_itens_processados, int itens_excluidos_sucesso, int itens_falha_excluir)
    """
    if not service:
        logger.error("Serviço Drive não fornecido para limpar_pasta_drive.")
        return 0, 0, 0
    if not folder_id:
        logger.error("Folder ID não fornecido para limpar_pasta_drive.")
        return 0, 0, 0

    logger.info(f"--- Iniciando limpeza da pasta do Drive ID: {folder_id} ---")
    page_token = None
    itens_processados = 0
    sucesso_excluir = 0
    falha_excluir = 0

    try:
        while True:
            response = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType)',
                pageToken=page_token
            ).execute()

            items = response.get('files', [])
            if not items and page_token is None: # Adicionado 'and page_token is None' para primeira chamada
                logger.info(f"Nenhum item encontrado na pasta {folder_id} para limpar (ou pasta já vazia).")
                break
            if not items and page_token is not None: # Se não há mais itens na paginação
                 break


            for item in items:
                itens_processados += 1
                item_id = item.get('id')
                item_name = item.get('name')
                item_type = "Pasta" if item.get('mimeType') == 'application/vnd.google-apps.folder' else "Arquivo"
                logger.info(f"  Tentando mover para a lixeira: {item_type} '{item_name}' (ID: {item_id})")
                try:
                    service.files().update(fileId=item_id, body={'trashed': True}).execute()
                    logger.info(f"    ✓ SUCESSO: {item_type} '{item_name}' movido para a lixeira.")
                    sucesso_excluir += 1
                except HttpError as e_trash:
                    logger.error(f"    ✗ FALHA ao mover {item_type} '{item_name}' para a lixeira. Erro: {e_trash.resp.status} - {e_trash.content.decode()}")
                    falha_excluir += 1
                except Exception as e_generic:
                    logger.error(f"    ✗ FALHA INESPERADA ao mover {item_type} '{item_name}' para a lixeira: {e_generic}")
                    falha_excluir += 1

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        
        logger.info(f"--- Limpeza da pasta {folder_id} concluída ---")
        logger.info(f"Total de itens processados: {itens_processados}")
        logger.info(f"Itens movidos para a lixeira com sucesso: {sucesso_excluir}")
        logger.info(f"Falhas ao mover para a lixeira: {falha_excluir}")

    except HttpError as e_list:
        logger.error(f"Erro HTTP ao listar itens da pasta {folder_id} para limpeza: {e_list.resp.status} - {e_list.content.decode()}")
    except Exception as e_main:
        logger.error(f"Erro inesperado durante a limpeza da pasta {folder_id}: {e_main}")

    return itens_processados, sucesso_excluir, falha_excluir


# Sua função upload_files_to_drive MODIFICADA
def upload_files_to_drive(pasta_arquivos, drive_service):
    """
    Limpa a pasta de destino no Google Drive e depois faz upload de todos os
    arquivos de uma pasta local para essa pasta no Google Drive.
    
    Args:
        pasta_arquivos: Caminho da pasta local com arquivos para upload.
        drive_service: Serviço do Google Drive autenticado.
        
    Returns:
        dict: Resultado do upload e informações sobre a limpeza.
    """
    target_folder_id = os.getenv('TARGET_FOLDER_ID')
    if not target_folder_id:
        logger.error("TARGET_FOLDER_ID não definido para upload_files_to_drive.")
        return {
            "arquivos_enviados": 0,
            "arquivos_com_erro_upload": 0,
            "erro_upload": "TARGET_FOLDER_ID não definido"
        }

    if not drive_service: # Adicionado verificação do drive_service
        logger.error("Serviço Drive não fornecido para upload_files_to_drive.")
        return {
            "arquivos_enviados": 0,
            "arquivos_com_erro_upload": 0,
            "erro_upload": "Serviço Drive não fornecido"
        }

    if not os.path.exists(pasta_arquivos):
        logger.error(f"Pasta local de origem não encontrada: {pasta_arquivos}")
        return {
            "arquivos_enviados": 0,
            "arquivos_com_erro_upload": 0,
            "erro_upload": f"Pasta local de origem não encontrada: {pasta_arquivos}"
        }

    # --- ETAPA DE LIMPEZA DA PASTA DE DESTINO ---
    logger.info(f"Iniciando limpeza prévia da pasta de destino do Drive ID: {target_folder_id}")
    # A função limpar_pasta_drive retorna (itens_processados, sucesso_excluir, falha_excluir)
    _, sucesso_limpeza, falhas_limpeza = limpar_pasta_drive(drive_service, target_folder_id)
    
    if falhas_limpeza > 0:
        logger.warning(f"Houveram {falhas_limpeza} falhas ao tentar limpar a pasta de destino. O upload prosseguirá, mas a pasta pode não estar completamente vazia.")
    elif sucesso_limpeza > 0:
        logger.info(f"Limpeza prévia da pasta de destino concluída com sucesso. {sucesso_limpeza} item(ns) movido(s) para a lixeira.")
    else: # sucesso_limpeza == 0 e falhas_limpeza == 0
        logger.info("Pasta de destino já estava vazia ou nenhum item foi encontrado para limpar.")
    # --- FIM DA ETAPA DE LIMPEZA ---

    arquivos_enviados = 0
    arquivos_com_erro_upload = 0 # Renomeado para clareza

    logger.info(f"Iniciando upload dos arquivos de '{pasta_arquivos}' para a pasta Drive ID: {target_folder_id}")
    try:
        for arquivo_nome_local in os.listdir(pasta_arquivos):
            arquivo_path_local = os.path.join(pasta_arquivos, arquivo_nome_local)
            
            if os.path.isfile(arquivo_path_local):
                # upload_file_to_folder é sua função que faz o upload de um único arquivo
                # e lida com a tentativa de mudança de proprietário/permissão.
                if upload_file_to_folder(drive_service, arquivo_path_local, target_folder_id):
                    arquivos_enviados += 1
                else:
                    arquivos_com_erro_upload += 1
            else:
                logger.info(f"Item '{arquivo_nome_local}' em '{pasta_arquivos}' não é um arquivo, pulando upload.")


        logger.info(f"Upload em lote concluído: {arquivos_enviados} arquivo(s) enviado(s) com sucesso, {arquivos_com_erro_upload} falha(s) no upload.")
        return {
            "arquivos_enviados": arquivos_enviados,
            "arquivos_com_erro_upload": arquivos_com_erro_upload,
            "falhas_limpeza_pasta_destino": falhas_limpeza # Adicionando informação da limpeza
        }

    except Exception as e:
        logger.error(f"Erro crítico durante o processo de upload em lote: {e}")
        return {
            "arquivos_enviados": arquivos_enviados,
            "arquivos_com_erro_upload": arquivos_com_erro_upload,
            "falhas_limpeza_pasta_destino": falhas_limpeza,
            "erro_upload": str(e)
        }
    
    
def upload_devolucaoar_files_to_drive(lista_arquivos, drive_service):
    """
    Faz upload de arquivos DevolucaoAR para pasta específica
    
    Args:
        lista_arquivos: Lista de caminhos de arquivos
        drive_service: Serviço do Google Drive
        
    Returns:
        dict: Resultado do upload
    """
    target_folder_id = os.getenv('TARGET_FOLDER_ID_DEVOLUCAOAR_ARCHIVE')
    if not target_folder_id:
        logger.error("TARGET_FOLDER_ID_DEVOLUCAOAR_ARCHIVE não definido")
        return {"arquivos_enviados": 0, "erro": "TARGET_FOLDER_ID_DEVOLUCAOAR_ARCHIVE não definido"}

    arquivos_enviados = 0
    arquivos_com_erro = 0

    try:
        for arquivo_path in lista_arquivos:
            if os.path.exists(arquivo_path):
                if upload_file_to_folder(drive_service, arquivo_path, target_folder_id):
                    arquivos_enviados += 1
                else:
                    arquivos_com_erro += 1
            else:
                logger.warning(f"Arquivo DevolucaoAR não encontrado: {arquivo_path}")
                arquivos_com_erro += 1

        logger.info(f"Upload DevolucaoAR concluído: {arquivos_enviados} sucesso(s), {arquivos_com_erro} erro(s)")
        return {
            "arquivos_enviados": arquivos_enviados,
            "arquivos_com_erro": arquivos_com_erro
        }

    except Exception as e:
        logger.error(f"Erro durante upload DevolucaoAR: {e}")
        return {"arquivos_enviados": arquivos_enviados, "erro": str(e)}

def main():
    """Função main para compatibilidade com a API"""
    return get_drive_service()

if __name__ == '__main__':
    print("--- Testando módulo upload_gdrive.py ---")

    # ✅ Teste de conexão
    print("\n--- Teste de Conexão ---")
    sucesso, mensagem = test_drive_connection()
    print(f"Resultado: {mensagem}")

    # ✅ Teste de upload (se configurado)
    tg_folder_id_test = os.getenv('TARGET_FOLDER_ID_TESTE_DRIVE', os.getenv('TARGET_FOLDER_ID'))
    if not tg_folder_id_test:
        print("\nAVISO: TARGET_FOLDER_ID_TESTE_DRIVE não definida para teste de upload")
    else:
        print("\n--- Teste de Upload ---")
        
        # ✅ CORREÇÃO: Usar diretório temporário para arquivo de teste
        temp_dir = tempfile.gettempdir()
        example_file = os.path.join(temp_dir, 'test_drive_upload.txt')
        
        try:
            # Criar arquivo de teste
            with open(example_file, 'w') as f:
                f.write('Teste upload Drive!')

            # Testar upload
            test_drive_svc, test_drive_creds = get_drive_service()
            if test_drive_svc:
                result = upload_file_to_folder(test_drive_svc, example_file, tg_folder_id_test)
                if result:
                    print(f"✓ Upload de teste bem-sucedido (ID: {result})")
                else:
                    print("✗ Falha no upload de teste")
            else:
                print("✗ Falha ao obter serviço Drive para teste")

        except Exception as e:
            print(f"Erro no teste de upload: {e}")
        finally:
            # Limpar arquivo de teste
            if os.path.exists(example_file):
                os.remove(example_file)
                print("Arquivo de teste removido")

    print("\n--- Fim dos testes upload_gdrive.py ---")