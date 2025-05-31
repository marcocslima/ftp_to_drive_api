# files_to_drive.py
import os
from dotenv import load_dotenv
import time
import logging

try:
    import ecarta_processor
    import upload_gdrive as gdrive_uploader # Módulo do Drive
except ImportError as e:
    logging.error(f"ERRO de importação: {e}")
    raise

load_dotenv()

# Configurar logging
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES DE PASTAS DO DRIVE ---
TARGET_DRIVE_FOLDER_ID_PRINCIPAL = os.getenv('TARGET_FOLDER_ID')
TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE = os.getenv('TARGET_FOLDER_ID_DEVOLUCAOAR_ARCHIVE')

# --- CONFIGURAÇÕES DE FTP ---
HOST_FTP = os.getenv('HOST')
PORT_FTP = int(os.getenv('PORT', 21))
USUARIO_FTP = os.getenv('USER_ECARTA')
SENHA_FTP = os.getenv('PASSWORD')
DIRETORIO_FTP = os.getenv('DIRECTORY')

def validar_configuracoes():
    """Valida se todas as configurações necessárias estão definidas"""
    erros = []

    if not TARGET_DRIVE_FOLDER_ID_PRINCIPAL:
        erros.append("TARGET_FOLDER_ID (pasta principal do Drive) não definido no .env")

    if not TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE:
        erros.append("TARGET_FOLDER_ID_DEVOLUCAOAR_ARCHIVE (pasta de arquivo DevolucaoAR) não definido no .env")

    if not HOST_FTP:
        erros.append("HOST do FTP não definido no .env")

    if not USUARIO_FTP:
        erros.append("USER_ECARTA não definido no .env")

    if not SENHA_FTP:
        erros.append("PASSWORD do FTP não definido no .env")

    if erros:
        for erro in erros:
            logger.error(f"ERRO: {erro}")
        raise ValueError(f"Configurações inválidas: {'; '.join(erros)}")

    return True

def processar_files_to_drive():
    """
    Função principal que processa arquivos do FTP para o Drive
    Retorna um dicionário com o resultado do processamento
    """
    logger.info("--- Iniciando fluxo: Processamento eCarta, Uploads para Google Drive, Limpeza FTP ---")
    start_time_total = time.perf_counter()

    resultado = {
        "sucesso": False,
        "etapas": {
            "validacao": False,
            "drive_service": False,
            "processamento_local": False,
            "upload_pdfs_finais": False,
            "upload_arquivos_devolucaoAR": False,
            "exclusao_ftp": False
        },
        "detalhes": {},
        "tempo_total": 0,
        "mensagem": ""
    }

    try:
        # Validação inicial das configurações
        validar_configuracoes()
        resultado["etapas"]["validacao"] = True
        logger.info("✓ Configurações validadas com sucesso")

        # Obter serviço do Drive e Credenciais
        logger.info("Obtendo serviço do Google Drive...")
        drive_service, drive_credentials = gdrive_uploader.get_drive_service()

        if not drive_service or not drive_credentials:
            raise Exception("Falha ao obter o serviço do Google Drive ou credenciais")

        resultado["etapas"]["drive_service"] = True
        logger.info("✓ Serviço do Google Drive obtido com sucesso")

        # 1. Processar arquivos eCarta
        logger.info("\n--- Fase 1: Processamento de arquivos eCarta ---")
        resultado_proc = ecarta_processor.processar_arquivos_ecarta_ftp()

        if resultado_proc is None or resultado_proc[0] is None:
            raise Exception("Processamento eCarta falhou ou não retornou pasta de arquivos")

        pasta_pdfs_finais, nomes_todos_arquivos_baixados_ftp, caminhos_locais_devolucaoAR_originais = resultado_proc

        if not os.path.isdir(pasta_pdfs_finais):
            raise Exception(f"Pasta de PDFs finais '{pasta_pdfs_finais}' não é um diretório válido")

        resultado["etapas"]["processamento_local"] = True
        resultado["detalhes"]["pasta_pdfs_finais"] = pasta_pdfs_finais
        resultado["detalhes"]["arquivos_baixados_ftp"] = len(nomes_todos_arquivos_baixados_ftp) if nomes_todos_arquivos_baixados_ftp else 0
        logger.info("✓ Processamento local dos arquivos concluído")

        # 2. Upload dos PDFs FINAIS para a pasta principal do Drive
        arquivos_para_upload_principal = [
            os.path.join(root, fn) for root, _, fns in os.walk(pasta_pdfs_finais)
            for fn in fns if os.path.isfile(os.path.join(root, fn))
        ]

        if arquivos_para_upload_principal:
            logger.info(f"\n--- Fase 2.1: Upload de {len(arquivos_para_upload_principal)} PDFs FINAIS para Drive ---")
            sucesso = 0
            falha = 0

            for arq_path in arquivos_para_upload_principal:
                if gdrive_uploader.upload_file_to_folder(drive_service, arq_path, TARGET_DRIVE_FOLDER_ID_PRINCIPAL):
                    sucesso += 1
                else:
                    falha += 1

            logger.info(f"Uploads de PDFs finais: {sucesso} sucesso(s), {falha} falha(s)")
            resultado["detalhes"]["upload_pdfs"] = {"sucesso": sucesso, "falha": falha}

            if falha == 0:
                resultado["etapas"]["upload_pdfs_finais"] = True
                logger.info("✓ Upload de PDFs finais concluído com sucesso")
            else:
                raise Exception(f"Falhas no upload dos PDFs finais: {falha} arquivo(s)")
        else:
            logger.info("Nenhum PDF final para upload na pasta principal do Drive")
            resultado["etapas"]["upload_pdfs_finais"] = True
            resultado["detalhes"]["upload_pdfs"] = {"sucesso": 0, "falha": 0}

        # 3. Upload dos ARQUIVOS DEVOLUCAOAR ORIGINAIS
        if caminhos_locais_devolucaoAR_originais:
            logger.info(f"\n--- Fase 2.2: Upload de {len(caminhos_locais_devolucaoAR_originais)} ARQUIVOS DEVOLUCAOAR ORIGINAIS ---")
            sucesso_dev = 0
            falha_dev = 0

            for arq_dev_path in caminhos_locais_devolucaoAR_originais:
                if os.path.exists(arq_dev_path):
                    if gdrive_uploader.upload_file_to_folder(drive_service, arq_dev_path, TARGET_DRIVE_FOLDER_ID_DEVOLUCAOAR_ARCHIVE):
                        sucesso_dev += 1
                    else:
                        falha_dev += 1
                else:
                    logger.warning(f"Arquivo DevolucaoAR original '{arq_dev_path}' não encontrado")
                    falha_dev += 1

            logger.info(f"Uploads de arquivos DevolucaoAR: {sucesso_dev} sucesso(s), {falha_dev} falha(s)")
            resultado["detalhes"]["upload_devolucaoAR"] = {"sucesso": sucesso_dev, "falha": falha_dev}

            if falha_dev == 0:
                resultado["etapas"]["upload_arquivos_devolucaoAR"] = True
                logger.info("✓ Upload de arquivos DevolucaoAR concluído com sucesso")
            else:
                raise Exception(f"Falhas no upload dos arquivos DevolucaoAR: {falha_dev} arquivo(s)")
        else:
            logger.info("Nenhum arquivo DevolucaoAR original para upload")
            resultado["etapas"]["upload_arquivos_devolucaoAR"] = True
            resultado["detalhes"]["upload_devolucaoAR"] = {"sucesso": 0, "falha": 0}

        # 4. Excluir arquivos do FTP se tudo deu certo
        if nomes_todos_arquivos_baixados_ftp:
            logger.info(f"\n--- Fase 3: Exclusão de {len(nomes_todos_arquivos_baixados_ftp)} arquivos do servidor FTP ---")
            ecarta_processor.excluir_arquivos_do_ftp(
                HOST_FTP, PORT_FTP, USUARIO_FTP, SENHA_FTP, DIRETORIO_FTP,
                nomes_todos_arquivos_baixados_ftp
            )
            resultado["etapas"]["exclusao_ftp"] = True
            resultado["detalhes"]["arquivos_excluidos_ftp"] = len(nomes_todos_arquivos_baixados_ftp)
            logger.info("✓ Exclusão de arquivos do FTP concluída")
        else:
            logger.warning("Lista de arquivos para excluir do FTP está vazia")
            resultado["etapas"]["exclusao_ftp"] = True

        # Sucesso geral
        resultado["sucesso"] = True
        resultado["mensagem"] = "Processamento concluído com sucesso"

    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")
        resultado["mensagem"] = f"Erro: {str(e)}"
        resultado["sucesso"] = False

    finally:
        end_time_total = time.perf_counter()
        resultado["tempo_total"] = round(end_time_total - start_time_total, 2)
        logger.info(f"\n--- Processo completo ---\nTempo total: {resultado['tempo_total']} segundos")

    return resultado

def main():
    """Função main para compatibilidade com a API e execução direta"""
    return processar_files_to_drive()

if __name__ == "__main__":
    resultado = main()

    if resultado["sucesso"]:
        print("✓ Processamento concluído com sucesso!")
        print(f"Tempo total: {resultado['tempo_total']} segundos")
    else:
        print(f"✗ Erro no processamento: {resultado['mensagem']}")
        exit(1)