# ecarta_processor.py

from ftplib import FTP
import zipfile
import os
import shutil
from dotenv import load_dotenv
import logging

load_dotenv()

# Configurar logging
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
def resource_path(relative_to_base_dir):
    return os.path.join(BASE_DIR, relative_to_base_dir)

HOST_FTP = os.getenv('HOST')
PORT_FTP = int(os.getenv('PORT', 21))
USUARIO_FTP = os.getenv('USER_ECARTA')
SENHA_FTP = os.getenv('PASSWORD')
DIRETORIO_FTP = os.getenv('DIRECTORY')

DOWNLOADS_FOLDER = resource_path('arquivos/downloads')
UNZIP_FILES_FOLDER = resource_path('arquivos/unzip_files')
TMP_FOLDER = resource_path('arquivos/tmp')

def limpar_e_recriar_pasta(folder_path):
    """Limpa e recria uma pasta"""
    if os.path.exists(folder_path):
        try:
            shutil.rmtree(folder_path)
            logger.info(f"Pasta '{folder_path}' removida")
        except OSError as e:
            logger.error(f"Erro ao remover pasta '{folder_path}': {e}")
            raise
    try:
        os.makedirs(folder_path, exist_ok=True)
        logger.info(f"Pasta '{folder_path}' criada")
    except OSError as e:
        logger.error(f"Erro crítico ao criar pasta '{folder_path}': {e}")
        raise

def download_files_from_ftp(host, port, usuario, senha, remote_directory, local_downloads_folder):
    """Baixa arquivos do FTP"""
    if not os.path.isdir(local_downloads_folder):
        os.makedirs(local_downloads_folder, exist_ok=True)

    arquivos_baixados_info = []
    try:
        with FTP() as ftp:
            ftp.connect(host, port)
            ftp.login(usuario, senha)
            ftp.cwd(remote_directory)
            logger.info(f"Conectado ao FTP: {host}, diretório: {remote_directory}")

            files_in_remote_dir = ftp.nlst()
            logger.info(f"Arquivos encontrados no FTP: {len(files_in_remote_dir)} arquivo(s)")

            for file_name in files_in_remote_dir:
                local_file_path = os.path.join(local_downloads_folder, file_name)
                try:
                    with open(local_file_path, "wb") as local_file:
                        logger.info(f"Baixando {file_name}...")
                        ftp.retrbinary(f"RETR {file_name}", local_file.write)
                    logger.info(f"✓ {file_name} baixado com sucesso")
                    arquivos_baixados_info.append({"nome_ftp": file_name, "caminho_local": local_file_path})
                except Exception as e_dl:
                    logger.error(f"Erro ao baixar '{file_name}': {e_dl}")

        logger.info(f"Download concluído: {len(arquivos_baixados_info)} arquivo(s) baixado(s)")
        return arquivos_baixados_info
    except Exception as e:
        logger.error(f"Erro na operação FTP (download): {e}")
        return []

def descompactar_zip(caminho_arquivo_zip, pasta_destino):
    """Descompacta um arquivo ZIP"""
    if not (os.path.exists(caminho_arquivo_zip) and zipfile.is_zipfile(caminho_arquivo_zip)):
        logger.error(f"Arquivo ZIP inválido ou não encontrado: {caminho_arquivo_zip}")
        return False

    os.makedirs(pasta_destino, exist_ok=True)
    try:
        with zipfile.ZipFile(caminho_arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(pasta_destino)
            logger.info(f"✓ Descompactado '{os.path.basename(caminho_arquivo_zip)}' em '{pasta_destino}'")
            return True
    except Exception as e:
        logger.error(f"Erro ao descompactar '{caminho_arquivo_zip}': {e}")
        return False

def excluir_arquivos_do_ftp(host, port, usuario, senha, remote_directory, lista_nomes_arquivos_para_excluir):
    """Exclui arquivos do FTP"""
    if not lista_nomes_arquivos_para_excluir:
        logger.info("Nenhum arquivo especificado para exclusão no FTP")
        return

    logger.info(f"Iniciando exclusão de {len(lista_nomes_arquivos_para_excluir)} arquivos no FTP")
    excluidos_com_sucesso = 0
    erros_exclusao = 0

    try:
        with FTP() as ftp:
            ftp.connect(host, port)
            ftp.login(usuario, senha)
            ftp.cwd(remote_directory)
            logger.info("Conectado ao FTP para exclusão")

            for nome_arquivo in lista_nomes_arquivos_para_excluir:
                try:
                    ftp.delete(nome_arquivo)
                    logger.info(f"✓ Arquivo '{nome_arquivo}' excluído do FTP")
                    excluidos_com_sucesso += 1
                except Exception as e_del:
                    logger.error(f"Erro ao excluir '{nome_arquivo}' do FTP: {e_del}")
                    erros_exclusao += 1

        logger.info(f"Exclusão concluída: {excluidos_com_sucesso} sucesso(s), {erros_exclusao} erro(s)")
    except Exception as e:
        logger.error(f"Erro durante operação de exclusão no FTP: {e}")

def processar_arquivos_ecarta_ftp():
    """
    Função principal que processa arquivos eCarta do FTP
    Retorna: (pasta_pdfs_finais, nomes_arquivos_ftp_para_excluir, caminhos_devolucaoAR_originais)
    """
    logger.info("Iniciando processo de tratamento de arquivos eCarta")
    caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar = []
    nomes_todos_arquivos_baixados_ftp = []

    try:
        # Limpeza inicial das pastas
        logger.info("Limpando e recriando pastas de trabalho")
        limpar_e_recriar_pasta(DOWNLOADS_FOLDER)
        limpar_e_recriar_pasta(UNZIP_FILES_FOLDER)
        limpar_e_recriar_pasta(TMP_FOLDER)
    except Exception as e_limpeza:
        logger.error(f"Erro crítico durante limpeza inicial: {e_limpeza}")
        raise Exception(f"Falha na limpeza inicial: {e_limpeza}")

    # Etapa 1: Download de arquivos do FTP
    logger.info("--- Etapa 1: Download de arquivos do FTP ---")
    info_arquivos_baixados = download_files_from_ftp(
        HOST_FTP, PORT_FTP, USUARIO_FTP, SENHA_FTP, DIRETORIO_FTP, DOWNLOADS_FOLDER
    )

    if not info_arquivos_baixados:
        logger.warning("Nenhum arquivo baixado do FTP")
        return UNZIP_FILES_FOLDER, [], []

    # Identificar arquivos DevolucaoAR e preparar lista de exclusão
    for info_arquivo in info_arquivos_baixados:
        nomes_todos_arquivos_baixados_ftp.append(info_arquivo["nome_ftp"])
        if "devolucaoar" in info_arquivo["nome_ftp"].lower():
            caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar.append(info_arquivo["caminho_local"])

    logger.info(f"Arquivos DevolucaoAR originais identificados: {len(caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar)}")

    # Etapa 2: Processamento dos arquivos ZIP
    logger.info("--- Etapa 2: Processamento dos arquivos ZIP baixados ---")
    arquivos_zip_para_processar_info = [info for info in info_arquivos_baixados if info["nome_ftp"].lower().endswith('.zip')]

    if not arquivos_zip_para_processar_info:
        logger.info("Nenhum arquivo .zip encontrado para processar")
        return UNZIP_FILES_FOLDER, nomes_todos_arquivos_baixados_ftp, caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar

    for info_zip in arquivos_zip_para_processar_info:
        nome_arquivo_zip = info_zip["nome_ftp"]
        caminho_zip_original_em_downloads = info_zip["caminho_local"]

        if not os.path.exists(caminho_zip_original_em_downloads):
            logger.warning(f"Arquivo ZIP '{nome_arquivo_zip}' não encontrado. Pulando")
            continue

        caminho_zip_para_processar_em_tmp = os.path.join(TMP_FOLDER, nome_arquivo_zip)
        logger.info(f">>> Processando arquivo ZIP: {nome_arquivo_zip} <<<")

        try:
            # Copiar DevolucaoAR, Mover os outros
            if "devolucaoar" in nome_arquivo_zip.lower():
                logger.info(f"Copiando '{nome_arquivo_zip}' (DevolucaoAR) para processamento")
                shutil.copy2(caminho_zip_original_em_downloads, caminho_zip_para_processar_em_tmp)
            else:
                logger.info(f"Movendo '{nome_arquivo_zip}' para processamento")
                shutil.move(caminho_zip_original_em_downloads, caminho_zip_para_processar_em_tmp)

            # Descompactar
            if not descompactar_zip(caminho_zip_para_processar_em_tmp, TMP_FOLDER):
                logger.error(f"Falha ao descompactar '{nome_arquivo_zip}'. Pulando")
                if os.path.exists(caminho_zip_para_processar_em_tmp):
                    os.remove(caminho_zip_para_processar_em_tmp)
                continue

            # Procurar arquivo DevolucaoAR.txt
            arquivo_devolucao_ar_txt_path = None
            for item in os.listdir(TMP_FOLDER):
                if "devolucaoar" in item.lower() and item.lower().endswith(".txt"):
                    arquivo_devolucao_ar_txt_path = os.path.join(TMP_FOLDER, item)
                    break

            if arquivo_devolucao_ar_txt_path:
                logger.info(f"Arquivo DevolucaoAR.txt encontrado: {os.path.basename(arquivo_devolucao_ar_txt_path)}")
                # Processar arquivo DevolucaoAR.txt
                linhas_do_arquivo_devolucao = []
                try:
                    with open(arquivo_devolucao_ar_txt_path, 'r', encoding='latin-1') as f_txt:
                        linhas_do_arquivo_devolucao = [line.strip() for line in f_txt if line.strip()]
                except UnicodeDecodeError:
                    try:
                         with open(arquivo_devolucao_ar_txt_path, 'r', encoding='utf-8') as f_txt:
                             linhas_do_arquivo_devolucao = [line.strip() for line in f_txt if line.strip()]
                    except Exception as e_decode:
                        logger.error(f"Erro ao ler '{arquivo_devolucao_ar_txt_path}': {e_decode}")
                        continue

                pdfs_processados = 0
                for idx, linha_dados in enumerate(linhas_do_arquivo_devolucao):
                    try:
                        campos = linha_dados.split('|')
                        if len(campos) < 7: continue
                        nome_pdf_original = campos[6].strip()
                        novo_nome_pdf_base = campos[3].strip()
                        novo_nome_pdf = f"{novo_nome_pdf_base}.pdf" if not novo_nome_pdf_base.lower().endswith('.pdf') else novo_nome_pdf_base
                        pdf_orig_tmp = os.path.join(TMP_FOLDER, nome_pdf_original)
                        pdf_dest_unzip = os.path.join(UNZIP_FILES_FOLDER, novo_nome_pdf)
                        os.makedirs(UNZIP_FILES_FOLDER, exist_ok=True)
                        if os.path.exists(pdf_orig_tmp):
                            shutil.move(pdf_orig_tmp, pdf_dest_unzip)
                            pdfs_processados += 1
                        else:
                            logger.warning(f"PDF '{nome_pdf_original}' não encontrado em tmp")
                    except Exception as e_linha:
                        logger.error(f"Erro ao processar linha DevolucaoAR: {e_linha}")

                logger.info(f"✓ {pdfs_processados} PDFs processados com base no DevolucaoAR.txt")
                os.remove(arquivo_devolucao_ar_txt_path)
            else:
                logger.info(f"Nenhum 'DevolucaoAR.txt' encontrado. Movendo conteúdo para UNZIP")
                os.makedirs(UNZIP_FILES_FOLDER, exist_ok=True)
                arquivos_movidos = 0
                for item_descompactado in os.listdir(TMP_FOLDER):
                    orig_item_tmp = os.path.join(TMP_FOLDER, item_descompactado)
                    if item_descompactado == nome_arquivo_zip: continue
                    dest_item_unzip = os.path.join(UNZIP_FILES_FOLDER, item_descompactado)
                    try:
                        if os.path.isfile(orig_item_tmp):
                            shutil.move(orig_item_tmp, dest_item_unzip)
                            arquivos_movidos += 1
                        elif os.path.isdir(orig_item_tmp):
                            if os.path.isdir(dest_item_unzip):
                                for sub_item in os.listdir(orig_item_tmp):
                                    shutil.move(os.path.join(orig_item_tmp, sub_item), dest_item_unzip)
                                shutil.rmtree(orig_item_tmp)
                            else:
                                shutil.move(orig_item_tmp, UNZIP_FILES_FOLDER)
                            arquivos_movidos += 1
                    except Exception as e_mv:
                        logger.error(f"Erro ao mover {item_descompactado}: {e_mv}")
                logger.info(f"✓ {arquivos_movidos} itens movidos para UNZIP")

            # Remover ZIP da pasta TMP
            if os.path.exists(caminho_zip_para_processar_em_tmp):
                os.remove(caminho_zip_para_processar_em_tmp)
                logger.info(f"ZIP '{nome_arquivo_zip}' removido da pasta TMP")

        except Exception as e_process_zip:
            logger.error(f"ERRO CRÍTICO ao processar ZIP '{nome_arquivo_zip}': {e_process_zip}")
            import traceback
            traceback.print_exc()
        finally:
            # Limpar resíduos da pasta TMP
            logger.info(f"Limpando resíduos de '{nome_arquivo_zip}' da pasta TMP")
            for item_tmp in os.listdir(TMP_FOLDER):
                eh_outro_zip_aguardando = item_tmp.lower().endswith('.zip') and \
                                          any(info_zip_pendente["nome_ftp"] == item_tmp for info_zip_pendente in arquivos_zip_para_processar_info if info_zip_pendente["nome_ftp"] != nome_arquivo_zip)
                if not eh_outro_zip_aguardando:
                    caminho_item_tmp_del = os.path.join(TMP_FOLDER, item_tmp)
                    try:
                        if os.path.isfile(caminho_item_tmp_del) or os.path.islink(caminho_item_tmp_del):
                            os.unlink(caminho_item_tmp_del)
                        elif os.path.isdir(caminho_item_tmp_del):
                            shutil.rmtree(caminho_item_tmp_del)
                    except Exception as e_clean:
                        logger.error(f"Erro ao limpar '{item_tmp}' de tmp: {e_clean}")
            os.makedirs(TMP_FOLDER, exist_ok=True)

    logger.info("✓ Processamento de todos os arquivos eCarta concluído")
    return UNZIP_FILES_FOLDER, nomes_todos_arquivos_baixados_ftp, caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar

def main():
    """Função main para compatibilidade com a API"""
    return processar_arquivos_ecarta_ftp()

if __name__ == "__main__":
    print("--- Executando ecarta_processor.py diretamente para TESTE ---")
    try:
        pasta_pdfs, ftp_para_excluir, devolucoes_para_arquivar_drive = main()
        if pasta_pdfs:
            print(f"[TESTE] Pasta de PDFs finais: {pasta_pdfs}")
        print(f"[TESTE] Arquivos do FTP para excluir: {len(ftp_para_excluir)} arquivo(s)")
        print(f"[TESTE] Arquivos DevolucaoAR originais para arquivar: {len(devolucoes_para_arquivar_drive)} arquivo(s)")
        print("--- Fim do TESTE de ecarta_processor.py ---")
    except Exception as e:
        print(f"ERRO no teste: {e}")
        exit(1)