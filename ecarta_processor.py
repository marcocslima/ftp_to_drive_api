# ecarta_processor.py

from ftplib import FTP
import zipfile
import os
import shutil
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
def resource_path(relative_to_base_dir): return os.path.join(BASE_DIR, relative_to_base_dir)

HOST_FTP = os.getenv('HOST')
PORT_FTP = int(os.getenv('PORT', 21))
USUARIO_FTP = os.getenv('USER_ECARTA')
SENHA_FTP = os.getenv('PASSWORD')
DIRETORIO_FTP = os.getenv('DIRECTORY')

DOWNLOADS_FOLDER = resource_path('arquivos/downloads')
UNZIP_FILES_FOLDER = resource_path('arquivos/unzip_files')
TMP_FOLDER = resource_path('arquivos/tmp')

# (Funções limpar_e_recriar_pasta, download_files_from_ftp, descompactar_zip, excluir_arquivos_do_ftp permanecem as mesmas)
def limpar_e_recriar_pasta(folder_path):
    if os.path.exists(folder_path):
        try: shutil.rmtree(folder_path)
        except OSError as e: print(f"Erro ao remover pasta '{folder_path}': {e}.")
    try:
        os.makedirs(folder_path, exist_ok=True)
        print(f"Pasta '{folder_path}' limpa/criada.")
    except OSError as e:
        print(f"Erro crítico ao criar pasta '{folder_path}': {e}")
        raise

def download_files_from_ftp(host, port, usuario, senha, remote_directory, local_downloads_folder):
    if not os.path.isdir(local_downloads_folder): os.makedirs(local_downloads_folder, exist_ok=True)
    arquivos_baixados_info = [] 
    try:
        with FTP() as ftp:
            ftp.connect(host, port); ftp.login(usuario, senha); ftp.cwd(remote_directory)
            print(f"Conectado ao FTP: {host}, diretório: {remote_directory}")
            files_in_remote_dir = ftp.nlst()
            print(f"Arquivos encontrados no FTP: {files_in_remote_dir}")
            for file_name in files_in_remote_dir:
                local_file_path = os.path.join(local_downloads_folder, file_name)
                try:
                    with open(local_file_path, "wb") as local_file:
                        print(f"Baixando {file_name}...")
                        ftp.retrbinary(f"RETR {file_name}", local_file.write)
                    print(f"{file_name} baixado para {local_file_path}.")
                    arquivos_baixados_info.append({"nome_ftp": file_name, "caminho_local": local_file_path})
                except Exception as e_dl: print(f"Erro ao baixar '{file_name}': {e_dl}")
        return arquivos_baixados_info
    except Exception as e:
        print(f"Erro na operação FTP (download): {e}")
        return []

def descompactar_zip(caminho_arquivo_zip, pasta_destino):
    if not (os.path.exists(caminho_arquivo_zip) and zipfile.is_zipfile(caminho_arquivo_zip)):
        print(f"Erro: Arquivo ZIP inválido ou não encontrado: {caminho_arquivo_zip}")
        return False
    os.makedirs(pasta_destino, exist_ok=True)
    try:
        with zipfile.ZipFile(caminho_arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(pasta_destino)
            print(f"Descompactado '{os.path.basename(caminho_arquivo_zip)}' em '{pasta_destino}'.")
            return True
    except Exception as e: print(f"Erro ao descompactar '{caminho_arquivo_zip}': {e}"); return False

def excluir_arquivos_do_ftp(host, port, usuario, senha, remote_directory, lista_nomes_arquivos_para_excluir):
    if not lista_nomes_arquivos_para_excluir:
        print("Nenhum arquivo especificado para exclusão no FTP.")
        return
    print(f"\n--- Iniciando exclusão de {len(lista_nomes_arquivos_para_excluir)} arquivos no FTP: {remote_directory} ---")
    try:
        with FTP() as ftp:
            ftp.connect(host, port); ftp.login(usuario, senha); ftp.cwd(remote_directory)
            print(f"Conectado ao FTP para exclusão.")
            for nome_arquivo in lista_nomes_arquivos_para_excluir:
                try:
                    print(f"Tentando excluir '{nome_arquivo}' do FTP...")
                    ftp.delete(nome_arquivo)
                    print(f"Arquivo '{nome_arquivo}' excluído do FTP.")
                except Exception as e_del: print(f"Erro ao excluir '{nome_arquivo}' do FTP: {e_del}")
        print("--- Exclusão de arquivos no FTP concluída. ---")
    except Exception as e: print(f"Erro durante operação de exclusão no FTP: {e}")


# --- FUNÇÃO PRINCIPAL MODIFICADA ---
def processar_arquivos_ecarta_ftp():
    print("Iniciando processo de tratamento de arquivos eCarta...")
    caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar = [] # Caminhos em DOWNLOADS_FOLDER
    nomes_todos_arquivos_baixados_ftp = []

    try:
        limpar_e_recriar_pasta(DOWNLOADS_FOLDER)
        limpar_e_recriar_pasta(UNZIP_FILES_FOLDER)
        limpar_e_recriar_pasta(TMP_FOLDER)
    except Exception as e_limpeza:
        print(f"Erro crítico durante limpeza inicial: {e_limpeza}. Abortando.")
        return None, [], []

    print("\n--- Etapa 1: Download de arquivos do FTP ---")
    info_arquivos_baixados = download_files_from_ftp(
        HOST_FTP, PORT_FTP, USUARIO_FTP, SENHA_FTP, DIRETORIO_FTP, DOWNLOADS_FOLDER
    )

    if not info_arquivos_baixados:
        print("Nenhum arquivo baixado do FTP ou erro. Encerrando processamento eCarta.")
        return UNZIP_FILES_FOLDER, [], []

    for info_arquivo in info_arquivos_baixados:
        nomes_todos_arquivos_baixados_ftp.append(info_arquivo["nome_ftp"])
        if "devolucaoar" in info_arquivo["nome_ftp"].lower():
            # Adiciona o caminho local do arquivo ORIGINAL DevolucaoAR (em DOWNLOADS_FOLDER)
            caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar.append(info_arquivo["caminho_local"])

    print(f"\nArquivos DevolucaoAR originais (em Downloads) identificados para arquivamento no Drive: {len(caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar)}")
    for p in caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar: print(f"  - {p}")


    print("\n--- Etapa 2: Processamento dos arquivos ZIP baixados ---")
    arquivos_zip_para_processar_info = [info for info in info_arquivos_baixados if info["nome_ftp"].lower().endswith('.zip')]
    
    if not arquivos_zip_para_processar_info:
        print("Nenhum arquivo .zip encontrado entre os baixados para processar.")
        return UNZIP_FILES_FOLDER, nomes_todos_arquivos_baixados_ftp, caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar

    for info_zip in arquivos_zip_para_processar_info:
        nome_arquivo_zip = info_zip["nome_ftp"]
        caminho_zip_original_em_downloads = info_zip["caminho_local"]
        
        if not os.path.exists(caminho_zip_original_em_downloads):
            print(f"AVISO: Arquivo ZIP '{nome_arquivo_zip}' não encontrado em '{caminho_zip_original_em_downloads}'. Pulando.")
            continue

        caminho_zip_para_processar_em_tmp = os.path.join(TMP_FOLDER, nome_arquivo_zip)
        print(f"\n>>> Processando arquivo ZIP: {nome_arquivo_zip} <<<")
        try:
            # MODIFICAÇÃO AQUI: Copiar DevolucaoAR, Mover os outros
            if "devolucaoar" in nome_arquivo_zip.lower():
                print(f"COPIANDO '{nome_arquivo_zip}' (DevolucaoAR) de downloads para '{TMP_FOLDER}' para processamento...")
                shutil.copy2(caminho_zip_original_em_downloads, caminho_zip_para_processar_em_tmp) # copy2 preserva metadados
            else:
                print(f"MOVENDO '{nome_arquivo_zip}' de downloads para '{TMP_FOLDER}' para processamento...")
                shutil.move(caminho_zip_original_em_downloads, caminho_zip_para_processar_em_tmp)

            print(f"Descompactando '{nome_arquivo_zip}' (de '{caminho_zip_para_processar_em_tmp}') em '{TMP_FOLDER}'...")
            if not descompactar_zip(caminho_zip_para_processar_em_tmp, TMP_FOLDER):
                print(f"Erro ao descompactar '{nome_arquivo_zip}' da pasta tmp. Pulando.")
                # Se a cópia/movimentação para tmp falhou ou descompactação falhou, limpar o zip de tmp se existir
                if os.path.exists(caminho_zip_para_processar_em_tmp):
                    os.remove(caminho_zip_para_processar_em_tmp)
                continue

            arquivo_devolucao_ar_txt_path = None
            for item in os.listdir(TMP_FOLDER):
                if "devolucaoar" in item.lower() and item.lower().endswith(".txt"):
                    arquivo_devolucao_ar_txt_path = os.path.join(TMP_FOLDER, item)
                    break
            
            if arquivo_devolucao_ar_txt_path:
                print(f"Arquivo '{os.path.basename(arquivo_devolucao_ar_txt_path)}' encontrado dentro de '{nome_arquivo_zip}'.")
                # (Lógica de ler o .txt e renomear/mover PDFs para UNZIP_FILES_FOLDER - sem alteração)
                linhas_do_arquivo_devolucao = []
                try:
                    with open(arquivo_devolucao_ar_txt_path, 'r', encoding='latin-1') as f_txt: linhas_do_arquivo_devolucao = [line.strip() for line in f_txt if line.strip()]
                except UnicodeDecodeError:
                    try:
                         with open(arquivo_devolucao_ar_txt_path, 'r', encoding='utf-8') as f_txt: linhas_do_arquivo_devolucao = [line.strip() for line in f_txt if line.strip()]
                    except Exception as e_decode: print(f"ERRO ao ler '{arquivo_devolucao_ar_txt_path}': {e_decode}"); continue
                for idx, linha_dados in enumerate(linhas_do_arquivo_devolucao):
                    try:
                        campos = linha_dados.split('|')
                        if len(campos) < 7: continue
                        nome_pdf_original = campos[6].strip(); novo_nome_pdf_base = campos[3].strip()
                        novo_nome_pdf = f"{novo_nome_pdf_base}.pdf" if not novo_nome_pdf_base.lower().endswith('.pdf') else novo_nome_pdf_base
                        pdf_orig_tmp = os.path.join(TMP_FOLDER, nome_pdf_original); pdf_dest_unzip = os.path.join(UNZIP_FILES_FOLDER, novo_nome_pdf)
                        os.makedirs(UNZIP_FILES_FOLDER, exist_ok=True)
                        if os.path.exists(pdf_orig_tmp): shutil.move(pdf_orig_tmp, pdf_dest_unzip); print(f"  PDF movido: {nome_pdf_original} -> {novo_nome_pdf}")
                        else: print(f"  AVISO: PDF '{nome_pdf_original}' não encontrado em tmp.")
                    except Exception as e_linha: print(f"  Erro linha DevolucaoAR: {e_linha}")
                os.remove(arquivo_devolucao_ar_txt_path)
            else:
                print(f"Nenhum 'DevolucaoAR.txt' encontrado dentro de '{nome_arquivo_zip}'. Movendo conteúdo para UNZIP.")
                os.makedirs(UNZIP_FILES_FOLDER, exist_ok=True)
                for item_descompactado in os.listdir(TMP_FOLDER):
                    orig_item_tmp = os.path.join(TMP_FOLDER, item_descompactado)
                    if item_descompactado == nome_arquivo_zip: continue
                    dest_item_unzip = os.path.join(UNZIP_FILES_FOLDER, item_descompactado)
                    try:
                        if os.path.isfile(orig_item_tmp): shutil.move(orig_item_tmp, dest_item_unzip)
                        elif os.path.isdir(orig_item_tmp):
                            if os.path.isdir(dest_item_unzip):
                                for sub_item in os.listdir(orig_item_tmp): shutil.move(os.path.join(orig_item_tmp, sub_item), dest_item_unzip)
                                shutil.rmtree(orig_item_tmp)
                            else: shutil.move(orig_item_tmp, UNZIP_FILES_FOLDER)
                        print(f"  Movido (sem DevolucaoAR.txt): {item_descompactado}")
                    except Exception as e_mv: print(f"  Erro ao mover {item_descompactado}: {e_mv}")
            
            # Após processar o conteúdo do ZIP, remover o ZIP da pasta TMP (que foi copiado ou movido para lá)
            if os.path.exists(caminho_zip_para_processar_em_tmp):
                print(f"Removendo ZIP '{nome_arquivo_zip}' (de '{caminho_zip_para_processar_em_tmp}') da pasta TMP.")
                os.remove(caminho_zip_para_processar_em_tmp)

        except Exception as e_process_zip:
            print(f"ERRO CRÍTICO ao processar ZIP '{nome_arquivo_zip}': {e_process_zip}")
            import traceback; traceback.print_exc()
        finally:
            # Limpar resíduos da pasta TMP
            print(f"Limpando resíduos de '{nome_arquivo_zip}' da pasta '{TMP_FOLDER}'...")
            # (Lógica de limpeza da TMP_FOLDER permanece a mesma)
            for item_tmp in os.listdir(TMP_FOLDER):
                eh_outro_zip_aguardando = item_tmp.lower().endswith('.zip') and \
                                          any(info_zip_pendente["nome_ftp"] == item_tmp for info_zip_pendente in arquivos_zip_para_processar_info if info_zip_pendente["nome_ftp"] != nome_arquivo_zip)
                if not eh_outro_zip_aguardando:
                    caminho_item_tmp_del = os.path.join(TMP_FOLDER, item_tmp)
                    try:
                        if os.path.isfile(caminho_item_tmp_del) or os.path.islink(caminho_item_tmp_del): os.unlink(caminho_item_tmp_del)
                        elif os.path.isdir(caminho_item_tmp_del): shutil.rmtree(caminho_item_tmp_del)
                    except Exception as e_clean: print(f"  Erro ao limpar '{item_tmp}' de tmp: {e_clean}")
            os.makedirs(TMP_FOLDER, exist_ok=True)

    print("\n--- Processamento de todos os arquivos eCarta concluído. ---")
    return UNZIP_FILES_FOLDER, nomes_todos_arquivos_baixados_ftp, caminhos_locais_arquivos_devolucaoAR_originais_para_arquivar

# (Bloco if __name__ == "__main__" para teste - sem alteração na chamada, mas o terceiro item da tupla agora são os caminhos de DOWNLOADS_FOLDER)
if __name__ == "__main__":
    print("--- Executando ecarta_processor.py diretamente para TESTE ---")
    pasta_pdfs, ftp_para_excluir, devolucoes_para_arquivar_drive = processar_arquivos_ecarta_ftp()
    if pasta_pdfs: print(f"[TESTE] Pasta de PDFs finais: {pasta_pdfs}")
    print(f"[TESTE] Arquivos do FTP para excluir (TODOS os baixados): {ftp_para_excluir}")
    print(f"[TESTE] Arquivos DevolucaoAR originais (caminhos locais em Downloads) para arquivar no Drive: {devolucoes_para_arquivar_drive}")
    print("--- Fim do TESTE de ecarta_processor.py ---")