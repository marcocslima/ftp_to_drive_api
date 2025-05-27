# ecarta_processor.py
# (Imports e configurações permanecem os mesmos da versão anterior SEM e-mail)
from ftplib import FTP
import zipfile
import os
import shutil # time não é mais necessário aqui
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURAÇÕES GERAIS ---
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

# --- FUNÇÕES AUXILIARES ---
# (limpar_e_recriar_pasta, download_files_from_ftp, descompactar_zip permanecem iguais)
def limpar_e_recriar_pasta(folder_path):
    if os.path.exists(folder_path):
        try:
            shutil.rmtree(folder_path)
        except OSError as e:
            print(f"Erro ao remover a pasta '{folder_path}': {e}. Tentando continuar...")
    try:
        os.makedirs(folder_path, exist_ok=True)
        print(f"Pasta '{folder_path}' limpa/criada.")
    except OSError as e:
        print(f"Erro crítico ao criar a pasta '{folder_path}': {e}")
        raise

def download_files_from_ftp(host, port, usuario, senha, remote_directory, local_downloads_folder):
    if not os.path.isdir(local_downloads_folder):
        os.makedirs(local_downloads_folder, exist_ok=True)
    arquivos_baixados_nomes = []
    try:
        with FTP() as ftp:
            print(f"Conectando a {host}:{port}...")
            ftp.connect(host, port)
            ftp.login(usuario, senha)
            print(f"Logado. Mudando para o diretório '{remote_directory}'...")
            ftp.cwd(remote_directory)
            files_in_remote_dir = ftp.nlst()
            print(f"Arquivos encontrados no FTP: {files_in_remote_dir}")
            for file_name in files_in_remote_dir:
                local_file_path = os.path.join(local_downloads_folder, file_name)
                try:
                    with open(local_file_path, "wb") as local_file:
                        print(f"Baixando {file_name}...")
                        ftp.retrbinary(f"RETR {file_name}", local_file.write)
                    print(f"{file_name} baixado com sucesso para {local_file_path}.")
                    arquivos_baixados_nomes.append(file_name)
                except Exception as e_download_file:
                    print(f"Erro ao baixar o arquivo '{file_name}': {e_download_file}")
                    if os.path.exists(local_file_path):
                        os.remove(local_file_path)
        return arquivos_baixados_nomes
    except Exception as e:
        print(f"Erro na operação FTP: {e}")
        return []

def descompactar_zip(caminho_arquivo_zip, pasta_destino):
    if not os.path.exists(caminho_arquivo_zip):
        print(f"Erro: Arquivo ZIP não encontrado em '{caminho_arquivo_zip}'")
        return False
    if not zipfile.is_zipfile(caminho_arquivo_zip):
        print(f"Erro: '{caminho_arquivo_zip}' não é um ZIP válido.")
        return False
    os.makedirs(pasta_destino, exist_ok=True)
    try:
        with zipfile.ZipFile(caminho_arquivo_zip, 'r') as zip_ref:
            print(f"Descompactando '{os.path.basename(caminho_arquivo_zip)}' para '{pasta_destino}'...")
            zip_ref.extractall(pasta_destino)
            print("Descompactação concluída.")
            return True
    except zipfile.BadZipFile:
        print(f"Erro: Arquivo ZIP corrompido: '{caminho_arquivo_zip}'")
    except Exception as e:
        print(f"Erro inesperado ao descompactar '{caminho_arquivo_zip}': {e}")
    return False

# ---------------------------------------------------------------------------
# NOVA FUNÇÃO PARA EXCLUIR ARQUIVOS DO FTP
# ---------------------------------------------------------------------------
def excluir_arquivos_do_ftp(host, port, usuario, senha, remote_directory, lista_arquivos_para_excluir):
    """
    Exclui arquivos especificados de um diretório FTP.

    Args:
        host (str): Endereço do servidor FTP.
        port (int): Porta do servidor FTP.
        usuario (str): Nome de usuário para autenticação.
        senha (str): Senha para autenticação.
        remote_directory (str): Diretório remoto onde os arquivos estão.
        lista_arquivos_para_excluir (list): Lista de nomes de arquivos a serem excluídos.
    """
    if not lista_arquivos_para_excluir:
        print("Nenhum arquivo especificado para exclusão no FTP.")
        return

    print(f"\n--- Iniciando exclusão de arquivos no FTP: {remote_directory} ---")
    try:
        with FTP() as ftp:
            print(f"Conectando a {host}:{port} para exclusão...")
            ftp.connect(host, port)
            ftp.login(usuario, senha)
            print(f"Logado para exclusão. Mudando para o diretório '{remote_directory}'...")
            ftp.cwd(remote_directory)

            arquivos_no_servidor = ftp.nlst() # Obter lista atual para verificação

            for nome_arquivo in lista_arquivos_para_excluir:
                if "devolucaoar" in nome_arquivo.lower(): # Não excluir arquivos DevolucaoAR
                    print(f"Ignorando exclusão de '{nome_arquivo}' (contém DevolucaoAR).")
                    continue

                if nome_arquivo in arquivos_no_servidor:
                    try:
                        print(f"Tentando excluir '{nome_arquivo}' do FTP...")
                        ftp.delete(nome_arquivo)
                        print(f"Arquivo '{nome_arquivo}' excluído com sucesso do FTP.")
                    except Exception as e_delete:
                        print(f"Erro ao excluir o arquivo '{nome_arquivo}' do FTP: {e_delete}")
                else:
                    print(f"AVISO: Arquivo '{nome_arquivo}' não encontrado no servidor FTP para exclusão.")
        print("--- Exclusão de arquivos no FTP concluída. ---")
    except Exception as e:
        print(f"Erro durante a operação de exclusão no FTP: {e}")


# ---------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL MODIFICADA
# ---------------------------------------------------------------------------
def processar_arquivos_ecarta_ftp():
    """
    Processa arquivos eCarta.
    Retorna uma tupla: (caminho_pasta_final, lista_nomes_zips_processados_para_exclusao_ftp)
    Retorna (None, []) em caso de falha crítica inicial.
    """
    print("Iniciando processo de tratamento de arquivos eCarta...")
    nomes_zips_processados_com_sucesso = [] # Nova lista para rastrear

    try:
        limpar_e_recriar_pasta(DOWNLOADS_FOLDER)
        limpar_e_recriar_pasta(UNZIP_FILES_FOLDER)
        limpar_e_recriar_pasta(TMP_FOLDER)
    except Exception as e_limpeza:
        print(f"Erro crítico durante a limpeza inicial das pastas: {e_limpeza}. Abortando.")
        return None, []

    print("\n--- Etapa 1: Download de arquivos do FTP ---")
    nomes_arquivos_baixados_do_ftp = download_files_from_ftp(
        HOST_FTP, PORT_FTP, USUARIO_FTP, SENHA_FTP, DIRETORIO_FTP, DOWNLOADS_FOLDER
    )

    if not nomes_arquivos_baixados_do_ftp:
        print("Nenhum arquivo foi baixado do FTP ou ocorreu um erro. Encerrando processamento eCarta.")
        return UNZIP_FILES_FOLDER, []

    print("\n--- Etapa 2: Processamento dos arquivos ZIP baixados ---")
    arquivos_zip_para_processar = [f for f in nomes_arquivos_baixados_do_ftp if f.lower().endswith('.zip')]
    
    if not arquivos_zip_para_processar:
        print("Nenhum arquivo .zip encontrado entre os baixados para processar.")
        # Adicionar arquivos não-zip baixados à lista de "exclusão" se eles devem ser excluídos também
        # Por ora, focamos nos ZIPs processados.
        # Poderíamos retornar nomes_arquivos_baixados_do_ftp aqui se quiséssemos excluir todos os baixados.
        return UNZIP_FILES_FOLDER, []


    for nome_arquivo_zip in arquivos_zip_para_processar:
        caminho_zip_na_pasta_downloads = os.path.join(DOWNLOADS_FOLDER, nome_arquivo_zip)
        if not os.path.exists(caminho_zip_na_pasta_downloads):
            print(f"AVISO: Arquivo ZIP '{nome_arquivo_zip}' não encontrado em '{DOWNLOADS_FOLDER}'. Pulando.")
            continue
        caminho_zip_em_tmp = os.path.join(TMP_FOLDER, nome_arquivo_zip)

        print(f"\n>>> Processando arquivo ZIP: {nome_arquivo_zip} <<<")
        sucesso_processamento_zip_atual = False # Flag para este ZIP
        try:
            print(f"Movendo '{nome_arquivo_zip}' para '{TMP_FOLDER}'...")
            shutil.move(caminho_zip_na_pasta_downloads, caminho_zip_em_tmp)

            print(f"Descompactando '{nome_arquivo_zip}' em '{TMP_FOLDER}'...")
            if not descompactar_zip(caminho_zip_em_tmp, TMP_FOLDER):
                print(f"Erro ao descompactar '{nome_arquivo_zip}'. Pulando para o próximo.")
                # Não adicionar à lista de sucesso se a descompactação falhar
                continue # Pula para o próximo arquivo ZIP

            arquivo_devolucao_ar_encontrado_path = None
            for item in os.listdir(TMP_FOLDER):
                if "devolucaoar" in item.lower() and item.lower().endswith(".txt"):
                    arquivo_devolucao_ar_encontrado_path = os.path.join(TMP_FOLDER, item)
                    break
            
            if arquivo_devolucao_ar_encontrado_path:
                print(f"Arquivo 'DevolucaoAR.txt' encontrado: {arquivo_devolucao_ar_encontrado_path}")
                # ... (lógica de leitura do DevolucaoAR.txt e processamento dos PDFs) ...
                # (código omitido por brevidade, é o mesmo da sua versão anterior)
                linhas_do_arquivo_devolucao = []
                try:
                    with open(arquivo_devolucao_ar_encontrado_path, 'r', encoding='latin-1') as f_txt:
                        linhas_do_arquivo_devolucao = [line.strip() for line in f_txt if line.strip()]
                except UnicodeDecodeError:
                    print(f"AVISO: Falha ao decodificar {arquivo_devolucao_ar_encontrado_path} com latin-1. Tentando utf-8...")
                    try:
                         with open(arquivo_devolucao_ar_encontrado_path, 'r', encoding='utf-8') as f_txt:
                            linhas_do_arquivo_devolucao = [line.strip() for line in f_txt if line.strip()]
                    except Exception as e_decode:
                        print(f"ERRO: Não foi possível ler o arquivo {arquivo_devolucao_ar_encontrado_path}: {e_decode}")
                        continue # Pula este ZIP
                
                print(f"Processando {len(linhas_do_arquivo_devolucao)} linhas do arquivo DevolucaoAR.")
                for idx, linha_dados in enumerate(linhas_do_arquivo_devolucao):
                    try:
                        campos = linha_dados.split('|')
                        if len(campos) < 7:
                            print(f"  Linha {idx+1} inválida (campos < 7): '{linha_dados}'")
                            continue
                        nome_pdf_original_no_zip = campos[6].strip()
                        novo_nome_base_pdf = campos[3].strip()
                        if not novo_nome_base_pdf.lower().endswith('.pdf'): novo_nome_pdf_com_ext = f"{novo_nome_base_pdf}.pdf"
                        else: novo_nome_pdf_com_ext = novo_nome_base_pdf
                        caminho_pdf_original_em_tmp = os.path.join(TMP_FOLDER, nome_pdf_original_no_zip)
                        os.makedirs(UNZIP_FILES_FOLDER, exist_ok=True)
                        caminho_pdf_renomeado_destino = os.path.join(UNZIP_FILES_FOLDER, novo_nome_pdf_com_ext)
                        if os.path.exists(caminho_pdf_original_em_tmp):
                            print(f"  Renomeando e movendo '{nome_pdf_original_no_zip}' para '{novo_nome_pdf_com_ext}' -> '{UNZIP_FILES_FOLDER}'")
                            shutil.move(caminho_pdf_original_em_tmp, caminho_pdf_renomeado_destino)
                        else: print(f"  AVISO: PDF '{nome_pdf_original_no_zip}' (linha {idx+1}) não encontrado em '{TMP_FOLDER}'.")
                    except Exception as e_linha: print(f"  Erro ao processar linha {idx+1} ('{linha_dados}') do DevolucaoAR.txt: {e_linha}")
                
                print(f"Apagando arquivo '{os.path.basename(arquivo_devolucao_ar_encontrado_path)}' de '{TMP_FOLDER}'...")
                os.remove(arquivo_devolucao_ar_encontrado_path)
                # O arquivo ZIP original (caminho_zip_em_tmp) não será excluído aqui se continha DevolucaoAR,
                # pois a regra é não excluir *DevolucaoAR* do FTP. Para consistência, não o apagamos localmente
                # se ele mesmo for um *DevolucaoAR*.zip. Mas se o ZIP não é DevolucaoAR mas contém um .txt DevolucaoAR,
                # o ZIP *será* apagado do tmp.
                # A regra de exclusão no FTP é baseada no nome do arquivo ZIP.
                # Se o *arquivo ZIP em si* se chamar algo como "LOTE_DEVOLUCAOAR_123.zip", ele não será excluído do FTP.
                # Se o arquivo ZIP se chamar "LOTE_NORMAL_456.zip" e DENTRO dele tiver um "DevolucaoAR.txt",
                # então "LOTE_NORMAL_456.zip" será candidato à exclusão do FTP.
                # E o "LOTE_NORMAL_456.zip" será excluído da pasta tmp local.
                if "devolucaoar" not in nome_arquivo_zip.lower() and os.path.exists(caminho_zip_em_tmp):
                     print(f"Apagando arquivo ZIP original '{nome_arquivo_zip}' (que continha DevolucaoAR.txt) de '{TMP_FOLDER}'...")
                     os.remove(caminho_zip_em_tmp)
                elif os.path.exists(caminho_zip_em_tmp):
                     print(f"Mantendo arquivo ZIP original '{nome_arquivo_zip}' (que é um DevolucaoAR) em '{TMP_FOLDER}' por enquanto.")


            else: # Sem DevolucaoAR.txt
                print("Arquivo 'DevolucaoAR.txt' NÃO encontrado.")
                # ... (lógica de mover arquivos descompactados para UNZIP_FILES_FOLDER) ...
                # (código omitido por brevidade, é o mesmo da sua versão anterior)
                os.makedirs(UNZIP_FILES_FOLDER, exist_ok=True)
                itens_em_tmp = os.listdir(TMP_FOLDER)
                for item_descompactado_nome in itens_em_tmp:
                    caminho_item_origem = os.path.join(TMP_FOLDER, item_descompactado_nome)
                    if item_descompactado_nome == nome_arquivo_zip: continue
                    caminho_item_destino = os.path.join(UNZIP_FILES_FOLDER, item_descompactado_nome)
                    try:
                        if os.path.isfile(caminho_item_origem):
                            print(f"  Movendo arquivo '{item_descompactado_nome}'...")
                            shutil.move(caminho_item_origem, caminho_item_destino)
                        elif os.path.isdir(caminho_item_origem):
                            print(f"  Movendo diretório '{item_descompactado_nome}'...")
                            if os.path.isdir(caminho_item_destino):
                                print(f"    Destino '{caminho_item_destino}' já é um diretório. Mesclando conteúdo.")
                                for sub_item_nome in os.listdir(caminho_item_origem): shutil.move(os.path.join(caminho_item_origem, sub_item_nome), caminho_item_destino)
                                shutil.rmtree(caminho_item_origem)
                            elif os.path.exists(caminho_item_destino): print(f"    ERRO: Destino '{caminho_item_destino}' existe e não é um diretório. Não foi possível mover '{item_descompactado_nome}'.")
                            else: shutil.move(caminho_item_origem, UNZIP_FILES_FOLDER)
                    except Exception as e_move: print(f"    Erro ao mover '{item_descompactado_nome}' para '{UNZIP_FILES_FOLDER}': {e_move}")
                print("  Movimentação (sem DevolucaoAR) concluída.")

                # Apagar o arquivo ZIP original de tmp_folder (se não era DevolucaoAR)
                if "devolucaoar" not in nome_arquivo_zip.lower() and os.path.exists(caminho_zip_em_tmp):
                    print(f"Apagando arquivo ZIP original '{nome_arquivo_zip}' de '{TMP_FOLDER}' (sem DevolucaoAR)...")
                    os.remove(caminho_zip_em_tmp)
                elif os.path.exists(caminho_zip_em_tmp):
                     print(f"Mantendo arquivo ZIP original '{nome_arquivo_zip}' (que é um DevolucaoAR) em '{TMP_FOLDER}' por enquanto.")

            sucesso_processamento_zip_atual = True # Marcar como sucesso para este ZIP

        except Exception as e_process_zip:
            print(f"ERRO CRÍTICO ao processar o arquivo ZIP '{nome_arquivo_zip}': {e_process_zip}")
            import traceback
            traceback.print_exc()
            # Não adicionar à lista de sucesso se houver erro crítico
        finally:
            # Adicionar à lista de exclusão APENAS se o processamento do ZIP foi bem-sucedido
            # E se o próprio nome do arquivo ZIP não contiver "DevolucaoAR"
            if sucesso_processamento_zip_atual and "devolucaoar" not in nome_arquivo_zip.lower():
                nomes_zips_processados_com_sucesso.append(nome_arquivo_zip)
            elif sucesso_processamento_zip_atual and "devolucaoar" in nome_arquivo_zip.lower():
                print(f"Arquivo ZIP '{nome_arquivo_zip}' processado, mas não será adicionado à lista de exclusão do FTP por conter 'DevolucaoAR'.")


            print(f"Limpando conteúdo da pasta '{TMP_FOLDER}' para o próximo ciclo...")
            for item_para_limpar in os.listdir(TMP_FOLDER):
                # Não apagar o ZIP DevolucaoAR que foi mantido intencionalmente
                if item_para_limpar == nome_arquivo_zip and "devolucaoar" in nome_arquivo_zip.lower():
                    print(f"  Preservando '{item_para_limpar}' em tmp (é um DevolucaoAR).")
                    continue

                caminho_item_para_limpar = os.path.join(TMP_FOLDER, item_para_limpar)
                try:
                    if os.path.isfile(caminho_item_para_limpar) or os.path.islink(caminho_item_para_limpar):
                        os.unlink(caminho_item_para_limpar)
                    elif os.path.isdir(caminho_item_para_limpar):
                        shutil.rmtree(caminho_item_para_limpar)
                except Exception as e_clean_item:
                    print(f"  Erro ao limpar item '{item_para_limpar}' da pasta tmp: {e_clean_item}")
            os.makedirs(TMP_FOLDER, exist_ok=True)

    print("\n--- Processamento de todos os arquivos ZIP eCarta concluído. ---")
    return UNZIP_FILES_FOLDER, nomes_zips_processados_com_sucesso

# (Bloco if __name__ == "__main__" para teste permanece o mesmo, mas agora espera uma tupla)
if __name__ == "__main__":
    print("--- Executando ecarta_processor.py diretamente para TESTE ---")
    pasta_final_teste, zips_para_excluir_teste = processar_arquivos_ecarta_ftp()

    if pasta_final_teste and os.path.exists(pasta_final_teste):
        print(f"\n[TESTE] Processamento eCarta concluído. Arquivos finais esperados em: '{pasta_final_teste}'")
        # ... (código de listagem do conteúdo) ...
    # ... (resto do bloco de teste) ...
    print(f"[TESTE] Arquivos ZIP que seriam candidatos à exclusão do FTP: {zips_para_excluir_teste}")
    if zips_para_excluir_teste:
        print("[TESTE] Simulando exclusão do FTP (não vai realmente excluir):")
        excluir_arquivos_do_ftp("test.ftp.com", 21, "user", "pass", "/testdir", zips_para_excluir_teste) # Exemplo
    print("--- Fim do TESTE de ecarta_processor.py ---")