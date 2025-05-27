import sys
import os

def resource_path(relative_path):
    """ Retorna o caminho absoluto para o recurso, funciona para dev e para PyInstaller """
    try:
        # PyInstaller cria uma pasta temporária e armazena o caminho em _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        # _MEIPASS não definido, значит estamos no modo de desenvolvimento
        base_path = os.path.abspath(".") # Caminho do script em execução

    return os.path.join(base_path, relative_path)

# Exemplo de uso no seu código:
if __name__ == '__main__':
    # Para acessar settings.ini dentro da pasta 'config'
    caminho_settings = resource_path("config/settings.ini")
    print(f"Tentando ler: {caminho_settings}")
    try:
        with open(caminho_settings, 'r') as f:
            # Faça algo com o arquivo de configuração
            print(f.read())
    except FileNotFoundError:
        print(f"ERRO: Arquivo de configuração não encontrado em {caminho_settings}")
    except Exception as e:
        print(f"ERRO ao ler o arquivo de configuração: {e}")


    # Para acessar logo.png na raiz
    caminho_logo = resource_path("logo.png")
    print(f"Caminho do logo: {caminho_logo}")
    # ... faça algo com o logo ...