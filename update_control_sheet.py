import gspread
import os # Para lidar com caminhos de arquivo

# --- Configurações ---
SERVICE_ACCOUNT_FILE = 'credentials/ecarta-gsheet.json' 

# Nome da sua planilha (o nome que aparece no Google Drive/Sheets)
SPREADSHEET_NAME = 'e-Carta' 

# Nome da aba/folha dentro da planilha (ex: 'Sheet1', 'Dados', etc.)
WORKSHEET_NAME = 'DadosEnvio'

# O valor que você quer procurar na coluna A
VALOR_A_PROCURAR = 'Produto X'

# O valor que você quer escrever na coluna B, se encontrar
VALOR_A_ESCREVER = 'Status: Processado'

# --- Autenticação ---
def get_gspread_service():
  try:
      # Verifica se o arquivo da chave existe
      if not os.path.exists(SERVICE_ACCOUNT_FILE):
          raise FileNotFoundError(f"Arquivo de chave '{SERVICE_ACCOUNT_FILE}' não encontrado. Certifique-se de que está no diretório correto.")
          
      gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
      print("Autenticação com a conta de serviço realizada com sucesso.")
  except FileNotFoundError as e:
      print(f"ERRO: {e}")
      print("Por favor, baixe o arquivo JSON da conta de serviço do Google Cloud Console e coloque-o na mesma pasta deste script.")
      print("Certifique-se também de que o nome do arquivo em SERVICE_ACCOUNT_FILE está correto.")
      exit()
  except Exception as e:
      print(f"ERRO: Não foi possível autenticar com a conta de serviço. Detalhes: {e}")
      print("Verifique as permissões da conta de serviço e se a API do Google Sheets está ativada no seu projeto GCP.")
      exit()

  # --- Abrir a planilha e a aba ---
  try:
      spreadsheet = gc.open(SPREADSHEET_NAME)
      worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
      print(f"Planilha '{SPREADSHEET_NAME}' e aba '{WORKSHEET_NAME}' abertas com sucesso.")
  except gspread.exceptions.SpreadsheetNotFound:
      print(f"ERRO: Planilha '{SPREADSHEET_NAME}' não encontrada.")
      print("Verifique o nome da planilha e se a conta de serviço tem permissão de 'Editor' nela.")
      exit()
  except gspread.exceptions.WorksheetNotFound:
      print(f"ERRO: Aba '{WORKSHEET_NAME}' não encontrada na planilha '{SPREADSHEET_NAME}'.")
      print("Verifique o nome da aba.")
      exit()
  except Exception as e:
      print(f"ERRO ao abrir a planilha ou aba. Detalhes: {e}")
      exit()
  return worksheet

def update_control_sheet(worksheet):
  all_data = worksheet.get_all_values() 

  found_and_updated = False

  for i in all_data:
      print(i)

  # for row_index, row_data in enumerate(all_data):
  #     if row_data and row_data[0] == VALOR_A_PROCURAR:
  #         sheet_row_number = row_index + 1 # Convertendo o índice para o número da linha da planilha (1-baseado)
          
  #         print(f"'{VALOR_A_PROCURAR}' encontrado na linha {sheet_row_number}, coluna A.")

  #         try:
  #             worksheet.update_cell(sheet_row_number, 2, VALOR_A_ESCREVER)
  #             print(f"'{VALOR_A_ESCREVER}' escrito na célula B{sheet_row_number}.")
  #             found_and_updated = True
  #             break # Parar após encontrar e atualizar a primeira ocorrência
  #         except Exception as e:
  #             print(f"ERRO: Não foi possível escrever na célula B{sheet_row_number}. Detalhes: {e}")
  #             break 

  # if not found_and_updated:
  #     print(f"'{VALOR_A_PROCURAR}' não foi encontrado na coluna A, ou ocorreu um erro durante a escrita.")


if __name__ == "__main__":
  # --- Autenticação e abertura da planilha ---
  worksheet = get_gspread_service()

  # --- Atualiza a planilha de controle ---
  update_control_sheet(worksheet)

  # --- Mensagem final ---
  if worksheet:
      print("Atualização da planilha de controle concluída.")
  else:
      print("Falha ao atualizar a planilha de controle.")
  
  print("\nProcesso concluído.")