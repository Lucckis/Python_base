import json
import oracledb

NOME_TABELA = 'T_ALERTA'
NOME_TABELA2 = 'T_LOCAIS_SEGUROS'
NOME_TABELA3 = 'T_AREA_DE_RISCO'
NOME_ARQUIVO_JSON = NOME_TABELA + '.json'
NOME_ARQUIVO2_JSON = NOME_TABELA2 + '.json'
NOME_ARQUIVO3_JSON = NOME_TABELA3 + '.json'

def pega_credenciais():
    user = 'rm559607'
    senha = '190706'
    return user, senha

def abre_conexao(user, senha):
    dsn = oracledb.makedsn(host='oracle.fiap.com.br', port=1521, service_name='ORCL')
    return oracledb.connect(user=user, password=senha, dsn=dsn)

def gera_conexao():
    user, senha = pega_credenciais()
    conexao = abre_conexao(user, senha)
    return conexao

def procura_alertas(cursor, tabela):
    query = f'SELECT ID_ALERTA, QTD_ALERTA, TP_ALERTA, DS_ALERTA FROM {tabela} WHERE QTD_ALERTA = 1'
    cursor.execute(query)
    return cursor.fetchall()

def busca_locais_seguros(cursor, tabela):
    query = f'SELECT ID_LOCAIS_SEGUROS, NM_LUGAR_SEGURO, DS_LOCAL_SEGURO, ST_LUGAR_SEGURO FROM {tabela}'
    cursor.execute(query)
    return cursor.fetchall()

def busca_areas_risco(cursor, tabela):
    query = f'SELECT ID_AREA_RISCO, DS_AREA_RISCO, LOC_AREA_RISCO FROM {tabela}'
    cursor.execute(query)
    return cursor.fetchall()

def insere_cliente(cursor, cliente):  # Necessario para os inserts em outras tabelas
    query = f"""
        INSERT INTO T_CLIENTE (id_cliente, nm_cliente)
        VALUES (:1, :2)
    """
    cursor.execute(query, (cliente['id_cliente'], cliente['nm_cliente']))


def insere_area_risco(cursor, area):
    query = f"""
        INSERT INTO {NOME_TABELA3} (id_area_risco, id_cliente, ds_area_risco, loc_area_risco)
        VALUES (:1, :2, :3, :4)
    """
    cursor.execute(query, (area['id_area_risco'], area['id_cliente'], area['ds_area_risco'], area['loc_area_risco']))

def remove_alerta(cursor, id_para_remover):
    query = f'DELETE FROM {NOME_TABELA} WHERE id_alerta = :1'
    cursor.execute(query, (id_para_remover))

    linhas_afetadas = cursor.rowcount
    return linhas_afetadas > 0

def constroi_lista_alertas(cursor, tabela):
    resultados = procura_alertas(cursor, tabela)
    
    descricoes_tabela = cursor.description
    nomes_colunas = [ descricao[0] for descricao in descricoes_tabela ]
    numero_colunas = len(nomes_colunas)
    
    print('Nomes das colunas:')
    print(nomes_colunas)
    
    lista_resultados = []
    for resultado in resultados:
        dado = {} 
        
        for i in range(numero_colunas):
            chave = nomes_colunas[i]
            conteudo = resultado[i]
            
            dado[chave] = conteudo
        
        lista_resultados.append(dado)
    
    return lista_resultados

def constroi_lista_locais_seguros(cursor, tabela):
    resultados = busca_locais_seguros(cursor, tabela)
    
    descricoes_tabela = cursor.description
    nomes_colunas = [ descricao[0] for descricao in descricoes_tabela ]
    numero_colunas = len(nomes_colunas)
    
    print('Nomes das colunas:')
    print(nomes_colunas)
    
    lista_resultados = []
    for resultado in resultados:
        dado = {} 
        
        for i in range(numero_colunas):
            chave = nomes_colunas[i]
            conteudo = resultado[i]
            
            dado[chave] = conteudo
        
        lista_resultados.append(dado)
    
    return lista_resultados

def constroi_lista_areas_risco(cursor, tabela):
    resultados = busca_areas_risco(cursor, tabela)
    
    descricoes_tabela = cursor.description
    nomes_colunas = [ descricao[0] for descricao in descricoes_tabela ]
    numero_colunas = len(nomes_colunas)
    
    print('Nomes das colunas:')
    print(nomes_colunas)
    
    lista_resultados = []
    for resultado in resultados:
        dado = {} 
        
        for i in range(numero_colunas):
            chave = nomes_colunas[i]
            conteudo = resultado[i]
            
            dado[chave] = conteudo
        
        lista_resultados.append(dado)
    
    return lista_resultados

def main():
    conexao = gera_conexao()
    print(f'Conexão bem-sucedida com user {conexao.username} do BD.')
    cursor = conexao.cursor()

    programa_menu = True
    while programa_menu:  
        print('\nBem-vindo ao Hydro!')
        print('Selecione como deseja continuar: ')
        print('0 - Ver alertas mais recentes')
        print('1 - Ver locais seguros existentes')
        print('2 - Adicionar nova área de risco')
        print('3 - Remover alerta')
        print('4 - Exportar dados dos alertas, locais seguros ou áreas de risco para json')
        print('5 - Finalizar programa')
        escolha = input('Digite sua escolha: ')

        try:
            if escolha == '0':
                alertas = procura_alertas(cursor, NOME_TABELA)
                for alerta in alertas:
                    print(alerta)

            elif escolha == '1':
                locais = busca_locais_seguros(cursor, NOME_TABELA2)
                for local in locais:
                    print(local)

            elif escolha == '2':
                print('Digite os dados da nova área de risco:')
                id_area_risco = input('ID da área de risco: ')
                id_cliente = input('ID do cliente: ')
                nm_cliente = input('Nome do cliente: ')
                ds_area_risco = input('Descrição da área de risco: ')
                loc_area_risco = input('Localização da area de risco: ')

                cliente = {
                    'id_cliente': id_cliente,
                    'nm_cliente': nm_cliente
                }
                area = {
                    'id_area_risco': id_area_risco,
                    'id_cliente': id_cliente,
                    'ds_area_risco': ds_area_risco,
                    'loc_area_risco': loc_area_risco
                }

                insere_cliente(cursor, cliente)
                insere_area_risco(cursor, area)
                conexao.commit()
                print('Área adicionada com sucesso!')

            elif escolha == '3':
                id_remover = input('Digite o ID do alerta a ser removido: ')
                id_para_remover = [id_remover]
                sucesso = remove_alerta(cursor, id_para_remover)
                if sucesso:
                    conexao.commit()
                    print('Alerta removido com sucesso.')
                else:
                    print('Nenhum alerta encontrado com esse ID.')

            elif escolha == '4':
                opcao = input("Você gostaria de exportar os alertas, locais seguros ou áreas de risco? (Escolha 1, 2 ou 3): ")
                if opcao == '1':
                    resultados = constroi_lista_alertas(cursor, NOME_TABELA)
                    with open(NOME_ARQUIVO_JSON, 'w', encoding='utf-8') as arquivo:
                        json.dump(resultados, arquivo, indent=4, ensure_ascii=False)
                    print(f'Alertas exportados para {NOME_ARQUIVO_JSON}')
                elif opcao == '2':
                    resultados = constroi_lista_locais_seguros(cursor, NOME_TABELA2)
                    with open(NOME_ARQUIVO2_JSON, 'w', encoding='utf-8') as arquivo:
                        json.dump(resultados, arquivo, indent=4, ensure_ascii=False)
                    print(f'Locais seguros exportados para {NOME_ARQUIVO2_JSON}')
                elif opcao == '3':
                    resultados = constroi_lista_areas_risco(cursor, NOME_TABELA3)
                    with open(NOME_ARQUIVO3_JSON, 'w', encoding='utf-8') as arquivo:
                        json.dump(resultados, arquivo, indent=4, ensure_ascii=False)
                    print(f'Áreas de risco exportadas para {NOME_ARQUIVO3_JSON}')
                else:
                    print("Opção inválida. Tente novamente, escrevendo apenas algum dos números solicitados.")

            elif escolha == '5':
                programa_menu = False  
                print('Encerrando programa...')

            else:
                print('Comando inválido, por favor tente novamente.')
        except Exception as e:
            print(f"Ocorreu um erro: {e}")

    cursor.close()
    conexao.close()

if __name__ == '__main__':
    main()


