import json
import cx_Oracle

NOME_TABELA = 'T_COORDENADA'
NOME_TABELA2 = 'T_ESTACAO'
NOME_TABELA3 = 'T_PONTO_TURISTICO'
NOME_ARQUIVO2_JSON = NOME_TABELA2 + '.json'
NOME_ARQUIVO3_JSON = NOME_TABELA3 + '.json'

def pega_credenciais():
    user = 'rm559607'
    senha = '190706'
    return user, senha

def abre_conexao(user, senha):
    dsn = cx_Oracle.makedsn(host='oracle.fiap.com.br', port=1521, service_name='ORCL')
    return cx_Oracle.connect(user, senha, dsn)

def gera_conexao():
    user, senha = pega_credenciais()
    conexao = abre_conexao(user, senha)
    return conexao

def busca_estacoes(cursor, tabela):
    query = f'SELECT ID_ESTACAO, NM_ESTACAO, ENDERECO FROM {tabela}'
    cursor.execute(query)
    return cursor.fetchall()

def busca_pontos(cursor, tabela):
    query = f'SELECT ID_PONTO_TURISTICO, NM_PONTO_TURISTICO, DS_PONTO_TURISTICO, HR_FUNCIONAMENTO_PONTO FROM {tabela}'
    cursor.execute(query)
    return cursor.fetchall()

def insere_localizacao(cursor, localizacao):
    query = f"""
        INSERT INTO {NOME_TABELA} (id_localizacao, latitude, longitude)
        VALUES (:1, :2, :3)
    """
    cursor.execute(query, (localizacao['id_localizacao'], localizacao['latidude'], localizacao['longitude']))


def insere_estacao(cursor, estacao):
    query = f"""
        INSERT INTO {NOME_TABELA2} (id_estacao, id_localizacao, nm_estacao, endereco, acessibilidade)
        VALUES (:1, :2, :3, :4, :5)
    """
    cursor.execute(query, (estacao['id_estacao'], estacao['id_localizacao'], estacao['nm_estacao'], estacao['endereco'], estacao['acessibilidade']))

def remove_estacao(cursor, id_para_remover):
    query = f'DELETE FROM {NOME_TABELA2} WHERE id_estacao = :1'
    cursor.execute(query, (id_para_remover))

    linhas_afetadas = cursor.rowcount
    return linhas_afetadas > 0

def constroi_lista_estacoes(cursor, tabela):
    resultados = busca_estacoes(cursor, tabela)
    
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

def constroi_lista_pontos(cursor, tabela):
    resultados = busca_pontos(cursor, tabela)
    
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
        print('\nBem-vindo ao Metro Smart!')
        print('Selecione como deseja continuar: ')
        print('0 - Ver estações de metro disponíveis')
        print('1 - Ver pontos de interesse disponíveis em cada estação')
        print('2 - Adicionar nova estação')
        print('3 - Remover estação')
        print('4 - Exportar dados das estações ou pontos de interesse para json')
        print('5 - Finalizar programa')
        escolha = input('Digite sua escolha: ')

        try:
            if escolha == '0':
                estacoes = busca_estacoes(cursor, NOME_TABELA2)
                for estacao in estacoes:
                    print(estacao)

            elif escolha == '1':
                pontos = busca_pontos(cursor, NOME_TABELA3)
                for ponto in pontos:
                    print(ponto)

            elif escolha == '2':
                print('Digite os dados da nova estação:')
                id_estacao = input('ID da estação: ')
                id_localizacao = input('ID da localização: ')
                nm_estacao = input('Nome da estação: ')
                endereco = input('Endereço: ')
                acessibilidade = input('Acessibilidade (sim/não): ')
                latitude = input('Latitude da localização: ')
                longitude = input('Longitude da localização: ')

                localizacao = {
                    'id_localizacao': id_localizacao,
                    'latidude': latitude,
                    'longitude': longitude
                }
                estacao = {
                    'id_estacao': id_estacao,
                    'id_localizacao': id_localizacao,
                    'nm_estacao': nm_estacao,
                    'endereco': endereco,
                    'acessibilidade': acessibilidade
                }

                insere_localizacao(cursor, localizacao)
                insere_estacao(cursor, estacao)
                conexao.commit()
                print('Estação adicionada com sucesso!')

            elif escolha == '3':
                id_para_remover = input('Digite o ID da estação a ser removida: ')
                sucesso = remove_estacao(cursor, id_para_remover)
                if sucesso:
                    conexao.commit()
                    print('Estação removida com sucesso.')
                else:
                    print('Nenhuma estação encontrada com esse ID.')

            elif escolha == '4':
                opcao = input("Você gostaria de exportar as Estações ou Pontos de Interesse? (Escolha 1 ou 2): ")
                if opcao == '1':
                    resultados = constroi_lista_estacoes(cursor, NOME_TABELA2)
                    with open(NOME_ARQUIVO2_JSON, 'w', encoding='utf-8') as arquivo:
                        json.dump(resultados, arquivo, indent=4, ensure_ascii=False)
                    print(f'Estações exportadas para {NOME_ARQUIVO2_JSON}')
                elif opcao == '2':
                    resultados = constroi_lista_pontos(cursor, NOME_TABELA3)
                    with open(NOME_ARQUIVO3_JSON, 'w', encoding='utf-8') as arquivo:
                        json.dump(resultados, arquivo, indent=4, ensure_ascii=False)
                    print(f'Pontos de interesse exportados para {NOME_ARQUIVO3_JSON}')
                else:
                    print("Opção inválida. Tente novamente, escrevendo exatamente como solicitado.")

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


