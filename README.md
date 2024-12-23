# Processamento de Logs de Website de E-commerce com PySpark

Este repositório contém um script PySpark para processar logs de um website de e-commerce, realizando limpeza, transformação e análise dos dados. O objetivo é responder a perguntas de negócio importantes e preparar os dados para análises mais aprofundadas por cientistas de dados.

## Dataset

O dataset utilizado é um arquivo CSV chamado `E-commerce Website Logs.csv`. Ele contém informações como data de acesso, duração da sessão, protocolo de rede, endereço IP, bytes transferidos, navegador, idade, gênero, país, nível de membro, idioma, valor das vendas, status de retorno e método de pagamento.

## Preparação do Ambiente

Para executar este código, você precisa ter o seguinte:

*   **Java 8:** Necessário para o Spark.
*   **Apache Spark 3.5.3:** O framework de processamento distribuído.
*   **Python 3:** A linguagem de programação.
*   **PySpark:** A API Python para o Spark.
*   **findspark:** Uma biblioteca para tornar o Spark encontrável pelo Python.

## Carregamento e Tratamento dos Dados

O script carrega o CSV usando `spark.read.csv` com `inferSchema=True` e `header=True`, o que infere automaticamente o esquema e usa a primeira linha como cabeçalho. Em seguida, realiza as seguintes transformações:

*   Adiciona colunas `hora`, `dia_semana` e `mes` extraídas da coluna `data_acesso`.
*   Renomeia as colunas para melhor clareza usando `withColumnsRenamed`.
*   Seleciona as colunas relevantes usando `select`.
*   Verifica a existência de valores nulos em cada coluna.
*   Filtra os dados removendo linhas com valores nulos na coluna 'idade'.

## Perguntas do Projeto e Análises

O script aborda as seguintes perguntas do projeto:

1.  **Quais são os 5 maiores horários de pico de acesso à plataforma durante a semana?**
    *   Agrupa os dados por `hora` e conta o número de acessos.
    *   Ordena os resultados em ordem decrescente de contagem.
    *   Exibe os 5 maiores horários de pico.

2.  **Existe uma diferença significativa na duração média das sessões entre usuários de diferentes navegadores?**
    *   Agrupa os dados por `navegador` e calcula a média da `duracao`.
    *   Ordena os resultados em ordem decrescente de duração média.
    *   Exibe os resultados.

3.  **Qual o método de pagamento mais utilizado pelos usuários em cada país?**
    *   Agrupa os dados por `pais` e `metodo_pagamento` e conta o número de ocorrências.
    *   Utiliza Window functions para rankear os métodos de pagamento dentro de cada país.
    *   Filtra para mostrar apenas o método de pagamento com maior contagem (Rank = 1) para cada país.

4.  **Qual país compra mais?**
    *   Agrupa os dados por `pais` e soma o valor das `vendas`.
    *   Ordena os resultados em ordem decrescente de valor total de vendas.
    *   Exibe os resultados.

## Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues e pull requests.
