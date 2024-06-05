## Sobre o Projeto

Esse projeto é parte maior da iniciativa **Intelli.gente do MCTI e do IARA Data Science** e visa **automatizar a coleta de dados de bases governamentais abertas** para alimentar um Data Warehouse com esses dados para serem futuramente usado nos cálculos dos indicadores.

O nome do módulo principal se refere à sigla **ETL** (Extract Transform and Load), que denota o processo que os dados extraídos seguem antes de serem carregados.

## Objetivos Principais do Projeto

* **Coleta automática de dados**: Eliminar ou reduzir ao mínimo a necessidade de interferência humana no processo de alimentar as bases do Intelli.gente e IARA

* **Resiliência na Coleta**: Favorecer métodos robustos de coleta de dados como APIs e quando isso não for possível, usar métodos de WebScrapping mais robustos e com menor chance de se tornarem obsoletos

* **Séries Históricas**: Coletar a maior e mais recente série histórica de dados disponível, além de obter os meta-dados com o tamanho e anos dessa série

* **Tratamento dos Dados**: Tratar os dados de acordo com padrões gerais e colocar eles num formato (estrutura do dado) em que ele possa ser carregado no Data Warehouse.

## Escolhas de Design atuais (Sujeito a Mudança):

1) Separar Classes que fazem o WebScrapping das que extraem os dados vindo das anteriores: isso foi feito já que a lógica de extração do arquivo de dados das páginas geramente é bem complexo, então seria uma boa prática separar as responsabilidades nesse caso.

<br>

2) **Dependency Injection** das Classes Scrapper nas Classes que extraem os dados: Esse padrão de Design em POO é bem comum , ele é implementado simplesmente passando a classe Scrapper como argum. para os métodos das classes extratoras, assim desacoplando as duas classes mas permitindo a mesma funcionalidade final.

3) **Interfaces com classes Abstratas**:




## Módulos Principais

### Webscrapping: 

Classes que implementam a extração de dados por meio de webscrapping

#### Webscrapping.ScrapperClasses
Classes que fazem o webscrapping de sites oficiais e retornam um dataframe extraído do site, com nenhum ou muito pouco pre-processamento (apenas se estritamente necessário)


#### Webscrapping.ExtractorClasses
Classes que recebem as classes de Scrapper como parâmetros (Dependecy Injection), chamam a função das classes Scrapper que retornam um DF e processam esse DF para retornar uma estrutura de dados que represeta os dados processados e prontos para serem inseridos no Banco de Dados / Data Warehouse

### Api Extractor
Classes que implementam a extração de dados por meio de APIs do governo

### DB interface
Lógica de conectar os dados extraídos com o Banco de Dados/Data Warehouse em nuvem para ingestão dos dados, também tem a classe ProcessedDataCollection, que é a estrutura de dados que representa os dados na forma processada e pronta para ser carregada no BD

## problemas conhecidos

1) Auto-complete de imports do VSCODE n funciona com o package local do InteligenteEtl

   Resolução: No arquivo settings.json do VScode adicione esse pedaço de código:

   ```json
      "python.autoComplete.extraPaths": [
         "${workspaceFolder}/InteligenteEtl"
      ]
   ```