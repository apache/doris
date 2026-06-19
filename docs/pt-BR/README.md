<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## 🌍 Leia em outros idiomas

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](../es-ES/README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

<div align="center">

# Apache Doris

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![GitHub release](https://img.shields.io/github/release/apache/doris.svg)](https://github.com/apache/doris/releases)
[![OSSRank](https://shields.io/endpoint?url=https://ossrank.com/shield/516)](https://ossrank.com/p/516)
[![Commit activity](https://img.shields.io/github/commit-activity/m/apache/doris)](https://github.com/apache/doris/commits/master/)
[![EN doc](https://img.shields.io/badge/Docs-English-blue.svg)](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](https://doris.apache.org/zh-CN/docs/gettingStarted/what-is-apache-doris)

<div>

[![Official Website](<https://img.shields.io/badge/-Visit%20the%20Official%20Website%20%E2%86%92-rgb(15,214,106)?style=for-the-badge>)](https://doris.apache.org/)
[![Quick Download](<https://img.shields.io/badge/-Quick%20%20Download%20%E2%86%92-rgb(66,56,255)?style=for-the-badge>)](https://doris.apache.org/download)


</div>


<div>
    <a href="https://twitter.com/doris_apache"><img src="https://img.shields.io/badge/- @Doris_Apache -424549?style=social&logo=x" height=25></a>
    &nbsp;
    <a href="https://github.com/apache/doris/discussions"><img src="https://img.shields.io/badge/- Discussion -red?style=social&logo=discourse" height=25></a>
    &nbsp;
    <a href="https://doris.apache.org/slack"><img src="https://img.shields.io/badge/-Slack-4A154B?style=social&logo=slack" height=25 alt="Slack"></a>
    &nbsp;
    <a href="https://medium.com/@ApacheDoris"><img src="https://img.shields.io/badge/-Medium-red?style=social&logo=medium" height=25></a>

</div>

</div>

---

<p align="center">

  <a href="https://trendshift.io/repositories/1156" target="_blank"><img src="https://trendshift.io/api/badge/repositories/1156" alt="apache%2Fdoris | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

</p>




Apache Doris é um banco de dados analítico fácil de usar, de alto desempenho e em tempo real baseado na arquitetura MPP, conhecido por sua velocidade extrema e facilidade de uso. Requer apenas um tempo de resposta inferior a um segundo para retornar resultados de consulta sob dados massivos e pode suportar não apenas cenários de consulta pontual de alta concorrência, mas também cenários de análise complexa de alto throughput.

Tudo isso torna o Apache Doris uma ferramenta ideal para cenários incluindo análise de relatórios, consulta ad-hoc, data warehouse unificado e aceleração de consulta de data lake. No Apache Doris, os usuários podem construir várias aplicações, como análise de comportamento do usuário, plataforma de teste AB, análise de recuperação de logs, análise de perfil do usuário e análise de pedidos.

🎉 Confira 🔗[Todas as versões](https://doris.apache.org/docs/releasenotes/all-release), onde você encontrará um resumo cronológico das versões do Apache Doris lançadas no ano passado.

👀 Explore o 🔗[Site oficial](https://doris.apache.org/) para descobrir em detalhes os recursos principais, blogs e casos de uso do Apache Doris.

## 📈 Cenários de uso

Como mostrado na figura abaixo, após várias integrações e processamentos de dados, as fontes de dados geralmente são armazenadas no data warehouse em tempo real Apache Doris e no data lake offline ou data warehouse (em Apache Hive, Apache Iceberg ou Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris é amplamente usado nos seguintes cenários:

- **Análise de dados em tempo real**:

  - **Relatórios e tomada de decisão em tempo real**: Doris fornece relatórios e painéis atualizados em tempo real para uso empresarial interno e externo, apoiando a tomada de decisão em tempo real em processos automatizados.
  
  - **Análise ad-hoc**: Doris oferece capacidades de análise de dados multidimensionais, permitindo análises rápidas de business intelligence e consultas ad-hoc para ajudar os usuários a descobrir rapidamente insights de dados complexos.
  
  - **Perfilamento de usuários e análise de comportamento**: Doris pode analisar comportamentos de usuários como participação, retenção e conversão, enquanto também suporta cenários como insights demográficos e seleção de grupos para análise de comportamento.

- **Análise de data lake**:

  - **Aceleração de consulta de data lake**: Doris acelera consultas de dados de data lake com seu motor de consulta eficiente.
  
  - **Análise federada**: Doris suporta consultas federadas em várias fontes de dados, simplificando a arquitetura e eliminando silos de dados.
  
  - **Processamento de dados em tempo real**: Doris combina capacidades de processamento de fluxos de dados em tempo real e em lote para atender às necessidades de alta concorrência e baixa latência de requisitos comerciais complexos.

- **Observabilidade baseada em SQL**:

  - **Análise de logs e eventos**: Doris permite análise em tempo real ou em lote de logs e eventos em sistemas distribuídos, ajudando a identificar problemas e otimizar o desempenho.


## Arquitetura geral

Apache Doris usa o protocolo MySQL, é altamente compatível com a sintaxe MySQL e suporta SQL padrão. Os usuários podem acessar o Apache Doris através de várias ferramentas cliente e ele se integra perfeitamente com ferramentas BI.

### Arquitetura integrada de armazenamento e computação

A arquitetura integrada de armazenamento e computação do Apache Doris é simplificada e fácil de manter. Como mostrado na figura abaixo, consiste apenas em dois tipos de processos:

- **Frontend (FE):** Principalmente responsável por lidar com solicitações de usuários, análise e planejamento de consultas, gerenciamento de metadados e tarefas de gerenciamento de nós.

- **Backend (BE):** Principalmente responsável pelo armazenamento de dados e execução de consultas. Os dados são particionados em fragmentos e armazenados com múltiplas réplicas em nós BE.

![A arquitetura geral do Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

Em um ambiente de produção, vários nós FE podem ser implantados para recuperação de desastres. Cada nó FE mantém uma cópia completa dos metadados. Os nós FE são divididos em três funções:

| Função      | Descrição                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | O nó FE Master é responsável pelas operações de leitura e gravação de metadados. Quando ocorrem alterações de metadados no Master, elas são sincronizadas para nós Follower ou Observer através do protocolo BDB JE. |
| Follower  | O nó Follower é responsável por ler metadados. Se o nó Master falhar, um nó Follower pode ser selecionado como novo Master. |
| Observer  | O nó Observer é responsável por ler metadados e é usado principalmente para aumentar a concorrência de consultas. Ele não participa das eleições de liderança do cluster. |

Tanto os processos FE quanto BE são escaláveis horizontalmente, permitindo que um único cluster suporte centenas de máquinas e dezenas de petabytes de capacidade de armazenamento. Os processos FE e BE usam um protocolo de consistência para garantir alta disponibilidade de serviços e alta confiabilidade de dados. A arquitetura integrada de armazenamento e computação é altamente integrada, reduzindo significativamente a complexidade operacional de sistemas distribuídos.


## Recursos principais do Apache Doris

- **Alta disponibilidade**: No Apache Doris, tanto os metadados quanto os dados são armazenados com múltiplas réplicas, sincronizando logs de dados através do protocolo quorum. A gravação de dados é considerada bem-sucedida assim que a maioria das réplicas concluir a gravação, garantindo que o cluster permaneça disponível mesmo se alguns nós falharem. Apache Doris suporta recuperação de desastres na mesma cidade e entre regiões, permitindo modos mestre-escravo de cluster duplo. Quando alguns nós experimentam falhas, o cluster pode isolar automaticamente os nós com falha, impedindo que a disponibilidade geral do cluster seja afetada.

- **Alta compatibilidade**: Apache Doris é altamente compatível com o protocolo MySQL e suporta sintaxe SQL padrão, cobrindo a maioria das funções MySQL e Hive. Esta alta compatibilidade permite que os usuários migrem e integrem facilmente aplicações e ferramentas existentes. Apache Doris suporta o ecossistema MySQL, permitindo que os usuários conectem Doris usando ferramentas cliente MySQL para operações e manutenção mais convenientes. Também suporta compatibilidade de protocolo MySQL para ferramentas de relatório BI e ferramentas de transmissão de dados, garantindo eficiência e estabilidade nos processos de análise de dados e transmissão de dados.

- **Data warehouse em tempo real**: Com base no Apache Doris, um serviço de data warehouse em tempo real pode ser construído. Apache Doris oferece capacidades de ingestão de dados em nível de segundo, capturando mudanças incrementais de bancos de dados transacionais online upstream no Doris em questão de segundos. Aproveitando motores vetorizados, arquitetura MPP e motores de execução Pipeline, Doris fornece capacidades de consulta de dados inferiores a um segundo, construindo assim uma plataforma de data warehouse em tempo real de alto desempenho e baixa latência.

- **Data lake unificado**: Apache Doris pode construir uma arquitetura de data lake unificada baseada em fontes de dados externas, como data lakes ou bancos de dados relacionais. A solução de data lake unificada do Doris permite integração perfeita e fluxo livre de dados entre data lakes e data warehouses, ajudando os usuários a utilizar diretamente as capacidades do data warehouse para resolver problemas de análise de dados em data lakes, enquanto aproveita plenamente as capacidades de gerenciamento de dados do data lake para aumentar o valor dos dados.

- **Modelagem flexível**: Apache Doris oferece várias abordagens de modelagem, como modelos de tabela larga, modelos de pré-agregação, esquemas estrela/floco de neve, etc. Durante a importação de dados, os dados podem ser achatados em tabelas largas e escritos no Doris através de motores de computação como Flink ou Spark, ou os dados podem ser importados diretamente no Doris, realizando operações de modelagem de dados através de visualizações, visualizações materializadas ou junções multi-tabela em tempo real.

## Visão geral técnica

Doris fornece uma interface SQL eficiente e é totalmente compatível com o protocolo MySQL. Seu motor de consulta é baseado em uma arquitetura MPP (processamento massivamente paralelo), capaz de executar eficientemente consultas analíticas complexas e alcançar consultas em tempo real de baixa latência. Através da tecnologia de armazenamento em colunas para codificação e compressão de dados, otimiza significativamente o desempenho da consulta e a taxa de compressão do armazenamento.

### Interface

Apache Doris adota o protocolo MySQL, suporta SQL padrão e é altamente compatível com a sintaxe MySQL. Os usuários podem acessar o Apache Doris através de várias ferramentas cliente e integrá-lo perfeitamente com ferramentas BI, incluindo, mas não limitado a Smartbi, DataEase, FineBI, Tableau, Power BI e Apache Superset. Apache Doris pode funcionar como fonte de dados para qualquer ferramenta BI que suporte o protocolo MySQL.

### Motor de armazenamento

Apache Doris tem um motor de armazenamento em colunas, que codifica, comprime e lê dados por coluna. Isso permite uma taxa de compressão de dados muito alta e reduz muito a varredura desnecessária de dados, fazendo assim um uso mais eficiente dos recursos IO e CPU.

Apache Doris suporta várias estruturas de índice para minimizar varreduras de dados:

- **Índice de chave composta ordenada**: Os usuários podem especificar no máximo três colunas para formar uma chave de classificação composta. Isso pode efetivamente podar dados para melhor suportar cenários de relatórios altamente concorrentes.

- **Índice Min/Max**: Isso permite filtragem efetiva de dados em consultas de equivalência e intervalo de tipos numéricos.

- **Índice BloomFilter**: Isso é muito eficaz na filtragem de equivalência e poda de colunas de alta cardinalidade.

- **Índice invertido**: Isso permite busca rápida para qualquer campo.

Apache Doris suporta uma variedade de modelos de dados e os otimizou para diferentes cenários:

- **Modelo de detalhe (Modelo de chave duplicada):** Um modelo de dados de detalhe projetado para atender aos requisitos de armazenamento detalhado de tabelas de fatos.

- **Modelo de chave primária (Modelo de chave única):** Garante chaves únicas; dados com a mesma chave são sobrescritos, permitindo atualizações de dados em nível de linha.

- **Modelo de agregação (Modelo de chave de agregação):** Mescla colunas de valores com a mesma chave, melhorando significativamente o desempenho através de pré-agregação.

Apache Doris também suporta visualizações materializadas de tabela única fortemente consistentes e visualizações materializadas multi-tabela atualizadas de forma assíncrona. Visualizações materializadas de tabela única são atualizadas e mantidas automaticamente pelo sistema, sem exigir intervenção manual dos usuários. Visualizações materializadas multi-tabela podem ser atualizadas periodicamente usando agendamento dentro do cluster ou ferramentas de agendamento externas, reduzindo a complexidade da modelagem de dados.

### 🔍 Motor de consulta

Apache Doris tem um motor de consulta baseado em MPP para execução paralela entre e dentro de nós. Suporta junção shuffle distribuída para tabelas grandes para lidar melhor com consultas complicadas.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

O motor de consulta do Apache Doris é totalmente vetorizado, com todas as estruturas de memória dispostas em um formato de colunas. Isso pode reduzir muito as chamadas de função virtual, aumentar as taxas de acerto do cache e fazer uso eficiente de instruções SIMD. Apache Doris oferece um desempenho de 5 a 10 vezes maior em cenários de agregação de tabela larga do que motores não vetorizados.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris usa tecnologia de execução de consulta adaptativa para ajustar dinamicamente o plano de execução com base em estatísticas de tempo de execução. Por exemplo, pode gerar um filtro de tempo de execução e empurrá-lo para o lado da sonda. Especificamente, empurra os filtros para o nó de varredura de nível mais baixo no lado da sonda, o que reduz muito a quantidade de dados a serem processados e aumenta o desempenho da junção. O filtro de tempo de execução do Apache Doris suporta In/Min/Max/Bloom Filter.

Apache Doris usa um motor de execução Pipeline que divide consultas em várias sub-tarefas para execução paralela, aproveitando plenamente as capacidades de CPU multi-core. Simultaneamente aborda o problema de explosão de threads limitando o número de threads de consulta. O motor de execução Pipeline reduz cópia e compartilhamento de dados, otimiza operações de classificação e agregação, melhorando assim significativamente a eficiência e o throughput da consulta.

Em termos do otimizador, Apache Doris emprega uma estratégia de otimização combinada de CBO (otimizador baseado em custo), RBO (otimizador baseado em regras) e HBO (otimizador baseado em histórico). RBO suporta dobramento constante, reescrita de subconsulta, pushdown de predicado e mais. CBO suporta reordenação de junção e outras otimizações. HBO recomenda o plano de execução ótimo com base em informações de consulta histórica. Essas múltiplas medidas de otimização garantem que Doris possa enumerar planos de consulta de alto desempenho para vários tipos de consultas.


## 🎆 Por que escolher Apache Doris?

- 🎯 **Fácil de usar**: Dois processos, sem outras dependências; escalonamento de cluster online, recuperação automática de réplicas; compatível com protocolo MySQL e usando SQL padrão.

- 🚀 **Alto desempenho**: Desempenho extremamente rápido para consultas de baixa latência e alto throughput com motor de armazenamento em colunas, arquitetura MPP moderna, motor de consulta vetorizado, visualização materializada pré-agregada e índice de dados.

- 🖥️ **Unificado único**: Um único sistema pode suportar cenários de serviço de dados em tempo real, análise de dados interativa e processamento de dados offline.

- ⚛️ **Consulta federada**: Suporta consulta federada de data lakes como Hive, Iceberg, Hudi e bancos de dados como MySQL e Elasticsearch.

- ⏩ **Vários métodos de importação de dados**: Suporta importação em lote de HDFS/S3 e importação de fluxo de MySQL Binlog/Kafka; suporta escrita micro-lote através de interface HTTP e escrita em tempo real usando Insert em JDBC.

- 🚙 **Ecologia rica**: Spark usa Spark-Doris-Connector para ler e escrever Doris; Flink-Doris-Connector permite que Flink CDC implemente escrita de dados exatamente uma vez no Doris; DBT Doris Adapter é fornecido para transformar dados no Doris com DBT.

## 🙌 Contribuidores

**Apache Doris se formou com sucesso no incubador Apache e se tornou um projeto de nível superior em junho de 2022**.

Agradecemos profundamente os 🔗[contribuidores da comunidade](https://github.com/apache/doris/graphs/contributors) por sua contribuição ao Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Usuários

Apache Doris agora tem uma ampla base de usuários na China e em todo o mundo, e até hoje, **Apache Doris é usado em ambientes de produção em milhares de empresas em todo o mundo.** Mais de 80% das 50 principais empresas de Internet na China em termos de capitalização de mercado ou avaliação têm usado Apache Doris por muito tempo, incluindo Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo e Ke Holdings. Também é amplamente usado em algumas indústrias tradicionais, como finanças, energia, manufatura e telecomunicações.

Os usuários do Apache Doris: 🔗[Usuários](https://doris.apache.org/users)

Adicione o logotipo da sua empresa no site do Apache Doris: 🔗[Adicionar sua empresa](https://github.com/apache/doris/discussions/27683)
 
## 👣 Começar

### 📚 Documentação

Toda a documentação   🔗[Documentação](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Download 

Todas as versões de release e binárias 🔗[Download](https://doris.apache.org/download) 

### 🗄️ Compilar

Veja como compilar  🔗[Compilação](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Instalar

Veja como instalar e implantar 🔗[Instalação e implantação](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Componentes

### 📝 Doris Connector

Doris fornece suporte para Spark/Flink para ler dados armazenados no Doris através do Connector e também suporta escrever dados no Doris através do Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Comunidade e suporte

### 📤 Inscrever-se nas listas de correio

A lista de correio é a forma mais reconhecida de comunicação na comunidade Apache. Veja como 🔗[Inscrever-se nas listas de correio](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Relatar problemas ou enviar Pull Request

Se você tiver alguma dúvida, sinta-se à vontade para apresentar um 🔗[GitHub Issue](https://github.com/apache/doris/issues) ou publicá-lo em 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) e corrigi-lo enviando um 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 Como contribuir

Acolhemos suas sugestões, comentários (incluindo críticas), comentários e contribuições. Veja 🔗[Como contribuir](https://doris.apache.org/community/how-to-contribute/) e 🔗[Guia de envio de código](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Propostas de melhoria do Doris (DSIP)

🔗[Proposta de melhoria do Doris (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) pode ser pensada como **Uma coleção de documentos de design para todas as atualizações ou melhorias principais de recursos**.

### 🔑 Especificação de codificação Backend C++
🔗 [Especificação de codificação Backend C++](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) deve ser rigorosamente seguida, o que nos ajudará a alcançar melhor qualidade de código.

## 💬 Entre em contato conosco

Entre em contato conosco através da seguinte lista de correio.

| Nome                                                                          | Escopo                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Discussões relacionadas ao desenvolvimento | [Inscrever-se](mailto:dev-subscribe@doris.apache.org)   | [Cancelar inscrição](mailto:dev-unsubscribe@doris.apache.org)   | [Arquivos](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Links

* Site oficial do Apache Doris - [Site](https://doris.apache.org)
* Lista de correio de desenvolvedores - <dev@doris.apache.org>. Envie um e-mail para <dev-subscribe@doris.apache.org>, siga a resposta para se inscrever na lista de correio.
* Canal Slack - [Junte-se ao Slack](https://doris.apache.org/slack)
* Twitter - [Seguir @doris_apache](https://twitter.com/doris_apache)


## 📜 Licença

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Nota**
> Algumas licenças das dependências de terceiros não são compatíveis com a licença Apache 2.0. Portanto, você precisa desabilitar
alguns recursos do Doris para estar em conformidade com a licença Apache 2.0. Para detalhes, consulte o arquivo `thirdparty/LICENSE.txt`


