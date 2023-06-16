# ed-obitos-covid
Projeto com o objetivo de fazer uma leitura dos dados de óbitos da covid de Alagoas e Minas Gerais do ano de 2020. Após isso tratar os dados e gerar algumas respostas sobre o tema.

<h3>Grupo: Caio Cesar Silva.</h3>

<h3><ins>Para testar o projeto siga os passos abaixo:</ins><h3>

<h3>Pré-requisitos:</h3>
<ul> 
  <li> Ter o docker instalado na sua máquina. </li>
</ul>

<h3>Passo a Passo:</h3>
<p>1 - Clonar o projeto em uma pasta.</p>
<p>2 - Abrir o terminal na pasta onde se encontra o projeto e digitar o seguinte comando: <ins>docker-compose up airflow-init</ins></p>
<p>3 - Após isso rodar o comando <ins>docker-compose up</ins></p>
<p>4 - Quando o servidor da aplicação iniciar, entrar no link http://localhost:8080, usando no login e na senha "airflow"</p>
<p>5 - Usando o comando <ins>docker-ls</ins> procurar o container_id do banco de dados Postgres</p>
<p>6 - Após identificar o container_id do Postgres usar o comando <ins>docker-inspect image xxx</ins>, onde xxx é o valor do container_id, para achar o "IPAddress"</p>
<p>7 - Substituir o valor do campo "host" na dag "dag_pos.py" pelo valor do "IPAddress" presente no código</p>
<p>8 - Criar um banco chamado "pos" e adicionar esse banco no airFlow</p>
<p>9 - executar a dag_pos</p>
<p>10 - executar o arquivo de SQL "respostas.sql" no banco de dados para conferir as respostas geradas a partir da leitura os dados de óbitos da covid de Alagoas e Minas Gerais do ano de 2020</p>
