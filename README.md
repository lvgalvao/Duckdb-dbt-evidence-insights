# Duckdb-dbt-evidence-insights

Projeto de Python para gerar insights do número de downloads do Duckdb no Pypi

## Instalar e Configurar

### 1) Baixar o repo

```bash
git clone git@github.com:lvgalvao/Duckdb-dbt-evidence-insights.git
cd Duckdb-dbt-evidence-insights
```

### 2) Configurar versão do Python

```bash
pyenv local 3.11.5
```

### 3) Configurar ambiente virtual
```bash
poetry init
poetry env use 3.11.5
poetry shell
poety install
```

### 4) Realizar os testes

```bash
task tests
```

### 5) Rodar a Pipeline
```bash
task run
```
