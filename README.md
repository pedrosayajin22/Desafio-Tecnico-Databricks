#  Desafio Técnico - Databricks

## 📌 Descrição do Projeto
Este projeto tem como objetivo a **ingestão, transformação e agregação de dados** de uma API externa (*Open Brewery DB*) utilizando **Databricks e Delta Lake**. O pipeline segue a arquitetura **Bronze → Prata → Ouro**.

---

## 🛠️ **Tecnologias Utilizadas**
- **Databricks**: Orquestração do pipeline e processamento distribuído.
- **Delta Lake**: Armazenamento otimizado e versionado.
- **PySpark**: Processamento e transformação dos dados.
- **API Open Brewery DB**: Fonte de dados utilizada.
- **Databricks Jobs**: Agendamento e execução automatizada do pipeline.

---

## 📊 **Arquitetura do Pipeline**
O pipeline é dividido em três camadas:

###  **Camada Bronze**
📥 **Objetivo**: Ingestão bruta dos dados da API, sem transformações.
- Coleta os dados da **API Open Brewery DB**.
- Implementa **tratamento de erros** e logs no Delta Lake.
- Adiciona um **timestamp de ingestão** para rastreabilidade.

📌 **Armazenamento**: `/mnt/datalake/bronze/cervejarias`

---

###  **Camada Prata**
🛠️ **Objetivo**: Limpeza e padronização dos dados.
- Remove **valores nulos e inconsistências**.
- Padroniza **nomes e formatações**.
- Remove **duplicatas** considerando nome, localização e endereço.
- Cria **colunas derivadas** como:
  - `Possui_Site?` (booleano indicando se há site cadastrado).
  - `Esta_Ativa?` (se a cervejaria está ativa ou fechada).
  - `Localizacao` (concatenação de país, estado e cidade).

📌 **Armazenamento**: `/mnt/datalake/prata/cervejarias`

---

### **Camada Ouro**
📊 **Objetivo**: Agregação dos dados para análise.
- **Consolida os dados** removendo colunas desnecessárias.
- Realiza **merge incremental** para evitar duplicações.
- Gera uma **visão agregada** com a **quantidade de cervejarias por tipo e localização**.

📌 **Armazenamento**:
- `/mnt/datalake/ouro/cervejarias`
- `/mnt/datalake/ouro/cervejarias_agregadas` (dados agregados)

---

## 🔄 **Gerenciamento de Erros e Rollback**
Para garantir resiliência, o pipeline implementa um **mecanismo de rollback**. Caso uma etapa falhe, o Databricks executa um rollback na camada correspondente.

- **Se a Camada Bronze falhar**, executa `Rollback_Bronze` e tenta continuar.
- **Se a Camada Prata falhar**, executa `Rollback_Prata` e reprocessa os dados.
- **Se a Camada Ouro falhar**, executa `Rollback_Ouro` e tenta reprocessar os dados agregados.

✅ **Reprocessamento automático**: Se um rollback for bem-sucedido, a camada correspondente é **reexecutada automaticamente**.

---

## **Orquestração do Pipeline**
O pipeline é executado via **Databricks Jobs**, configurado da seguinte forma:

### 🔹 Fluxo de Execução:
1. **Camada Bronze** → Captura os dados da API.
2. **Camada Prata** → Limpa e transforma os dados.
3. **Camada Ouro** → Agrega os dados para análise.
4. **Rollback automático** em caso de falha.

### 🔹 Configuração do Job:
- **`max_retries: 3`** → Cada etapa pode ser reexecutada até 3 vezes.
- **`retry_on_timeout: false`** → Não reexecuta em caso de timeout.
- **`run_if: ALL_SUCCESS`** → Uma etapa só inicia se a anterior for bem-sucedida.
- **`run_if: ALL_FAILED`** → Ativa rollback em caso de falha.

---

## **Time Travel e Versionamento**
O Delta Lake permite recuperar versões anteriores dos dados caso necessário. Isso é útil para auditoria e rollback manual.
