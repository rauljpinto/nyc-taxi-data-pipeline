# NYC Taxi Data Pipeline
Projeto de ETL e análise de dados de viagens de taxi usando Databricks e AWS. Este projeto busca demonstrar habilidades em Engenharia de Dados, orquestração, segurança e boas práticas de ETL.

### Dataset

- **Fonte**: https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data
- **Descrição**: Este dataset contém informações sobre viagems de taxi realizadas na cidade de Nova York, como tempo de viagem, horário de saída e chegada, distância e localização de saída e chegada.

### Ferramentas utilizadas

- AWS (Armazenamento)
- Databricks Community + Spark (Processamento e Orquestração)

### Instruções de execução

1. Clone o repositório
```bash
git clone https://github.com/rauljpinto/nyc-taxi-data-pipeline.git
```
2. Importe os notebooks no ambiente Databricks
3. Configure o Job usando `infrastructure/job_config.json`
4. Execute o Job manualmente pela UI se necessário
   
### Limitações

O Projeto foi desenvolvido usando licenças gratuitas da AWS e do Databricks, o que trouxe algumas limitações no desenvolvimento:

- **Bucket S3 Público**: O ambiente do Databricks Community só permite acesso a buckets públicos e não possui suporte e integração com IAM Roles, porém ainda é possível utilizar criptografia SSE-S3.
- **Escrita em buckets S3**: O Databricks Community também não permite escrita em buckets S3 devido a restrição de acesso anônimo, então a solução encontrada para o armazenamento das tabelas foi salvar no catálogo do próprio Databricks.
