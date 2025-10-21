# DDL (Documentação)

Esta pasta mantém apenas documentação sobre as decisões de DDL do projeto. As instruções de criação, atualização e validação de tabelas são implementadas diretamente nos pipelines Python, o que garante versionamento único do comportamento.

## Onde as DDL estão definidas

- **Dimensões**: cada arquivo em `src/models/dimensions/` recria sua tabela (DROP + CREATE) e povoa os dados, incluindo chaves primárias e registros `SK=0`.
- **Fatos**: os arquivos em `src/models/facts/` criam as tabelas fato com todas as `FOREIGN KEY` necessárias.
- **Rotinas utilitárias**: scripts como `run_all_dimensions.py` e `run_all_raw.py` orquestram a execução completa do ciclo de carga.

## Como garantir integridade

1. Execute as rotinas RAW (`run_all_raw.py`).
2. Carregue dimensões (`run_all_dimensions.py`).
3. Gere as tabelas fato necessárias (`src/models/facts/*.py`).

Durante esses passos, as constraints de PK/FK são recriadas automaticamente. Não há scripts manuais adicionais nesta pasta.
