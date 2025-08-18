"""
Sistema de Migrations para Data Warehouse OES-NPG
Gerador de Scripts DDL para Oracle e PostgreSQL
Autor: Sistema IA
Data: 2024-12-28
"""

import os
import json
from typing import Dict, List, Tuple
from datetime import datetime
from dataclasses import dataclass, asdict
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()


@dataclass
class Column:
    """Representa uma coluna da tabela"""
    name: str
    data_type: str
    size: int = None
    precision: int = None
    scale: int = None
    not_null: bool = False
    primary_key: bool = False
    foreign_key: str = None
    default: str = None
    comment: str = None


@dataclass
class Table:
    """Representa uma tabela do DW"""
    name: str
    columns: List[Column]
    table_type: str  # 'dimension' ou 'fact'
    comment: str = None


@dataclass
class Schema:
    """Representa o schema completo do DW"""
    name: str
    tables: List[Table]
    sequences: List[str] = None


class DatabaseDialect:
    """Classe base para dialetos de banco"""
    
    def __init__(self):
        self.type_mapping = {}
        self.sequence_template = ""
        self.table_template = ""
        self.pk_template = ""
        self.fk_template = ""
        self.comment_template = ""
    
    def map_data_type(self, column: Column) -> str:
        """Mapeia tipos de dados para o dialeto específico"""
        raise NotImplementedError
    
    def generate_create_table(self, table: Table) -> str:
        """Gera DDL CREATE TABLE"""
        raise NotImplementedError
    
    def generate_primary_key(self, table: Table) -> str:
        """Gera DDL PRIMARY KEY"""
        raise NotImplementedError
    
    def generate_foreign_keys(self, table: Table) -> List[str]:
        """Gera DDL FOREIGN KEY"""
        raise NotImplementedError
    
    def generate_sequences(self, table: Table) -> List[str]:
        """Gera DDL SEQUENCE"""
        raise NotImplementedError


class PostgreSQLDialect(DatabaseDialect):
    """Dialeto PostgreSQL"""
    
    def __init__(self):
        super().__init__()
        self.type_mapping = {
            'INTEGER': 'INTEGER',
            'BIGINT': 'BIGINT',
            'VARCHAR': 'VARCHAR',
            'TEXT': 'TEXT',
            'DATE': 'DATE',
            'TIMESTAMP': 'TIMESTAMP',
            'DECIMAL': 'DECIMAL',
            'BOOLEAN': 'BOOLEAN',
            'SERIAL': 'SERIAL'
        }
    
    def map_data_type(self, column: Column) -> str:
        """Mapeia tipos PostgreSQL"""
        base_type = self.type_mapping.get(column.data_type, column.data_type)
        
        if column.data_type == 'VARCHAR' and column.size:
            return f"VARCHAR({column.size})"
        elif column.data_type == 'DECIMAL' and column.precision and column.scale:
            return f"DECIMAL({column.precision},{column.scale})"
        
        return base_type
    
    def generate_create_table(self, table: Table) -> str:
        """Gera CREATE TABLE PostgreSQL"""
        lines = [f"-- Tabela: {table.name}"]
        if table.comment:
            lines.append(f"-- {table.comment}")
        
        lines.append(f"CREATE TABLE {table.name} (")
        
        column_definitions = []
        for col in table.columns:
            col_def = f"    {col.name} {self.map_data_type(col)}"
            
            if col.not_null:
                col_def += " NOT NULL"
            
            if col.default:
                col_def += f" DEFAULT {col.default}"
            
            column_definitions.append(col_def)
        
        lines.append(",\n".join(column_definitions))
        lines.append(");")
        
        # Comentários das colunas
        for col in table.columns:
            if col.comment:
                lines.append(f"COMMENT ON COLUMN {table.name}.{col.name} IS '{col.comment}';")
        
        if table.comment:
            lines.append(f"COMMENT ON TABLE {table.name} IS '{table.comment}';")
        
        return "\n".join(lines)
    
    def generate_primary_key(self, table: Table) -> str:
        """Gera PRIMARY KEY PostgreSQL"""
        pk_columns = [col.name for col in table.columns if col.primary_key]
        if pk_columns:
            pk_name = f"pk_{table.name}"
            return f"ALTER TABLE {table.name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({', '.join(pk_columns)});"
        return ""
    
    def generate_foreign_keys(self, table: Table) -> List[str]:
        """Gera FOREIGN KEYs PostgreSQL"""
        fks = []
        for col in table.columns:
            if col.foreign_key:
                fk_name = f"fk_{table.name}_{col.name}"
                fks.append(f"ALTER TABLE {table.name} ADD CONSTRAINT {fk_name} FOREIGN KEY ({col.name}) REFERENCES {col.foreign_key};")
        return fks
    
    def generate_sequences(self, table: Table) -> List[str]:
        """Gera SEQUENCEs PostgreSQL"""
        sequences = []
        for col in table.columns:
            if col.data_type == 'SERIAL':
                seq_name = f"seq_{table.name}_{col.name}"
                sequences.append(f"CREATE SEQUENCE {seq_name} START 1 INCREMENT 1;")
        return sequences


class OracleDialect(DatabaseDialect):
    """Dialeto Oracle"""
    
    def __init__(self):
        super().__init__()
        self.type_mapping = {
            'INTEGER': 'NUMBER(10)',
            'BIGINT': 'NUMBER(19)',
            'VARCHAR': 'VARCHAR2',
            'TEXT': 'CLOB',
            'DATE': 'DATE',
            'TIMESTAMP': 'TIMESTAMP',
            'DECIMAL': 'NUMBER',
            'BOOLEAN': 'NUMBER(1)',
            'SERIAL': 'NUMBER(10)'
        }
    
    def map_data_type(self, column: Column) -> str:
        """Mapeia tipos Oracle"""
        base_type = self.type_mapping.get(column.data_type, column.data_type)
        
        if column.data_type == 'VARCHAR' and column.size:
            return f"VARCHAR2({column.size})"
        elif column.data_type == 'DECIMAL' and column.precision and column.scale:
            return f"NUMBER({column.precision},{column.scale})"
        
        return base_type
    
    def generate_create_table(self, table: Table) -> str:
        """Gera CREATE TABLE Oracle"""
        lines = [f"-- Tabela: {table.name}"]
        if table.comment:
            lines.append(f"-- {table.comment}")
        
        lines.append(f"CREATE TABLE {table.name} (")
        
        column_definitions = []
        for col in table.columns:
            col_def = f"    {col.name} {self.map_data_type(col)}"
            
            if col.not_null:
                col_def += " NOT NULL"
            
            if col.default:
                col_def += f" DEFAULT {col.default}"
            
            column_definitions.append(col_def)
        
        lines.append(",\n".join(column_definitions))
        lines.append(");")
        
        # Comentários das colunas
        for col in table.columns:
            if col.comment:
                lines.append(f"COMMENT ON COLUMN {table.name}.{col.name} IS '{col.comment}';")
        
        if table.comment:
            lines.append(f"COMMENT ON TABLE {table.name} IS '{table.comment}';")
        
        return "\n".join(lines)
    
    def generate_primary_key(self, table: Table) -> str:
        """Gera PRIMARY KEY Oracle"""
        pk_columns = [col.name for col in table.columns if col.primary_key]
        if pk_columns:
            pk_name = f"PK_{table.name.upper()}"
            return f"ALTER TABLE {table.name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({', '.join(pk_columns)});"
        return ""
    
    def generate_foreign_keys(self, table: Table) -> List[str]:
        """Gera FOREIGN KEYs Oracle"""
        fks = []
        for col in table.columns:
            if col.foreign_key:
                fk_name = f"FK_{table.name.upper()}_{col.name.upper()}"
                fks.append(f"ALTER TABLE {table.name} ADD CONSTRAINT {fk_name} FOREIGN KEY ({col.name}) REFERENCES {col.foreign_key};")
        return fks
    
    def generate_sequences(self, table: Table) -> List[str]:
        """Gera SEQUENCEs Oracle"""
        sequences = []
        for col in table.columns:
            if col.data_type == 'SERIAL':
                seq_name = f"SEQ_{table.name.upper()}_{col.name.upper()}"
                sequences.append(f"CREATE SEQUENCE {seq_name} START WITH 1 INCREMENT BY 1 NOCACHE;")
        return sequences


class MigrationGenerator:
    """Gerador principal de migrations"""
    
    def __init__(self):
        self.postgresql_dialect = PostgreSQLDialect()
        self.oracle_dialect = OracleDialect()
        self.schema = self._define_schema()
    
    def _define_schema(self) -> Schema:
        """Define o schema completo do DW OES-NPG"""
        
        # Dimensão Tempo
        dim_tempo = Table(
            name="dim_tempo",
            table_type="dimension",
            comment="Dimensão temporal com hierarquia ano/mês/dia",
            columns=[
                Column("sk_tempo", "SERIAL", primary_key=True, comment="Chave surrogate temporal"),
                Column("data_completa", "DATE", not_null=True, comment="Data completa"),
                Column("ano", "INTEGER", not_null=True, comment="Ano"),
                Column("mes", "INTEGER", not_null=True, comment="Mês"),
                Column("dia", "INTEGER", not_null=True, comment="Dia"),
                Column("trimestre", "INTEGER", not_null=True, comment="Trimestre"),
                Column("semestre", "INTEGER", not_null=True, comment="Semestre"),
                Column("nome_mes", "VARCHAR", size=20, comment="Nome do mês"),
                Column("nome_dia_semana", "VARCHAR", size=20, comment="Nome do dia da semana"),
                Column("fim_de_semana", "BOOLEAN", comment="Indicador de fim de semana"),
                Column("feriado", "BOOLEAN", comment="Indicador de feriado")
            ]
        )
        
        # Dimensão Localidade
        dim_localidade = Table(
            name="dim_localidade",
            table_type="dimension",
            comment="Dimensão geográfica com hierarquia região/UF/município",
            columns=[
                Column("sk_localidade", "SERIAL", primary_key=True, comment="Chave surrogate geográfica"),
                Column("codigo_uf", "VARCHAR", size=2, not_null=True, comment="Código UF IBGE"),
                Column("nome_uf", "VARCHAR", size=50, not_null=True, comment="Nome da UF"),
                Column("sigla_uf", "VARCHAR", size=2, not_null=True, comment="Sigla da UF"),
                Column("codigo_regiao", "INTEGER", not_null=True, comment="Código da região IBGE"),
                Column("nome_regiao", "VARCHAR", size=20, not_null=True, comment="Nome da região"),
                Column("codigo_municipio", "VARCHAR", size=7, comment="Código do município IBGE"),
                Column("nome_municipio", "VARCHAR", size=100, comment="Nome do município"),
                Column("capital", "BOOLEAN", comment="Indicador de capital"),
                Column("populacao", "INTEGER", comment="População estimada"),
                Column("area_km2", "DECIMAL", precision=10, scale=2, comment="Área em km²")
            ]
        )
        
        # Dimensão Tema
        dim_tema = Table(
            name="dim_tema",
            table_type="dimension",
            comment="Dimensão de temas de pesquisa com hierarquia área/subárea",
            columns=[
                Column("sk_tema", "SERIAL", primary_key=True, comment="Chave surrogate do tema"),
                Column("codigo_area_conhecimento", "VARCHAR", size=10, not_null=True, comment="Código da área CAPES"),
                Column("nome_area_conhecimento", "VARCHAR", size=200, not_null=True, comment="Nome da área de conhecimento"),
                Column("codigo_subarea_conhecimento", "VARCHAR", size=10, comment="Código da subárea CAPES"),
                Column("nome_subarea_conhecimento", "VARCHAR", size=200, comment="Nome da subárea de conhecimento"),
                Column("codigo_especialidade", "VARCHAR", size=10, comment="Código da especialidade"),
                Column("nome_especialidade", "VARCHAR", size=200, comment="Nome da especialidade"),
                Column("grande_area", "VARCHAR", size=100, comment="Grande área do conhecimento"),
                Column("ativo", "BOOLEAN", default="true", comment="Indicador de tema ativo")
            ]
        )
        
        # Dimensão ODS
        dim_ods = Table(
            name="dim_ods",
            table_type="dimension",
            comment="Dimensão dos Objetivos de Desenvolvimento Sustentável (ONU)",
            columns=[
                Column("sk_ods", "SERIAL", primary_key=True, comment="Chave surrogate ODS"),
                Column("numero_ods", "INTEGER", not_null=True, comment="Número do ODS (1-17)"),
                Column("titulo_ods", "VARCHAR", size=200, not_null=True, comment="Título do ODS"),
                Column("descricao_ods", "TEXT", comment="Descrição detalhada do ODS"),
                Column("cor_oficial", "VARCHAR", size=7, comment="Cor oficial do ODS (hex)"),
                Column("icone_url", "VARCHAR", size=500, comment="URL do ícone oficial"),
                Column("categoria", "VARCHAR", size=50, comment="Categoria do ODS"),
                Column("prioridade_brasil", "INTEGER", comment="Prioridade no contexto brasileiro"),
                Column("ativo", "BOOLEAN", default="true", comment="Indicador de ODS ativo")
            ]
        )
        
        # Dimensão IES
        dim_ies = Table(
            name="dim_ies",
            table_type="dimension",
            comment="Dimensão das Instituições de Ensino Superior",
            columns=[
                Column("sk_ies", "SERIAL", primary_key=True, comment="Chave surrogate da IES"),
                Column("codigo_ies", "VARCHAR", size=10, not_null=True, comment="Código MEC da IES"),
                Column("nome_ies", "VARCHAR", size=200, not_null=True, comment="Nome da IES"),
                Column("sigla_ies", "VARCHAR", size=20, comment="Sigla da IES"),
                Column("natureza_juridica", "VARCHAR", size=50, comment="Natureza jurídica"),
                Column("categoria_administrativa", "VARCHAR", size=50, comment="Categoria administrativa"),
                Column("organizacao_academica", "VARCHAR", size=50, comment="Organização acadêmica"),
                Column("sk_localidade", "INTEGER", foreign_key="dim_localidade(sk_localidade)", comment="FK para localidade"),
                Column("endereco", "VARCHAR", size=300, comment="Endereço completo"),
                Column("cep", "VARCHAR", size=10, comment="CEP"),
                Column("telefone", "VARCHAR", size=20, comment="Telefone"),
                Column("site_oficial", "VARCHAR", size=200, comment="Site oficial"),
                Column("ano_fundacao", "INTEGER", comment="Ano de fundação"),
                Column("credenciamento_mec", "DATE", comment="Data de credenciamento MEC"),
                Column("ativa", "BOOLEAN", default="true", comment="Indicador de IES ativa")
            ]
        )
        
        # Dimensão PPG
        dim_ppg = Table(
            name="dim_ppg",
            table_type="dimension",
            comment="Dimensão dos Programas de Pós-Graduação",
            columns=[
                Column("sk_ppg", "SERIAL", primary_key=True, comment="Chave surrogate do PPG"),
                Column("codigo_ppg", "VARCHAR", size=10, not_null=True, comment="Código CAPES do PPG"),
                Column("nome_ppg", "VARCHAR", size=200, not_null=True, comment="Nome do programa"),
                Column("nivel_ppg", "VARCHAR", size=20, not_null=True, comment="Nível (M/D/F)"),
                Column("modalidade", "VARCHAR", size=50, comment="Modalidade do programa"),
                Column("sk_ies", "INTEGER", foreign_key="dim_ies(sk_ies)", comment="FK para IES"),
                Column("sk_tema", "INTEGER", foreign_key="dim_tema(sk_tema)", comment="FK para área de conhecimento"),
                Column("nota_capes", "INTEGER", comment="Nota CAPES (1-7)"),
                Column("conceito_capes", "VARCHAR", size=10, comment="Conceito CAPES"),
                Column("ano_inicio", "INTEGER", comment="Ano de início do programa"),
                Column("ano_recomendacao", "INTEGER", comment="Ano de recomendação CAPES"),
                Column("situacao", "VARCHAR", size=20, comment="Situação do programa"),
                Column("periodicidade_selecao", "VARCHAR", size=50, comment="Periodicidade de seleção"),
                Column("tem_mestrado", "BOOLEAN", comment="Oferece mestrado"),
                Column("tem_doutorado", "BOOLEAN", comment="Oferece doutorado"),
                Column("ativo", "BOOLEAN", default="true", comment="Indicador de PPG ativo")
            ]
        )
        
        # Dimensão Produção
        dim_producao = Table(
            name="dim_producao",
            table_type="dimension",
            comment="Dimensão dos tipos de produção acadêmica",
            columns=[
                Column("sk_producao", "SERIAL", primary_key=True, comment="Chave surrogate da produção"),
                Column("tipo_producao", "VARCHAR", size=50, not_null=True, comment="Tipo de produção"),
                Column("subtipo_producao", "VARCHAR", size=100, comment="Subtipo específico"),
                Column("categoria_qualis", "VARCHAR", size=10, comment="Categoria Qualis"),
                Column("peso_producao", "DECIMAL", precision=5, scale=2, comment="Peso para cálculo de índices"),
                Column("descricao", "TEXT", comment="Descrição detalhada"),
                Column("criterios_qualidade", "TEXT", comment="Critérios de qualidade"),
                Column("periodicidade", "VARCHAR", size=20, comment="Periodicidade típica"),
                Column("indexadores", "VARCHAR", size=200, comment="Principais indexadores"),
                Column("ativo", "BOOLEAN", default="true", comment="Indicador de tipo ativo")
            ]
        )
        
        # Dimensão Docente
        dim_docente = Table(
            name="dim_docente",
            table_type="dimension",
            comment="Dimensão dos docentes permanentes",
            columns=[
                Column("sk_docente", "SERIAL", primary_key=True, comment="Chave surrogate do docente"),
                Column("id_pessoa", "VARCHAR", size=20, not_null=True, comment="ID único da pessoa"),
                Column("nome_docente", "VARCHAR", size=200, not_null=True, comment="Nome completo"),
                Column("nome_citacao", "VARCHAR", size=200, comment="Nome para citação"),
                Column("sexo", "VARCHAR", size=1, comment="Sexo (M/F)"),
                Column("data_nascimento", "DATE", comment="Data de nascimento"),
                Column("pais_nascimento", "VARCHAR", size=50, comment="País de nascimento"),
                Column("sk_localidade_nascimento", "INTEGER", foreign_key="dim_localidade(sk_localidade)", comment="FK para UF nascimento"),
                Column("titulacao_maxima", "VARCHAR", size=20, comment="Maior titulação"),
                Column("ano_titulacao", "INTEGER", comment="Ano da titulação máxima"),
                Column("sk_ies_titulacao", "INTEGER", foreign_key="dim_ies(sk_ies)", comment="FK para IES de titulação"),
                Column("pais_titulacao", "VARCHAR", size=50, comment="País de titulação"),
                Column("sk_tema_titulacao", "INTEGER", foreign_key="dim_tema(sk_tema)", comment="FK para área de titulação"),
                Column("categoria_profissional", "VARCHAR", size=50, comment="Categoria profissional"),
                Column("regime_trabalho", "VARCHAR", size=20, comment="Regime de trabalho"),
                Column("funcao_administrativa", "VARCHAR", size=100, comment="Função administrativa"),
                Column("bolsista_produtividade", "BOOLEAN", comment="É bolsista produtividade CNPq"),
                Column("nivel_bolsa_produtividade", "VARCHAR", size=10, comment="Nível da bolsa produtividade"),
                Column("orcid", "VARCHAR", size=50, comment="ORCID ID"),
                Column("lattes_id", "VARCHAR", size=50, comment="ID Lattes"),
                Column("data_atualizacao_lattes", "DATE", comment="Última atualização Lattes"),
                Column("ativo", "BOOLEAN", default="true", comment="Indicador de docente ativo")
            ]
        )
        
        # Tabela Fato Principal
        fato_pos_graduacao = Table(
            name="fato_pos_graduacao",
            table_type="fact",
            comment="Fato principal consolidando métricas da pós-graduação brasileira",
            columns=[
                # Chaves estrangeiras (dimensões)
                Column("sk_tempo", "INTEGER", not_null=True, foreign_key="dim_tempo(sk_tempo)", comment="FK para tempo"),
                Column("sk_localidade", "INTEGER", not_null=True, foreign_key="dim_localidade(sk_localidade)", comment="FK para localidade"),
                Column("sk_tema", "INTEGER", not_null=True, foreign_key="dim_tema(sk_tema)", comment="FK para tema"),
                Column("sk_ods", "INTEGER", not_null=True, foreign_key="dim_ods(sk_ods)", comment="FK para ODS"),
                Column("sk_ies", "INTEGER", not_null=True, foreign_key="dim_ies(sk_ies)", comment="FK para IES"),
                Column("sk_ppg", "INTEGER", not_null=True, foreign_key="dim_ppg(sk_ppg)", comment="FK para PPG"),
                Column("sk_producao", "INTEGER", not_null=True, foreign_key="dim_producao(sk_producao)", comment="FK para produção"),
                Column("sk_docente", "INTEGER", not_null=True, foreign_key="dim_docente(sk_docente)", comment="FK para docente"),
                
                # Métricas de Programas
                Column("qtd_programas_mestrado", "INTEGER", default="0", comment="Quantidade de programas de mestrado"),
                Column("qtd_programas_doutorado", "INTEGER", default="0", comment="Quantidade de programas de doutorado"),
                Column("qtd_programas_nota_3", "INTEGER", default="0", comment="Programas com nota 3"),
                Column("qtd_programas_nota_4", "INTEGER", default="0", comment="Programas com nota 4"),
                Column("qtd_programas_nota_5", "INTEGER", default="0", comment="Programas com nota 5"),
                Column("qtd_programas_nota_6_7", "INTEGER", default="0", comment="Programas com nota 6 ou 7"),
                
                # Métricas de Docentes
                Column("qtd_docentes_permanentes", "INTEGER", default="0", comment="Quantidade de docentes permanentes"),
                Column("qtd_docentes_colaboradores", "INTEGER", default="0", comment="Quantidade de docentes colaboradores"),
                Column("qtd_docentes_visitantes", "INTEGER", default="0", comment="Quantidade de docentes visitantes"),
                Column("qtd_bolsistas_produtividade", "INTEGER", default="0", comment="Quantidade de bolsistas produtividade"),
                Column("qtd_doutores", "INTEGER", default="0", comment="Quantidade de doutores"),
                
                # Métricas de Produção
                Column("qtd_artigos_a1", "INTEGER", default="0", comment="Artigos Qualis A1"),
                Column("qtd_artigos_a2", "INTEGER", default="0", comment="Artigos Qualis A2"),
                Column("qtd_artigos_b1", "INTEGER", default="0", comment="Artigos Qualis B1"),
                Column("qtd_artigos_b2", "INTEGER", default="0", comment="Artigos Qualis B2"),
                Column("qtd_livros", "INTEGER", default="0", comment="Quantidade de livros"),
                Column("qtd_capitulos", "INTEGER", default="0", comment="Quantidade de capítulos"),
                Column("qtd_trabalhos_eventos", "INTEGER", default="0", comment="Trabalhos em eventos"),
                
                # Métricas de Formação
                Column("qtd_dissertacoes_defendidas", "INTEGER", default="0", comment="Dissertações defendidas"),
                Column("qtd_teses_defendidas", "INTEGER", default="0", comment="Teses defendidas"),
                Column("qtd_mestres_titulados", "INTEGER", default="0", comment="Mestres titulados"),
                Column("qtd_doutores_titulados", "INTEGER", default="0", comment="Doutores titulados"),
                
                # Métricas Financeiras e de Recursos
                Column("valor_financiamento_capes", "DECIMAL", precision=15, scale=2, default="0", comment="Financiamento CAPES (R$)"),
                Column("valor_financiamento_cnpq", "DECIMAL", precision=15, scale=2, default="0", comment="Financiamento CNPq (R$)"),
                Column("valor_outros_financiamentos", "DECIMAL", precision=15, scale=2, default="0", comment="Outros financiamentos (R$)"),
                
                # Data de carga
                Column("data_carga", "TIMESTAMP", default="CURRENT_TIMESTAMP", comment="Data/hora da carga dos dados")
            ]
        )
        
        return Schema(
            name="dw_oesnpg",
            tables=[
                dim_tempo, dim_localidade, dim_tema, dim_ods,
                dim_ies, dim_ppg, dim_producao, dim_docente,
                fato_pos_graduacao
            ]
        )
    
    def generate_migration(self, database_type: str, migration_name: str = None) -> str:
        """Gera migration completa para o banco especificado"""
        
        if migration_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            migration_name = f"{timestamp}_create_dw_oesnpg"
        
        if database_type.lower() == 'postgresql':
            dialect = self.postgresql_dialect
        elif database_type.lower() == 'oracle':
            dialect = self.oracle_dialect
        else:
            raise ValueError(f"Banco não suportado: {database_type}")
        
        migration_content = []
        migration_content.append(f"-- Migration: {migration_name}")
        migration_content.append(f"-- Database: {database_type.upper()}")
        migration_content.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        migration_content.append(f"-- Schema: Data Warehouse OES-NPG")
        migration_content.append("")
        
        # Gerar sequences (se necessário)
        migration_content.append("-- ======================")
        migration_content.append("-- SEQUENCES")
        migration_content.append("-- ======================")
        migration_content.append("")
        
        for table in self.schema.tables:
            sequences = dialect.generate_sequences(table)
            for seq in sequences:
                migration_content.append(seq)
                migration_content.append("")
        
        # Gerar tabelas
        migration_content.append("-- ======================")
        migration_content.append("-- TABLES")
        migration_content.append("-- ======================")
        migration_content.append("")
        
        for table in self.schema.tables:
            migration_content.append(dialect.generate_create_table(table))
            migration_content.append("")
            migration_content.append("")
        
        # Gerar primary keys
        migration_content.append("-- ======================")
        migration_content.append("-- PRIMARY KEYS")
        migration_content.append("-- ======================")
        migration_content.append("")
        
        for table in self.schema.tables:
            pk_sql = dialect.generate_primary_key(table)
            if pk_sql:
                migration_content.append(pk_sql)
                migration_content.append("")
        
        # Gerar foreign keys
        migration_content.append("-- ======================")
        migration_content.append("-- FOREIGN KEYS")
        migration_content.append("-- ======================")
        migration_content.append("")
        
        for table in self.schema.tables:
            fks = dialect.generate_foreign_keys(table)
            for fk in fks:
                migration_content.append(fk)
                migration_content.append("")
        
        # Gerar índices adicionais
        migration_content.append("-- ======================")
        migration_content.append("-- ADDITIONAL INDEXES")
        migration_content.append("-- ======================")
        migration_content.append("")
        
        # Índices para performance em queries analíticas
        if database_type.lower() == 'postgresql':
            additional_indexes = [
                "CREATE INDEX idx_fato_tempo ON fato_pos_graduacao(sk_tempo);",
                "CREATE INDEX idx_fato_localidade ON fato_pos_graduacao(sk_localidade);",
                "CREATE INDEX idx_fato_tema ON fato_pos_graduacao(sk_tema);",
                "CREATE INDEX idx_fato_ies ON fato_pos_graduacao(sk_ies);",
                "CREATE INDEX idx_fato_ppg ON fato_pos_graduacao(sk_ppg);",
                "CREATE INDEX idx_fato_docente ON fato_pos_graduacao(sk_docente);",
                "CREATE INDEX idx_tempo_ano_mes ON dim_tempo(ano, mes);",
                "CREATE INDEX idx_localidade_uf ON dim_localidade(codigo_uf);",
                "CREATE INDEX idx_tema_area ON dim_tema(codigo_area_conhecimento);",
                "CREATE INDEX idx_ies_codigo ON dim_ies(codigo_ies);",
                "CREATE INDEX idx_ppg_codigo ON dim_ppg(codigo_ppg);",
                "CREATE INDEX idx_docente_nome ON dim_docente(nome_docente);"
            ]
        else:  # Oracle
            additional_indexes = [
                "CREATE INDEX IDX_FATO_TEMPO ON fato_pos_graduacao(sk_tempo);",
                "CREATE INDEX IDX_FATO_LOCALIDADE ON fato_pos_graduacao(sk_localidade);",
                "CREATE INDEX IDX_FATO_TEMA ON fato_pos_graduacao(sk_tema);",
                "CREATE INDEX IDX_FATO_IES ON fato_pos_graduacao(sk_ies);",
                "CREATE INDEX IDX_FATO_PPG ON fato_pos_graduacao(sk_ppg);",
                "CREATE INDEX IDX_FATO_DOCENTE ON fato_pos_graduacao(sk_docente);",
                "CREATE INDEX IDX_TEMPO_ANO_MES ON dim_tempo(ano, mes);",
                "CREATE INDEX IDX_LOCALIDADE_UF ON dim_localidade(codigo_uf);",
                "CREATE INDEX IDX_TEMA_AREA ON dim_tema(codigo_area_conhecimento);",
                "CREATE INDEX IDX_IES_CODIGO ON dim_ies(codigo_ies);",
                "CREATE INDEX IDX_PPG_CODIGO ON dim_ppg(codigo_ppg);",
                "CREATE INDEX IDX_DOCENTE_NOME ON dim_docente(nome_docente);"
            ]
        
        for idx in additional_indexes:
            migration_content.append(idx)
            migration_content.append("")
        
        migration_content.append("-- Migration completed successfully")
        
        return "\n".join(migration_content)
    
    def save_migration(self, database_type: str, output_dir: str = None, migration_name: str = None) -> str:
        """Salva migration em arquivo"""
        
        if output_dir is None:
            base_dir = os.getenv('BASE_DIR', '.')
            output_base = os.getenv('OUTPUT_DIR', 'output')
            output_dir = os.path.join(base_dir, output_base, database_type.lower())
        
        os.makedirs(output_dir, exist_ok=True)
        
        if migration_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            migration_name = f"{timestamp}_create_dw_oesnpg"
        
        filename = f"{migration_name}.sql"
        filepath = os.path.join(output_dir, filename)
        
        migration_content = self.generate_migration(database_type, migration_name)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(migration_content)
        
        return filepath
    
    def generate_schema_json(self) -> str:
        """Gera representação JSON do schema"""
        schema_dict = {
            "name": self.schema.name,
            "tables": []
        }
        
        for table in self.schema.tables:
            table_dict = {
                "name": table.name,
                "type": table.table_type,
                "comment": table.comment,
                "columns": []
            }
            
            for col in table.columns:
                col_dict = asdict(col)
                table_dict["columns"].append(col_dict)
            
            schema_dict["tables"].append(table_dict)
        
        return json.dumps(schema_dict, indent=2, ensure_ascii=False)
    
    def save_schema_documentation(self, output_dir: str = None) -> str:
        """Salva documentação completa do schema"""
        
        if output_dir is None:
            base_dir = os.getenv('BASE_DIR', '.')
            output_base = os.getenv('OUTPUT_DIR', 'output')
            output_dir = os.path.join(base_dir, output_base, 'docs')
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Salvar JSON
        json_path = os.path.join(output_dir, "schema_dw_oesnpg.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            f.write(self.generate_schema_json())
        
        # Salvar documentação Markdown
        md_path = os.path.join(output_dir, "SCHEMA_DOCUMENTATION.md")
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(self._generate_markdown_documentation())
        
        return f"Documentação salva em: {json_path}, {md_path}"
    
    def _generate_markdown_documentation(self) -> str:
        """Gera documentação em Markdown"""
        
        content = []
        content.append("# Data Warehouse OES-NPG - Documentação do Schema")
        content.append("")
        content.append(f"**Gerado em:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        content.append("")
        content.append("## Visão Geral")
        content.append("")
        content.append("Este documento descreve o schema completo do Data Warehouse OES-NPG (Observatório do Ensino Superior e Núcleo de Pesquisa em Gestão), incluindo todas as dimensões e a tabela fato principal.")
        content.append("")
        content.append("## Arquitetura")
        content.append("")
        content.append("O DW segue o modelo Star Schema com as seguintes características:")
        content.append("- **8 Dimensões**: Tempo, Localidade, Tema, ODS, IES, PPG, Produção, Docente")
        content.append("- **1 Tabela Fato**: fato_pos_graduacao")
        content.append("- **Granularidade**: Registro de docente por programa por período temporal")
        content.append("")
        
        # Dimensões
        content.append("## Dimensões")
        content.append("")
        
        for table in self.schema.tables:
            if table.table_type == "dimension":
                content.append(f"### {table.name}")
                content.append("")
                if table.comment:
                    content.append(f"**Descrição:** {table.comment}")
                    content.append("")
                
                content.append("| Coluna | Tipo | Descrição |")
                content.append("|--------|------|-----------|")
                
                for col in table.columns:
                    type_info = col.data_type
                    if col.size:
                        type_info += f"({col.size})"
                    elif col.precision and col.scale:
                        type_info += f"({col.precision},{col.scale})"
                    
                    constraints = []
                    if col.primary_key:
                        constraints.append("PK")
                    if col.foreign_key:
                        constraints.append("FK")
                    if col.not_null:
                        constraints.append("NOT NULL")
                    
                    constraint_str = " ".join(constraints)
                    if constraint_str:
                        type_info += f" {constraint_str}"
                    
                    comment = col.comment or ""
                    content.append(f"| {col.name} | {type_info} | {comment} |")
                
                content.append("")
        
        # Tabela Fato
        content.append("## Tabela Fato")
        content.append("")
        
        for table in self.schema.tables:
            if table.table_type == "fact":
                content.append(f"### {table.name}")
                content.append("")
                if table.comment:
                    content.append(f"**Descrição:** {table.comment}")
                    content.append("")
                
                # Separar FKs e métricas
                fk_columns = [col for col in table.columns if col.foreign_key]
                metric_columns = [col for col in table.columns if not col.foreign_key and col.name != "data_carga"]
                
                content.append("#### Chaves Estrangeiras")
                content.append("")
                content.append("| Coluna | Referência | Descrição |")
                content.append("|--------|------------|-----------|")
                
                for col in fk_columns:
                    ref = col.foreign_key if col.foreign_key else ""
                    comment = col.comment or ""
                    content.append(f"| {col.name} | {ref} | {comment} |")
                
                content.append("")
                content.append("#### Métricas de Negócio")
                content.append("")
                content.append("| Coluna | Tipo | Descrição |")
                content.append("|--------|------|-----------|")
                
                for col in metric_columns:
                    type_info = col.data_type
                    if col.precision and col.scale:
                        type_info += f"({col.precision},{col.scale})"
                    
                    comment = col.comment or ""
                    content.append(f"| {col.name} | {type_info} | {comment} |")
                
                content.append("")
        
        # Relacionamentos
        content.append("## Relacionamentos")
        content.append("")
        content.append("O modelo segue as seguintes regras de relacionamento:")
        content.append("")
        content.append("1. **Todas as dimensões** se relacionam com a tabela fato através de chaves surrogate")
        content.append("2. **Chaves Surrogate** iniciam em 1 (registros válidos) e 0 (registros não identificados)")
        content.append("3. **Cardinalidade** é sempre 1:N (dimensão:fato)")
        content.append("4. **Integridade Referencial** mantida através de Foreign Keys")
        content.append("")
        
        # Índices
        content.append("## Estratégia de Indexação")
        content.append("")
        content.append("### Índices Automáticos")
        content.append("- Primary Keys (todas as tabelas)")
        content.append("- Foreign Keys (tabela fato)")
        content.append("")
        content.append("### Índices Adicionais")
        content.append("- Colunas de filtro frequente nas dimensões")
        content.append("- Combinações ano/mês na dimensão tempo")
        content.append("- Códigos de negócio (UF, área conhecimento, etc.)")
        content.append("")
        
        return "\n".join(content)


def main():
    """Função principal para execução standalone"""
    
    print("=== Sistema de Migrations DW OES-NPG ===")
    print()
    
    generator = MigrationGenerator()
    
    # Obter configurações do .env
    base_dir = os.getenv('BASE_DIR', '.')
    output_dir = os.getenv('OUTPUT_DIR', 'output')
    
    # Gerar migrations para ambos os bancos
    databases = ['postgresql', 'oracle']
    
    for db in databases:
        print(f"Gerando migration para {db.upper()}...")
        
        # Definir diretório de saída usando output_dir do .env
        db_output_dir = os.path.join(base_dir, output_dir, db)
        
        # Gerar e salvar migration
        filepath = generator.save_migration(db, db_output_dir)
        print(f"✅ Migration salva: {filepath}")
    
    # Gerar documentação do schema
    print("\nGerando documentação do schema...")
    docs_output_dir = os.path.join(base_dir, output_dir, 'docs')
    doc_result = generator.save_schema_documentation(docs_output_dir)
    print(f"✅ {doc_result}")
    
    print("\n=== Migrations geradas com sucesso! ===")
    print()
    print("Próximos passos:")
    print("1. Revisar os arquivos SQL gerados")
    print("2. Testar em ambiente de desenvolvimento")
    print("3. Executar migrations em produção")


if __name__ == "__main__":
    main()
