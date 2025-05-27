import os
import zipfile
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# CONFIGURAÇÕES
ZIP_PATH = r"C:\Adegax\Ciência de dados - ADEGAS\5º semestre\Reconhecimento de padrões\Projeto II\testes.zip"
EXTRACT_DIR = "testes_logs"

# 1. Descompactar o arquivo
with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
    zip_ref.extractall(EXTRACT_DIR)


# 2. Função para extrair métricas de um arquivo de log
def parse_log_file(file_path, filename):
    metrics = {
        'file': filename,
        'chunk_size_MB': None,
        'num_machines': None,
        'location': None
    }

    # Novos padrões de nome de arquivo
    # Padrão 1: test-64-137
    match_simple = re.match(r"test-(\d+)-(\d+)", filename)

    # Padrão 2: test-Nancy-64-282
    match_city = re.match(r"test-([A-Za-z]+)-(\d+)-(\d+)", filename)

    if match_city:
        metrics['location'] = match_city.group(1)
        metrics['chunk_size_MB'] = int(match_city.group(2))
        metrics['num_machines'] = int(match_city.group(3))
    elif match_simple:
        metrics['chunk_size_MB'] = int(match_simple.group(1))
        metrics['num_machines'] = int(match_simple.group(2))
    else:
        print(f"Formato não reconhecido: {filename}")
        return None


    # Expressões regulares para extrair métricas do conteúdo
    patterns = {
        'slots_m_map': r'SLOTS_MILLIS_MAPS=(\d+)',
        'rack_local_map_tasks': r"Rack-local map tasks=(\d+)",
        'launched_map_tasks': r"Launched map tasks=(\d+)",
        'data_local_map_tasks': r"Data-local map tasks=(\d+)",
        'slots_m_reduce': r'SLOTS_MILLIS_REDUCES=(\d+)',
        'file_input_bytes': r"File Input Format Counters.*?Bytes Read=(\d+)",
        'file_output_bytes': r"File Output Format Counters.*?Bytes Written=(\d+)",
        'fs_local_read_bytes': r"FILE_BYTES_READ=(\d+)",
        'fs_hdfs_read_bytes': r"HDFS_BYTES_READ=(\d+)",
        'fs_local_write_bytes': r"FILE_BYTES_WRITTEN=(\d+)",
        'fs_hdfs_write_bytes': r"HDFS_BYTES_WRITTEN=(\d+)",
        'map_out_mater_bytes': r'Map output materialized bytes=(\d+)',
        'map_input_bytes': r"Map input bytes=(\d+)",
        'reduce_shuffle_bytes': r"Reduce shuffle bytes=(\d+)",
        'splilled_records': r'Spilled Records=(\d+)',
        'map_output_bytes': r'Map output bytes=(\d+)',
        'heap_usage_bytes': r"Total committed heap usage \(bytes\)=(\d+)",
        'total_com_usage': r'Total committed head usage \(bytes\)=(\d+)',
        'cpu_time_ms': r"CPU time spent \(ms\)=(\d+)",
        'map_input_bytes': r'Map input bytes=(\d+)',
        'split_raw_bytes': r'SPLIT_RAW_BYTES=(\d+)',
        'combine_in_records': r'Combine input records=(\d+)',
        'reduce_input_records': r"Reduce input records=(\d+)",
        'reduce_input_groups': r"Reduce input groups=(\d+)",
        'combine_out_records': r'Combine output records=(\d+)',
        'physical_memory_bytes': r"Physical memory \(bytes\) snapshot=(\d+)",
        'reduce_output_records': r"Reduce output records=(\d+)",
        'virtual_memory_bytes': r"Virtual memory \(bytes\) snapshot=(\d+)",
        'map_output_records': r"Map output records=(\d+)"
    }

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

        for key, pattern in patterns.items():
            match = re.search(pattern, content, re.DOTALL)
            metrics[key] = int(match.group(1)) if match else 0

    return metrics


# 3. Processar todos os arquivos
data = []
for root, _, files in os.walk(EXTRACT_DIR):
    for file in files:
        if file.endswith(".txt") or file.startswith("test-"):
            full_path = os.path.join(root, file)
            metrics = parse_log_file(full_path, file)
            if metrics:
                data.append(metrics)


# 4. Criar DataFrame
df = pd.DataFrame(data)

# Lista das métricas mais relevantes
'''
Tipo	                    Métrica                     Comentário
CPU	                        cpu_time_ms	                Tempo total de CPU usado. Essencial.

Memória física	            physical_memory_bytes	    Uso real de memória, mais relevante que virtual.

Disco (FS Local)	        fs_local_read_bytes	        Leitura local de disco.

Disco (HDFS)	            fs_hdfs_read_bytes	        Leitura de dados do HDFS.

Entrada/Saída MapReduce     map_input_bytes	            Tamanho de dados de entrada no Map.
                            map_output_bytes            Tamanho de saída do Map.
                            reduce_shuffle_bytes	    Dados transferidos entre Map e Reduce.

Redução	                    reduce_input_records	    Registros processados no Reduce.

Desempenho geral	        slots_m_map	                Tempo ocupado nos slots de Map.
                            slots_m_reduce	            Tempo ocupado nos slots de Reduce.
                            heap_usage_bytes            
'''


selected_columns = [
    'cpu_time_ms',
    'physical_memory_bytes',
    'fs_local_read_bytes',
    'fs_hdfs_read_bytes',
    'map_input_bytes',
    'map_output_bytes',
    'reduce_shuffle_bytes',
    'reduce_input_records',
    'slots_m_map',
    'slots_m_reduce',
    'heap_usage_bytes'
]

# Garante que as colunas existem
selected_columns = [col for col in selected_columns if col in df.columns]
selected_df = df[selected_columns]

# Calcula e plota a matriz de correlação
correlation_matrix = selected_df.corr()

plt.figure(figsize=(15, 10))
sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='coolwarm', square=True, linewidths=0.5)
plt.title("Matriz de Correlação - Métricas Selecionadas")
plt.tight_layout()
plt.show()


correlation_matrix.to_csv("analise_logs_correlacao.csv", index=False)






'''
# 5. Converter tempo de CPU de ms para minutos
df['cpu_time_min'] = df['cpu_time_ms'] / 60000


# CRIANDO NOVAS VARIÁVEIS:

# Throughput de leitura (MB/s)
df['throughput_read_MBps'] = (df['map_input_bytes'] / (df['cpu_time_ms'] / 1000)) / (1024 * 1024)


# Eficiência por máquina (tempo médio por máquina
df['cpu_time_per_machine_min'] = df['cpu_time_min'] / df['num_machines']


# Relação entre entrada e saída de dados
df['input_output_ratio'] = df['map_input_bytes'] / (df['file_output_bytes'] + 1)


# Shuffle ratio - custo de comunicação entre tarefas
df['shuffle_ratio'] = df['reduce_shuffle_bytes'] / (df['map_output_bytes'] + 1)


# Uso de heap por byte processado
df['heap_per_input_MB'] = df['heap_usage_bytes'] / (df['map_input_bytes'] / (1024 * 1024))


# Gráfico eficiência por máquina vs número de máquinas:
sns.lineplot(data=df, x='num_machines', y='cpu_time_per_machine_min', marker='o')
plt.title("Tempo de CPU por Máquina vs Nº de Máquinas")


# Throughput de leitura:
sns.barplot(data=df, x='num_machines', y='throughput_read_MBps', hue='location')
plt.title("Throughput de Leitura por Nº de Máquinas")


df_corr = df.copy()

# Função auxiliar para desenhar a matriz de correlação
def plot_corr_matrix(df, columns, title):
    corr = df[columns].corr()
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title(title)
    plt.show()

# CPU & Máquinas
plot_corr_matrix(df_corr, ['cpu_time_min', 'num_machines'], "Correlação: CPU x Máquinas")

# Dados processados
plot_corr_matrix(df_corr, ['map_input_bytes', 'reduce_shuffle_bytes', 'file_input_bytes', 'file_output_bytes'], "Correlação: Volume de Dados")

# Uso de memória
plot_corr_matrix(df_corr, ['heap_usage_bytes', 'physical_memory_bytes', 'virtual_memory_bytes'], "Correlação: Uso de Memória")

# Registros
plot_corr_matrix(df_corr, ['map_output_records', 'reduce_input_records', 'reduce_output_records'], "Correlação: Registros")

# Bytes intermediários
plot_corr_matrix(df_corr, ['map_output_bytes', 'reduce_shuffle_bytes'], "Correlação: Bytes Intermediários (Map > Reduce)")

# Exibir resumo
print(f"Resumo da correlação: \n{df_corr.describe(include='all')}")

df_corr.to_csv("analise_correlacao_df.csv", index=False)




print(f"Resumo do dataframe:\n{df.describe(include='all')}")

# Correlação entre variáveis (apenas se chunk_size_MB for numérico)
df_numeric = df[pd.to_numeric(df['chunk_size_MB'], errors='coerce').notnull()].copy()
df_numeric['chunk_size_MB'] = df_numeric['chunk_size_MB'].astype(int)

plt.figure(figsize=(14, 10))
sns.heatmap(df_numeric.corr(numeric_only=True), annot=True, cmap="coolwarm")
plt.title("Matriz de Correlação")
plt.show()


# Relação entre número de máquinas e tempo de CPU
plt.figure(figsize=(8, 5))
sns.scatterplot(data=df_numeric, x="num_machines", y="cpu_time_min", hue="chunk_size_MB", palette="viridis")
plt.title("Relação entre Número de Máquinas e Tempo de CPU (min)")
plt.xlabel("Máquinas")
plt.ylabel("Tempo CPU (min)")
plt.grid(True)
plt.show()


# Relação entre map output e shuffle
plt.figure(figsize=(8, 5))
sns.scatterplot(data=df_numeric, x="map_output_records", y="reduce_shuffle_bytes", hue="num_machines")
plt.title("Map Output vs Reduce Shuffle")
plt.xlabel("Map Output Records")
plt.ylabel("Reduce Shuffle Bytes")
plt.grid(True)
plt.show()


# Uso de CPU vs Bytes lidos do HDFS
plt.figure(figsize=(8, 5))
sns.scatterplot(data=df_numeric, x="fs_hdfs_read_bytes", y="cpu_time_min", hue="num_machines", palette="plasma")
plt.title("Uso de CPU vs HDFS Bytes Lidos")
plt.xlabel("HDFS Bytes Lidos")
plt.ylabel("Tempo de CPU (min)")
plt.grid(True)
plt.tight_layout()
plt.show()


# Uso de memória física vs Virtual
plt.figure(figsize=(8, 5))
sns.scatterplot(data=df_numeric, x="physical_memory_bytes", y="virtual_memory_bytes", hue="chunk_size_MB", palette="coolwarm")
plt.title("Memória Física vs Memória Virtual")
plt.xlabel("Memória Física (bytes)")
plt.ylabel("Memória Virtual (bytes)")
plt.grid(True)
plt.tight_layout()
plt.show()


# Quantidade de registros de entrada e saída
plt.figure(figsize=(8, 5))
sns.scatterplot(data=df_numeric, x="map_output_records", y="reduce_output_records", hue="chunk_size_MB", palette="magma")
plt.title("Map Output Records vs Reduce Output Records")
plt.xlabel("Map Output Records")
plt.ylabel("Reduce Output Records")
plt.grid(True)
plt.tight_layout()
plt.show()


# Comparação de tamanhos de chunck (MB) com CPU
plt.figure(figsize=(8, 5))
sns.boxplot(data=df_numeric, x="chunk_size_MB", y="cpu_time_min", hue="chunk_size_MB", legend=False)
plt.title("Tempo de CPU por Tamanho de Chunk (MB)")
plt.xlabel("Chunk Size (MB)")
plt.ylabel("Tempo de CPU (min)")
plt.grid(True)
plt.tight_layout()
plt.show()



# Exportar para CSV
df.to_csv("analise_logs_mapreduce.csv", index=False)'''