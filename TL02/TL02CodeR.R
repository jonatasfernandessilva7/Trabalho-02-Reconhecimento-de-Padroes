# Instalação e carregamento de pacotes necessários
install.packages("tidyverse") # Para manipulação de dados e gráficos
install.packages("stringr")   # Para manipulação de strings
install.packages("jsonlite")  # Se os logs fossem JSON (neste caso, não são, mas é útil saber)

library(tidyverse)
library(stringr)

# --- 1. Definição do Diretório e Função de Leitura ---
# Substitua 'caminho/para/seus/testes' pelo diretório onde você descompactou testes.zip
log_directory <- "testes/"

# Função para extrair métricas de um único arquivo de log
extract_metrics <- function(log_file_path) {
  lines <- readLines(log_file_path)

  # Extrair informações do nome do arquivo
  file_name <- basename(log_file_path)
  # Ex: test-64-137.log -> chunk_size = 64, num_machines = 137
  parts <- str_match(file_name, "test-(\\d+)-(\\d+)\\.log")
  chunk_size <- as.numeric(parts[1, 2])
  num_machines <- as.numeric(parts[1, 3])

  # Inicializar um vetor para armazenar as métricas
  metrics <- list(
    file_name = file_name,
    chunk_size = chunk_size,
    num_machines = num_machines
  )

  # Expressões regulares para extrair as métricas
  # Adaptar as regex conforme o formato exato dos seus logs.
  # Estas são apenas exemplos baseados nas descrições fornecidas.
  regex_patterns <- list(
    "Map output records" = "Map output records=(\\d+)",
    "Map input bytes" = "Map input bytes=(\\d+)",
    "Reduce input records" = "Reduce input records=(\\d+)",
    "Reduce output records" = "Reduce output records=(\\d+)",
    "CPU time spent" = "CPU time spent=(\\d+)",
    "Total committed heap usage" = "Total committed heap usage=(\\d+)",
    "Reduce shuffle bytes" = "Reduce shuffle bytes=(\\d+)",
    "FileSystemCounters - BYTES_READ" = "BYTES_READ=(\\d+)", # Exemplo para FileSystemCounters
    "FileSystemCounters - BYTES_WRITTEN" = "BYTES_WRITTEN=(\\d+)", # Exemplo para FileSystemCounters
    "Physical memory bytes" = "Physical memory bytes=(\\d+)",
    "Virtual memory bytes" = "Virtual memory bytes=(\\d+)",
    "Job Counters - Launched map tasks" = "Launched map tasks=(\\d+)",
    "Job Counters - Data-local map tasks" = "Data-local map tasks=(\\d+)",
    "Job Counters - Rack-local map tasks" = "Rack-local map tasks=(\\d+)",
    "File Input Format Counters - Bytes Read" = "File Input Format Counters - Bytes Read=(\\d+)",
    "File Output Format Counters - Bytes Written" = "File Output Format Counters - Bytes Written=(\\d+)"
  )

  for (metric_name in names(regex_patterns)) {
    pattern <- regex_patterns[[metric_name]]
    # Encontra a linha que contém a métrica
    matched_line <- lines[str_detect(lines, fixed(str_extract(pattern, "^[^=]+")))] # Extrai a parte da string antes do '=' para buscar a linha
    if (length(matched_line) > 0) {
      value <- str_match(matched_line, pattern)
      if (!is.na(value[1, 2])) {
        metrics[[metric_name]] <- as.numeric(value[1, 2])
      } else {
        metrics[[metric_name]] <- NA # Atribui NA se o valor não for encontrado
      }
    } else {
      metrics[[metric_name]] <- NA # Atribui NA se a linha não for encontrada
    }
  }

  return(as_tibble(metrics))
}

# --- 2. Processamento de Todos os Arquivos de Log ---
log_files <- list.files(log_directory, pattern = "\\.log$", full.names = TRUE)

# Cria um data frame vazio para armazenar todos os resultados
all_metrics_df <- tibble()

# Itera sobre cada arquivo de log e extrai as métricas
for (file in log_files) {
  message(paste("Processando:", basename(file)))
  current_metrics <- extract_metrics(file)
  all_metrics_df <- bind_rows(all_metrics_df, current_metrics)
}

print(colnames(all_metrics_df))

# --- 3. Limpeza e Preparação dos Dados ---
# Converter colunas numéricas para o tipo correto (garantir)
all_metrics_df <- all_metrics_df %>%
  mutate(across(starts_with(c("Map", "Reduce", "CPU", "Total", "Reduce", "FileSystemCounters", "Physical", "Virtual", "Job", "File")), as.numeric))

# Calcular métricas adicionais ou de proporção
all_metrics_df <- all_metrics_df %>%
  mutate(
    # Proporção de tarefas locais vs. remotas
    data_local_ratio = `Job Counters - Data-local map tasks` / `Job Counters - Launched map tasks`,
    rack_local_ratio = `Job Counters - Rack-local map tasks` / `Job Counters - Launched map tasks`,
    # Bytes lidos/escritos por máquina (para normalização)
    bytes_read_per_machine = `File Input Format Counters - Bytes Read` / num_machines,
    bytes_written_per_machine = `File Output Format Counters - Bytes Written` / num_machines,
    # Tempo de CPU por máquina
    cpu_time_per_machine = `CPU time spent` / num_machines
  )

# --- 4. Análise Exploratória e Visualização ---

# Visualização da Leitura vs. Número de Máquinas
ggplot(all_metrics_df, aes(x = num_machines, y = `File Input Format Counters - Bytes Read`)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE, color = "blue") +
  labs(title = "Bytes Lidos vs. Número de Máquinas",
       x = "Número de Máquinas",
       y = "Bytes Lidos (HDFS)") +
  theme_minimal()

# Visualização da Escrita vs. Número de Máquinas
ggplot(all_metrics_df, aes(x = num_machines, y = `File Output Format Counters - Bytes Written`)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE, color = "red") +
  labs(title = "Bytes Escritos vs. Número de Máquinas",
       x = "Número de Máquinas",
       y = "Bytes Escritos (HDFS)") +
  theme_minimal()

# Visualização do Tempo de CPU vs. Número de Máquinas
ggplot(all_metrics_df, aes(x = num_machines, y = `CPU time spent`)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE, color = "darkgreen") +
  labs(title = "Tempo de CPU Total vs. Número de Máquinas",
       x = "Número de Máquinas",
       y = "Tempo de CPU Gasto") +
  theme_minimal()

# Tempo de CPU por máquina (para avaliar eficiência)
ggplot(all_metrics_df, aes(x = num_machines, y = cpu_time_per_machine)) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE, color = "purple") +
  labs(title = "Tempo de CPU por Máquina vs. Número de Máquinas",
       x = "Número de Máquinas",
       y = "Tempo de CPU Gasto por Máquina") +
  theme_minimal()


# Correlação entre Bytes Lidos e Escritos
ggplot(all_metrics_df, aes(x = `File Input Format Counters - Bytes Read`, y = `File Output Format Counters - Bytes Written`)) +
  geom_point() +
  labs(title = "Correlação entre Leitura e Escrita",
       x = "Bytes Lidos",
       y = "Bytes Escritos") +
  theme_minimal()

# Correlação entre Shuffle e Reduce Input Records
ggplot(all_metrics_df, aes(x = `Reduce input records`, y = `Reduce shuffle bytes`)) +
  geom_point() +
  labs(title = "Correlação entre Dados de Entrada do Reduce e Shuffle",
       x = "Registros de Entrada do Reduce",
       y = "Bytes de Shuffle do Reduce") +
  theme_minimal()

# Proporção de tarefas locais vs. desempenho
ggplot(all_metrics_df, aes(x = data_local_ratio, y = `CPU time spent`)) +
  geom_point() +
  labs(title = "Relação entre Proporção de Tarefas Locais e Tempo de CPU",
       x = "Proporção de Tarefas Data-Local",
       y = "Tempo de CPU Gasto") +
  theme_minimal()

# --- 5. Análise de Correlações (Matriz de Correlação) ---
# Selecionar as colunas numéricas para a matriz de correlação
numeric_cols <- all_metrics_df %>%
  select_if(is.numeric) %>%
  select(-chunk_size) # chunk_size é constante neste dataset

correlation_matrix <- cor(numeric_cols, use = "pairwise.complete.obs") # "pairwise.complete.obs" lida com NAs

# Exibir a matriz de correlação (pode ser grande, considere visualizar apenas partes)
print(correlation_matrix)

# Exemplo de visualização de correlações com o pacote `corrplot` (necessita instalação)
# install.packages("corrplot")
# library(corrplot)
# corrplot(correlation_matrix, method = "circle")

# --- 6. Sumário dos Dados ---
summary(all_metrics_df)

# Visualizar a estrutura do dataframe final
str(all_metrics_df)

# Exibir as primeiras linhas do dataframe resultante
head(all_metrics_df)