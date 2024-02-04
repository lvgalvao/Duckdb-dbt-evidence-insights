from loguru import logger
import sys

# Remove o destino padrão que envia mensagens para stdout
logger.remove(0)

# Configuração padrão para a saída no console
logger.add(
    sys.stdout, colorize=True, format="<green>{time}</green> <level>{message}</level>"
)


# Função para configurar o destino do arquivo de log
def configure_file_logger(log_file_path):
    # Adiciona um arquivo como destino dos logs, em modo de adição (append)
    logger.add(
        log_file_path,
        level="INFO",
        rotation="10 MB",
        retention="10 days",
        mode="a",
    )


# Função para configurar o nível mínimo de log
def set_log_level(log_level):
    logger.level(log_level)
