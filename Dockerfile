# Usa a imagem base oficial do Python 3.11
FROM python:3.11-slim-bookworm

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia os arquivos de dependência (requirements.txt e setup.py)
COPY requirements.txt ./
COPY InteligenteEtl/setup.py ./

# Instala as dependências Python
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir .

# Instala o Chromium e suas dependências
RUN apt-get update && apt-get install -y chromium chromium-driver

# Define a variável de ambiente para o Selenium encontrar o Chromium
ENV PATH="/usr/lib/chromium-browser:${PATH}"
ENV CHROME_DRIVER_PATH=/usr/bin/chromedriver
# Define onde o selenium vai procurar pelo binario do chrome
ENV CHROME_BIN=/usr/bin/chromium-browser

# Copia o restante dos arquivos do projeto para o container
COPY . .

# Define o comando padrão para executar quando o container for iniciado
CMD ["python", "main.py"]