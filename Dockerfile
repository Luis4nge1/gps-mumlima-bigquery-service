FROM node:20-alpine

# Instalar dumb-init para manejo de señales
RUN apk add --no-cache dumb-init curl

WORKDIR /app

# Copiar package files
COPY package*.json ./

# Instalar dependencias
RUN npm ci --only=production && npm cache clean --force

# Copiar código fuente
COPY . .

# Asegurar que service-account.json sea un archivo
COPY service-account.json /app/service-account.json

# Crear directorios necesarios
RUN mkdir -p tmp logs data

# Exponer puerto
EXPOSE 3003

# Iniciar aplicación
CMD ["dumb-init", "node", "src/index.js"]