#!/bin/bash

# Script para configurar y ejecutar el microservicio con Docker

set -e

echo "🐳 GPS BigQuery Microservice - Docker Setup"
echo "=========================================="

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para imprimir mensajes coloreados
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Verificar si Docker está instalado
check_docker() {
    print_status "Verificando Docker..."
    if ! command -v docker &> /dev/null; then
        print_error "Docker no está instalado. Por favor instala Docker primero."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose no está instalado. Por favor instala Docker Compose primero."
        exit 1
    fi
    
    print_success "Docker y Docker Compose están instalados"
}

# Verificar archivo .env
check_env_file() {
    print_status "Verificando archivo .env..."
    
    if [ ! -f .env ]; then
        print_warning "Archivo .env no encontrado. Creando desde .env.example..."
        cp .env.example .env
        print_warning "⚠️  IMPORTANTE: Configura las variables de Redis Cloud en .env"
        print_warning "   - REDIS_HOST=tu-host-redis-cloud.redislabs.com"
        print_warning "   - REDIS_PASSWORD=tu-password-redis-cloud"
        echo
        read -p "¿Has configurado Redis Cloud en .env? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_error "Por favor configura Redis Cloud en .env antes de continuar"
            exit 1
        fi
    fi
    
    print_success "Archivo .env encontrado"
}

# Crear directorios necesarios
create_directories() {
    print_status "Creando directorios necesarios..."
    
    mkdir -p data/backup
    mkdir -p logs
    mkdir -p tmp
    
    print_success "Directorios creados"
}

# Construir imagen Docker
build_image() {
    print_status "Construyendo imagen Docker..."
    
    docker build -t gps-bigquery-service:latest .
    
    print_success "Imagen Docker construida exitosamente"
}

# Función para mostrar ayuda
show_help() {
    echo "Uso: $0 [OPCIÓN]"
    echo
    echo "Opciones:"
    echo "  build     - Solo construir la imagen Docker"
    echo "  dev       - Ejecutar en modo desarrollo"
    echo "  prod      - Ejecutar en modo producción"
    echo "  stop      - Detener todos los contenedores"
    echo "  logs      - Mostrar logs del contenedor"
    echo "  status    - Mostrar estado de los contenedores"
    echo "  clean     - Limpiar contenedores e imágenes"
    echo "  help      - Mostrar esta ayuda"
    echo
}

# Ejecutar en modo desarrollo
run_dev() {
    print_status "Ejecutando en modo desarrollo..."
    
    docker-compose up --build -d
    
    print_success "Servicio ejecutándose en modo desarrollo"
    print_status "Logs: docker-compose logs -f gps-service"
    print_status "Health: curl http://localhost:3000/health"
}

# Ejecutar en modo producción
run_prod() {
    print_status "Ejecutando en modo producción..."
    
    docker-compose -f docker-compose.prod.yml up --build -d
    
    print_success "Servicio ejecutándose en modo producción"
    print_status "Logs: docker-compose -f docker-compose.prod.yml logs -f"
    print_status "Health: curl http://localhost:3000/health"
}

# Detener servicios
stop_services() {
    print_status "Deteniendo servicios..."
    
    docker-compose down
    docker-compose -f docker-compose.prod.yml down 2>/dev/null || true
    
    print_success "Servicios detenidos"
}

# Mostrar logs
show_logs() {
    print_status "Mostrando logs..."
    
    if docker-compose ps | grep -q gps-bigquery-service; then
        docker-compose logs -f gps-service
    elif docker-compose -f docker-compose.prod.yml ps | grep -q gps-bigquery-prod; then
        docker-compose -f docker-compose.prod.yml logs -f gps-service
    else
        print_warning "No hay contenedores ejecutándose"
    fi
}

# Mostrar estado
show_status() {
    print_status "Estado de los contenedores:"
    echo
    
    docker-compose ps
    echo
    docker-compose -f docker-compose.prod.yml ps 2>/dev/null || true
}

# Limpiar contenedores e imágenes
clean_all() {
    print_status "Limpiando contenedores e imágenes..."
    
    docker-compose down --rmi all --volumes --remove-orphans 2>/dev/null || true
    docker-compose -f docker-compose.prod.yml down --rmi all --volumes --remove-orphans 2>/dev/null || true
    
    # Limpiar imágenes huérfanas
    docker image prune -f
    
    print_success "Limpieza completada"
}

# Función principal
main() {
    case "${1:-help}" in
        "build")
            check_docker
            check_env_file
            create_directories
            build_image
            ;;
        "dev")
            check_docker
            check_env_file
            create_directories
            run_dev
            ;;
        "prod")
            check_docker
            check_env_file
            create_directories
            run_prod
            ;;
        "stop")
            stop_services
            ;;
        "logs")
            show_logs
            ;;
        "status")
            show_status
            ;;
        "clean")
            clean_all
            ;;
        "help"|*)
            show_help
            ;;
    esac
}

# Ejecutar función principal
main "$@"