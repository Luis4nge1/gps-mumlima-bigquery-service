# GPS BigQuery Microservice

Microservicio escalable y modular para procesamiento automático de datos GPS con simulación de subida a BigQuery.

## 🚀 Características

- **API REST v3**: API versionada con prefijo `/api/v3/` y compatibilidad legacy
- **Procesamiento Automático**: Scheduler configurable para procesar datos GPS cada X minutos
- **Arquitectura Modular**: Estructura escalable con separación de responsabilidades
- **Sistema Híbrido**: Migración gradual entre flujos legacy y nuevos con rollback automático
- **Validación Robusta**: Validación completa de datos GPS con limpieza automática
- **Manejo de Errores**: Sistema robusto de manejo de errores con reintentos automáticos
- **Métricas y Monitoreo**: Recolección de métricas detalladas para monitoreo
- **Configuración Flexible**: Configuración completa via variables de entorno
- **Backup Automático**: Sistema de backup opcional para datos procesados
- **Health Checks**: Endpoints de salud para monitoreo de servicios

## 📋 Requisitos

- Node.js >= 18.0.0
- Redis Server
- Acceso de escritura al directorio `tmp/`

## 🛠️ Instalación

1. **Clonar el repositorio**
```bash
git clone <repository-url>
cd gps-bigquery-microservice
```

2. **Instalar dependencias**
```bash
npm install
```

3. **Configurar variables de entorno**
```bash
cp .env.example .env
# Editar .env con tu configuración
```

4. **Crear directorios necesarios**
```bash
mkdir -p tmp/backup
```

## ⚙️ Configuración

### Variables de Entorno Principales

```env
# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
GPS_LIST_KEY=gps:history:global

# Scheduler
SCHEDULER_INTERVAL_MINUTES=5
SCHEDULER_ENABLED=true

# Archivos de salida
GPS_OUTPUT_FILE=tmp/gps_data.txt
GPS_BACKUP_ENABLED=true
GPS_BACKUP_PATH=tmp/backup/

# Backup Local (Procesamiento Atómico)
BACKUP_MAX_RETRIES=3
BACKUP_RETENTION_HOURS=24
BACKUP_STORAGE_PATH=tmp/atomic-backups/
ATOMIC_PROCESSING_ENABLED=true
```

Ver `.env.example` para configuración completa.

### Configuración de Backup Local

El sistema incluye un mecanismo de backup local para garantizar que no se pierdan datos durante el procesamiento atómico:

```env
# Número máximo de reintentos para procesar un backup
BACKUP_MAX_RETRIES=3

# Tiempo en horas para mantener archivos de backup antes de eliminarlos
BACKUP_RETENTION_HOURS=24

# Directorio donde se almacenan los archivos de backup
BACKUP_STORAGE_PATH=tmp/atomic-backups/

# Intervalo en minutos para ejecutar limpieza automática de backups
BACKUP_CLEANUP_INTERVAL_MINUTES=60

# Habilitar/deshabilitar procesamiento atómico (feature flag)
ATOMIC_PROCESSING_ENABLED=true

# Timeout en milisegundos para operaciones de procesamiento atómico
ATOMIC_PROCESSING_TIMEOUT_MS=30000
```

**Comportamiento del Sistema de Backup:**
- Cuando falla la subida a GCS, los datos se guardan en backup local
- El sistema reintenta procesar backups antes de procesar nuevos datos
- Los backups se eliminan automáticamente después del tiempo de retención
- Si un backup excede el máximo de reintentos, se preserva para revisión manual

## 🚀 Uso

### Modo Continuo (con Scheduler)
```bash
npm start
```

### Ejecución Única
```bash
npm run start:once
```

### Modo Desarrollo
```bash
npm run dev
```

### Producción
```bash
npm run start:production
```

## 🌐 API REST v3

El microservicio expone una API REST versionada con el prefijo `/api/v3/`.

### Configuración de API

```env
# Configuración de API
API_VERSION=v3
API_BASE_PATH=/api/v3
API_BASE_URL=http://localhost:3003
```

### Endpoints Principales

#### Health Checks
- `GET /api/v3/health` - Health check básico
- `GET /api/v3/health/detailed` - Health check detallado
- `GET /api/v3/health/gcs` - Health check de Google Cloud Storage
- `GET /api/v3/health/bigquery` - Health check de BigQuery

#### Métricas
- `GET /api/v3/metrics` - Métricas generales del sistema
- `GET /api/v3/metrics/gcs` - Métricas específicas de GCS
- `GET /api/v3/metrics/bigquery` - Métricas específicas de BigQuery
- `GET /api/v3/metrics/costs` - Métricas de costos GCP

#### Procesamiento
- `GET /api/v3/status` - Estado del procesador
- `POST /api/v3/process` - Ejecutar procesamiento manual
- `POST /api/v3/batch-process` - Ejecutar procesamiento batch

#### Sistema Híbrido/Migración
- `GET /api/v3/hybrid/status` - Estado del sistema híbrido
- `GET /api/v3/hybrid/metrics` - Métricas de migración
- `GET /api/v3/migration/phase` - Fase actual de migración
- `POST /api/v3/migration/phase` - Cambiar fase de migración

### Compatibilidad Legacy

Las rutas legacy `/api/massive-data/` siguen funcionando y son automáticamente redirigidas a `/api/v3/`.

### Documentación Completa

Ver [API_DOCUMENTATION.md](./API_DOCUMENTATION.md) para documentación completa de la API.

## 📊 Comandos Útiles

### Verificar Salud del Sistema
```bash
npm run health
```

### Ver Métricas
```bash
npm run metrics
```

### Validar Configuración
```bash
npm run validate-config
```

### Validar Configuración de Backup
```bash
npm run validate-backup-config
```

### Testing en Staging
```bash
# Ejecutar suite completa de tests de staging
npm run test:staging

# El test verifica:
# - Configuración del sistema
# - Feature flags de procesamiento atómico
# - Extracción atómica de datos
# - Sistema de backup local
# - Recovery de backups
# - Limpieza automática
# - Monitoreo y métricas
# - Manejo de carga
```

## 🏗️ Arquitectura

```
src/
├── config/           # Configuraciones
│   ├── env.js       # Variables de entorno
│   └── redis.js     # Configuración Redis
├── services/         # Lógica de negocio
│   ├── GPSProcessorService.js  # Procesamiento principal
│   └── SchedulerService.js     # Programación automática
├── repositories/     # Acceso a datos
│   └── RedisRepository.js      # Operaciones Redis
├── adapters/         # Integraciones externas
│   └── BigQueryAdapter.js      # Simulación BigQuery
├── utils/           # Utilidades
│   ├── logger.js    # Sistema de logging
│   ├── FileUtils.js # Utilidades de archivos
│   └── MetricsCollector.js # Recolección de métricas
├── middleware/      # Middleware
│   └── ErrorHandler.js # Manejo de errores
├── validators/      # Validaciones
│   └── GPSValidator.js # Validación GPS
└── types/           # Definiciones de tipos
    └── GPSTypes.js  # Tipos y esquemas GPS
```

## 🔄 Flujo de Procesamiento

### Flujo Atómico (Nuevo - Recomendado)

**Problema Resuelto:** El procesamiento atómico elimina la pérdida de datos que ocurría cuando llegaban nuevos datos mientras se procesaban los existentes.

#### Flujo Normal con Procesamiento Atómico
1. **Scheduler** ejecuta cada X minutos (configurable)
2. **AtomicRedisProcessor** extrae TODOS los datos de Redis de una vez:
   - Obtiene todos los datos de `gps:history:global`
   - Obtiene todos los datos de `mobile:history:global`
   - **Limpia inmediatamente ambas keys de Redis**
   - Redis queda disponible para recibir nuevos datos
3. **GPSValidator** valida y limpia los datos extraídos
4. **GPSProcessorService** procesa los datos en lotes
5. **GCS Upload** sube archivos a Google Cloud Storage
6. **BigQuery** procesa archivos desde GCS
7. **MetricsCollector** registra estadísticas del procesamiento

#### Flujo con Backup Local (cuando falla GCS)
1. **AtomicRedisProcessor** extrae datos de Redis y limpia inmediatamente
2. **GCS Upload** falla → **BackupManager** guarda datos en backup local
3. **Scheduler** en siguiente ejecución:
   - Procesa backups pendientes PRIMERO
   - **BackupManager** reintenta subir backup a GCS
   - Si éxito → elimina backup
   - Si falla → incrementa contador de reintentos
   - Continúa con procesamiento normal de nuevos datos

#### Ventajas del Procesamiento Atómico
- ✅ **Cero pérdida de datos**: Redis se limpia inmediatamente después de la extracción
- ✅ **Disponibilidad continua**: Redis recibe nuevos datos mientras se procesan los anteriores
- ✅ **Recuperación automática**: Backups locales garantizan que los datos no se pierdan
- ✅ **Monitoreo completo**: Métricas detalladas de backups y reintentos

### Flujo Legacy (Deshabilitado por defecto)
El flujo anterior se mantiene disponible configurando `ATOMIC_PROCESSING_ENABLED=false`, pero **no se recomienda** debido al riesgo de pérdida de datos.

## 📈 Métricas Disponibles

- **Procesamiento**: Total de ejecuciones, éxito/fallo, tiempo promedio
- **Redis**: Conexiones, errores, estadísticas de datos
- **BigQuery**: Subidas exitosas/fallidas, registros procesados
- **Validación**: Tasa de validación, registros válidos/inválidos
- **Sistema**: Tiempo de actividad, uso de memoria

## 🛡️ Manejo de Errores

- **Reintentos Automáticos**: Para errores recuperables
- **Circuit Breaker**: Previene cascadas de fallos
- **Clasificación de Errores**: Categorización automática de errores
- **Logging Detallado**: Logs estructurados para debugging

## 📝 Formato de Datos GPS

### Entrada (Redis)
```json
{
  "latitude": -12.0464,
  "longitude": -77.0428,
  "timestamp": "2024-01-15T10:30:00Z",
  "speed": 45.5,
  "heading": 180,
  "altitude": 150,
  "accuracy": 5,
  "device_id": "device_001"
}
```

### Salida (BigQuery/Archivo)
```json
{
  "id": "gps_device_001_1705312200000",
  "latitude": -12.0464,
  "longitude": -77.0428,
  "timestamp": "2024-01-15T10:30:00Z",
  "speed": 45.5,
  "heading": 180,
  "altitude": 150,
  "accuracy": 5,
  "device_id": "device_001",
  "processed_at": "2024-01-15T10:30:05Z",
  "validated_at": "2024-01-15T10:30:05Z",
  "validation_version": "1.0"
}
```

## 🔧 Desarrollo

### Estructura de Commits
- `feat:` Nueva funcionalidad
- `fix:` Corrección de bugs
- `docs:` Documentación
- `refactor:` Refactorización
- `test:` Pruebas

### Agregar Nueva Funcionalidad

1. **Servicios**: Lógica de negocio en `src/services/`
2. **Repositorios**: Acceso a datos en `src/repositories/`
3. **Adaptadores**: Integraciones en `src/adapters/`
4. **Validadores**: Validaciones en `src/validators/`

## 🚨 Troubleshooting

### Error de Conexión Redis
```bash
# Verificar Redis
redis-cli ping

# Verificar configuración
npm run validate-config
```

### Sin Datos para Procesar
```bash
# Verificar datos en Redis
redis-cli llen gps:history:global
```

### Errores de Permisos
```bash
# Verificar permisos del directorio tmp
chmod 755 tmp/
```

### Problemas con Backup Local

#### Verificar Estado de Backups
```bash
# Validar configuración de backup
npm run validate-backup-config

# Verificar archivos de backup pendientes
ls -la tmp/atomic-backups/

# Ver detalles de un backup específico
cat tmp/atomic-backups/backup_gps_20250125_164500_abc123.json | jq '.'

# Verificar métricas de backup en el endpoint de salud
curl http://localhost:3000/health | jq '.backups'
```

#### Backup Files Pendientes
```bash
# Listar backups por tipo
ls -la tmp/atomic-backups/backup_gps_* | wc -l    # Backups GPS
ls -la tmp/atomic-backups/backup_mobile_* | wc -l # Backups Mobile

# Verificar backups con muchos reintentos
find tmp/atomic-backups/ -name "*.json" -exec grep -l '"retryCount":[2-9]' {} \;

# Ver backups que exceden el máximo de reintentos
find tmp/atomic-backups/ -name "*.json" -exec sh -c 'echo "=== $1 ==="; cat "$1" | jq ".metadata | select(.retryCount >= .maxRetries)"' _ {} \;
```

#### Resolución de Problemas Comunes

**1. Backups Acumulándose (no se procesan)**
```bash
# Verificar que el scheduler esté habilitado
grep SCHEDULER_ENABLED .env

# Verificar logs del scheduler
tail -f logs/app.log | grep -i "backup\|scheduler"

# Forzar procesamiento manual de backups (si existe el comando)
npm run process-backups
```

**2. Backups Fallando Repetidamente**
```bash
# Verificar conectividad a GCS
npm run validate-gcp-setup

# Verificar permisos del bucket
gsutil ls -L gs://your-bucket-name/

# Ver errores específicos en los backups
find tmp/atomic-backups/ -name "*.json" -exec sh -c 'echo "=== $1 ==="; cat "$1" | jq ".metadata.errors"' _ {} \;
```

**3. Espacio en Disco por Backups**
```bash
# Ver tamaño total de backups
du -sh tmp/atomic-backups/

# Limpiar backups antiguos manualmente (más de 24 horas)
find tmp/atomic-backups/ -name "*.json" -mtime +1 -delete

# Limpiar backups que exceden máximo de reintentos (revisar primero)
find tmp/atomic-backups/ -name "*.json" -exec sh -c 'cat "$1" | jq -e ".metadata.retryCount >= .metadata.maxRetries" > /dev/null && echo "$1"' _ {} \;
```

**4. Backup Corrupto o Inválido**
```bash
# Validar formato JSON de backups
find tmp/atomic-backups/ -name "*.json" -exec sh -c 'echo "Validating $1"; cat "$1" | jq . > /dev/null || echo "INVALID: $1"' _ {} \;

# Mover backups corruptos a directorio de revisión
mkdir -p tmp/corrupted-backups/
find tmp/atomic-backups/ -name "*.json" -exec sh -c 'cat "$1" | jq . > /dev/null || mv "$1" tmp/corrupted-backups/' _ {} \;
```

#### Comandos de Mantenimiento
```bash
# Estadísticas de backups
echo "Total backups: $(ls tmp/atomic-backups/*.json 2>/dev/null | wc -l)"
echo "GPS backups: $(ls tmp/atomic-backups/backup_gps_*.json 2>/dev/null | wc -l)"
echo "Mobile backups: $(ls tmp/atomic-backups/backup_mobile_*.json 2>/dev/null | wc -l)"

# Backup más antiguo
ls -lt tmp/atomic-backups/*.json 2>/dev/null | tail -1

# Backup más reciente
ls -lt tmp/atomic-backups/*.json 2>/dev/null | head -1

# Limpiar todos los backups (¡CUIDADO!)
# rm -f tmp/atomic-backups/*.json
```

## 📊 Monitoreo en Producción

### Health Check
```bash
curl http://localhost:3000/health
```

### Métricas (si está habilitado)
```bash
curl http://localhost:9090/metrics
```

### Logs
```bash
tail -f logs/app.log
```

## 🚀 Checklist de Despliegue a Producción

### Pre-Despliegue
- [ ] **Configuración validada**: `npm run validate-config`
- [ ] **Tests de staging pasados**: `npm run test:staging`
- [ ] **Feature flags configurados**: `ATOMIC_PROCESSING_ENABLED=true`
- [ ] **Backup configurado**: Verificar `BACKUP_MAX_RETRIES`, `BACKUP_RETENTION_HOURS`
- [ ] **Conectividad GCS**: `npm run validate-gcp`
- [ ] **Monitoreo configurado**: Endpoints `/health` funcionando

### Durante el Despliegue
- [ ] **Backup de configuración actual**
- [ ] **Despliegue gradual**: Comenzar con un servidor
- [ ] **Monitoreo activo**: Verificar logs y métricas
- [ ] **Verificar procesamiento atómico**: Logs deben mostrar "extracción atómica"
- [ ] **Verificar backups**: No deben acumularse backups pendientes

### Post-Despliegue
- [ ] **Health check**: `curl http://servidor:3000/health/detailed`
- [ ] **Verificar métricas**: Confirmar `atomicProcessingEnabled: true`
- [ ] **Monitorear por 24h**: Verificar que no hay pérdida de datos
- [ ] **Verificar limpieza**: Backups antiguos se eliminan automáticamente
- [ ] **Alertas configuradas**: Para backups que exceden reintentos

### Rollback (si es necesario)
- [ ] **Deshabilitar procesamiento atómico**: `ATOMIC_PROCESSING_ENABLED=false`
- [ ] **Procesar backups pendientes**: `npm run recovery`
- [ ] **Verificar integridad de datos**
- [ ] **Restaurar configuración anterior**

## 🔮 Roadmap

- [x] ✅ Procesamiento atómico de Redis
- [x] ✅ Sistema de backup local automático
- [x] ✅ Feature flags para control de funcionalidad
- [x] ✅ Monitoreo y métricas detalladas
- [x] ✅ Tests de staging automatizados
- [ ] Integración real con BigQuery API
- [ ] API REST para control manual
- [ ] Dashboard web de monitoreo
- [ ] Docker containerization
- [ ] Kubernetes deployment
- [ ] Alertas automáticas avanzadas
- [ ] Compresión de datos

## 🤝 Contribución

1. Fork el proyecto
2. Crear rama feature (`git checkout -b feature/AmazingFeature`)
3. Commit cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abrir Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.

## 👥 Equipo

- **GPS Processing Team** - Desarrollo inicial

## 📞 Soporte

Para soporte técnico, crear un issue en el repositorio o contactar al equipo de desarrollo.