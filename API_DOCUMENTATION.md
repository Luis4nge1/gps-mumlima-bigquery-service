# GPS BigQuery Microservice API v3

## Información General

- **Versión**: v3
- **Base Path**: `/api/v3`
- **URL Base**: `http://localhost:3003/api/v3`
- **Formato**: JSON
- **Autenticación**: Opcional (API_KEY en headers)

## Configuración

La API se configura mediante variables de entorno:

```bash
# Configuración de API
API_VERSION=v3
API_BASE_PATH=/api/v3
API_BASE_URL=http://localhost:3003
```

## Endpoints Disponibles

### Health Checks

#### `GET /api/v3/health`
Health check básico del sistema.

**Respuesta:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T00:00:00.000Z",
  "uptime": 3600,
  "version": "2.0.0"
}
```

#### `GET /api/v3/health/detailed`
Health check detallado con información de todos los servicios.

#### `GET /api/v3/health/gcs`
Health check específico de Google Cloud Storage.

#### `GET /api/v3/health/bigquery`
Health check específico de BigQuery.

### Métricas

#### `GET /api/v3/metrics`
Métricas generales del sistema.

#### `GET /api/v3/metrics/gcs`
Métricas específicas de Google Cloud Storage.

#### `GET /api/v3/metrics/bigquery`
Métricas específicas de BigQuery.

#### `GET /api/v3/metrics/costs`
Métricas de costos de GCP.

### Estado y Procesamiento

#### `GET /api/v3/status`
Estado actual del procesador de datos.

#### `POST /api/v3/process`
Ejecuta procesamiento manual de datos GPS.

#### `POST /api/v3/batch-process`
Ejecuta procesamiento batch manual.

### Recovery

#### `GET /api/v3/recovery`
Estado del sistema de recovery.

#### `POST /api/v3/recovery`
Ejecuta recovery manual de backups.

#### `GET /api/v3/recovery/gcs-status`
Estado específico del recovery de GCS.

### GCS File Management

#### `GET /api/v3/gcs/files`
Estadísticas de archivos en Google Cloud Storage.

### Sistema Híbrido/Migración

#### `GET /api/v3/hybrid/status`
Estado del sistema híbrido de migración.

**Respuesta:**
```json
{
  "migration": {
    "currentPhase": "hybrid",
    "newFlowEnabled": true,
    "hybridMode": true,
    "rollbackEnabled": true
  },
  "metrics": {
    "legacy": { ... },
    "newFlow": { ... }
  }
}
```

#### `GET /api/v3/hybrid/metrics`
Métricas del sistema híbrido.

#### `GET /api/v3/hybrid/rollbacks`
Historial de rollbacks ejecutados.

#### `GET /api/v3/hybrid/comparisons`
Comparaciones entre flujo legacy y nuevo.

#### `GET /api/v3/migration/phase`
Obtiene la fase actual de migración.

#### `POST /api/v3/migration/phase`
Cambia la fase de migración.

**Body:**
```json
{
  "phase": "hybrid"
}
```

**Fases válidas:**
- `legacy`: Solo flujo legacy
- `hybrid`: Ambos flujos para comparación
- `migration`: Nuevo flujo con rollback automático
- `new`: Solo nuevo flujo
- `rollback`: Rollback temporal

## Compatibilidad con Versiones Anteriores

La API mantiene compatibilidad con las rutas legacy `/api/massive-data/` que son automáticamente redirigidas a `/api/v3/`.

### Mapeo de Rutas Legacy

| Ruta Legacy | Nueva Ruta |
|-------------|------------|
| `/api/massive-data/health` | `/api/v3/health` |
| `/api/massive-data/metrics` | `/api/v3/metrics` |
| `/api/massive-data/status` | `/api/v3/status` |
| ... | ... |

## Rutas Especiales (Sin Versionado)

Algunas rutas mantienen su estructura original:

- `/dashboard` - Dashboard web
- `/api/dashboard/*` - API del dashboard
- `/api/monitoring/*` - API de monitoreo

## Headers de Respuesta

Todas las respuestas incluyen headers informativos:

```
X-API-Version: v3
X-Response-Time: 45ms
Content-Type: application/json
Access-Control-Allow-Origin: *
```

## Códigos de Estado

- `200` - Éxito
- `404` - Endpoint no encontrado
- `405` - Método no permitido
- `500` - Error interno del servidor
- `503` - Servicio no disponible (health checks)

## Ejemplos de Uso

### Verificar Estado del Sistema
```bash
curl http://localhost:3003/api/v3/health
```

### Obtener Métricas
```bash
curl http://localhost:3003/api/v3/metrics
```

### Ejecutar Procesamiento Manual
```bash
curl -X POST http://localhost:3003/api/v3/process
```

### Cambiar Fase de Migración
```bash
curl -X POST http://localhost:3003/api/v3/migration/phase \
  -H "Content-Type: application/json" \
  -d '{"phase": "hybrid"}'
```

## Herramientas de Gestión

### Migration Manager CLI
```bash
# Ver estado
node scripts/migration-manager.js status

# Cambiar fase
node scripts/migration-manager.js phase hybrid

# Monitorear en tiempo real
node scripts/migration-manager.js monitor
```

## Configuración de Desarrollo

Para desarrollo local, asegúrate de tener configuradas las variables de entorno:

```bash
# .env
API_VERSION=v3
API_BASE_PATH=/api/v3
API_BASE_URL=http://localhost:3003
PORT=3003
```

## Notas de Migración

1. **Compatibilidad**: Las rutas legacy siguen funcionando
2. **Versionado**: Usa `/api/v3/` para nuevas integraciones
3. **Headers**: Incluye `X-API-Version` para identificar la versión
4. **Configuración**: Personalizable via variables de entorno