import { MetricsCollector } from './MetricsCollector.js';

/**
 * Instancia singleton de métricas exportada
 * Evita múltiples cargas del archivo de métricas
 */
export const metrics = MetricsCollector.getInstance();

// Exportar también la clase por si se necesita
export { MetricsCollector };