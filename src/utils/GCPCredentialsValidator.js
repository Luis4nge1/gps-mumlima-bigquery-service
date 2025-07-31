import fs from 'fs/promises';
import { logger } from './logger.js';

/**
 * Validador de credenciales de Google Cloud Platform
 */
export class GCPCredentialsValidator {
  constructor(serviceAccountPath = './service-account.json') {
    this.serviceAccountPath = serviceAccountPath;
  }

  /**
   * Valida que existan las credenciales y tengan el formato correcto
   */
  async validateCredentials() {
    try {
      logger.info('🔐 Validando credenciales de Google Cloud...');

      // Verificar que el archivo existe
      const exists = await this.fileExists(this.serviceAccountPath);
      if (!exists) {
        throw new Error(`Archivo de credenciales no encontrado: ${this.serviceAccountPath}`);
      }

      // Leer y parsear el archivo
      const credentialsContent = await fs.readFile(this.serviceAccountPath, 'utf8');
      const credentials = JSON.parse(credentialsContent);

      // Validar estructura requerida
      const validation = this.validateCredentialStructure(credentials);
      if (!validation.isValid) {
        throw new Error(`Credenciales inválidas: ${validation.errors.join(', ')}`);
      }

      logger.info('✅ Credenciales de Google Cloud validadas exitosamente');
      logger.info(`📋 Proyecto: ${credentials.project_id}`);
      logger.info(`📧 Service Account: ${credentials.client_email}`);

      return {
        isValid: true,
        projectId: credentials.project_id,
        clientEmail: credentials.client_email,
        credentials
      };

    } catch (error) {
      logger.error('❌ Error validando credenciales GCP:', error.message);
      return {
        isValid: false,
        error: error.message
      };
    }
  }

  /**
   * Valida la estructura del archivo de credenciales
   */
  validateCredentialStructure(credentials) {
    const errors = [];
    const requiredFields = [
      'type',
      'project_id',
      'private_key_id',
      'private_key',
      'client_email',
      'client_id',
      'auth_uri',
      'token_uri'
    ];

    // Verificar que sea un service account
    if (credentials.type !== 'service_account') {
      errors.push('El tipo debe ser "service_account"');
    }

    // Verificar campos requeridos
    for (const field of requiredFields) {
      if (!credentials[field]) {
        errors.push(`Campo requerido faltante: ${field}`);
      }
    }

    // Validar formato de email
    if (credentials.client_email && !this.isValidEmail(credentials.client_email)) {
      errors.push('Formato de client_email inválido');
    }

    // Validar que la private key tenga el formato correcto
    if (credentials.private_key && !credentials.private_key.includes('BEGIN PRIVATE KEY')) {
      errors.push('Formato de private_key inválido');
    }

    return {
      isValid: errors.length === 0,
      errors
    };
  }

  /**
   * Verifica si un archivo existe
   */
  async fileExists(filePath) {
    try {
      await fs.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Valida formato de email básico
   */
  isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }

  /**
   * Obtiene información básica de las credenciales sin validación completa
   */
  async getCredentialsInfo() {
    try {
      const exists = await this.fileExists(this.serviceAccountPath);
      if (!exists) {
        return {
          exists: false,
          path: this.serviceAccountPath
        };
      }

      const credentialsContent = await fs.readFile(this.serviceAccountPath, 'utf8');
      const credentials = JSON.parse(credentialsContent);

      return {
        exists: true,
        path: this.serviceAccountPath,
        projectId: credentials.project_id || 'unknown',
        clientEmail: credentials.client_email || 'unknown',
        type: credentials.type || 'unknown'
      };

    } catch (error) {
      return {
        exists: true,
        path: this.serviceAccountPath,
        error: error.message
      };
    }
  }

  /**
   * Crea un archivo de credenciales de ejemplo para desarrollo
   */
  async createExampleCredentials() {
    const exampleCredentials = {
      type: 'service_account',
      project_id: 'your-gcp-project-id',
      private_key_id: 'your-private-key-id',
      private_key: '-----BEGIN PRIVATE KEY-----\\nYOUR_PRIVATE_KEY_HERE\\n-----END PRIVATE KEY-----\\n',
      client_email: 'your-service-account@your-project.iam.gserviceaccount.com',
      client_id: 'your-client-id',
      auth_uri: 'https://accounts.google.com/o/oauth2/auth',
      token_uri: 'https://oauth2.googleapis.com/token',
      auth_provider_x509_cert_url: 'https://www.googleapis.com/oauth2/v1/certs',
      client_x509_cert_url: 'https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project.iam.gserviceaccount.com'
    };

    const examplePath = './service-account.json.example';
    
    try {
      await fs.writeFile(examplePath, JSON.stringify(exampleCredentials, null, 2));
      logger.info(`📝 Archivo de ejemplo creado: ${examplePath}`);
      return examplePath;
    } catch (error) {
      logger.error('❌ Error creando archivo de ejemplo:', error.message);
      throw error;
    }
  }
}