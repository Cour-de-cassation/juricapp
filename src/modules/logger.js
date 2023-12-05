require('./env');
const pino = require('pino');

const Logger = pino({
  name: process.env.APP_ID,
  level: process.env.NODE_ENV === 'production' || process.env.NODE_ENV === 'prod' ? 'info' : 'debug',
  transport: {
    target: 'pino-pretty',
    options: {
      singleLine: true,
      colorize: true,
      translateTime: 'UTC:dd-mm-yyyy - HH:MM:ss Z',
    },
  },
  base: { appName: `${process.env.APP_NAME}` },
  formatters: {
    level: (label) => {
      return {
        logLevel: label.toUpperCase(),
      };
    },
  },
  timestamp: () => `,"timestamp":"${new Date(Date.now()).toISOString()}"`,
  redact: {
    paths: ['req', 'res', 'headers', 'ip', 'responseTime', 'hostname', 'pid', 'level'],
    censor: '',
    remove: true,
  },
  autoLogging: false,
});

exports.Logger = Logger;
