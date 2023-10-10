const axios = require('axios');
const { Logger } = require('./logger');
const logger = Logger.child({
  moduleName: require('path').basename(__filename, '.js'),
});

class Normalize {
  constructor() {}

  async normalizeDecision(source, decision, previousDecision, ignorePreviousContent) {
    try {
      return await axios.post(`${process.env.INDEX_URI}/normalizeDecision`, {
        source: source,
        decision: decision,
        previousDecision: previousDecision,
        ignorePreviousContent: ignorePreviousContent,
      });
    } catch (e) {
      logger.error(e);
      return false;
    }
  }
}

exports.Normalize = new Normalize();
