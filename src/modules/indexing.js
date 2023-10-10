const axios = require('axios');
const { Logger } = require('./logger');
const logger = Logger.child({
  moduleName: require('path').basename(__filename, '.js'),
});

class Indexing {
  constructor() {}

  async indexDecision(source, decision, duplicateId, message, error) {
    try {
      return await axios.post(`${process.env.INDEX_URI}/indexDecision`, {
        source: source,
        decision: decision,
        duplicateId: duplicateId,
        message: message,
        error: error,
      });
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async updateDecision(source, decision, duplicateId, message, error) {
    try {
      return await axios.post(`${process.env.INDEX_URI}/updateDecision`, {
        source: source,
        decision: decision,
        duplicateId: duplicateId,
        message: message,
        error: error,
      });
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async indexAffaire(source, decision) {
    try {
      return await axios.post(`${process.env.INDEX_URI}/indexAffaire`, { source: source, decision: decision });
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async normalizeDecision(source, decision, previousDecision, ignorePreviousContent, cleanContent) {
    try {
      return await axios.post(`${process.env.INDEX_URI}/normalizeDecision`, {
        source: source,
        decision: decision,
        previousDecision: previousDecision,
        ignorePreviousContent: ignorePreviousContent,
        cleanContent: cleanContent,
      });
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async shouldBeRejected(source, nac, np, publicCheckbox) {
    try {
      return await axios.post(`${process.env.INDEX_URI}/shouldBeRejected`, {
        source: source,
        nac: nac,
        np: np,
        publicCheckbox: publicCheckbox,
      });
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async isPartiallyPublic(source, nac, np, publicCheckbox) {
    try {
      return await axios.post(`${process.env.INDEX_URI}/isPartiallyPublic`, {
        source: source,
        nac: nac,
        np: np,
        publicCheckbox: publicCheckbox,
      });
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async shouldBeSentToJudifiltre(source, nac, np, publicCheckbox) {
    try {
      return await axios.post(`${process.env.INDEX_URI}/shouldBeSentToJudifiltre`, {
        source: source,
        nac: nac,
        np: np,
        publicCheckbox: publicCheckbox,
      });
    } catch (e) {
      logger.error(e);
      return false;
    }
  }
}

exports.Indexing = new Indexing();
