const axios = require('axios');
const { Logger } = require('./logger');
const logger = Logger.child({
  moduleName: require('path').basename(__filename, '.js'),
});

function retrieveObjectId(document) {
  Object.keys(document).forEach(function (key) {
    if (/id/i.test(key) && typeof document[key] === 'string' && ObjectId.isValid(document[key])) {
      document[key] = new ObjectId(document[key]);
    }
  });
  return document;
}

class Indexing {
  constructor() {}

  async indexDecision(source, decision, duplicateId, message, error) {
    try {
      return retrieveObjectId(
        (
          await axios.post(`${process.env.INDEX_URI}/indexDecision`, {
            source: source,
            decision: decision,
            duplicateId: duplicateId,
            message: message,
            error: error,
          })
        ).data,
      );
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async updateDecision(source, decision, duplicateId, message, error) {
    try {
      return retrieveObjectId(
        (
          await axios.post(`${process.env.INDEX_URI}/updateDecision`, {
            source: source,
            decision: decision,
            duplicateId: duplicateId,
            message: message,
            error: error,
          })
        ).data,
      );
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async indexAffaire(source, decision) {
    try {
      return retrieveObjectId(
        (await axios.post(`${process.env.INDEX_URI}/indexAffaire`, { source: source, decision: decision })).data,
      );
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async normalizeDecision(source, decision, previousDecision, ignorePreviousContent, cleanContent) {
    try {
      return retrieveObjectId(
        (
          await axios.post(`${process.env.INDEX_URI}/normalizeDecision`, {
            source: source,
            decision: decision,
            previousDecision: previousDecision,
            ignorePreviousContent: ignorePreviousContent,
            cleanContent: cleanContent,
          })
        ).data,
      );
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async cleanContent(source, content) {
    try {
      return retrieveObjectId(
        (
          await axios.post(`${process.env.INDEX_URI}/cleanContent`, {
            source: source,
            content: content,
          })
        ).data,
      );
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async shouldBeRejected(source, nac, np, publicCheckbox) {
    try {
      return retrieveObjectId(
        (
          await axios.post(`${process.env.INDEX_URI}/shouldBeRejected`, {
            source: source,
            nac: nac,
            np: np,
            publicCheckbox: publicCheckbox,
          })
        ).data,
      );
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async isPartiallyPublic(source, nac, np, publicCheckbox) {
    try {
      return retrieveObjectId(
        (
          await axios.post(`${process.env.INDEX_URI}/isPartiallyPublic`, {
            source: source,
            nac: nac,
            np: np,
            publicCheckbox: publicCheckbox,
          })
        ).data,
      );
    } catch (e) {
      logger.error(e);
      return false;
    }
  }

  async shouldBeSentToJudifiltre(source, nac, np, publicCheckbox) {
    try {
      return retrieveObjectId(
        (
          await axios.post(`${process.env.INDEX_URI}/shouldBeSentToJudifiltre`, {
            source: source,
            nac: nac,
            np: np,
            publicCheckbox: publicCheckbox,
          })
        ).data,
      );
    } catch (e) {
      logger.error(e);
      return false;
    }
  }
}

exports.Indexing = new Indexing();
