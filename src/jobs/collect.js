require('../modules/env');
const { Logger } = require('../modules/logger');
const logger = Logger.child({
  jobName: require('path').basename(__filename, '.js'),
});
const { Collector } = require('../modules/collector');

async function main() {
  logger.info('Start');
  let decisions;

  if (process.env.USE_SI_API === 'ON') {
    logger.info('Collect using SI API');
    decisions = await Collector.collectNewDecisionsUsingAPI();
  } else {
    logger.info('Collect using direct DB access');
    decisions = await Collector.collectNewDecisionsUsingDB();
  }

  if (decisions && decisions.collected && Array.isArray(decisions.collected) && decisions.collected.length > 0) {
    logger.info(`${decisions.collected.length} decision(s) collected`);

    if (process.env.USE_DBSDER_API === 'ON') {
      logger.info('Store and normalize using DBSDER API');
      await Collector.storeAndNormalizeNewDecisionsUsingDB(decisions.collected);
    } else {
      logger.info('Store and normalize using direct DB access');
      await Collector.storeAndNormalizeNewDecisionsUsingDB(decisions.collected);
    }
  } else {
    logger.info('No decision collected');
  }

  logger.info('End');
  process.exit(0);
}

main();
